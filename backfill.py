"""Backfill: read a channel's history from Discord and append it to the archive.

The live bot (``main.py``) archives the stream as it happens and never looks
back. This does the other half, on demand: walk backwards through a channel's
history, turn each message into the same row the bot would have written, and push
it through the same inbox pipeline.

Going through the inbox rather than editing transcripts is the whole point. The
archive's two invariants -- the pushing side only ever adds new files, and the
muncher merges keyed on message id -- are what make this safe to run against a
live archive:

* a backfill never rewrites a transcript, so it costs what it read rather than
  what the archive weighs;
* rows it fetches that the bot already captured are deduplicated by the muncher,
  not duplicated, so a backfill can overlap live capture freely;
* history that predates the bot is appended and sorted into place, and the
  transcript stays ordered by message id either way.

It logs in without opening the gateway: history is read over the REST API, so a
backfill needs neither the message cache nor a member cache.

    python backfill.py --channel 1234567890
    python backfill.py --server 123456789 --limit 500
    python backfill.py --server 123456789 --list
    python backfill.py --channel 1234567890 --dry-run
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import discord
from dotenv import load_dotenv

from capture import build_records
from formatter import USER_MAP_FILE, append_row, merge_user_map, message_row, sanitize_name
from logger import setup_logging
from pusher import ArchiveError, GitHubArchive, publish_batch

load_dotenv()

logger = logging.getLogger("DihScrapper.backfill")

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))

# 0 means every message Discord will hand over
LIMIT = int(os.environ.get("BACKFILL_LIMIT", "0"))
# Rows per inbox batch, and so the merge cadence: every 10k rows this pushes a batch,
# and that push is what makes the archive repo's muncher merge those rows into the
# transcripts on its main branch. Bigger batches mean fewer, larger commits and fewer
# workflow runs; smaller ones bound how much a failed run has to re-read, since unsent
# rows in CI die with the runner.
CHUNK = int(os.environ.get("BACKFILL_CHUNK", "10000"))
# Discord serves history 100 at a time and rate-limits by bucket; the delay is
# politeness between pages, not a correctness requirement.
PAGE_SIZE = 100
PAGE_DELAY = float(os.environ.get("BACKFILL_DELAY", "0"))


class BackfillError(RuntimeError):
    pass


@dataclass
class Stats:
    """What one channel, or one whole server, cost."""

    channels: int = 0
    pages: int = 0
    fetched: int = 0
    rows: int = 0
    archived: int = 0
    batches: int = 0
    failures: int = 0

    def add(self, other: Stats) -> None:
        self.channels += other.channels
        self.pages += other.pages
        self.fetched += other.fetched
        self.rows += other.rows
        self.archived += other.archived
        self.batches += other.batches
        self.failures += other.failures

    def describe(self) -> str:
        return (
            f"{self.channels} channel(s): {self.fetched} message(s) read over {self.pages} page(s), "
            f"{self.rows} row(s) archived in {self.batches} batch(es)"
        )


def transcript_path(channel: discord.abc.GuildChannel) -> Path:
    guild = channel.guild
    assert guild is not None  # callers reject DMs before getting here
    return DATA_ROOT / sanitize_name(guild.name) / f"{sanitize_name(channel.name)}.csv"


def user_map_path(channel: discord.abc.GuildChannel) -> Path:
    guild = channel.guild
    assert guild is not None
    return DATA_ROOT / sanitize_name(guild.name) / USER_MAP_FILE


async def flush(archive: GitHubArchive, stats: Stats, dry_run: bool) -> None:
    """Push everything buffered so far, as one batch.

    A dry run leaves the staged batch on disk: nothing was published, so there is
    nothing to clear, and keeping it lets a real run pick up exactly those rows.
    """
    batch = archive.stage_batch()
    if batch is None:
        return

    published = await publish_batch(archive, batch, dry_run=dry_run)
    rows = sum(len(ids) for ids in published.values())
    stats.batches += 1
    stats.archived += rows
    if not dry_run:
        archive.clear_published(published)


async def read_page(
    channel: discord.abc.GuildChannel, size: int, before: int | None
) -> list[discord.Message]:
    """One page of history, newest first."""
    try:
        return [
            message
            async for message in channel.history(limit=size, before=before, oldest_first=False)
        ]
    except discord.Forbidden as exc:
        raise BackfillError(
            f"cannot read history in #{channel.name}: the bot needs View Channel and "
            f"Read Message History there ({exc.status})"
        ) from exc
    except discord.HTTPException as exc:
        raise BackfillError(f"cannot read history in #{channel.name}: {exc}") from exc


async def backfill_channel(
    bot: discord.Client,
    archive: GitHubArchive,
    channel: discord.abc.GuildChannel,
    *,
    limit: int,
    resolve_replies: bool = False,
    delay: float = PAGE_DELAY,
    dry_run: bool = False,
) -> Stats:
    """Read a channel's history and archive it, a batch at a time.

    Reply columns are filled from the page in hand, which costs nothing. A reply
    to something more than a page older is left empty unless ``resolve_replies``
    is set, because resolving those costs one API call per reply and a walk
    backwards over a busy channel can be tens of thousands of them.
    """
    stats = Stats(channels=1)
    path = transcript_path(channel)
    users = user_map_path(channel)
    bot_id = bot.user.id if bot.user else None
    before: int | None = None
    pending = 0

    while not limit or stats.rows < limit:
        size = PAGE_SIZE if not limit else min(PAGE_SIZE, limit - stats.rows)
        page = await read_page(channel, size, before)
        if not page:
            break

        stats.pages += 1
        stats.fetched += len(page)
        records = await build_records(page, resolve_missing=resolve_replies)
        # The bot's own messages are not archived, live or backfilled: a backfill
        # of a channel the bot talks in would otherwise fill the transcript with
        # its own replies.
        records = [record for record in records if record["author_id"] != bot_id]

        if records:
            for record in records:
                append_row(path, message_row(record))
            merge_user_map(users, records)
            stats.rows += len(records)
            pending += len(records)

        # oldest id on the page, whatever order it arrived in, is the next cursor
        before = min(message.id for message in page)
        if len(page) < size:
            break  # a short page is the end of history

        if pending >= CHUNK and not dry_run:
            # A dry run stages once at the end instead: each stage copies the whole
            # buffer, so chunking a run that publishes nothing would write every row
            # again for every chunk.
            await flush(archive, stats, dry_run)
            pending = 0
        if delay:
            await asyncio.sleep(delay)

    await flush(archive, stats, dry_run)
    logger.info("#%s in %s: %s", channel.name, channel.guild, stats.describe())
    return stats


async def backfill_guild(
    bot: discord.Client,
    archive: GitHubArchive,
    guild: discord.Guild,
    **kwargs,
) -> Stats:
    """Every text channel in a server, oldest id first for a reproducible order.

    A channel the bot cannot read is logged and skipped rather than ending the
    run: a server has hundreds of them and one missing permission is not a reason
    to abandon the rest.
    """
    total = Stats()
    for channel in sorted(guild.text_channels, key=lambda item: item.id):
        try:
            total.add(await backfill_channel(bot, archive, channel, **kwargs))
        except BackfillError as exc:
            total.failures += 1
            logger.warning("%s -- skipping this channel", exc)
    return total


async def fetch_channel(bot: discord.Client, channel_id: str) -> discord.abc.GuildChannel:
    try:
        channel = await bot.fetch_channel(int(channel_id))
    except (discord.NotFound, discord.Forbidden, discord.HTTPException) as exc:
        raise BackfillError(f"cannot open channel {channel_id}: {exc}") from exc
    if channel.guild is None:
        raise BackfillError(f"channel {channel_id} is a DM; the archive is keyed by server")
    if not hasattr(channel, "history"):
        raise BackfillError(f"channel {channel_id} is not a text channel or thread")
    return channel


async def fetch_guild(bot: discord.Client, guild_id: str) -> discord.Guild:
    try:
        return await bot.fetch_guild(int(guild_id))
    except (discord.NotFound, discord.Forbidden, discord.HTTPException) as exc:
        raise BackfillError(
            f"cannot open server {guild_id}: the bot is not a member of it ({exc})"
        ) from exc


async def list_channels(bot: discord.Client, guild: discord.Guild) -> None:
    """Report what a backfill of this server would cover, archiving nothing.

    Readability is probed with a one-message history call rather than a permission
    lookup, because computing permissions needs a member cache this process
    deliberately does not build.
    """
    for channel in sorted(guild.text_channels, key=lambda item: item.id):
        try:
            page = await read_page(channel, 1, None)
        except BackfillError as exc:
            logger.warning("#%s: %s", channel.name, exc)
            continue
        logger.info(
            "#%s (%s): readable, %s",
            channel.name,
            channel.id,
            "history available" if page else "no messages",
        )


async def run(args: argparse.Namespace) -> int:
    token = os.environ.get("DISCORD_API") or os.environ.get("DISCORD_TOKEN")
    if not token:
        raise SystemExit("DISCORD_API is not set")
    if not args.channel and not args.server:
        raise SystemExit("nothing to do: pass --channel, --server, or both")
    if args.list and not args.server:
        # better a refusal than a full backfill from someone who meant to preview one
        raise SystemExit("--list works with --server; name the server to report on")

    # No ceiling: a run reads the whole channel, or as much of it as the token, the
    # runner and the clock allow. --limit is there for a deliberate partial read, not
    # as a safety stop, and chunking means an interrupted run keeps what it pushed.
    limit = max(args.limit, 0)

    intents = discord.Intents.default()
    intents.message_content = True
    bot = discord.Client(intents=intents)
    archive = GitHubArchive(data_root=DATA_ROOT)
    total = Stats()

    try:
        await bot.login(token)
        logger.info("Authenticated as %s", bot.user)

        if args.server:
            guild = await fetch_guild(bot, args.server)
            if args.list:
                await list_channels(bot, guild)
                return 0
            total.add(
                await backfill_guild(
                    bot,
                    archive,
                    guild,
                    limit=limit,
                    resolve_replies=args.resolve_replies,
                    delay=args.delay,
                    dry_run=args.dry_run,
                )
            )

        for channel_id in args.channel:
            try:
                channel = await fetch_channel(bot, channel_id)
                total.add(
                    await backfill_channel(
                        bot,
                        archive,
                        channel,
                        limit=limit,
                        resolve_replies=args.resolve_replies,
                        delay=args.delay,
                        dry_run=args.dry_run,
                    )
                )
            except BackfillError as exc:
                total.failures += 1
                logger.error("%s", exc)
    finally:
        if not bot.is_closed():
            await bot.close()
        await archive.close()

    if total.channels == 0 and total.failures:
        return 1
    # partial success is reported, not failed: an unreadable channel in a server
    # with hundreds of them should not paint every run red
    logger.info("Backfill complete: %s", total.describe())
    if total.failures:
        logger.warning("%d channel(s) could not be read", total.failures)
    return 0


def main() -> int:
    setup_logging()
    parser = argparse.ArgumentParser(
        description="Read a channel's Discord history and append it to the archive.",
        epilog="The archive is only appended to; overlapping the bot's live capture "
        "is deduplicated by message id rather than duplicated.",
    )
    parser.add_argument(
        "--channel",
        action="append",
        default=[],
        metavar="ID",
        help="channel or thread id to backfill; repeatable",
    )
    parser.add_argument("--server", metavar="ID", help="backfill every text channel in this server")
    parser.add_argument(
        "--limit",
        type=int,
        default=LIMIT,
        metavar="N",
        help=f"messages per channel, 0 for all of it (default {LIMIT})",
    )
    parser.add_argument(
        "--resolve-replies",
        action="store_true",
        help="fetch reply parents older than the current page, one API call each",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=PAGE_DELAY,
        metavar="S",
        help=f"seconds between history pages (default {PAGE_DELAY})",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="with --server, report which channels are readable and archive nothing",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="read and stage everything, publish nothing, clear nothing",
    )
    args = parser.parse_args()
    try:
        return asyncio.run(run(args))
    except (BackfillError, ArchiveError) as exc:
        logger.error("Backfill failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
