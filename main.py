"""DihScrapper: captures live Discord messages as CSV and appends them to a
private GitHub archive. Attachments are recorded by CDN URL, never downloaded.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import signal
import time
from pathlib import Path

import discord

from archive_format import (
    MESSAGE_FIELDS,
    USER_FIELDS,
    append_row,
    message_row,
    read_rows,
    rewrite,
)
from logger import setup_logging
from uploader import GitHubArchive

# writes the log file; shipping is a separate process so it outlives us
setup_logging()
logger = logging.getLogger("DihScrapper")

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))

intents = discord.Intents.default()
intents.message_content = True
# members stays off on purpose. It is never queried: record_users builds its map
# from the author data already in the message payload. Enabling it makes discord.py
# cache every member of every guild for the process lifetime, which is unbounded
# and was a prime suspect for the OOM kills seen next to a second bot on the host.
bot = discord.Client(intents=intents)

archive = GitHubArchive(data_root=DATA_ROOT)
_push_task: asyncio.Task[None] | None = None

# on_message and the publish cycle both mutate these files, so serialise them.
_buffer_lock = asyncio.Lock()

_unsafe = re.compile(r"[^A-Za-z0-9_-]")


def sanitize(name: str) -> str:
    return _unsafe.sub("_", name).strip("_")[:64] or "unnamed"


def server_dir(guild: discord.Guild) -> Path:
    return DATA_ROOT / sanitize(guild.name)


def channel_path(channel: discord.TextChannel) -> Path:
    return server_dir(channel.guild) / f"{sanitize(channel.name)}.csv"


def user_map_path(guild: discord.Guild) -> Path:
    return server_dir(guild) / "user_map.csv"


def record_users(guild: discord.Guild, messages: list[dict]) -> None:
    path = user_map_path(guild)
    rows = {row["user_id"]: row for row in read_rows(path, USER_FIELDS)}
    for message in messages:
        name = message.get("author")
        if name:
            rows[str(message["author_id"])] = {
                "username": str(name),
                "user_id": str(message["author_id"]),
            }
    rewrite(path, USER_FIELDS, sorted(rows.values(), key=lambda r: r["username"].lower()))


async def fetch_reference(channel: discord.TextChannel, message_id: int) -> discord.Message | None:
    try:
        return await channel.fetch_message(message_id)
    except discord.NotFound:
        return None
    except discord.HTTPException as exc:
        logger.warning("Could not fetch referenced message %s: %s", message_id, exc)
        return None


async def build_message(message: discord.Message) -> dict:
    # Nothing is downloaded: every attachment kind keeps its CDN URL.
    attachments = [attachment.url for attachment in message.attachments]

    reply_to = None
    if message.reference and message.reference.message_id:
        parent = await fetch_reference(message.channel, message.reference.message_id)
        if parent is not None:
            reply_to = {
                "message_id": parent.id,
                "author_id": parent.author.id,
                "author_username": str(parent.author),
                "content_snippet": (parent.content or "")[:200],
            }

    return {
        "id": message.id,
        "timestamp": message.created_at.isoformat(),
        "author": message.author.global_name or message.author.name,
        "author_id": message.author.id,
        "content": message.content or "",
        "attachments": attachments,
        "reply_to": reply_to,
    }


async def push_once() -> None:
    """One cycle: append the buffer, verify, then clear it.

    Clearing is keyed on the ids actually published, so a message buffered while
    the push is in flight is not in that set and stays on disk. The lock is not
    what makes this safe; it only keeps local file mutations from interleaving.
    """
    try:
        async with _buffer_lock:
            payloads = archive.pending_payloads()

        published = await archive.publish(payloads=payloads)
        if not published:
            logger.info("Nothing new to push")
            return

        total = sum(len(ids) for ids in published.values())
        missing = await archive.verify(published)
        if missing:
            logger.error(
                "Archive is missing %d message(s) after push, keeping local buffer",
                len(missing),
            )
            return

        async with _buffer_lock:
            archive.clear_published(published)
        logger.info("Pushed and verified %d message(s); local buffer cleared", total)
    except Exception:
        # A raised error here would end push_loop and silently stop archiving.
        logger.exception("Push cycle failed, local buffer left intact")


async def push_loop() -> None:
    await bot.wait_until_ready()
    while True:
        started = time.monotonic()
        logger.debug("Push cycle starting")
        try:
            await push_once()
        except Exception:
            logger.exception("Push cycle raised unexpectedly; continuing")
        elapsed = time.monotonic() - started
        if elapsed > PUSH_INTERVAL / 2:
            logger.warning("Push cycle took %.1fs of a %ds interval", elapsed, PUSH_INTERVAL)
        await asyncio.sleep(PUSH_INTERVAL)


@bot.event
async def on_ready() -> None:
    global _push_task
    logger.info("Authenticated as %s, listening for new messages", bot.user)
    if _push_task is None or _push_task.done():
        _push_task = asyncio.create_task(push_loop())


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.id == bot.user.id:
        return
    if not isinstance(message.channel, discord.TextChannel):
        return

    record = await build_message(message)
    row = message_row(record)
    async with _buffer_lock:
        append_row(channel_path(message.channel), MESSAGE_FIELDS, row)
        record_users(message.guild, [record])
    logger.debug("Buffered message %s from #%s", message.id, message.channel.name)

    if bot.user in message.mentions:
        # Guarded: a failed reply must not cost us the archive row above.
        try:
            await message.reply("Makima is Listening :3")
        except discord.HTTPException as exc:
            logger.warning("Could not reply to mention in #%s: %s", message.channel, exc)


async def wait_for_shutdown(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))
    await stop.wait()


async def main() -> None:
    stop = asyncio.Event()
    runner = asyncio.create_task(bot.start(os.environ["DISCORD_API"]))
    waiter = asyncio.create_task(wait_for_shutdown(stop))
    done, pending = await asyncio.wait({runner, waiter}, return_when=asyncio.FIRST_COMPLETED)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if _push_task is not None:
        _push_task.cancel()
        await asyncio.gather(_push_task, return_exceptions=True)
    if not bot.is_closed():
        await bot.close()
    # no final log upload: the sidecar owns that, and a half-done upload here
    # would only delay a hard death
    await archive.close()
    if runner in done and (failure := runner.exception()) is not None:
        raise failure
    logger.info("Shutdown complete")


if __name__ == "__main__":
    token = os.environ.get("DISCORD_API") or os.environ.get("DISCORD_TOKEN")
    if not token:
        raise SystemExit("DISCORD_API is not set")
    os.environ["DISCORD_API"] = token
    asyncio.run(main())
