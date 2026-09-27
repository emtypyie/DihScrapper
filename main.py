"""DihScrapper — live Discord chat scraper.

Captures messages as they arrive and buffers them as CSV. Every PUSH_INTERVAL
seconds the buffer is appended to the private archive repository, verified, and
only then cleared, so the archive is the durable record and the local disk stays
transient.

Media is never downloaded. Every attachment is recorded by its CDN URL.
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
from dotenv import load_dotenv

from archive_format import (
    MESSAGE_FIELDS,
    USER_FIELDS,
    append_row,
    message_row,
    read_rows,
    rewrite,
)
from uploader import GitHubArchive

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DihScrapper")

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = discord.Client(intents=intents)

archive = GitHubArchive(data_root=DATA_ROOT)
_push_task: asyncio.Task[None] | None = None

# on_message and the publish cycle both touch the same files, so every mutation
# of the local buffer is serialised.
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
    """Upsert authors into the server's user directory, keyed by user id."""
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


async def fetch_reference(
    channel: discord.TextChannel, message_id: int
) -> discord.Message | None:
    try:
        return await channel.fetch_message(message_id)
    except discord.NotFound:
        return None
    except discord.HTTPException as exc:
        logger.warning("Could not fetch referenced message %s: %s", message_id, exc)
        return None


async def build_message(message: discord.Message) -> dict:
    # Media is recorded by reference only: nothing is downloaded, and images,
    # videos and files alike keep their CDN URL.
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
    """One cycle: append the buffer to the archive, verify, then clear it.

    The clear is gated on verification, so a failed or partial push leaves the
    buffer intact and the next cycle retries the same rows. Because the merge is
    keyed on message id, a retry is idempotent even if the commit landed.

    The lock is held only while reading and rewriting local files, never across
    the network round trips. Clearing is keyed on the ids actually published, so
    a message buffered while the push is in flight is simply not in that set and
    is left on disk for the next cycle -- the lock is not what makes this safe.
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
        # Never let an unexpected error here end the cycle: the next tick simply
        # retries, and the buffer is only ever cleared after verification.
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

    if bot.user in message.mentions:
        print("Makima is Listening :3")

    record = await build_message(message)
    row = message_row(record)
    async with _buffer_lock:
        append_row(channel_path(message.channel), MESSAGE_FIELDS, row)
        record_users(message.guild, [record])
    logger.debug("Buffered message %s from #%s", message.id, message.channel.name)


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
    if not bot.is_closed():
        await bot.close()
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
