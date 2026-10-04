"""DihScrapper: captures live Discord messages as CSV and appends them to a
private GitHub archive. Attachments are recorded by CDN URL, never downloaded.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from pathlib import Path

import discord

from capture import build_message
from formatter import (
    USER_MAP_FILE,
    append_row,
    merge_user_map,
    message_row,
    sanitize_name,
)
from logger import setup_logging
from pusher import ArchiveError, GitHubArchive, publish_batch

# writes the log file; shipping is a separate process so it outlives us
setup_logging()
logger = logging.getLogger("DihScrapper")

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))

intents = discord.Intents.default()
intents.message_content = True
# members stays off on purpose. It is never queried: the user map is built from the
# author data already in the message payload. Enabling it makes discord.py cache
# every member of every guild for the process lifetime, which is unbounded and was
# a prime suspect for the OOM kills seen next to a second bot on the host.
bot = discord.Client(intents=intents)

archive = GitHubArchive(data_root=DATA_ROOT)
_push_task: asyncio.Task[None] | None = None

# on_message and the publish cycle both mutate these files, so serialise them.
_buffer_lock = asyncio.Lock()


def server_dir(guild: discord.Guild) -> Path:
    return DATA_ROOT / sanitize_name(guild.name)


def channel_path(channel: discord.TextChannel) -> Path:
    return server_dir(channel.guild) / f"{sanitize_name(channel.name)}.csv"


def user_map_path(guild: discord.Guild) -> Path:
    return server_dir(guild) / USER_MAP_FILE


async def push_once() -> None:
    """One cycle: stage the buffer into a batch, push it, verify, then clear it.

    Clearing is keyed on the ids that were staged, so a message buffered while
    the push is in flight is not in that set and stays on disk. The lock is not
    what makes this safe; it only keeps local file mutations from interleaving.
    """
    try:
        async with _buffer_lock:
            batch = archive.stage_batch()
        if batch is None:
            logger.info("Nothing new to push")
            return

        published = await publish_batch(archive, batch)
        if not published:
            return
        total = sum(len(ids) for ids in published.values())

        async with _buffer_lock:
            archive.clear_published(published)
        logger.info("Pushed and verified %d message(s); local buffer cleared", total)
    except ArchiveError as exc:
        # the expected failure mode -- GitHub being slow, throttling or 5xx --
        # already carries the call, the attempt count and the last error. A
        # traceback here buried the one useful line under twenty of aiohttp's.
        logger.error("Push cycle failed, local buffer left intact: %s", exc)
    except Exception:
        # a raised error here would end push_loop and silently stop archiving.
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
        append_row(channel_path(message.channel), row)
        merge_user_map(user_map_path(message.guild), [record])
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
