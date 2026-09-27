"""DihScrapper — Discord chat scraper and media archiver."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
from pathlib import Path

import aiohttp
import discord
from dotenv import load_dotenv

from upload_data import DataRepoSync

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DihScrapper")

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
MEDIA_MAX_SIZE = int(os.environ.get("MEDIA_MAX_SIZE", "25")) * 1024 * 1024
CATCHUP_LIMIT = int(os.environ.get("CATCHUP_LIMIT", "200"))
CATCHUP_MAX = int(os.environ.get("CATCHUP_MAX", "5000"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))
CHUNK_SIZE = 64 * 1024

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = discord.Client(intents=intents)

archive = DataRepoSync(data_root=DATA_ROOT)
_push_task: asyncio.Task[None] | None = None

_unsafe = re.compile(r"[^A-Za-z0-9_-]")


def sanitize(name: str) -> str:
    return _unsafe.sub("_", name).strip("_")[:64] or "unnamed"


def server_dir(guild: discord.Guild) -> Path:
    return DATA_ROOT / sanitize(guild.name)


def channel_path(channel: discord.TextChannel) -> Path:
    return server_dir(channel.guild) / f"{sanitize(channel.name)}.json"


def user_map_path(guild: discord.Guild) -> Path:
    return server_dir(guild) / "user_map.json"


def mediapool_dir(guild: discord.Guild) -> Path:
    return server_dir(guild) / "mediapool"


def load_messages(path: Path) -> list[dict]:
    if not path.exists():
        return []
    try:
        with path.open(encoding="utf-8") as handle:
            data = json.load(handle)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read %s (%s); starting a fresh log", path, exc)
        return []
    return data if isinstance(data, list) else []


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
    tmp.replace(path)


def merge_messages(existing: list[dict], incoming: list[dict]) -> list[dict]:
    known = {m.get("id") for m in existing}
    merged = list(existing)
    for message in incoming:
        if message.get("id") not in known:
            merged.append(message)
            known.add(message.get("id"))
    merged.sort(key=lambda m: m.get("id", 0))
    return merged


def record_users(guild: discord.Guild, messages: list[dict]) -> None:
    path = user_map_path(guild)
    try:
        with path.open(encoding="utf-8") as handle:
            users = json.load(handle)
        if not isinstance(users, dict):
            users = {}
    except (json.JSONDecodeError, OSError):
        users = {}
    for message in messages:
        name = message.get("author")
        if name:
            users[name] = str(message.get("author_id"))
    write_json(path, users)


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


async def archive_image(
    session: aiohttp.ClientSession,
    attachment: discord.Attachment,
    guild: discord.Guild,
) -> Path | None:
    """Stream an attachment into mediapool, discarding it if it exceeds the size cap."""
    destination = mediapool_dir(guild) / f"{attachment.id}_{attachment.filename}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    staging = destination.with_suffix(destination.suffix + ".part")
    written = 0
    try:
        async with session.get(attachment.url) as response:
            if response.status != 200:
                logger.warning("Media %s returned HTTP %s", attachment.id, response.status)
                return None
            async with staging.open("wb") as handle:
                async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                    written += len(chunk)
                    if written > MEDIA_MAX_SIZE:
                        logger.warning(
                            "Media %s exceeds %d MiB; keeping CDN URL",
                            attachment.id, MEDIA_MAX_SIZE // 1024**2,
                        )
                        return None
                    handle.write(chunk)
        staging.replace(destination)
    except (aiohttp.ClientError, OSError) as exc:
        logger.warning("Failed to archive media %s: %s", attachment.id, exc)
        return None
    finally:
        staging.unlink(missing_ok=True)
    return destination


async def build_message(message: discord.Message) -> dict:
    stored: list[str] = []
    oversized: list[str] = []

    for attachment in message.attachments:
        if not (attachment.content_type or "").startswith("image/"):
            oversized.append(attachment.url)
            continue
        async with aiohttp.ClientSession() as session:
            saved = await archive_image(session, attachment, message.guild)
        if saved is None:
            oversized.append(attachment.url)
        else:
            stored.append(saved.relative_to(DATA_ROOT).as_posix())

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

    record: dict = {
        "id": message.id,
        "timestamp": message.created_at.isoformat(),
        "author": message.author.global_name or message.author.name,
        "author_id": message.author.id,
        "content": message.content or "",
        "attachments": stored,
        "reply_to": reply_to,
    }
    if oversized:
        record["media_size_exceeded"] = oversized
    return record


async def collect_history(
    channel: discord.TextChannel, after_id: int | None
) -> list[dict]:
    if after_id:
        history = channel.history(limit=CATCHUP_MAX, after=discord.Object(after_id))
    else:
        history = channel.history(limit=CATCHUP_LIMIT)
    records = [await build_message(message) async for message in history]
    if len(records) >= CATCHUP_MAX:
        logger.warning(
            "Channel #%s hit the %d message catch-up cap; some history may be missing",
            channel.name, CATCHUP_MAX,
        )
    return records


async def archive_channel(channel: discord.TextChannel) -> int:
    path = channel_path(channel)
    existing = load_messages(path)
    watermark = max((m.get("id", 0) for m in existing), default=0)
    if not watermark:
        logger.info("#%s has no local history; backfilling %d messages", channel.name, CATCHUP_LIMIT)

    records = await collect_history(channel, watermark or None)
    if not records:
        return 0

    merged = merge_messages(existing, records)
    write_json(path, merged)
    record_users(channel.guild, records)
    logger.info(
        "Archived %d message(s) from #%s (%s); log now holds %d",
        len(records), channel.name, channel.guild.name, len(merged),
    )
    return len(records)


async def catchup_guild(guild: discord.Guild) -> None:
    logger.info("Catching up %s", guild.name)
    total = 0
    for channel in guild.text_channels:
        if not channel.permissions_for(guild.me).read_message_history:
            logger.debug("Skipping #%s: no read permission", channel.name)
            continue
        try:
            total += await archive_channel(channel)
        except discord.HTTPException as exc:
            logger.warning("Catch-up failed for #%s: %s", channel.name, exc)
    logger.info("Catch-up for %s added %d message(s)", guild.name, total)
    await push_archive()


async def push_archive() -> None:
    try:
        await archive.sync()
    except Exception as exc:
        logger.error("Archive push failed: %s", exc)


async def push_loop() -> None:
    await bot.wait_until_ready()
    while True:
        await asyncio.sleep(PUSH_INTERVAL)
        await push_archive()


@bot.event
async def on_ready() -> None:
    global _push_task
    logger.info("Authenticated as %s", bot.user)
    if _push_task is None or _push_task.done():
        _push_task = asyncio.create_task(push_loop())
    for guild in bot.guilds:
        await catchup_guild(guild)


@bot.event
async def on_resumed() -> None:
    logger.info("Gateway session resumed; re-running catch-up")
    for guild in bot.guilds:
        await catchup_guild(guild)


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.id == bot.user.id:
        return
    if not isinstance(message.channel, discord.TextChannel):
        return

    if bot.user in message.mentions:
        print("Makima is Listening :3")

    records = [await build_message(message)]
    path = channel_path(message.channel)
    write_json(path, merge_messages(load_messages(path), records))
    record_users(message.guild, records)
    logger.debug("Archived message %s from #%s", message.id, message.channel.name)


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
    done, pending = await asyncio.wait(
        {runner, waiter}, return_when=asyncio.FIRST_COMPLETED
    )
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    if not bot.is_closed():
        await bot.close()
    if runner in done and (failure := runner.exception()) is not None:
        raise failure
    logger.info("Shutdown complete")


if __name__ == "__main__":
    token = os.environ.get("DISCORD_API") or os.environ.get("DISCORD_TOKEN")
    if not token:
        raise SystemExit("DISCORD_API is not set")
    os.environ["DISCORD_API"] = token
    asyncio.run(main())
