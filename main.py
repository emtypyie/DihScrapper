"""DihScrapper — live Discord chat scraper and media archiver.

Captures messages as they arrive, stores the transcripts as JSON, and pushes media
straight into the private archive repository as git blobs. Media is never written to
the local filesystem.
"""

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

from upload_data import ArchiveError, GitHubArchive

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DihScrapper")

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
MEDIA_MAX_SIZE = int(os.environ.get("MEDIA_MAX_SIZE", "25")) * 1024 * 1024
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))
CHUNK_SIZE = 64 * 1024

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = discord.Client(intents=intents)

archive = GitHubArchive(data_root=DATA_ROOT)
_push_task: asyncio.Task[None] | None = None
_media_index: dict[str, str] = {}

_unsafe = re.compile(r"[^A-Za-z0-9_-]")


def sanitize(name: str) -> str:
    return _unsafe.sub("_", name).strip("_")[:64] or "unnamed"


def server_dir(guild: discord.Guild) -> Path:
    return DATA_ROOT / sanitize(guild.name)


def channel_path(channel: discord.TextChannel) -> Path:
    return server_dir(channel.guild) / f"{sanitize(channel.name)}.json"


def user_map_path(guild: discord.Guild) -> Path:
    return server_dir(guild) / "user_map.json"


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
    staging = path.with_name(path.name + ".tmp")
    with staging.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
    staging.replace(path)


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


async def store_image(
    session: aiohttp.ClientSession, attachment: discord.Attachment, guild: discord.Guild
) -> str | None:
    """Stream an attachment and commit it to the archive as a git blob.

    Returns the archive-relative path, or None if the media could not be stored.
    """
    buffer = bytearray()
    try:
        async with session.get(attachment.url) as response:
            if response.status != 200:
                logger.warning("Media %s returned HTTP %s", attachment.id, response.status)
                return None
            async for chunk in response.content.iter_chunked(CHUNK_SIZE):
                buffer.extend(chunk)
                if len(buffer) > MEDIA_MAX_SIZE:
                    logger.warning(
                        "Media %s exceeds the %d MiB cap; keeping the CDN URL",
                        attachment.id, MEDIA_MAX_SIZE // 1024**2,
                    )
                    return None
        relative = (
            Path(sanitize(guild.name))
            / "mediapool"
            / f"{attachment.id}_{sanitize(attachment.filename)}"
        ).as_posix()
        blob = await archive.store_blob(bytes(buffer))
        _media_index[relative] = blob
        archive.save_media_index(_media_index)
        return relative
    except (aiohttp.ClientError, OSError, ArchiveError) as exc:
        logger.warning("Failed to store media %s: %s", attachment.id, exc)
        return None


async def build_message(message: discord.Message) -> dict:
    stored: list[str] = []
    remote_only: list[str] = []

    if message.attachments:
        async with aiohttp.ClientSession() as session:
            for attachment in message.attachments:
                if not (attachment.content_type or "").startswith("image/"):
                    remote_only.append(attachment.url)
                    continue
                path = await store_image(session, attachment, message.guild)
                if path is None:
                    remote_only.append(attachment.url)
                else:
                    stored.append(path)

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
    if remote_only:
        record["media_size_exceeded"] = remote_only
    return record


async def publish() -> None:
    try:
        await archive.publish()
    except Exception as exc:
        logger.error("Archive publish failed: %s", exc)


async def push_loop() -> None:
    await bot.wait_until_ready()
    while True:
        await asyncio.sleep(PUSH_INTERVAL)
        await publish()


@bot.event
async def on_ready() -> None:
    global _push_task
    _media_index.update(archive.load_media_index())
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
    path = channel_path(message.channel)
    write_json(path, merge_messages(load_messages(path), [record]))
    record_users(message.guild, [record])
    logger.debug("Archived message %s from #%s", message.id, message.channel.name)


def merge_messages(existing: list[dict], incoming: list[dict]) -> list[dict]:
    known = {m.get("id") for m in existing}
    merged = list(existing)
    for message in incoming:
        if message.get("id") not in known:
            merged.append(message)
            known.add(message.get("id"))
    merged.sort(key=lambda m: m.get("id", 0))
    return merged


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
