"""DihScrapper — live Discord chat scraper.

Captures messages as they arrive, stores the transcripts as JSON, and pushes them to the
private archive repository. Media is never downloaded or stored: an image attachment is
recorded as the placeholder "[potential image]".
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import signal
from pathlib import Path

import discord
from dotenv import load_dotenv

from upload_data import GitHubArchive

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("DihScrapper")

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))
IMAGE_PLACEHOLDER = "[potential image]"

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = discord.Client(intents=intents)

archive = GitHubArchive(data_root=DATA_ROOT)
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


async def build_message(message: discord.Message) -> dict:
    attachments: list[str] = []
    for attachment in message.attachments:
        if (attachment.content_type or "").startswith("image/"):
            attachments.append(IMAGE_PLACEHOLDER)
        else:
            attachments.append(attachment.url)

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


async def publish() -> None:
    try:
        await archive.publish()
    except Exception as exc:
        logger.error("Archive publish failed: %s", exc)


async def push_loop() -> None:
    await bot.wait_until_ready()
    while True:
        await publish()
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
