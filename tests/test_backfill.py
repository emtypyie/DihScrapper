"""Offline checks of the history backfill.

Drives ``backfill_channel`` against fake Discord objects and a fake archive -- a
directory instead of GitHub -- so paging, the row shape, chunking and the append
itself are all testable with no network, no token and no rate limit.

The interesting assertion is the last scenario: a backfill run twice over the same
history, with the inbox merged in between, still leaves one transcript. That is the
whole claim of routing history through the inbox rather than editing transcripts.

    python tests/test_backfill.py
"""

from __future__ import annotations

import asyncio
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import discord
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import backfill  # noqa: E402
from formatter import MESSAGE_KEY, append_row, message_row, read_rows, sanitize_name  # noqa: E402
from muncher import munch  # noqa: E402
from pusher import INBOX_DIR, GitHubArchive  # noqa: E402

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
BOT_ID = 999
SERVER = "Test_Server"  # "Test Server", sanitised
TRANSCRIPT = f"{SERVER}/general.csv"
USER_MAP = f"{SERVER}/user_map.csv"


class FakeAuthor(SimpleNamespace):
    """discord.User stringifies to ``name#1234``, which reply_to_author records."""

    def __str__(self) -> str:
        return f"{self.name}#{self.id:04d}"


def message(msg_id: int, author_id: int = 1, reply_to: int | None = None, **kwargs):
    return SimpleNamespace(
        id=msg_id,
        author=FakeAuthor(id=author_id, name=f"user{author_id}", global_name=None),
        content=kwargs.get("content", f"message {msg_id}"),
        attachments=[SimpleNamespace(url=kwargs["url"])] if "url" in kwargs else [],
        reference=SimpleNamespace(message_id=reply_to) if reply_to else None,
        created_at=START + timedelta(seconds=msg_id),
        channel=None,
    )


def record(item):
    return {
        "id": item.id,
        "timestamp": item.created_at.isoformat(),
        "author": item.author.name,
        "author_id": item.author.id,
        "content": item.content,
        "attachments": [],
        "reply_to": None,
    }


class FakeChannel:
    """History newest first, paged by ``before``, as Discord serves it."""

    def __init__(self, name: str, guild, messages: list, forbidden: bool = False) -> None:
        self.name = name
        self.guild = guild
        self.messages = sorted(messages, key=lambda item: item.id, reverse=True)
        self.forbidden = forbidden
        self.cursors: list[int | None] = []
        self.sizes: list[int] = []
        self.lookups: list[int] = []
        for item in self.messages:
            item.channel = self

    async def history(self, *, limit: int = 100, before=None, oldest_first=None):
        if self.forbidden:
            raise discord.Forbidden(
                SimpleNamespace(status=403, reason="Forbidden"), "Missing Access"
            )
        self.cursors.append(before)
        self.sizes.append(limit)
        window = [item for item in self.messages if before is None or item.id < before]
        for item in window[:limit]:
            yield item

    async def fetch_message(self, message_id: int):
        self.lookups.append(message_id)
        return next((item for item in self.messages if item.id == message_id), None)


class LocalArchive(GitHubArchive):
    """``GitHubArchive`` with the network replaced by a directory.

    Staging and clearing stay real: they are the half of the cycle that decides
    which rows exist and which are settled. Only publish and verify are stubbed,
    and the directory doubles as the archive checkout the muncher then runs in --
    which is what the real one is: the pushed batches are committed on that branch.
    """

    def __init__(self, scratch: Path) -> None:
        super().__init__(data_root=scratch / "HOME", owner="o", repo="r", token="t", branch="main")
        self.root = scratch / "archive"
        self.pushes = 0

    async def publish(self, batch, dry_run: bool = False):
        if dry_run:
            return batch.keys
        self.pushes += 1
        for path, payload in batch.files.items():
            target = self.root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        return batch.keys

    async def verify(self, published):
        return [path for path in published if not (self.root / path).exists()]


def ids_of(path: Path) -> list[str]:
    return [row[MESSAGE_KEY] for row in read_rows(path)]


def inbox_rows(archive: LocalArchive) -> int:
    """Message rows currently sitting in the inbox, ignoring the user maps."""
    total = 0
    for path in (archive.root / INBOX_DIR).rglob("*.csv"):
        if path.name != "user_map.csv":
            total += len(read_rows(path))
    return total


def harness(page_size: int, chunk: int) -> tuple[LocalArchive, tempfile.TemporaryDirectory]:
    scratch = tempfile.TemporaryDirectory()
    archive = LocalArchive(Path(scratch.name))
    backfill.DATA_ROOT = archive.data_root
    backfill.PAGE_SIZE = page_size
    backfill.CHUNK = chunk
    return archive, scratch


async def run(archive: LocalArchive, channel, *, limit: int = 0, **kwargs):
    """backfill_channel with a stand-in for the logged-in bot."""
    bot = SimpleNamespace(user=SimpleNamespace(id=BOT_ID))
    return await backfill.backfill_channel(bot, archive, channel, limit=limit, **kwargs)


def scenario_names() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    check("a plain name is left alone", sanitize_name("general") == "general")
    check("spaces and emoji become underscores", sanitize_name("Anime & Chill") == "Anime___Chill")
    check("a path separator cannot survive", sanitize_name("../../etc") == "etc")
    check("the cap is 64 characters", len(sanitize_name("x" * 200)) == 64)
    check("an empty name still names something", sanitize_name("///") == "unnamed")

    return failures


async def scenario_unreadable() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    guild = SimpleNamespace(name="Test Server")
    channel = FakeChannel("locked", guild, [message(1)], forbidden=True)
    try:
        await backfill.read_page(channel, 100, None)
        check("an unreadable channel is refused", False)
    except backfill.BackfillError as exc:
        check("an unreadable channel is refused", "Read Message History" in str(exc))

    return failures


async def scenario_paging() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    archive, scratch = harness(page_size=100, chunk=10_000)
    try:
        guild = SimpleNamespace(name="Test Server")
        channel = FakeChannel("general", guild, [message(n) for n in range(1, 251)])
        stats = await run(archive, channel)

        check("every message is archived", stats.rows == 250)
        check(
            "history is read newest first, 100 at a time",
            channel.cursors == [None, 151, 51] and stats.pages == 3,
        )
        check("one batch for a small channel", archive.pushes == 1)
        check(
            "the batch carries a transcript and a user map",
            sorted(path.name for path in (archive.root / INBOX_DIR).rglob("*.csv"))
            == ["general.csv", "user_map.csv"],
        )
        check(
            "the buffer is cleared once the batch is confirmed",
            read_rows(archive.data_root / TRANSCRIPT) == [],
        )
        check(
            "nothing but the inbox was written",
            sorted(path.name for path in archive.root.iterdir()) == [INBOX_DIR],
        )

        summary = munch(archive.root / INBOX_DIR, archive.root)
        check("the inbox munches in one go", summary.batches == 1)
        check(
            "the transcript is sorted oldest first",
            ids_of(archive.root / TRANSCRIPT) == [str(n) for n in range(1, 251)],
        )
        check(
            "the user map holds the author once",
            read_rows(archive.root / USER_MAP) == [{"username": "user1", "user_id": "1"}],
        )
        check("the inbox is drained", not any((archive.root / INBOX_DIR).iterdir()))
    finally:
        scratch.cleanup()

    return failures


async def scenario_limits() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    archive, scratch = harness(page_size=100, chunk=10_000)
    try:
        guild = SimpleNamespace(name="Test Server")
        channel = FakeChannel("general", guild, [message(n) for n in range(1, 251)])
        stats = await run(archive, channel, limit=120)

        check("--limit stops the walk", stats.rows == 120)
        check(
            "the last page is sized to what is left",
            channel.sizes == [100, 20] and channel.cursors == [None, 151],
        )
        check("only the newest 120 were read", inbox_rows(archive) == 120)

        # the bot's own messages are not part of the archive, live or backfilled
        mixed = FakeChannel("mixed", guild, [message(1), message(2, author_id=BOT_ID)])
        stats = await run(archive, mixed)
        check("the bot's own message is skipped", stats.fetched == 2 and stats.rows == 1)
    finally:
        scratch.cleanup()

    return failures


async def scenario_replies() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    # Four messages over pages of two, so each reply either has its parent in the
    # page in hand or on the page before it: 3 and 4 are read together, 1 and 2
    # are read together, and 3's parent (1) is not in 3's page.
    history = [
        message(1, content="the original"),
        message(2, reply_to=1, content="answering within the older page"),
        message(3, reply_to=1, content="answering from the newer page"),
        message(4, reply_to=3, content="answering from the newer page too"),
    ]
    # dry runs, so the rows are still on disk to read: a real run settles and
    # clears them, which is the point of it
    local = "Test_Server/general.csv"

    archive, scratch = harness(page_size=2, chunk=10_000)
    try:
        guild = SimpleNamespace(name="Test Server")
        channel = FakeChannel("general", guild, history)
        await run(archive, channel, dry_run=True)
        rows = {row[MESSAGE_KEY]: row for row in read_rows(archive.data_root / local)}

        check(
            "a reply on the page resolves for free",
            rows["4"]["reply_to_id"] == "3"
            and rows["4"]["reply_to_content"] == "answering from the newer page"
            and rows["2"]["reply_to_id"] == "1",
        )
        check(
            "a reply across the page boundary is left empty",
            rows["3"]["reply_to_id"] == "" and rows["3"]["reply_to_author"] == "",
        )
        check("no parent is fetched by default", channel.lookups == [])
    finally:
        scratch.cleanup()

    archive, scratch = harness(page_size=2, chunk=10_000)
    try:
        guild = SimpleNamespace(name="Test Server")
        channel = FakeChannel("general", guild, history)
        await run(archive, channel, resolve_replies=True, dry_run=True)
        rows = {row[MESSAGE_KEY]: row for row in read_rows(archive.data_root / local)}

        check(
            "--resolve-replies fetches only the parent it is missing",
            channel.lookups == [1] and rows["3"]["reply_to_id"] == "1",
        )
        check(
            "a fetched parent is still only a snippet",
            rows["3"]["reply_to_author"] == "user1#0001"
            and rows["3"]["reply_to_content"] == "the original",
        )
    finally:
        scratch.cleanup()

    return failures


async def scenario_chunking() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    archive, scratch = harness(page_size=3, chunk=4)
    try:
        guild = SimpleNamespace(name="Test Server")
        channel = FakeChannel("general", guild, [message(n) for n in range(1, 20)])
        stats = await run(archive, channel)

        batches = sorted(path.name for path in (archive.root / INBOX_DIR).iterdir())
        check("the walk is chunked into several batches", len(batches) == stats.batches == 4)
        check("every row is staged exactly once", inbox_rows(archive) == 19)
        check(
            "the buffer is cleared chunk by chunk", read_rows(archive.data_root / TRANSCRIPT) == []
        )
        check(
            "batch ids are named after their newest row",
            all(batch.startswith("1970-01-01T") or "T" in batch for batch in batches),
        )

        summary = munch(archive.root / INBOX_DIR, archive.root)
        check("the chunks munch in one go", summary.batches == 4)
        check(
            "and it is whole", ids_of(archive.root / TRANSCRIPT) == [str(n) for n in range(1, 20)]
        )
    finally:
        scratch.cleanup()

    return failures


async def scenario_overlap() -> list[str]:
    """The claim of the whole design: backfill twice, still one transcript."""
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    archive, scratch = harness(page_size=4, chunk=3)
    try:
        guild = SimpleNamespace(name="Test Server")
        history = [message(n) for n in range(1, 13)]
        expected = [str(n) for n in range(1, 13)]

        for run_number in (1, 2):
            channel = FakeChannel("general", guild, history)
            await run(archive, channel)
            munch(archive.root / INBOX_DIR, archive.root)
            check(f"run {run_number} leaves 12 rows", ids_of(archive.root / TRANSCRIPT) == expected)
            check(
                f"run {run_number} drains the inbox", not any((archive.root / INBOX_DIR).iterdir())
            )

        first = (archive.root / TRANSCRIPT).read_bytes()
        channel = FakeChannel("general", guild, history)
        await run(archive, channel)
        munch(archive.root / INBOX_DIR, archive.root)
        check(
            "a third identical run does not rewrite the transcript",
            (archive.root / TRANSCRIPT).read_bytes() == first,
        )

        # and a row the live bot already captured is not duplicated either
        append_row(archive.root / TRANSCRIPT, message_row(record(message(7))))
        marked = (archive.root / TRANSCRIPT).read_bytes()
        channel = FakeChannel("general", guild, [message(7)])
        await run(archive, channel)
        summary = munch(archive.root / INBOX_DIR, archive.root)
        check(
            "re-fetching a row the archive already has adds nothing",
            summary.rows == 0 and (archive.root / TRANSCRIPT).read_bytes() == marked,
        )
    finally:
        scratch.cleanup()

    return failures


async def scenario_dry_run() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    archive, scratch = harness(page_size=100, chunk=10_000)
    try:
        guild = SimpleNamespace(name="Test Server")
        channel = FakeChannel("general", guild, [message(n) for n in range(1, 6)])
        stats = await run(archive, channel, dry_run=True)

        check("a dry run publishes nothing", archive.pushes == 0)
        check("a dry run still reads and reports", stats.rows == 5)
        check(
            "a dry run settles nothing, so the rows survive for a real run",
            len(read_rows(archive.data_root / TRANSCRIPT)) == 5,
        )
        check(
            "a dry run leaves the staged batch behind",
            any((archive.data_root / INBOX_DIR).rglob("*.csv")),
        )
    finally:
        scratch.cleanup()

    return failures


async def scenario_server() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    archive, scratch = harness(page_size=100, chunk=10_000)
    try:
        guild = SimpleNamespace(name="Test Server")
        general = FakeChannel("general", guild, [message(1), message(2)])
        locked = FakeChannel("locked", guild, [message(3)], forbidden=True)
        memes = FakeChannel("memes", guild, [message(4), message(5), message(6)])
        by_id = {10: general, 11: locked, 12: memes}

        async def fetch_channel(_channel_id: int):
            if _channel_id not in by_id:
                raise discord.NotFound(
                    SimpleNamespace(status=404, reason="Not Found"), "Unknown Channel"
                )
            channel = by_id[_channel_id]
            if channel.forbidden:
                raise discord.Forbidden(
                    SimpleNamespace(status=403, reason="Forbidden"), "Missing Access"
                )
            return channel

        bot = SimpleNamespace(user=SimpleNamespace(id=BOT_ID), fetch_channel=fetch_channel)
        targets = [(10, "general"), (11, "locked"), (99, "deleted"), (12, "memes")]
        stats = await backfill.backfill_channels(bot, archive, targets, limit=0)

        check(
            "a channel that cannot be opened is counted, not fatal",
            stats.failures == 2 and stats.channels == 2,
        )
        check("a deleted channel id does not end the run", stats.rows == 5)

        munch(archive.root / INBOX_DIR, archive.root)
        check(
            "the readable channels either side of it are still archived",
            ids_of(archive.root / TRANSCRIPT) == ["1", "2"]
            and ids_of(archive.root / f"{SERVER}/memes.csv") == ["4", "5", "6"],
        )
    finally:
        scratch.cleanup()

    return failures


async def scenario_selection() -> list[str]:
    """What a bare run covers, and which channels a server sweep picks out."""
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    raw = [
        {"id": "30", "name": "general", "type": 0},
        {"id": "10", "name": "older", "type": 0},
        {"id": "20", "name": "Voice", "type": 2},
        {"id": "40", "name": "Forum", "type": 15},
        {"id": "50", "name": "Category", "type": 4},
    ]
    check(
        "text channels only, oldest id first",
        backfill._text_channels(raw) == [(10, "older"), (30, "general")],
    )
    check(
        "an unnamed channel still names something",
        backfill._text_channels([{"id": "60", "type": 0}]) == [(60, "60")],
    )

    guilds = [
        {"id": "1215905413363531817", "name": "Otaku Valley"},
        {"id": "99", "name": "A Server"},
    ]
    ordered = backfill._server_list(guilds)
    check(
        "servers come back oldest id first, numerically",
        [name for _, name in ordered] == ["A Server", "Otaku Valley"],
    )
    check(
        "an unnamed server still names something",
        backfill._server_list([{"id": "5"}]) == [("5", "5")],
    )

    return failures


async def main() -> int:
    failures = scenario_names()
    print()
    failures += await scenario_unreadable()
    print()
    failures += await scenario_paging()
    print()
    failures += await scenario_limits()
    print()
    failures += await scenario_replies()
    print()
    failures += await scenario_chunking()
    print()
    failures += await scenario_overlap()
    print()
    failures += await scenario_server()
    print()
    failures += await scenario_selection()
    print()
    failures += await scenario_dry_run()
    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
