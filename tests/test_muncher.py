"""Offline checks of the inbox merge the muncher workflow runs.

Plain filesystem work, so no network and no credentials: build a fake inbox, munch
it, and assert the transcript is what the archive contract promises. This is where
the interesting assertions live now -- the merge used to happen inside the bot's
push cycle, where testing it meant creating a real GitHub repository.

    python tests/test_muncher.py
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from formatter import MESSAGE_KEY, append_row, message_row, read_rows  # noqa: E402
from muncher import munch  # noqa: E402
from pusher import INBOX_DIR  # noqa: E402

IMAGE = "https://cdn.discordapp.com/attachments/1/2/pic.png"
TRANSCRIPT = "TestServer/general.csv"
USER_MAP = "TestServer/user_map.csv"


def record(msg_id: int, content: str) -> dict:
    return {
        "id": msg_id,
        "timestamp": f"2026-10-01T15:5{msg_id % 10}:00+00:00",
        "author": f"user{msg_id}",
        "author_id": 1000 + msg_id,
        "content": content,
        "attachments": [],
        "reply_to": None,
    }


def batch(inbox: Path, batch_id: str, rows: dict[str, list[int]]) -> Path:
    """Write one inbox batch, the way stage_batch() would."""
    for relative, ids in rows.items():
        path = inbox / batch_id / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        for msg_id in ids:
            append_row(path, message_row(record(msg_id, f"message {msg_id}")))
    return inbox / batch_id


def ids_of(path: Path) -> list[str]:
    return [row[MESSAGE_KEY] for row in read_rows(path)]


def scenario() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    with tempfile.TemporaryDirectory() as scratch:
        root = Path(scratch)
        inbox, archive = root / "inbox", root / "archive"
        inbox.mkdir()
        archive.mkdir()

        # a fresh archive: the batch creates the transcript
        first = batch(inbox, "20261001T155501Z-ab12cd34", {TRANSCRIPT: [1, 2]})
        summary = munch(inbox, archive)
        check("the summary counts what was new", (summary.batches, summary.rows) == (1, 2))
        check(
            "the summary names the batch count",
            summary.message() == "Munch 2 row(s) from 1 batch(es)",
        )
        check("the transcript was created", ids_of(archive / TRANSCRIPT) == ["1", "2"])
        check("the batch was deleted", not first.exists())
        check("the inbox itself survives", inbox.is_dir())

        # a second batch appends to the existing transcript
        batch(inbox, "20261001T160000Z-99887766", {TRANSCRIPT: [3]})
        summary = munch(inbox, archive)
        check("a later batch appends", summary.rows == 1)
        check("earlier rows are kept", ids_of(archive / TRANSCRIPT) == ["1", "2", "3"])

        # a re-delivered row is not duplicated, and is not counted as new
        batch(inbox, "20261001T160500Z-11223344", {TRANSCRIPT: [3, 4]})
        summary = munch(inbox, archive)
        check("a re-delivered row is not counted", summary.rows == 1)
        check(
            "a re-delivered row is not duplicated",
            ids_of(archive / TRANSCRIPT) == ["1", "2", "3", "4"],
        )

        # ordering is numeric, not lexicographic
        big = batch(inbox, "20261001T161000Z-aabbccdd", {TRANSCRIPT: [10, 9, 100]})
        munch(inbox, archive)
        check(
            "snowflakes sort numerically",
            ids_of(archive / TRANSCRIPT) == ["1", "2", "3", "4", "9", "10", "100"],
        )
        check("the batch was deleted", not big.exists())

        # user_map.csv merges on user id, in its own column order
        (inbox / "20261001T161500Z-55667788" / "TestServer").mkdir(parents=True)
        users = inbox / "20261001T161500Z-55667788" / USER_MAP
        append_row(users, {"username": "renamed", "user_id": "1001"})
        append_row(users, {"username": "user9", "user_id": "1009"})
        munch(inbox, archive)
        check(
            "the user map lands",
            [r["user_id"] for r in read_rows(archive / USER_MAP)] == ["1001", "1009"],
        )
        check("the newest username wins", read_rows(archive / USER_MAP)[0]["username"] == "renamed")

        # nothing is ever removed: one archive file, one channel
        before = {p.relative_to(archive).as_posix() for p in archive.rglob("*") if p.is_file()}
        batch(inbox, "20261001T162000Z-ccddeeff", {TRANSCRIPT: [5], "OtherServer/chat.csv": [6]})
        munch(inbox, archive)
        after = {p.relative_to(archive).as_posix() for p in archive.rglob("*") if p.is_file()}
        check("no archive path disappeared", before <= after)
        check("a new channel's transcript appears", "OtherServer/chat.csv" in after)
        check("no batch is left behind", not any(inbox.iterdir()))

        # idempotence: nothing staged, nothing to do
        summary = munch(inbox, archive)
        check("an empty inbox munches nothing", (summary.batches, summary.rows) == (0, 0))
        check(
            "an empty inbox changes nothing",
            ids_of(archive / TRANSCRIPT) == ["1", "2", "3", "4", "5", "9", "10", "100"],
        )

        # dry run changes nothing
        batch(inbox, "20261001T162500Z-ffeeddcc", {TRANSCRIPT: [7]})
        summary = munch(inbox, archive, dry_run=True)
        check("a dry run reports without writing", summary.rows == 1)
        check("a dry run leaves the batch alone", (inbox / "20261001T162500Z-ffeeddcc").exists())
        check("a dry run leaves the transcript alone", "7" not in ids_of(archive / TRANSCRIPT))

    return failures


def seam() -> list[str]:
    """What the pusher stages is what the muncher reads.

    The two halves are tested against each other's idea of the layout separately,
    which is how a path gets renamed on one side and silently orphan half a
    batch. Staging for real and munching the result checks the contract itself.
    """
    from pusher import GitHubArchive

    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    with tempfile.TemporaryDirectory() as scratch:
        data_root = Path(scratch) / "HOME"
        archive = data_root.parent / "archive"
        archive.mkdir()

        # the bot's side: buffer two channels and a user map, then stage
        pusher = GitHubArchive(data_root=data_root, owner="o", repo="r", token="t")
        for msg_id in (1, 2, 3):
            append_row(data_root / TRANSCRIPT, message_row(record(msg_id, f"message {msg_id}")))
        append_row(data_root / USER_MAP, {"username": "user1", "user_id": "1001"})
        staged = pusher.stage_batch()

        check("staging produced a batch", staged is not None)
        check(
            "the batch lands under inbox/<batch id>",
            staged.id and f"{data_root.name}/{INBOX_DIR}" == "HOME/inbox",
        )
        check("the local buffer still holds the rows", len(read_rows(data_root / TRANSCRIPT)) == 3)

        # the archive's side: a checkout where the batch has been committed
        inbox = archive / INBOX_DIR
        shutil.copytree(data_root / INBOX_DIR / staged.id, inbox / staged.id)
        summary = munch(inbox, archive)
        check("a staged batch munches", (summary.batches, summary.rows) == (1, 4))
        check("the transcript came out right", ids_of(archive / TRANSCRIPT) == ["1", "2", "3"])
        check(
            "the user map came out right",
            read_rows(archive / USER_MAP) == [{"username": "user1", "user_id": "1001"}],
        )
        check("the batch is gone from the archive", not any(inbox.iterdir()))
        check(
            "staged paths match the archive's expectations",
            all(path.startswith(f"{INBOX_DIR}/{staged.id}/") for path in staged.keys),
        )

    return failures


def main() -> int:
    failures = scenario()
    print()
    failures += seam()
    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
