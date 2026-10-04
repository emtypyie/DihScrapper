"""The CSV shape of the archive, imported by everything that writes or reads it.

The bot buffers rows, the pusher stages them into the inbox and the muncher
merges the inbox into the archive, so the column order, the row flattening and --
above all -- the key rows are deduped on cannot drift apart.

The path helpers live here too, because the live bot and the backfill write to the
same tree and a name that is sanitised twice differently is two transcripts where
there should be one.

Standard library only, deliberately: the muncher runs in CI from a bare checkout
with nothing installed.
"""

from __future__ import annotations

import csv
import io
import re
from pathlib import Path, PurePath
from typing import Iterable, Mapping

MESSAGE_FIELDS = [
    "id",
    "timestamp",
    "author",
    "author_id",
    "content",
    "attachments",
    "reply_to_id",
    "reply_to_author_id",
    "reply_to_author",
    "reply_to_content",
]

USER_FIELDS = ["username", "user_id"]

MESSAGE_KEY = "id"
USER_KEY = "user_id"

# a path is a user map or a transcript by name; nothing else in the tree is a CSV
USER_MAP_FILE = "user_map.csv"

USERNAME_FIELD = "username"

# Discord names carry spaces, emoji and the occasional slash. Anything outside this
# set becomes an underscore, and the cap keeps a path under every limit git and
# Windows impose. An empty result still has to name something.
_UNSAFE_NAME = re.compile(r"[^A-Za-z0-9_-]")
NAME_MAX_CHARS = 64
FALLBACK_NAME = "unnamed"


def sanitize_name(name: str) -> str:
    """A server or channel name as one path segment."""
    return _UNSAFE_NAME.sub("_", name).strip("_")[:NAME_MAX_CHARS] or FALLBACK_NAME


def merge_user_map(path: Path, records: Iterable[Mapping[str, object]]) -> None:
    """Fold a batch of captured records into the server's user map.

    A record names its author the way a transcript row does -- ``author`` -- while
    the map itself says ``username``, and that difference is the whole reason this
    function exists rather than a bare rename at the call site.

    Keyed on user id rather than name, which is the point of the file: a user who
    renames keeps one row, and the newest sighting of a name is the one kept. The
    whole file is rewritten per call, so a caller with many records in hand should
    call once with all of them.
    """
    rows = {row[USER_KEY]: row for row in read_rows(path)}
    for record in records:
        name, user_id = record.get("author"), record.get("author_id")
        if name and user_id:
            rows[str(user_id)] = {USERNAME_FIELD: str(name), USER_KEY: str(user_id)}
    rewrite(path, sorted(rows.values(), key=lambda row: row[USERNAME_FIELD].lower()))


def key_for(path: str | Path | PurePath) -> str:
    """The column that identifies a row of this archive path."""
    name = Path(path).name
    return USER_KEY if name == USER_MAP_FILE else MESSAGE_KEY


def fields_for(path: str | Path | PurePath) -> list[str]:
    """The column order for this archive path."""
    return USER_FIELDS if key_for(path) == USER_KEY else MESSAGE_FIELDS


def sort_key(value: str) -> tuple[int, int, str]:
    """Sort snowflakes and user ids numerically, tolerating non-numeric input."""
    try:
        return (0, int(value), "")
    except (TypeError, ValueError):
        return (1, 0, str(value))


def message_row(record: Mapping[str, object]) -> dict[str, str]:
    """Flatten a captured message record into a single CSV row.

    Attachment URLs are space separated: a URL cannot contain a space, and a
    newline-laden cell is awkward to consume downstream.
    """
    reply = record.get("reply_to") or {}
    assert isinstance(reply, Mapping)
    attachments = record.get("attachments") or []
    assert isinstance(attachments, Iterable)
    return {
        "id": str(record["id"]),
        "timestamp": str(record["timestamp"]),
        "author": str(record["author"]),
        "author_id": str(record["author_id"]),
        "content": str(record.get("content") or ""),
        "attachments": " ".join(str(item) for item in attachments),
        "reply_to_id": str(reply.get("message_id") or ""),
        "reply_to_author_id": str(reply.get("author_id") or ""),
        "reply_to_author": str(reply.get("author_username") or ""),
        "reply_to_content": str(reply.get("content_snippet") or ""),
    }


def dump_csv(fields: list[str], rows: Iterable[Mapping[str, str]]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    return buffer.getvalue().encode("utf-8")


def load_csv(payload: bytes, fields: list[str]) -> list[dict[str, str]]:
    reader = csv.DictReader(io.StringIO(payload.decode("utf-8", errors="replace")))
    return [{field: (row.get(field) or "") for field in fields} for row in reader]


def append_row(path: Path, row: Mapping[str, str]) -> None:
    """Add a row to the buffer, writing the header only if the file is new."""
    fields = fields_for(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        if needs_header:
            writer.writeheader()
        writer.writerow(row)


def rewrite(path: Path, rows: Iterable[Mapping[str, str]]) -> None:
    """Replace a file with rows, sorted by the caller, via a staging file."""
    fields = fields_for(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_name(path.name + ".tmp")
    with staging.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    staging.replace(path)


def read_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return load_csv(path.read_bytes(), fields_for(path))
