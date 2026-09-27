"""Shared CSV schema for the archive.

The bot writes one CSV per channel and one per server for the user directory, and
the publisher merges those into the archive. Both sides import this module so the
column order and the flattening of a message record can never drift apart.
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
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


def message_row(record: Mapping[str, object]) -> dict[str, str]:
    """Flatten a captured message record into a single CSV row.

    Attachment URLs are space separated: a URL cannot contain a space, and CSV
    quoting of a newline-laden cell is awkward to consume downstream.
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
    return [
        {field: (row.get(field) or "") for field in fields}
        for row in reader
    ]


def append_row(path: Path, fields: list[str], row: Mapping[str, str]) -> None:
    """Append one row, writing the header first if the file is new or empty."""
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        if needs_header:
            writer.writeheader()
        writer.writerow(row)


def rewrite(path: Path, fields: list[str], rows: Iterable[Mapping[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    staging = path.with_name(path.name + ".tmp")
    with staging.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    staging.replace(path)


def read_rows(path: Path, fields: list[str]) -> list[dict[str, str]]:
    if not path.exists():
        return []
    return load_csv(path.read_bytes(), fields)
