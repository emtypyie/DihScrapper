"""Turning a discord.Message into the record the archive stores.

One place for it, because the row a live capture writes and the row a backfill
writes have to be the same row: a dataset built from both is only usable if the
columns mean the same thing. So this is the only code that knows what a reply is
worth.

Reply resolution is index-first. ``build_records`` hands every message on a page
to ``build_message`` along with an index of that page, so a reply whose parent is
in the fetched window costs nothing at all -- no API call. Only a parent missing
from the index is fetched, and only when the caller asks for it.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence

import discord

logger = logging.getLogger("DihScrapper.capture")

# a reply's parent is kept only as a snippet: enough to identify the message it
# answers without doubling the size of every row that is a reply
REPLY_SNIPPET_CHARS = 200


def reference_summary(message: discord.Message) -> dict:
    """The ``reply_to`` payload for a message used as someone else's parent."""
    return {
        "message_id": message.id,
        "author_id": message.author.id,
        "author_username": str(message.author),
        "content_snippet": (message.content or "")[:REPLY_SNIPPET_CHARS],
    }


async def fetch_reference(
    channel: discord.abc.Messageable, message_id: int
) -> discord.Message | None:
    try:
        return await channel.fetch_message(message_id)
    except discord.NotFound:
        return None
    except discord.HTTPException as exc:
        logger.warning("Could not fetch referenced message %s: %s", message_id, exc)
        return None


async def build_message(
    message: discord.Message,
    *,
    replies: Mapping[int, Mapping[str, object]] | None = None,
    resolve_missing: bool = True,
) -> dict:
    """One message as an archive record.

    Attachments are URLs only; nothing is downloaded, live or backfilled.

    ``replies`` is an index of already-seen messages by id -- usually the page
    being processed -- consulted before the network. ``resolve_missing`` decides
    what happens when the parent is not in it: fetch it (the live bot's case, since
    the parent of a reply that just arrived is usually recent), or leave the reply
    columns empty (a backfill walking backwards cannot know where it is going to
    stop, so a hundred messages of lookups per page is a cost worth opting into).
    """
    attachments = [attachment.url for attachment in message.attachments]

    reply_to = None
    reference_id = message.reference.message_id if message.reference else None
    if reference_id:
        cached = replies.get(reference_id) if replies is not None else None
        if cached is not None:
            reply_to = cached
        elif resolve_missing:
            parent = await fetch_reference(message.channel, reference_id)
            if parent is not None:
                reply_to = reference_summary(parent)

    return {
        "id": message.id,
        "timestamp": message.created_at.isoformat(),
        "author": message.author.global_name or message.author.name,
        "author_id": message.author.id,
        "content": message.content or "",
        "attachments": attachments,
        "reply_to": dict(reply_to) if reply_to else None,
    }


async def build_records(
    messages: Sequence[discord.Message],
    *,
    resolve_missing: bool = True,
) -> list[dict]:
    """Records for a whole page of history, replies resolved against the page."""
    index = {message.id: reference_summary(message) for message in messages}
    return [
        await build_message(message, replies=index, resolve_missing=resolve_missing)
        for message in messages
    ]
