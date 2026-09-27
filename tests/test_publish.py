"""End-to-end check of the append-only archive publisher.

Drives the real publish -> verify -> clear cycle against a throwaway private
repository, which is deleted afterwards, so the live archive is never touched.
Covers bootstrapping, appending, that nothing is ever deleted, that clearing is
gated on verification, that a row landing mid-publish survives the clear, and
that a re-push of identical rows is a no-op.

    python tests/test_publish.py
"""

from __future__ import annotations

import asyncio
import base64
import sys
import tempfile
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from archive_format import (  # noqa: E402
    MESSAGE_FIELDS,
    MESSAGE_KEY,
    USER_FIELDS,
    append_row,
    load_csv,
    message_row,
    read_rows,
)
from uploader import ArchiveError, GitHubArchive  # noqa: E402

TRANSCRIPT = "TestServer/general.csv"
USER_MAP = "TestServer/user_map.csv"
IMAGE = "https://cdn.discordapp.com/attachments/1/2/pic.png"
VIDEO = "https://cdn.discordapp.com/attachments/1/3/clip.mp4"


def record(msg_id: int, content: str, attachments: list[str] | None = None,
           reply: dict | None = None) -> dict:
    return {
        "id": msg_id,
        "timestamp": "2026-09-27T20:00:00+00:00",
        "author": f"user{msg_id}",
        "author_id": 1000 + msg_id,
        "content": content,
        "attachments": attachments or [],
        "reply_to": reply,
    }


async def strict_errors() -> list[str]:
    """A 404 must raise, never be swallowed into a silent success.

    Regression: ``_api`` used to default to ``allow_missing=(404,)``, so a ref
    update that 404'd returned ``None`` and the caller reported a successful
    publish while the branch had not moved.
    """
    from aiohttp import web

    import uploader

    hits: list[str] = []

    async def not_found(request: web.Request) -> web.Response:
        hits.append(f"{request.method} {request.path}")
        return web.json_response({"message": "Not Found"}, status=404)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", not_found)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = runner.addresses[0][1]

    original = uploader.API_ROOT
    uploader.API_ROOT = f"http://127.0.0.1:{port}"
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    archive = GitHubArchive(owner="o", repo="r", token="t")
    try:
        for label, coro in (
            ("ref update 404 raises", archive._write_ref("a" * 40, "b" * 40)),
            ("blob write 404 raises", archive._api(
                "POST", archive._repo_path("/git/blobs"), {"content": "", "encoding": "base64"})),
            ("tree write 404 raises", archive._api(
                "POST", archive._repo_path("/git/trees"), {"tree": []})),
            ("commit write 404 raises", archive._api(
                "POST", archive._repo_path("/git/commits"), {"message": "x"})),
            ("read of a missing commit 404 raises", archive._api(
                "GET", archive._repo_path(f"/git/commits/{'c' * 40}"))),
        ):
            try:
                await coro
                check(label, False)
            except ArchiveError:
                check(label, True)

        check("every probe hit the server", len(hits) == 5)
    finally:
        uploader.API_ROOT = original
        await archive.close()
        await runner.cleanup()
    return failures


async def resilient_cycle() -> list[str]:
    """push_once must contain failures and keep the buffer.

    Regression: clear_published() had no guard, so an error there propagated out
    of push_loop() and killed the task -- the bot kept claiming it was listening
    while silently archiving nothing.
    """
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    import main as bot_main

    class Boom(RuntimeError):
        pass

    class FakeArchive:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.cleared: list[dict] = []
            self.attempts = 0

        def pending_payloads(self) -> dict[str, bytes]:
            return {"TestServer/general.csv": b"x"}

        async def publish(self, payloads=None) -> dict[str, list[str]]:
            self.calls.append("publish")
            self.attempts += 1
            if self.attempts == 1:
                raise Boom("network went away")
            return {TRANSCRIPT: ["9"]}

        async def verify(self, published) -> list[str]:
            self.calls.append("verify")
            return []

        def clear_published(self, published) -> None:
            self.calls.append("clear")
            self.cleared.append(published)

    fake = FakeArchive()
    original = bot_main.archive
    bot_main.archive = fake
    try:
        await bot_main.push_once()
        check("a failing publish is contained", fake.calls == ["publish"])
        check("nothing is cleared when publish fails", fake.cleared == [])

        fake.calls.clear()
        await bot_main.push_once()
        check("a healthy cycle publishes, verifies, then clears",
              fake.calls == ["publish", "verify", "clear"])
        check("the cleared set is exactly what was published",
              fake.cleared == [{TRANSCRIPT: ["9"]}])

        class ClearBoom(FakeArchive):
            def clear_published(self, published) -> None:
                raise Boom("disk full")

        bot_main.archive = ClearBoom()
        await bot_main.push_once()
        check("an error while clearing is contained", True)
    finally:
        bot_main.archive = original

    return failures


async def scenario() -> list[str]:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    probe = GitHubArchive()
    if not probe.configured:
        print("SKIP  GITHUB_TOKEN is not configured")
        return failures

    owner = (await probe._api("GET", "/user", allow_missing=()))["login"]
    name = f"dihscrapper-test-{uuid.uuid4().hex[:8]}"
    await probe._api(
        "POST", "/user/repos",
        {"name": name, "private": True, "auto_init": False},
        allow_missing=(),
    )
    await probe.close()

    try:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "HOME"
            archive = GitHubArchive(data_root=root, owner=owner, repo=name, branch="main")
            try:
                head, _, _ = await archive._remote_state()
                check("fresh repository has no commits", head is None)

                (root / "TestServer").mkdir(parents=True, exist_ok=True)
                append_row(
                    root / "TestServer" / "general.csv",
                    MESSAGE_FIELDS,
                    message_row(record(1, "hello", [IMAGE])),
                )
                append_row(
                    root / "TestServer" / "general.csv",
                    MESSAGE_FIELDS,
                    message_row(record(2, "a video", [VIDEO])),
                )
                append_row(
                    root / "TestServer" / "user_map.csv",
                    USER_FIELDS,
                    {"username": "user1", "user_id": "1001"},
                )

                published = await archive.publish()
                check("publish reports the pushed keys", set(published) == {TRANSCRIPT, USER_MAP})
                check("publish returns every message id", sorted(published[TRANSCRIPT]) == ["1", "2"])

                missing = await archive.verify(published)
                check("verify finds nothing missing", missing == [])

                async def remote_rows(path: str, blob_sha: str) -> list[dict[str, str]]:
                    blob = await archive._api(
                        "GET", archive._repo_path(f"/git/blobs/{blob_sha}")
                    )
                    return load_csv(base64.b64decode(blob["content"]), MESSAGE_FIELDS)

                head, _, paths = await archive._remote_state()
                check("transcript committed", TRANSCRIPT in paths)
                check("user map committed", USER_MAP in paths)

                remote = await remote_rows(TRANSCRIPT, paths[TRANSCRIPT])
                check("image stored as its URL", remote[0]["attachments"] == IMAGE)
                check("video stored as its URL", remote[1]["attachments"] == VIDEO)
                check("no placeholder anywhere", not any(
                    "potential image" in v for row in remote for v in row.values()
                ))
                check("rows are sorted by snowflake",
                      [r[MESSAGE_KEY] for r in remote] == ["1", "2"])

                # A message that lands mid-publish must survive the clear.
                late = message_row(record(3, "arrived during publish", [IMAGE]))
                append_row(root / "TestServer" / "general.csv", MESSAGE_FIELDS, late)

                archive.clear_published(published)
                after_clear = read_rows(root / "TestServer" / "general.csv", MESSAGE_FIELDS)
                check("confirmed rows are cleared from the buffer",
                      [r[MESSAGE_KEY] for r in after_clear] == ["3"])
                check("unconfirmed row survives the clear", len(after_clear) == 1)

                check("republish of identical rows is a no-op", await archive.publish() is not None)
                second = await archive.publish()
                check("republish reports nothing new", second == {})

                append_row(
                    root / "TestServer" / "general.csv",
                    MESSAGE_FIELDS,
                    message_row(record(4, "fourth", [], reply={
                        "message_id": 1, "author_id": 1001,
                        "author_username": "user1", "content_snippet": "hello",
                    })),
                )
                pub2 = await archive.publish()
                check("later message is appended", sorted(pub2[TRANSCRIPT]) == ["4"])
                check("verify passes again", await archive.verify(pub2) == [])

                head_after, _, final_paths = await archive._remote_state()
                check("no path was ever deleted", set(final_paths) == set(paths))
                check("append created a new commit", head_after != head)

                final_rows = await remote_rows(TRANSCRIPT, final_paths[TRANSCRIPT])
                check("archive holds all four messages",
                      [r[MESSAGE_KEY] for r in final_rows] == ["1", "2", "3", "4"])
                check("reply columns recorded", final_rows[3]["reply_to_id"] == "1"
                      and final_rows[3]["reply_to_author"] == "user1")
            finally:
                await archive.close()
    finally:
        cleanup = GitHubArchive(owner=owner, repo=name)
        try:
            await cleanup._api("DELETE", f"/repos/{owner}/{name}", allow_missing=())
            print("cleaned up throwaway repository")
        finally:
            await cleanup.close()

    return failures


async def main() -> int:
    failures = await strict_errors()
    print()
    failures += await resilient_cycle()
    print()
    failures += await scenario()
    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
