"""End-to-end checks of staging and pushing, offline.

Drives stage -> push -> verify -> clear against a local fake GitHub and a
temporary DATA_ROOT, so the whole cycle runs without a network or a token. The
fake holds blobs, trees, commits and a ref in memory and enforces the two rules
that matter: a ref only moves fast-forward, and a blob must exist before a tree
can name it.

``--live`` additionally drives the same cycle against a throwaway private
repository, which is created and deleted, so the real archive is never touched.

    python tests/test_pusher.py            # offline
    python tests/test_pusher.py --live
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import sys
import tempfile
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from formatter import append_row, message_row, read_rows  # noqa: E402
from pusher import INBOX_DIR, ArchiveError, GitHubArchive  # noqa: E402

TRANSCRIPT = "TestServer/general.csv"
USER_MAP = "TestServer/user_map.csv"
IMAGE = "https://cdn.discordapp.com/attachments/1/2/pic.png"
VIDEO = "https://cdn.discordapp.com/attachments/1/3/clip.mp4"


def record(msg_id: int, content: str, attachments: list[str] | None = None) -> dict:
    return {
        "id": msg_id,
        "timestamp": f"2026-10-01T15:{msg_id:02d}:00+00:00",
        "author": f"user{msg_id}",
        "author_id": 1000 + msg_id,
        "content": content,
        "attachments": attachments or [],
        "reply_to": None,
    }


class FakeGitHub:
    """Just enough of the git data API to push a batch and read a branch back.

    Trees are stored flattened, which is what the real API is for and what the
    client flattens them into anyway. Every failure mode the client has an opinion
    about is reproduced: an unknown ref is a 404, a blob that was never written is
    a 422, and a ref update that is not a fast-forward is a 422.
    """

    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.trees: dict[str, dict[str, str]] = {}
        self.commits: dict[str, tuple[str, str | None]] = {}
        self.refs: dict[str, str] = {}
        self.hits: list[str] = []
        self.repo = False
        self._sha = 0

    # -- plumbing ---------------------------------------------------------
    def _next(self, prefix: str) -> str:
        self._sha += 1
        return f"{prefix}{self._sha:0{40 - len(prefix)}}"

    def paths(self, branch: str = "main") -> dict[str, str]:
        head = self.refs.get(branch)
        if head is None:
            return {}
        tree_sha = self.commits[head][0]
        return dict(self.trees[tree_sha])

    def blob_text(self, path: str, branch: str = "main") -> bytes:
        return self.blobs[self.paths(branch)[path]]

    # -- routing ----------------------------------------------------------
    async def handle(self, request) -> tuple[int, dict]:
        path = request.path
        method = request.method
        self.hits.append(f"{method} {path}")
        body = await request.json() if method in ("POST", "PUT", "PATCH") else {}

        if path.endswith("/git/refs") and method == "POST":
            return self._create_ref(body)
        # GitHub is inconsistent here: a ref is read at /git/ref/heads/{branch}
        # and updated at /git/refs/heads/{branch}
        if "/git/ref/heads/" in path or "/git/refs/heads/" in path:
            branch = path.rsplit("/", 1)[1]
            if method == "GET":
                head = self.refs.get(branch)
                return (200, {"object": {"sha": head}}) if head else (404, {"message": "Not Found"})
            return self._move_ref(branch, body)
        if "/git/commits/" in path and method == "GET":
            sha = path.rsplit("/", 1)[1]
            if sha not in self.commits:
                return 404, {"message": "Not Found"}
            tree, _ = self.commits[sha]
            return 200, {"tree": {"sha": tree}}
        if "/git/trees/" in path and method == "GET":
            sha = path.rsplit("/", 1)[1]
            if sha not in self.trees:
                return 404, {"message": "Not Found"}
            entries = [
                {"path": name, "type": "blob", "sha": blob}
                for name, blob in sorted(self.trees[sha].items())
            ]
            return 200, {"tree": entries}
        if "/git/blobs" in path and method == "POST":
            if not self.repo:
                return 404, {"message": "Not Found"}
            # a real blob lands under its real digest, which is how the client can
            # name a blob in a tree without reading back the write's response
            payload = base64.b64decode(body["content"])
            sha = hashlib.sha1(b"blob %d\x00" % len(payload) + payload).hexdigest()
            self.blobs[sha] = payload
            return 200, {"sha": sha}
        if "/git/blobs/" in path and method == "GET":
            sha = path.rsplit("/", 1)[1]
            if sha not in self.blobs:
                return 404, {"message": "Not Found"}
            return 200, {"sha": sha}
        if "/git/trees" in path and method == "POST":
            if not self.repo:
                return 404, {"message": "Not Found"}
            return self._write_tree(body)
        if "/git/commits" in path and method == "POST":
            if not self.repo:
                return 404, {"message": "Not Found"}
            return self._write_commit(body)
        if path.endswith("/contents/.archive") and method == "PUT":
            self.repo = True
            blob = self._next("b")
            self.blobs[blob] = base64.b64decode(body["content"])
            tree = self._next("t")
            self.trees[tree] = {".archive": blob}
            commit = self._next("c")
            self.commits[commit] = (tree, None)
            self.refs["main"] = commit
            return 200, {"commit": {"sha": commit}}
        if method == "GET":
            return 200, {"default_branch": "main"}
        return 404, {"message": f"unhandled {method} {path}"}

    # -- endpoints --------------------------------------------------------
    def _create_ref(self, body: dict) -> tuple[int, dict]:
        branch = body["ref"].split("/")[-1]
        if branch in self.refs:
            return 422, {"message": "Reference already exists"}
        self.repo = True
        self.refs[branch] = body["sha"]
        return 201, {"ref": body["ref"]}

    def _move_ref(self, branch: str, body: dict) -> tuple[int, dict]:
        head = self.refs.get(branch)
        if head is None:
            return 404, {"message": "Not Found"}
        target = self.commits.get(body["sha"])
        if target is None:
            return 422, {"message": "Object does not exist"}
        # fast-forward means the new commit descends from the current head
        if body.get("force") is not True and target[1] != head:
            return 422, {"message": "Update is not a fast forward"}
        self.refs[branch] = body["sha"]
        return 200, {"ref": f"refs/heads/{branch}"}

    def _write_tree(self, body: dict) -> tuple[int, dict]:
        entries = dict(self.trees.get(body.get("base_tree", ""), {}))
        for entry in body["tree"]:
            if entry["sha"] not in self.blobs:
                # the real API refuses a tree naming a blob that was never written
                return 422, {"message": f"blob {entry['sha']} does not exist"}
            entries[entry["path"]] = entry["sha"]
        tree = self._next("t")
        self.trees[tree] = entries
        return 201, {"sha": tree}

    def _write_commit(self, body: dict) -> tuple[int, dict]:
        if body["tree"] not in self.trees:
            return 422, {"message": "tree does not exist"}
        commit = self._next("c")
        parents = body.get("parents") or []
        self.commits[commit] = (body["tree"], parents[0] if parents else None)
        return 201, {"sha": commit}


async def serve(fake: FakeGitHub):
    """Start the fake on a real socket, because the client speaks HTTP."""
    from aiohttp import web

    async def handler(request):
        status, payload = await fake.handle(request)
        return web.json_response(payload, status=status)

    app = web.Application()
    app.router.add_route("*", "/{tail:.*}", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    return runner, f"http://127.0.0.1:{runner.addresses[0][1]}"


def buffer(root: Path, rows: list[dict], users: list[dict] | None = None) -> None:
    for entry in rows:
        append_row(root / TRANSCRIPT, message_row(entry))
    for user in users or []:
        append_row(root / USER_MAP, user)


async def push_once(archive: GitHubArchive) -> dict[str, list[str]]:
    batch = archive.stage_batch()
    if batch is None:
        return {}
    published = await archive.publish(batch)
    missing = await archive.verify(published)
    assert not missing, missing
    archive.clear_published(published)
    return published


async def stage_and_push() -> list[str]:
    """The whole cycle, offline: stage, push, verify, clear.

    Regression: the push used to merge locally, which meant downloading the whole
    remote transcript to dedupe against it. That cost grew with the archive until
    it outlasted any request timeout, and the push loop died for good. The
    assertions below pin the property that fixed it -- the client must never ask
    for a blob.
    """
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    fake = FakeGitHub()
    runner, api_root = await serve(fake)
    import pusher

    original = pusher.API_ROOT
    pusher.API_ROOT = api_root
    try:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "HOME"
            archive = GitHubArchive(data_root=root, owner="o", repo="r", token="t")
            try:
                check(
                    "an empty repo has no commits",
                    await archive._remote_state() == (None, None, {}),
                )

                buffer(
                    root,
                    [record(1, "hello", [IMAGE]), record(2, "a video", [VIDEO])],
                    [{"username": "user1", "user_id": "1001"}],
                )
                published = await push_once(archive)
                check(
                    "a batch is pushed",
                    set(published)
                    == {
                        f"{INBOX_DIR}/{next(iter(published)).split('/')[1]}/{TRANSCRIPT}",
                        f"{INBOX_DIR}/{next(iter(published)).split('/')[1]}/{USER_MAP}",
                    },
                )
                batch_id = next(iter(published)).split("/")[1]
                check("the batch id names its newest row", batch_id.startswith("20261001T1502"))

                paths = fake.paths()
                check(
                    "the transcript landed in the inbox",
                    f"{INBOX_DIR}/{batch_id}/{TRANSCRIPT}" in paths,
                )
                check(
                    "the client never read a blob",
                    not any("git/blobs/" in hit for hit in fake.hits),
                )
                check(
                    "attachments survive as URLs",
                    b"pic.png" in fake.blob_text(f"{INBOX_DIR}/{batch_id}/{TRANSCRIPT}"),
                )
                check(
                    "the video URL survives too",
                    b"clip.mp4" in fake.blob_text(f"{INBOX_DIR}/{batch_id}/{TRANSCRIPT}"),
                )
                check("the local buffer is cleared", read_rows(root / TRANSCRIPT) == [])
                check("the user map is cleared", read_rows(root / USER_MAP) == [])
                check("the local batch is gone", not (root / INBOX_DIR / batch_id).exists())
                check("a clear buffer stages nothing", archive.stage_batch() is None)

                # the muncher has not run, so the archive has no transcripts yet
                check(
                    "the bot writes no transcripts",
                    all(p == ".archive" or p.startswith(f"{INBOX_DIR}/") for p in paths),
                )

                # a lost reply: the rows are still buffered, so the same content is
                # staged under the same name, and the branch already has it
                fake.hits.clear()
                buffer(
                    root,
                    [record(1, "hello", [IMAGE]), record(2, "a video", [VIDEO])],
                    [{"username": "user1", "user_id": "1001"}],
                )
                again = await push_once(archive)
                check(
                    "a re-staged batch keeps its name", next(iter(again)).split("/")[1] == batch_id
                )
                check("the second push commits no new path", len(fake.paths()) == 3)
                check("the buffer is cleared anyway", read_rows(root / TRANSCRIPT) == [])

                # several channels in one cycle, one commit
                before = len(fake.paths())
                append_row(root / "OtherServer/chat.csv", message_row(record(4, "elsewhere")))
                append_row(root / TRANSCRIPT, message_row(record(5, "and back")))
                published = await push_once(archive)
                check("one batch can span channels", len(published) == 2)
                check("one batch is one commit", len({p.split("/")[1] for p in published}) == 1)
                check("both files landed", len(fake.paths()) == before + 2)
            finally:
                await archive.close()
    finally:
        pusher.API_ROOT = original
        await runner.cleanup()
    return failures


async def strict_errors() -> list[str]:
    """A 404 must raise, never be swallowed into a silent success.

    Regression: _api used to default to ``allow_missing=(404,)``, so a ref update
    that 404'd returned ``None`` and the caller reported a successful publish
    while the branch had not moved.
    """
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    import pusher

    fake = FakeGitHub()
    runner, api_root = await serve(fake)
    original = pusher.API_ROOT
    pusher.API_ROOT = api_root
    try:
        archive = GitHubArchive(owner="o", repo="r", token="t")
        try:
            for label, coro in (
                ("ref update 404 raises", archive._write_ref("a" * 40, "b" * 40)),
                (
                    "blob write on a missing repo raises",
                    archive._api(
                        "POST",
                        archive._repo_path("/git/blobs"),
                        {"content": "", "encoding": "base64"},
                    ),
                ),
                (
                    "tree write 422 raises",
                    archive._api(
                        "POST",
                        archive._repo_path("/git/trees"),
                        {"tree": [{"path": "x", "sha": "dead", "mode": "100644", "type": "blob"}]},
                    ),
                ),
                (
                    "read of a missing commit 404 raises",
                    archive._api("GET", archive._repo_path(f"/git/commits/{'c' * 40}")),
                ),
            ):
                try:
                    await coro
                    check(label, False)
                except ArchiveError:
                    check(label, True)

            check(
                "a missing branch reads as absent",
                await archive._api(
                    "GET", archive._repo_path("/git/ref/heads/nope"), allow_missing=(404,)
                )
                is None,
            )
        finally:
            await archive.close()
    finally:
        pusher.API_ROOT = original
        await runner.cleanup()
    return failures


async def slow_push() -> list[str]:
    """A flaky GitHub must not cost the cycle, or the buffer.

    Regression: one aggregate request timeout applied to calls of every size meant
    a slow write killed a cycle outright, with nothing in the log saying which
    write had failed. Timeouts and 5xx are now retried, and only a real failure
    leaves the buffer intact.
    """
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    import pusher

    class Flaky(FakeGitHub):
        """Fails the first ref update with a 503, then behaves."""

        def __init__(self) -> None:
            super().__init__()
            self.ref_failures = 1

        def _move_ref(self, branch, body):
            if self.ref_failures:
                self.ref_failures -= 1
                return 503, {"message": "Service unavailable"}
            return super()._move_ref(branch, body)

    fake = Flaky()
    runner, api_root = await serve(fake)
    original = (
        pusher.API_ROOT,
        pusher.API_ATTEMPTS,
        pusher.RETRY_BACKOFF,
        pusher.RETRY_BACKOFF_MAX,
    )
    pusher.API_ROOT = api_root
    pusher.API_ATTEMPTS = 3
    pusher.RETRY_BACKOFF = 0.0
    pusher.RETRY_BACKOFF_MAX = 0.0
    try:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "HOME"
            archive = GitHubArchive(data_root=root, owner="o", repo="r", token="t")
            try:
                buffer(root, [record(1, "flaky")])
                published = await push_once(archive)
                check("a 503 on the ref update is retried", len(published) == 1)
                check("the batch landed despite the flake", len(fake.paths()) == 2)
                check("the buffer was cleared", read_rows(root / TRANSCRIPT) == [])
                check("the retry really happened", fake.ref_failures == 0)
            finally:
                await archive.close()
    finally:
        pusher.API_ROOT, pusher.API_ATTEMPTS, pusher.RETRY_BACKOFF, pusher.RETRY_BACKOFF_MAX = (
            original
        )
        await runner.cleanup()

    # and when GitHub stays broken, the rows survive
    class Dead(FakeGitHub):
        def _move_ref(self, branch, body):
            return 503, {"message": "Service unavailable"}

        def _write_tree(self, body):
            return 503, {"message": "Service unavailable"}

    dead = Dead()
    runner, api_root = await serve(dead)
    pusher.API_ROOT = api_root
    try:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "HOME"
            archive = GitHubArchive(data_root=root, owner="o", repo="r", token="t")
            try:
                buffer(root, [record(1, "unlucky")])
                batch = archive.stage_batch()
                try:
                    await archive.publish(batch)
                    check("a GitHub that stays broken raises", False)
                except ArchiveError as exc:
                    check("a GitHub that stays broken raises", True)
                    check(
                        "the failure names the call and the attempts",
                        "/git/trees" in str(exc) and "3 attempt(s)" in str(exc),
                    )
                check(
                    "nothing landed", not any(p.startswith(f"{INBOX_DIR}/") for p in dead.paths())
                )
                check("the buffer is untouched", len(read_rows(root / TRANSCRIPT)) == 1)
            finally:
                await archive.close()
    finally:
        pusher.API_ROOT, pusher.RETRY_BACKOFF_MAX = original[0], original[3]
        pusher.API_ATTEMPTS, pusher.RETRY_BACKOFF = original[1], original[2]
        await runner.cleanup()
    return failures


async def lost_reply() -> list[str]:
    """A ref update rejected as not-a-fast-forward must not strand the buffer.

    Regression: a retried ref update, after the first one landed but lost its
    reply, comes back 422. Treating that as a failure left the rows archived but
    the buffer uncleared forever, because the next cycle found the batch already
    on the branch and returned before reaching the clear.
    """
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    import pusher

    class Racing(FakeGitHub):
        """Moves the ref, then rejects the client's update as non-fast-forward."""

        def _move_ref(self, branch, body):
            result = super()._move_ref(branch, body)
            if self.refs.get(branch) == body["sha"]:
                return 422, {"message": "Update is not a fast forward"}
            return result

    fake = Racing()
    runner, api_root = await serve(fake)
    original = pusher.API_ROOT
    pusher.API_ROOT = api_root
    try:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "HOME"
            archive = GitHubArchive(data_root=root, owner="o", repo="r", token="t")
            try:
                buffer(root, [record(1, "raced")])
                published = await push_once(archive)
                check("a ref that moved anyway is not an error", len(published) == 1)
                check(
                    "the batch is on the branch",
                    any(p.startswith(f"{INBOX_DIR}/") for p in fake.paths()),
                )
                check("the buffer is cleared, not stranded", read_rows(root / TRANSCRIPT) == [])
            finally:
                await archive.close()
    finally:
        pusher.API_ROOT = original
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

    import logger

    # main calls setup_logging() on import, which would append this run's noise to
    # the tracked LOGS/dihscrapper.log
    original_setup = logger.setup_logging
    logger.setup_logging = lambda *args, **kwargs: Path("discarded")
    try:
        import main as bot_main
    finally:
        logger.setup_logging = original_setup

    class Boom(RuntimeError):
        pass

    class FakeArchive:
        def __init__(self) -> None:
            self.calls: list[str] = []
            self.cleared: list[dict] = []
            self.attempts = 0

        def stage_batch(self):
            self.calls.append("stage")
            return "batch" if self.attempts >= 0 else None

        async def publish(self, batch=None, dry_run=False):
            self.calls.append("publish")
            self.attempts += 1
            if self.attempts == 1:
                raise Boom("network went away")
            return {TRANSCRIPT: ["9"]}

        async def verify(self, published):
            self.calls.append("verify")
            return []

        def clear_published(self, published):
            self.calls.append("clear")
            self.cleared.append(published)

    fake = FakeArchive()
    original = bot_main.archive
    bot_main.archive = fake
    try:
        await bot_main.push_once()
        check("a failing publish is contained", fake.calls == ["stage", "publish"])
        check("nothing is cleared when publish fails", fake.cleared == [])

        fake.calls.clear()
        await bot_main.push_once()
        check(
            "a healthy cycle stages, publishes, verifies, then clears",
            fake.calls == ["stage", "publish", "verify", "clear"],
        )
        check(
            "the cleared set is exactly what was published", fake.cleared == [{TRANSCRIPT: ["9"]}]
        )

        class ClearBoom(FakeArchive):
            def clear_published(self, published):
                raise Boom("disk full")

        bot_main.archive = ClearBoom()
        await bot_main.push_once()
        check("an error while clearing is contained", True)
    finally:
        bot_main.archive = original

    return failures


async def live() -> list[str]:
    """The same cycle against a real repository, if one is reachable."""
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
        "POST",
        "/user/repos",
        {"name": name, "private": True, "auto_init": False},
    )
    await probe.close()

    try:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "HOME"
            archive = GitHubArchive(data_root=root, owner=owner, repo=name, branch="main")
            try:
                buffer(
                    root,
                    [record(1, "hello", [IMAGE]), record(2, "a video", [VIDEO])],
                    [{"username": "user1", "user_id": "1001"}],
                )
                published = await push_once(archive)
                check("a batch is pushed", len(published) == 2)
                _, _, paths = await archive._remote_state()
                check("the branch shows the batch", len(paths) == 2)
                check(
                    "the bot wrote no transcript", all(p.startswith(f"{INBOX_DIR}/") for p in paths)
                )
                check("the buffer is cleared", read_rows(root / TRANSCRIPT) == [])

                head, _, _ = await archive._remote_state()
                append_row(root / TRANSCRIPT, message_row(record(3, "later")))
                published = await push_once(archive)
                check("a later batch is pushed", len(published) == 1)
                new_head, _, new_paths = await archive._remote_state()
                check("a second commit was made", new_head != head)
                check("the first batch was not rewritten", set(paths) < set(new_paths))
                check("verify passes again", await archive.verify(published) == [])
            finally:
                await archive.close()
    finally:
        cleanup = GitHubArchive(owner=owner, repo=name)
        try:
            await cleanup._api("DELETE", f"/repos/{owner}/{name}")
            print("cleaned up throwaway repository")
        finally:
            await cleanup.close()

    return failures


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live", action="store_true", help="also drive a throwaway real GitHub repository"
    )
    args = parser.parse_args()

    failures = await strict_errors()
    print()
    failures += await stage_and_push()
    print()
    failures += await slow_push()
    print()
    failures += await lost_reply()
    print()
    failures += await resilient_cycle()
    if args.live:
        print()
        failures += await live()
    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
