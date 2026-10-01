"""Push buffered messages to the archive repo as small new files under inbox/.

The bot buffers rows in DATA_ROOT. Each cycle this stages them into a batch --
one file per channel, named after the batch -- and commits those files to the
archive branch. It never reads an archive file and never rewrites one, so a
push costs what just arrived rather than the size of the archive.

Merging the inbox into the archive CSVs is ``muncher.py``'s job, run by
.github/workflows/muncher.yml. Rewriting a whole transcript grows more expensive
every week, and that belongs in a CI job with no request timeout rather than in
the bot's five minute loop.

Also usable on its own::

    python pusher.py            # stage, push, verify, then clear, and exit
    python pusher.py --dry-run  # report what would be pushed
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

from formatter import dump_csv, fields_for, key_for, read_rows, rewrite

load_dotenv()

logger = logging.getLogger("DihScrapper.push")

GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "myrachane")
GITHUB_DATA_REPO = os.environ.get("GITHUB_DATA_REPO", "ScrapedDih")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
API_ROOT = "https://api.github.com"

# the staging area on both sides: local batches, remote batches, same paths
INBOX_DIR = "inbox"
# one batch is a few KiB, so this many waiting means the muncher is not running
INBOX_BACKUP_WARN = int(os.environ.get("GITHUB_INBOX_WARN", "20"))

BOOTSTRAP_FILE = ".archive"
BOOTSTRAP_BODY = b"DihScrapper live archive. Managed automatically; do not edit.\n"
FILE_MODE = "100644"

# A call is bounded by connect and read timeouts, never by one aggregate
# budget. A stalled socket still trips sock_read, and API_ATTEMPTS bounds a
# server that accepts the connection and then stalls. There is no unbounded
# download left to punish here -- that was the whole point of the inbox.
CONNECT_TIMEOUT = float(os.environ.get("GITHUB_CONNECT_TIMEOUT", "20"))
READ_TIMEOUT = float(os.environ.get("GITHUB_READ_TIMEOUT", "120"))

# Retried, because these are GitHub's weather rather than the request's fault.
# Everything else -- a 404 on a missing ref, a 422 on a bad tree -- is
# deterministic, so retrying only burns the cycle.
API_ATTEMPTS = int(os.environ.get("GITHUB_API_ATTEMPTS", "3"))
RETRY_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
RETRY_BACKOFF = float(os.environ.get("GITHUB_RETRY_BACKOFF", "1"))
RETRY_BACKOFF_MAX = float(os.environ.get("GITHUB_RETRY_BACKOFF_MAX", "15"))
RETRY_AFTER_MAX = 60.0


def _retry_delay(headers: Mapping[str, str], attempt: int) -> float:
    """Back off exponentially, but let GitHub name the wait when it throttles.

    A 403 with Retry-After is GitHub's secondary rate limit and is not in
    RETRY_STATUSES, so the header itself is what makes a response retryable.
    """
    raw = headers.get("Retry-After")
    if raw:
        try:
            return min(float(raw), RETRY_AFTER_MAX)
        except ValueError:
            pass
    return min(RETRY_BACKOFF * 2 ** (attempt - 1), RETRY_BACKOFF_MAX)


def git_blob_sha(payload: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(payload)).encode() + b"\0" + payload).hexdigest()


class ArchiveError(RuntimeError):
    pass


@dataclass(frozen=True)
class Batch:
    """One cycle's buffered rows, addressed as new files in the inbox."""

    id: str
    files: dict[str, bytes]
    keys: dict[str, list[str]]


def _batch_stamp(rows_by_path: dict[str, list[dict[str, str]]]) -> str:
    """Name a batch after the newest row it carries.

    Taken from the rows rather than the clock so that re-staging an unchanged
    buffer lands on the same path. No colons: DATA_ROOT is a plain local path
    for anyone running this outside the container.
    """
    newest = max(
        (row.get("timestamp") or "" for rows in rows_by_path.values() for row in rows),
        default="",
    )
    try:
        moment = datetime.fromisoformat(newest).astimezone(timezone.utc)
    except ValueError:
        # nothing parseable to name the batch after. Falling back to the clock
        # costs a duplicate batch if we ever stage twice, which the muncher
        # dedupes; refusing to publish would cost the rows.
        moment = datetime.now(timezone.utc)
    return moment.strftime("%Y%m%dT%H%M%SZ")


class GitHubArchive:
    def __init__(
        self,
        data_root: Path | None = None,
        owner: str = GITHUB_OWNER,
        repo: str = GITHUB_DATA_REPO,
        token: str = GITHUB_TOKEN,
        branch: str = GITHUB_BRANCH,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        self.data_root = Path(data_root or DATA_ROOT)
        self.owner = owner
        self.repo = repo
        self.token = token
        self.branch = branch
        self._session = session
        self._owns_session = session is None

    @property
    def configured(self) -> bool:
        return bool(self.owner and self.repo and self.token)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "DihScrapper",
        }

    @staticmethod
    def blob_sha(payload: bytes) -> str:
        return git_blob_sha(payload)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            # total=None on purpose: see CONNECT_TIMEOUT above. The old total
            # budget is what turned one slow large read into a dead push loop.
            timeout = aiohttp.ClientTimeout(
                total=None, connect=CONNECT_TIMEOUT, sock_read=READ_TIMEOUT
            )
            self._session = aiohttp.ClientSession(headers=self._headers(), timeout=timeout)
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def _api(
        self,
        method: str,
        path: str,
        payload: object = None,
        allow_missing: tuple[int, ...] = (),
    ):
        """Call the GitHub API with bounded retries.

        ``allow_missing`` defaults to nothing so a 404 raises: a swallowed error
        returns ``None`` and lets a caller believe a write landed when it did
        not. The few sites that expect a missing object pass it explicitly.

        A timeout or a 5xx is retried with backoff instead of ending the cycle,
        because the next tick is five minutes away and the buffer is only safe,
        not useful, while it sits there. The final failure names the call and
        every attempt, which is the difference between "TimeoutError" in the log
        and knowing which write gave up.
        """
        if not self.configured:
            raise ArchiveError("GitHub archive is not configured")
        session = await self._ensure_session()
        body = json.dumps(payload).encode() if payload is not None else None
        url = f"{API_ROOT}{path}"
        last = "no attempt made"

        for attempt in range(1, API_ATTEMPTS + 1):
            try:
                async with session.request(method, url, data=body) as response:
                    status = response.status
                    data = await response.read()
                    if status in allow_missing:
                        return None
                    if status < 400:
                        return json.loads(data) if data else None
                    last = f"HTTP {status}: {data[:200].decode('utf-8', 'replace').strip()}"
                    delay = _retry_delay(response.headers, attempt)
                    retryable = status in RETRY_STATUSES or bool(
                        response.headers.get("Retry-After")
                    )
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                # aiohttp surfaces a stalled read as a TimeoutError out of the
                # stream, which used to escape raw and abort the whole cycle
                last = f"{type(exc).__name__}: {exc}"
                delay = _retry_delay({}, attempt)
                retryable = True

            if not retryable or attempt == API_ATTEMPTS:
                break
            logger.warning(
                "%s %s failed (%s); retrying in %.1fs [attempt %d of %d]",
                method,
                path,
                last,
                delay,
                attempt,
                API_ATTEMPTS,
            )
            await asyncio.sleep(delay)

        raise ArchiveError(f"{method} {path} failed after {API_ATTEMPTS} attempt(s): {last}")

    def _repo_path(self, suffix: str) -> str:
        return f"/repos/{self.owner}/{self.repo}{suffix}"

    async def _bootstrap(self) -> None:
        """Ensure the configured branch has a commit to build on.

        With no commits at all the repo cannot accept blobs, so it is seeded via
        the contents API -- which must omit ``branch``, since the API 404s for a
        branch that does not exist rather than creating it. Otherwise the branch
        only needs pointing at the default branch's head.
        """
        logger.info("Archive branch %s is absent; seeding it from the default branch", self.branch)
        repo = await self._api("GET", self._repo_path(""))
        ref = await self._api(
            "GET",
            self._repo_path(f"/git/ref/heads/{repo['default_branch']}"),
            allow_missing=(404, 409),
        )
        if ref is not None:
            seed = ref["object"]["sha"]
        else:
            result = await self._api(
                "PUT",
                self._repo_path(f"/contents/{BOOTSTRAP_FILE}"),
                {
                    "message": "Initialise archive",
                    "content": base64.b64encode(BOOTSTRAP_BODY).decode(),
                },
            )
            seed = result["commit"]["sha"]
        try:
            await self._api(
                "POST",
                self._repo_path("/git/refs"),
                {"ref": f"refs/heads/{self.branch}", "sha": seed},
            )
        except ArchiveError as exc:
            if "already exists" not in str(exc).lower():
                raise

    async def _remote_state(self) -> tuple[str | None, str | None, dict[str, str]]:
        ref = await self._api(
            "GET",
            self._repo_path(f"/git/ref/heads/{self.branch}"),
            allow_missing=(404, 409),
        )
        if ref is None:
            logger.info("Archive branch %s does not exist yet", self.branch)
            return None, None, {}
        commit_sha = ref["object"]["sha"]
        commit = await self._api("GET", self._repo_path(f"/git/commits/{commit_sha}"))
        tree_sha = commit["tree"]["sha"]
        paths = await self._collect_paths(tree_sha)
        return commit_sha, tree_sha, paths

    async def _collect_paths(self, tree_sha: str) -> dict[str, str]:
        """Flatten the branch's tree to path -> blob sha.

        Metadata only: this is how a push knows whether its batch is already
        there, and how a backlog in the inbox becomes visible at all.
        """
        collected: dict[str, str] = {}
        pending = [("", tree_sha)]
        while pending:
            prefix, sha = pending.pop()
            data = await self._api("GET", self._repo_path(f"/git/trees/{sha}"))
            for entry in data.get("tree", []):
                path = f"{prefix}{entry['path']}"
                if entry["type"] == "tree":
                    pending.append((f"{path}/", entry["sha"]))
                elif entry["type"] == "blob":
                    collected[path] = entry["sha"]
        return collected

    def stage_batch(self) -> Batch | None:
        """Copy the local buffer into a new inbox batch, or None if it is empty.

        Callers hold the buffer lock: this rewrites files that on_message is
        appending to. Rows are copied, not moved, so a push that never lands
        leaves the buffer exactly as it was.
        """
        rows_by_path: dict[str, list[dict[str, str]]] = {}
        for path in sorted(self.data_root.rglob("*")):
            if not path.is_file() or path.suffix != ".csv":
                continue
            relative = path.relative_to(self.data_root).as_posix()
            # our own batches, and rewrite()'s *.tmp staging files, are not
            # buffered rows -- *.tmp is already excluded by the suffix check
            if relative.split("/", 1)[0] == INBOX_DIR or path.name.startswith("."):
                continue
            rows = read_rows(path)
            if rows:
                rows_by_path[relative] = rows
        if not rows_by_path:
            return None

        digest = hashlib.sha1()
        for relative, rows in sorted(rows_by_path.items()):
            digest.update(dump_csv(fields_for(relative), rows))
        batch_id = f"{_batch_stamp(rows_by_path)}-{digest.hexdigest()[:8]}"

        files: dict[str, bytes] = {}
        keys: dict[str, list[str]] = {}
        for relative, rows in sorted(rows_by_path.items()):
            payload = dump_csv(fields_for(relative), rows)
            inbox_path = f"{INBOX_DIR}/{batch_id}/{relative}"
            target = self.data_root / inbox_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
            files[inbox_path] = payload
            keys[inbox_path] = [row[key_for(relative)] for row in rows]

        logger.info(
            "Staged batch %s: %d row(s) across %d file(s)",
            batch_id,
            sum(len(ids) for ids in keys.values()),
            len(files),
        )
        return Batch(id=batch_id, files=files, keys=keys)

    async def publish(self, batch: Batch, dry_run: bool = False) -> dict[str, list[str]]:
        """Append a batch's files to the archive branch.

        Every path is new, so this is the same handful of small calls whatever
        the archive weighs. The staged keys come back either way, including when
        every path was already there, because a batch that is in the archive is
        a batch that has been published and the buffer can go.
        """
        if not self.configured:
            logger.info("GitHub archive not configured; skipping publish")
            return {}

        head_sha, tree_sha, remote_paths = await self._remote_state()
        self._warn_on_backlog(remote_paths)

        entries: list[dict] = []
        blobs: dict[str, bytes] = {}
        for path, payload in batch.files.items():
            if path in remote_paths:
                continue  # this batch already landed, on a reply we never saw
            blobs[path] = payload
            entries.append(
                {
                    "path": path,
                    "mode": FILE_MODE,
                    "type": "blob",
                    "sha": git_blob_sha(payload),
                }
            )

        if dry_run:
            for path in batch.files:
                logger.info("[dry-run] push %s", path)
            return batch.keys
        if not entries:
            logger.info("Batch %s is already in the archive", batch.id)
            return batch.keys

        if head_sha is None:
            await self._bootstrap()
            head_sha, tree_sha, _ = await self._remote_state()

        for payload in blobs.values():
            await self._api(
                "POST",
                self._repo_path("/git/blobs"),
                {
                    "content": base64.b64encode(payload).decode(),
                    "encoding": "base64",
                },
            )

        new_tree = await self._api(
            "POST",
            self._repo_path("/git/trees"),
            {"base_tree": tree_sha, "tree": entries} if tree_sha else {"tree": entries},
        )
        commit = await self._api(
            "POST",
            self._repo_path("/git/commits"),
            {
                "message": self._commit_message(batch),
                "tree": new_tree["sha"],
                "parents": [head_sha] if head_sha else [],
            },
        )
        await self._write_ref(commit["sha"], head_sha)
        logger.info(
            "Pushed batch %s: %d row(s) across %d file(s) to %s/%s@%s",
            batch.id,
            sum(len(ids) for ids in batch.keys.values()),
            len(entries),
            self.owner,
            self.repo,
            self.branch,
        )
        return batch.keys

    async def verify(self, published: dict[str, list[str]]) -> list[str]:
        """Return any published path the branch does not show yet.

        A tree walk, not a blob read: the bytes were just uploaded, and what
        needs confirming before the buffer is dropped is that the commit landed.
        """
        _, _, remote_paths = await self._remote_state()
        return [path for path in published if path not in remote_paths]

    def clear_published(self, published: dict[str, list[str]]) -> None:
        """Drop the confirmed rows from the buffer and delete the local batch.

        Keyed rather than truncated, which is what preserves a message that
        arrived after the batch was staged.
        """
        for inbox_path, ids in published.items():
            relative = inbox_path.split("/", 2)[-1]
            buffer_path = self.data_root / relative
            if not buffer_path.exists():
                continue
            settled = set(ids)
            rewrite(
                buffer_path,
                [row for row in read_rows(buffer_path) if row[key_for(relative)] not in settled],
            )
        self._drop_batches(published)

    def _drop_batches(self, published: dict[str, list[str]]) -> None:
        inbox = self.data_root / INBOX_DIR
        for inbox_path in published:
            batch_dir = inbox / inbox_path.split("/")[1]
            if batch_dir.is_dir():
                shutil.rmtree(batch_dir, ignore_errors=True)

    def _warn_on_backlog(self, remote_paths: dict[str, str]) -> None:
        waiting = {
            path.split("/")[1]
            for path in remote_paths
            if path.startswith(f"{INBOX_DIR}/") and "/" in path[len(INBOX_DIR) + 1 :]
        }
        if len(waiting) > INBOX_BACKUP_WARN:
            logger.warning(
                "%d batch(es) are sitting in the inbox, so the muncher workflow is not "
                "keeping up: check the Actions tab",
                len(waiting),
            )

    @staticmethod
    def _commit_message(batch: Batch) -> str:
        return (
            f"Push batch {batch.id}: {sum(len(ids) for ids in batch.keys.values())} row(s) "
            f"across {len(batch.files)} file(s)"
        )

    async def _write_ref(self, commit_sha: str, previous: str | None) -> None:
        if previous is None:
            try:
                await self._api(
                    "POST",
                    self._repo_path("/git/refs"),
                    {"ref": f"refs/heads/{self.branch}", "sha": commit_sha},
                )
                return
            except ArchiveError as exc:
                if "already exists" not in str(exc).lower():
                    raise
        try:
            await self._api(
                "PATCH",
                self._repo_path(f"/git/refs/heads/{self.branch}"),
                {"sha": commit_sha, "force": False},
            )
        except ArchiveError:
            # A retried update, after the first one actually landed, comes back
            # as a not-a-fast-forward. The commit is on the branch either way, so
            # confirm that before failing: otherwise the rows are archived but the
            # buffer can never be cleared, because the next cycle finds the batch
            # already there and returns before reaching the clear.
            ref = await self._api("GET", self._repo_path(f"/git/refs/heads/{self.branch}"))
            if ref is None or ref["object"]["sha"] != commit_sha:
                raise
            logger.info("Ref %s already points at %s", self.branch, commit_sha)


async def _run(args: argparse.Namespace) -> None:
    archive = GitHubArchive()
    try:
        batch = archive.stage_batch()
        if batch is None:
            logger.info("Nothing new to push")
            return
        published = await archive.publish(batch, dry_run=args.dry_run)
        total = sum(len(ids) for ids in published.values())
        if args.dry_run:
            logger.info("[dry-run] %d row(s) would be pushed", total)
            return
        if not published:
            return
        missing = await archive.verify(published)
        if missing:
            raise ArchiveError(
                f"{len(missing)} batch file(s) missing from the archive after push, "
                f"local buffer left intact: {missing[:5]}"
            )
        archive.clear_published(published)
        logger.info("Pushed and verified %d row(s); local buffer cleared", total)
    except ArchiveError as exc:
        logger.error("Publish failed: %s", exc)
        raise SystemExit(1) from exc
    finally:
        await archive.close()


def main() -> None:
    from logger import setup_logging

    setup_logging()
    parser = argparse.ArgumentParser(
        description="Push the local buffer to the archive inbox. Never rewrites a file."
    )
    parser.add_argument("--dry-run", action="store_true", help="report without committing")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
