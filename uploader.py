"""Append the local buffer to the private GitHub data repository.

Publishing is strictly append-only: rows are merged into the remote copy keyed
on message id, one commit is made, and no path is ever deleted. An empty local
buffer therefore cannot damage the archive.

Each commit goes through the git data API -- blobs, then a tree layered on the
current remote tree, then a commit, then a ref update. Unchanged files are
skipped, so a steady-state sync costs two calls regardless of archive size.

Usable as a library from ``main.py`` or standalone::

    python uploader.py            # append, verify, then clear, and exit
    python uploader.py --dry-run  # report what would be appended
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

from archive_format import (
    MESSAGE_FIELDS,
    MESSAGE_KEY,
    USER_FIELDS,
    USER_KEY,
    dump_csv,
    load_csv,
    read_rows,
    rewrite,
)

load_dotenv()

logger = logging.getLogger("DihScrapper.upload")

GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "myrachane")
GITHUB_DATA_REPO = os.environ.get("GITHUB_DATA_REPO", "ScrapedDih")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))
API_ROOT = "https://api.github.com"

BOOTSTRAP_FILE = ".archive"
BOOTSTRAP_BODY = b"DihScrapper live archive. Managed automatically; do not edit.\n"
FILE_MODE = "100644"
USER_MAP_FILE = "user_map.csv"


@dataclass(frozen=True)
class CsvSpec:
    fields: list[str]
    key: str


_MESSAGE_SPEC = CsvSpec(MESSAGE_FIELDS, MESSAGE_KEY)
_USER_SPEC = CsvSpec(USER_FIELDS, USER_KEY)


def _sort_key(value: str) -> tuple[int, int, str]:
    """Sort snowflakes and user ids numerically, tolerating non-numeric input."""
    try:
        return (0, int(value), "")
    except (TypeError, ValueError):
        return (1, 0, str(value))


class ArchiveError(RuntimeError):
    pass


REQUEST_TIMEOUT = 30.0


def git_blob_sha(payload: bytes) -> str:
    return hashlib.sha1(b"blob " + str(len(payload)).encode() + b"\0" + payload).hexdigest()


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
            # aiohttp defaults to a 300s total timeout, so one hung request would
            # stall a cycle for five minutes with nothing logged. The next tick
            # retries anyway, so failing fast is the useful behaviour.
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
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
        """Call the GitHub API.

        ``allow_missing`` defaults to nothing so a 404 raises: a swallowed error
        returns ``None`` and lets a caller believe a write landed when it did
        not. The few sites that expect a missing object pass it explicitly.
        """
        if not self.configured:
            raise ArchiveError("GitHub archive is not configured")
        session = await self._ensure_session()
        body = json.dumps(payload).encode() if payload is not None else None
        async with session.request(method, f"{API_ROOT}{path}", data=body) as response:
            text = await response.text()
            if response.status in allow_missing:
                return None
            if response.status >= 400:
                raise ArchiveError(f"{method} {path} failed ({response.status}): {text[:300]}")
            return json.loads(text) if text else None

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
        repo = await self._api("GET", self._repo_path(""), allow_missing=())
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
                allow_missing=(),
            )
            seed = result["commit"]["sha"]
        try:
            await self._api(
                "POST",
                self._repo_path("/git/refs"),
                {"ref": f"refs/heads/{self.branch}", "sha": seed},
                allow_missing=(),
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

    def _local_payloads(self) -> dict[str, bytes]:
        payloads: dict[str, bytes] = {}
        for path in sorted(self.data_root.rglob("*")):
            if not path.is_file():
                continue
            relative = path.relative_to(self.data_root).as_posix()
            if relative.startswith(".") or path.name.startswith("."):
                continue
            payloads[relative] = path.read_bytes()
        return payloads

    def pending_payloads(self) -> dict[str, bytes]:
        """Snapshot the buffers; callers lock around this and clear_published."""
        return self._local_payloads()

    @staticmethod
    def _spec_for(path: str):
        if not path.endswith(".csv"):
            return None
        if path.rsplit("/", 1)[-1] == USER_MAP_FILE:
            return _USER_SPEC
        return _MESSAGE_SPEC

    async def _read_remote_rows(
        self, path: str, blob_sha: str | None, fields: list[str]
    ) -> list[dict[str, str]]:
        if not blob_sha:
            return []
        blob = await self._api("GET", self._repo_path(f"/git/blobs/{blob_sha}"))
        return load_csv(base64.b64decode(blob["content"]), fields)

    async def publish(
        self,
        dry_run: bool = False,
        payloads: dict[str, bytes] | None = None,
    ) -> dict[str, list[str]]:
        """Append buffered rows, returning the keys committed.

        ``payloads`` lets the caller pass a snapshot taken under its own lock, so
        this touches no local files and no lock is held across the round trips.
        """
        if not self.configured:
            logger.info("GitHub archive not configured; skipping publish")
            return {}

        head_sha, tree_sha, remote_paths = await self._remote_state()
        if payloads is None:
            payloads = self._local_payloads()

        entries: list[dict] = []
        blobs: dict[str, bytes] = {}
        published: dict[str, list[str]] = {}
        for path, payload in payloads.items():
            spec = self._spec_for(path)
            if spec is None:
                continue
            local_rows = load_csv(payload, spec.fields)
            if not local_rows:
                continue

            remote_rows = await self._read_remote_rows(path, remote_paths.get(path), spec.fields)
            merged = {row[spec.key]: row for row in remote_rows}
            fresh: list[dict[str, str]] = []
            for row in local_rows:
                key = row[spec.key]
                if key in merged:
                    continue
                merged[key] = row
                fresh.append(row)
            if not fresh:
                continue

            combined = sorted(merged.values(), key=lambda r: _sort_key(r[spec.key]))
            content = dump_csv(spec.fields, combined)
            blobs[path] = content
            entries.append(
                {
                    "path": path,
                    "mode": FILE_MODE,
                    "type": "blob",
                    "sha": git_blob_sha(content),
                }
            )
            published[path] = [row[spec.key] for row in fresh]

        if not entries:
            logger.info("Nothing new to append to %s/%s", self.owner, self.repo)
            return {}

        if dry_run:
            for path, keys in published.items():
                logger.info("[dry-run] append %d row(s) to %s", len(keys), path)
            return published

        if head_sha is None:
            await self._bootstrap()
            head_sha, tree_sha, _ = await self._remote_state()

        for path, content in blobs.items():
            await self._api(
                "POST",
                self._repo_path("/git/blobs"),
                {
                    "content": base64.b64encode(content).decode(),
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
                "message": self._commit_message(published),
                "tree": new_tree["sha"],
                "parents": [head_sha] if head_sha else [],
            },
        )
        await self._write_ref(commit["sha"], head_sha)
        logger.info(
            "Appended %d row(s) across %d file(s) to %s/%s@%s",
            sum(len(v) for v in published.values()),
            len(published),
            self.owner,
            self.repo,
            self.branch,
        )
        return published

    async def verify(self, published: dict[str, list[str]]) -> list[str]:
        """Return any pushed key not yet readable from the remote.

        An empty return is what gates clearing the local buffer.
        """
        _, _, remote_paths = await self._remote_state()
        missing: list[str] = []
        for path, keys in published.items():
            spec = self._spec_for(path)
            if spec is None:
                continue
            rows = await self._read_remote_rows(path, remote_paths.get(path), spec.fields)
            present = {row[spec.key] for row in rows}
            missing.extend(f"{path}:{key}" for key in keys if key not in present)
        return missing

    def clear_published(self, published: dict[str, list[str]]) -> None:
        """Drop just the confirmed rows; keying rather than truncating is what
        preserves a message that arrived mid-publish.
        """
        for path, keys in published.items():
            spec = self._spec_for(path)
            if spec is None:
                continue
            target = self.data_root / path
            settled = set(keys)
            rewrite(
                target,
                spec.fields,
                [row for row in read_rows(target, spec.fields) if row[spec.key] not in settled],
            )

    @staticmethod
    def _commit_message(published: dict[str, list[str]]) -> str:
        total = sum(len(keys) for keys in published.values())
        return f"Append {total} row(s) across {len(published)} file(s)"

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
        await self._api(
            "PATCH",
            self._repo_path(f"/git/refs/heads/{self.branch}"),
            {"sha": commit_sha, "force": False},
        )


async def _run(args: argparse.Namespace) -> None:
    archive = GitHubArchive()
    try:
        published = await archive.publish(dry_run=args.dry_run)
        total = sum(len(keys) for keys in published.values())
        if args.dry_run:
            logger.info("[dry-run] %d row(s) would be appended", total)
            return
        if not published:
            logger.info("Nothing new to push")
            return
        missing = await archive.verify(published)
        if missing:
            raise ArchiveError(
                f"{len(missing)} row(s) missing from the archive after push, "
                f"local buffer left intact: {missing[:5]}"
            )
        archive.clear_published(published)
        logger.info("Appended and verified %d row(s); local buffer cleared", total)
    except ArchiveError as exc:
        logger.error("Publish failed: %s", exc)
        raise SystemExit(1) from exc
    finally:
        await archive.close()


def main() -> None:
    from logger import setup_logging

    setup_logging()
    parser = argparse.ArgumentParser(
        description="Append the local buffer to the archive. Never deletes."
    )
    parser.add_argument("--dry-run", action="store_true", help="report without committing")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
