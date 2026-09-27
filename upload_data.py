"""Publish the local archive to the private GitHub data repository.

Media is never written to disk. Attachments are uploaded straight into the archive
repository as git blobs by :meth:`GitHubArchive.store_blob`, and the path-to-blob
mapping is kept in ``HOME/.media-index.json`` so later commits can reference those
blobs without ever holding the bytes locally. The local ``HOME`` tree therefore
contains JSON only.

Every commit is assembled through the git data API: blobs, then a tree layered on
the current remote tree, then a commit, then a ref update. Files whose git blob hash
already matches the remote are skipped, so a steady-state sync costs two API calls
regardless of archive size.

Usable as a library from ``main.py`` or standalone::

    python upload_data.py            # publish once, then exit
    python upload_data.py --dry-run  # report what would change
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("DihScrapper.upload")

GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "myrachane")
GITHUB_DATA_REPO = os.environ.get("GITHUB_DATA_REPO", "ScrapedDih")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))
API_ROOT = "https://api.github.com"

MEDIA_INDEX = ".media-index.json"
BOOTSTRAP_FILE = ".archive"
BOOTSTRAP_BODY = b"DihScrapper live archive. Managed automatically; do not edit.\n"
EMPTY_REPO = "git repository is empty"
FILE_MODE = "100644"


class ArchiveError(RuntimeError):
    pass


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

    @property
    def index_path(self) -> Path:
        return self.data_root / MEDIA_INDEX

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "DihScrapper",
        }

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            self._session = aiohttp.ClientSession(headers=self._headers())
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
        allow_missing: tuple[int, ...] = (404,),
    ):
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

    def load_media_index(self) -> dict[str, str]:
        try:
            with self.index_path.open(encoding="utf-8") as handle:
                data = json.load(handle)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return {}
        return data if isinstance(data, dict) else {}

    def save_media_index(self, index: dict[str, str]) -> None:
        self.data_root.mkdir(parents=True, exist_ok=True)
        staging = self.index_path.with_suffix(".tmp")
        with staging.open("w", encoding="utf-8") as handle:
            json.dump(index, handle, indent=1, sort_keys=True)
        staging.replace(self.index_path)

    async def store_blob(self, payload: bytes) -> str:
        """Upload bytes into the archive repository and return the git blob hash.

        GitHub rejects blob creation against a repository with no commits, so an
        empty archive is bootstrapped with an initial commit first.
        """
        encoded = base64.b64encode(payload).decode()
        try:
            return await self._create_blob(encoded)
        except ArchiveError as exc:
            if EMPTY_REPO not in str(exc).lower():
                raise
        await self._bootstrap()
        return await self._create_blob(encoded)

    async def _create_blob(self, encoded: str) -> str:
        result = await self._api(
            "POST",
            self._repo_path("/git/blobs"),
            {"content": encoded, "encoding": "base64"},
        )
        return result["sha"]

    async def _bootstrap(self) -> None:
        logger.info("Archive repository is empty; creating its initial commit")
        await self._api(
            "PUT",
            self._repo_path(f"/contents/{BOOTSTRAP_FILE}"),
            {
                "message": "Initialise archive",
                "content": base64.b64encode(BOOTSTRAP_BODY).decode(),
                "branch": self.branch,
            },
        )

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

    async def publish(self, dry_run: bool = False) -> bool:
        """Commit local JSON plus indexed media blobs to the archive repository."""
        if not self.configured:
            logger.info("GitHub archive not configured; skipping publish")
            return False

        head_sha, tree_sha, remote_paths = await self._remote_state()
        payloads = self._local_payloads()
        media = self.load_media_index()

        entries: list[dict] = []
        added: list[str] = []
        changed: list[str] = []
        for path, payload in payloads.items():
            blob = git_blob_sha(payload)
            if remote_paths.get(path) == blob:
                continue
            entries.append({"path": path, "mode": FILE_MODE, "type": "blob", "sha": blob})
            (added if path not in remote_paths else changed).append(path)

        for path, blob in media.items():
            if path in payloads or remote_paths.get(path) == blob:
                continue
            entries.append({"path": path, "mode": FILE_MODE, "type": "blob", "sha": blob})
            (added if path not in remote_paths else changed).append(path)

        keep = set(payloads) | set(media)
        removed = [
            path
            for path in remote_paths
            if path not in keep and not path.startswith(".")
        ]
        for path in removed:
            entries.append({"path": path, "mode": FILE_MODE, "type": "blob", "sha": None})

        if not entries:
            logger.info("Archive already up to date (%d file(s) tracked)", len(remote_paths))
            return False

        if dry_run:
            logger.info(
                "[dry-run] +%d ~%d -%d", len(added), len(changed), len(removed)
            )
            for path in added[:20]:
                logger.info("[dry-run] add    %s", path)
            for path in changed[:20]:
                logger.info("[dry-run] update %s", path)
            for path in removed[:20]:
                logger.info("[dry-run] remove %s", path)
            return True

        await self._create_blobs(payloads, entries)
        new_tree = await self._api(
            "POST",
            self._repo_path("/git/trees"),
            {
                "base_tree": tree_sha,
                "tree": entries,
            } if tree_sha else {"tree": entries},
        )
        parents = [head_sha] if head_sha else []
        commit = await self._api(
            "POST",
            self._repo_path("/git/commits"),
            {
                "message": f"Archive sync: +{len(added)} ~{len(changed)} -{len(removed)}",
                "tree": new_tree["sha"],
                "parents": parents,
            },
        )
        await self._write_ref(commit["sha"], head_sha)
        logger.info(
            "Published archive to %s/%s@%s (+%d ~%d -%d)",
            self.owner, self.repo, self.branch, len(added), len(changed), len(removed),
        )
        return True

    async def _create_blobs(self, payloads: dict[str, bytes], entries: list[dict]) -> None:
        wanted = {
            path: payload
            for path, payload in payloads.items()
            if any(e["path"] == path and e.get("sha") for e in entries)
        }
        for path, payload in wanted.items():
            await self.store_blob(payload)

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
        await archive.publish(dry_run=args.dry_run)
    except ArchiveError as exc:
        logger.error("Publish failed: %s", exc)
        raise SystemExit(1) from exc
    finally:
        await archive.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description="Publish the archive to GitHub.")
    parser.add_argument("--dry-run", action="store_true", help="report without committing")
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
