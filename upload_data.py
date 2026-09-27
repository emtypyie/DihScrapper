"""Publish the local archive to the private GitHub data repository.

Only JSON is ever archived. Media is not downloaded and not stored anywhere: the
bot records an image attachment as the placeholder ``[potential image]`` in the
transcript, so the local ``HOME`` tree and the remote archive contain nothing but
text.

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

BOOTSTRAP_FILE = ".archive"
BOOTSTRAP_BODY = b"DihScrapper live archive. Managed automatically; do not edit.\n"
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

    async def _bootstrap(self) -> None:
        """Ensure the archive has a commit for the configured branch to build on.

        Two distinct cases. A repository with no commits at all cannot accept
        blobs, so it is seeded with a commit through the contents API -- that
        request must omit ``branch``, because the API answers 404 for a branch
        that does not exist yet rather than creating it. A repository that
        already has commits only needs the configured branch pointed at the
        default branch's head.
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

    async def publish(self, dry_run: bool = False) -> bool:
        """Commit the local JSON tree to the archive repository."""
        if not self.configured:
            logger.info("GitHub archive not configured; skipping publish")
            return False

        head_sha, tree_sha, remote_paths = await self._remote_state()
        payloads = self._local_payloads()

        entries: list[dict] = []
        added: list[str] = []
        changed: list[str] = []
        for path, payload in payloads.items():
            blob = git_blob_sha(payload)
            if remote_paths.get(path) == blob:
                continue
            entries.append({"path": path, "mode": FILE_MODE, "type": "blob", "sha": blob})
            (added if path not in remote_paths else changed).append(path)

        removed = [path for path in remote_paths if path not in payloads and not path.startswith(".")]
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

        if head_sha is None:
            await self._bootstrap()
            head_sha, tree_sha, _ = await self._remote_state()

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
            path
            for entry in entries
            if entry.get("sha")
            for path in (entry["path"],)
            if path in payloads
        }
        for path in wanted:
            await self._api(
                "POST",
                self._repo_path("/git/blobs"),
                {
                    "content": base64.b64encode(payloads[path]).decode(),
                    "encoding": "base64",
                },
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
