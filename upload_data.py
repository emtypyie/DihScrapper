"""Sync the local HOME data directory to the private ScrapedDih GitHub repository.

Usable as a library from ``main.py`` or as a one-shot CLI::

    python upload_data.py            # one sync then exit
    python upload_data.py --loop     # sync every PUSH_INTERVAL seconds
    python upload_data.py --dry-run  # report what would be pushed
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import stat
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("DihScrapper.upload")

GITHUB_OWNER = os.environ.get("GITHUB_OWNER", "emtypyie")
GITHUB_DATA_REPO = os.environ.get("GITHUB_DATA_REPO", "ScrapedDih")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
GITHUB_BRANCH = os.environ.get("GITHUB_BRANCH", "main")
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "HOME"))
PUSH_INTERVAL = int(os.environ.get("PUSH_INTERVAL", "300"))
SOFT_SIZE_LIMIT = int(os.environ.get("REPO_SOFT_SIZE_LIMIT", str(1024**3)))
COMMIT_NAME = os.environ.get("GIT_COMMIT_NAME", "DihScrapper Bot")
COMMIT_EMAIL = os.environ.get("GIT_COMMIT_EMAIL", "dihscrapper@users.noreply.github.com")

ASKPASS_SH = """#!/bin/sh
case "$1" in
  *sername*) printf '%s' "$GIT_ASKPASS_USERNAME" ;;
  *) printf '%s' "$GIT_ASKPASS_PASSWORD" ;;
esac
"""

ASKPASS_BAT = """@echo off
echo %1 | findstr /i "username" >nul
if %ERRORLEVEL%==0 (echo %GIT_ASKPASS_USERNAME%) else (echo %GIT_ASKPASS_PASSWORD%)
"""

REMOTE_EMPTY_MARKERS = ("couldn't find remote ref", "remote branch does not exist")


class SyncError(RuntimeError):
    pass


class DataRepoSync:
    """Commits the data directory to the remote archive repository."""

    def __init__(
        self,
        data_root: Path | None = None,
        owner: str = GITHUB_OWNER,
        repo: str = GITHUB_DATA_REPO,
        token: str = GITHUB_TOKEN,
        branch: str = GITHUB_BRANCH,
        dry_run: bool = False,
    ) -> None:
        self.data_root = Path(data_root or DATA_ROOT)
        self.owner = owner
        self.repo = repo
        self.token = token
        self.branch = branch
        self.dry_run = dry_run
        self.remote = f"https://github.com/{owner}/{repo}.git"
        self._askpass_dir: Path | None = None

    @property
    def configured(self) -> bool:
        return bool(self.owner and self.repo and self.token)

    def _git_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env["GIT_TERMINAL_PROMPT"] = "0"
        env["GIT_ASKPASS_USERNAME"] = "x-access-token"
        env["GIT_ASKPASS_PASSWORD"] = self.token
        env["LC_ALL"] = "C"
        if self._askpass_dir:
            script = self._askpass_dir / (
                "askpass.bat" if os.name == "nt" else "askpass.sh"
            )
            env["GIT_ASKPASS"] = str(script)
        return env

    async def _git(self, *args: str, check: bool = True) -> tuple[int, str, str]:
        cmd = ["git", "-c", "safe.directory=*", *args]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=self.data_root,
            env=self._git_env(),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        out, err = await proc.communicate()
        stdout, stderr = out.decode("utf-8", "replace"), err.decode("utf-8", "replace")
        if check and proc.returncode != 0:
            raise SyncError(f"git {' '.join(args)} failed: {stderr.strip() or stdout.strip()}")
        return proc.returncode, stdout, stderr

    def _write_askpass(self) -> None:
        self._askpass_dir = Path(tempfile.mkdtemp(prefix="dihscrapper-askpass-"))
        if os.name == "nt":
            script = self._askpass_dir / "askpass.bat"
            script.write_text(ASKPASS_BAT, encoding="utf-8")
        else:
            script = self._askpass_dir / "askpass.sh"
            script.write_text(ASKPASS_SH, encoding="utf-8")
            script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)

    def _cleanup_askpass(self) -> None:
        if self._askpass_dir and self._askpass_dir.exists():
            for child in self._askpass_dir.iterdir():
                child.unlink(missing_ok=True)
            self._askpass_dir.rmdir()
        self._askpass_dir = None

    def data_size(self) -> int:
        return sum(p.stat().st_size for p in self.data_root.rglob("*") if p.is_file())

    async def ensure_repo(self) -> None:
        self.data_root.mkdir(parents=True, exist_ok=True)
        self._write_askpass()
        if not (self.data_root / ".git").exists():
            await self._git("init", "-b", self.branch)
            await self._git("remote", "add", "origin", self.remote)
            logger.info("Initialised data repository for %s", self.remote)
        else:
            code, out, _ = await self._git("remote", "get-url", "origin", check=False)
            if code != 0:
                await self._git("remote", "add", "origin", self.remote)
            elif out.strip() != self.remote:
                await self._git("remote", "set-url", "origin", self.remote)

    async def _integrate_remote(self) -> None:
        code, _, err = await self._git("fetch", "--depth", "1", "origin", self.branch, check=False)
        remote_exists = not (code != 0 and any(m in err for m in REMOTE_EMPTY_MARKERS))

        has_local = (await self._git("rev-parse", "--verify", "HEAD", check=False))[0] == 0

        if not remote_exists:
            if not has_local:
                await self._git("commit", "--allow-empty", "-m", "Initialise archive", check=False)
            return

        if not has_local:
            await self._git("reset", "--hard", "FETCH_HEAD")
            return

        code, _, err = await self._git("rebase", "FETCH_HEAD", check=False)
        if code != 0:
            await self._git("rebase", "--abort", check=False)
            logger.warning("Rebase onto remote failed (%s); resetting to remote state", err.strip())
            await self._git("reset", "--hard", "FETCH_HEAD")

    async def _commit_changes(self) -> bool:
        await self._git("add", "-A", ".")
        code, out, _ = await self._git("diff", "--cached", "--quiet", check=False)
        if code == 0:
            logger.info("No new data to push")
            return False
        _, staged_out, _ = await self._git("diff", "--cached", "--name-only", check=False)
        staged = len([line for line in staged_out.splitlines() if line.strip()])
        message = f"Archive sync: {staged} path(s) updated"
        await self._git(
            "-c", f"user.name={COMMIT_NAME}",
            "-c", f"user.email={COMMIT_EMAIL}",
            "commit", "-m", message,
        )
        logger.info("Committed %d changed path(s)", staged)
        return True

    async def sync(self) -> bool:
        """Push local data to the remote archive. Returns True if a push happened."""
        if not self.configured:
            logger.info("GitHub archive not configured (owner/repo/token); skipping sync")
            return False

        size = self.data_size()
        if size > SOFT_SIZE_LIMIT:
            logger.warning(
                "Archive directory is %.1f MiB, above the %.1f MiB soft limit; "
                "GitHub will reject repositories past its hard cap",
                size / 1024**2, SOFT_SIZE_LIMIT / 1024**2,
            )

        await self.ensure_repo()
        try:
            await self._integrate_remote()
            if self.dry_run:
                code, out, _ = await self._git("status", "--porcelain", check=False)
                changed = [line for line in out.splitlines() if line.strip()]
                logger.info("[dry-run] %d path(s) would be pushed", len(changed))
                return bool(changed)

            if not await self._commit_changes():
                return False

            code, _, err = await self._git("push", "origin", self.branch, check=False)
            if code != 0:
                raise SyncError(f"push failed: {err.strip()}")
            logger.info("Pushed archive data to %s", self.remote)
            return True
        finally:
            self._cleanup_askpass()


async def _loop(interval: int, dry_run: bool) -> None:
    sync = DataRepoSync(dry_run=dry_run)
    while True:
        try:
            await sync.sync()
        except SyncError as exc:
            logger.error("Archive sync failed: %s", exc)
        except Exception:
            logger.exception("Unexpected archive sync failure")
        await asyncio.sleep(interval)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--loop", action="store_true", help="sync continuously")
    parser.add_argument("--dry-run", action="store_true", help="do not commit or push")
    parser.add_argument(
        "--interval", type=int, default=PUSH_INTERVAL, help="seconds between syncs"
    )
    args = parser.parse_args()

    if args.loop:
        asyncio.run(_loop(args.interval, args.dry_run))
    else:
        asyncio.run(DataRepoSync(dry_run=args.dry_run).sync())


if __name__ == "__main__":
    main()
