"""Ships log files to a private DihLogger repo.

Separate process on purpose: the crash we want to catch kills main.py, and a
shipper inside that process dies with it. Sidecar on the shared log volume.

Local file is the source of truth, not the upload. Writer flushes per record,
the volume outlives the container, and whole files are overwritten (never
appended) so a torn trailing line can only cost the newest bytes.

    python logger.py           # watch and ship
    python logger.py --once    # one cycle, then exit
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import logging
import logging.handlers
import os
import signal
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

from pusher import GITHUB_OWNER, GITHUB_TOKEN, ArchiveError, GitHubArchive

load_dotenv()

logger = logging.getLogger("DihScrapper.logs")

LOG_DIR = Path(os.environ.get("LOG_DIR", "LOGS"))
LOG_NAME = os.environ.get("LOG_NAME", "dihscrapper.log")
LOG_REPO = os.environ.get("GITHUB_LOG_REPO", "DihLogger")
LOG_BRANCH = os.environ.get("GITHUB_LOG_BRANCH", "main")
SHIP_INTERVAL = int(os.environ.get("SHIP_INTERVAL", "60"))
LOG_MAX_BYTES = int(os.environ.get("LOG_MAX_BYTES", str(5 * 1024 * 1024)))
LOG_BACKUPS = int(os.environ.get("LOG_BACKUPS", "5"))

# own file: a shipper failure is itself recorded
SHIPPER_LOG_NAME = "dihscrapper.logshipper.log"

FILE_MODE = "100644"
LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
REPO_CREATE_ATTEMPTS = 10
REPO_CREATE_DELAY = 1.0


def _widen_console() -> None:
    """Let the console hold a channel name.

    A Windows console defaults to cp1252, and one of the servers this bot is in
    has a name outside it. Logging that name then raises UnicodeEncodeError from
    inside the logging handler, which takes the run down over a cosmetic fault.
    The file handler is utf-8 either way; only the terminal needed widening.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (ValueError, OSError):
                pass


def setup_logging(name: str = LOG_NAME, log_dir: Path | None = None) -> Path:
    # Defaults to the bot's log, not the shipper's. The shipper passes
    # SHIPPER_LOG_NAME explicitly: two processes rotating one file is unsafe,
    # and the bot has no reason to write to the shipper's log.
    # RotatingFileHandler flushes per emit, so no buffered lines are lost.
    _widen_console()
    directory = Path(log_dir or LOG_DIR)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for handler in list(root.handlers):
        root.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT)
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(formatter)

    rotating = logging.handlers.RotatingFileHandler(
        path,
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUPS,
        encoding="utf-8",
    )
    rotating.setFormatter(formatter)

    root.addHandler(stream)
    root.addHandler(rotating)
    return path


class LogShipper:
    def __init__(
        self,
        log_dir: Path | None = None,
        archive: GitHubArchive | None = None,
        names: tuple[str, ...] = (LOG_NAME, SHIPPER_LOG_NAME),
    ) -> None:
        self.log_dir = Path(log_dir or LOG_DIR)
        self._names = names
        self._archive = archive or GitHubArchive(
            owner=GITHUB_OWNER,
            repo=LOG_REPO,
            token=GITHUB_TOKEN,
            branch=LOG_BRANCH,
        )
        self._shipped: dict[str, str] = {}
        self._repo_ready = False

    @property
    def configured(self) -> bool:
        return self._archive.configured

    def _local_files(self) -> list[Path]:
        # globs *.1/*.2 too, so a rotation does not skip a cycle
        if not self.log_dir.is_dir():
            return []
        found: list[Path] = []
        for name in self._names:
            found.extend(p for p in self.log_dir.glob(f"{name}*") if p.is_file())
        return sorted(set(found))

    async def _ensure_repo(self) -> None:
        archive = self._archive
        if self._repo_ready:
            return
        found = await archive._api("GET", archive._repo_path(""), allow_missing=(404,))
        if found is None:
            logger.info("Creating private log repository %s/%s", archive.owner, archive.repo)
            await archive._api(
                "POST",
                "/user/repos",
                # auto_init: the git data API needs a commit to build on
                {"name": archive.repo, "private": True, "auto_init": True},
            )
            # repo creation is eventually consistent, so early reads can 404
            for _ in range(REPO_CREATE_ATTEMPTS):
                found = await archive._api("GET", archive._repo_path(""), allow_missing=(404,))
                if found is not None:
                    break
                await asyncio.sleep(REPO_CREATE_DELAY)
            if found is None:
                raise ArchiveError(f"log repository {archive.owner}/{archive.repo} did not appear")
        self._repo_ready = True

    async def ship(self) -> int:
        if not self.configured:
            logger.info("Log shipper not configured; skipping")
            return 0

        await self._ensure_repo()
        archive = self._archive

        payloads: dict[str, bytes] = {}
        entries: list[dict] = []
        for path in self._local_files():
            # a mid-write read may see a partial line; the next cycle rewrites
            # the file whole, so the torn line is replaced, not kept
            data = path.read_bytes()
            sha = archive.blob_sha(data)
            if self._shipped.get(path.name) == sha:
                continue
            payloads[path.name] = data
            entries.append({"path": path.name, "mode": FILE_MODE, "type": "blob", "sha": sha})
            self._shipped[path.name] = sha

        if not entries:
            return 0

        head_sha, tree_sha, _ = await archive._remote_state()
        if head_sha is None:
            await archive._bootstrap()
            head_sha, tree_sha, _ = await archive._remote_state()

        for data in payloads.values():
            await archive._api(
                "POST",
                archive._repo_path("/git/blobs"),
                {"content": base64.b64encode(data).decode(), "encoding": "base64"},
            )

        new_tree = await archive._api(
            "POST",
            archive._repo_path("/git/trees"),
            {"base_tree": tree_sha, "tree": entries} if tree_sha else {"tree": entries},
        )
        commit = await archive._api(
            "POST",
            archive._repo_path("/git/commits"),
            {
                "message": f"Update {', '.join(sorted(payloads))}",
                "tree": new_tree["sha"],
                "parents": [head_sha] if head_sha else [],
            },
        )
        await archive._write_ref(commit["sha"], head_sha)
        logger.info("Shipped %d log file(s) to %s", len(entries), archive.repo)
        return len(entries)

    async def ship_quietly(self, reason: str) -> None:
        try:
            await self.ship()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Could not ship logs during %s", reason)

    async def close(self) -> None:
        await self._archive.close()


async def _watch(shipper: LogShipper, stop: asyncio.Event) -> None:
    logger.info("Watching %s for new log output", shipper.log_dir)
    while True:
        started = time.monotonic()
        try:
            await shipper.ship()
        except asyncio.CancelledError:
            raise
        except Exception:
            # never let an error end the loop
            logger.exception("Ship cycle failed; continuing")
        if stop.is_set():
            break
        elapsed = time.monotonic() - started
        if elapsed > SHIP_INTERVAL:
            logger.warning("Ship cycle took %.1fs of a %ds interval", elapsed, SHIP_INTERVAL)
        try:
            await asyncio.wait_for(stop.wait(), timeout=SHIP_INTERVAL)
        except asyncio.TimeoutError:
            continue
    await shipper.ship_quietly("shutdown")


async def _run(once: bool) -> int:
    shipper = LogShipper()
    if not shipper.configured:
        logger.error("GITHUB_TOKEN is not set; cannot ship logs")
        return 1

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stop.set))

    if once:
        count = await shipper.ship()
        logger.info("Shipped %d log file(s)", count)
        await shipper.close()
        return 0

    try:
        await _watch(shipper, stop)
    finally:
        await shipper.close()
    logger.info("Log shipper stopped")
    return 0


def main() -> int:
    setup_logging(name=SHIPPER_LOG_NAME)
    parser = argparse.ArgumentParser(description="Ship log files to the private log repository.")
    parser.add_argument("--once", action="store_true", help="ship a single cycle, then exit")
    args = parser.parse_args()
    try:
        return asyncio.run(_run(args.once))
    except KeyboardInterrupt:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
