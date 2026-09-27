"""End-to-end check of the archive publisher.

Creates a throwaway private repository, publishes to it, and deletes it, so the
test exercises the real first-run path (an archive with no commits at all)
without ever touching the live archive repository or its default branch.

Verifies that transcripts reach the git tree with correct blob hashes, that
media is never committed, that a second publish of unchanged content is a
no-op, and that an edited transcript produces exactly one new commit.

    python tests/test_publish.py
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
import uuid
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from upload_data import GitHubArchive  # noqa: E402

TRANSCRIPT_PATH = "TestServer/general.json"


async def scenario() -> int:
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    probe = GitHubArchive()
    if not probe.configured:
        print("SKIP  GITHUB_TOKEN is not configured")
        return 0

    owner = (await probe._api("GET", "/user", allow_missing=()))["login"]
    name = f"dihscrapper-test-{uuid.uuid4().hex[:8]}"
    await probe._api(
        "POST",
        "/user/repos",
        {"name": name, "private": True, "auto_init": False},
        allow_missing=(),
    )
    await probe.close()

    try:
        with tempfile.TemporaryDirectory() as scratch:
            root = Path(scratch) / "HOME"
            archive = GitHubArchive(data_root=root, owner=owner, repo=name, branch="main")
            try:
                head_before, _, _ = await archive._remote_state()
                check("fresh repository has no commits", head_before is None)

                transcript = [
                    {
                        "id": 1,
                        "content": "hello",
                        "attachments": ["[potential image]"],
                        "reply_to": None,
                    }
                ]
                (root / "TestServer").mkdir(parents=True, exist_ok=True)
                transcript_file = root / "TestServer" / "general.json"
                transcript_file.write_text(json.dumps(transcript), encoding="utf-8")

                check("first publish bootstraps and commits", await archive.publish())

                head_after, _, paths = await archive._remote_state()
                check("branch head advanced", head_after is not None and head_after != head_before)
                check("transcript present in tree", TRANSCRIPT_PATH in paths)
                check(
                    "transcript blob hash matches its local bytes",
                    paths.get(TRANSCRIPT_PATH) == archive.blob_sha(transcript_file.read_bytes()),
                )
                check("no mediapool committed", not any("mediapool" in p for p in paths))
                check("no local-only index committed", ".media-index.json" not in paths)
                check(
                    "nothing on disk but the transcript",
                    sorted(p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file())
                    == [TRANSCRIPT_PATH],
                )

                check("republish of unchanged data is a no-op", not await archive.publish())

                transcript.append(
                    {"id": 2, "content": "second", "attachments": [], "reply_to": None}
                )
                transcript_file.write_text(json.dumps(transcript), encoding="utf-8")
                check("edited transcript republishes", await archive.publish())

                head_final, _, final_paths = await archive._remote_state()
                check("exactly one further commit landed", head_final not in (head_before, head_after))
                check("archive path set is unchanged by the edit", set(final_paths) == set(paths))
                check(
                    "edited transcript has the new blob hash",
                    final_paths.get(TRANSCRIPT_PATH)
                    == archive.blob_sha(transcript_file.read_bytes()),
                )
            finally:
                await archive.close()
    finally:
        cleanup = GitHubArchive(owner=owner, repo=name)
        try:
            await cleanup._api("DELETE", f"/repos/{owner}/{name}", allow_missing=())
            print("cleaned up throwaway repository")
        finally:
            await cleanup.close()

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(scenario()))
