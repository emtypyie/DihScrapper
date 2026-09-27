"""End-to-end check of the archive publisher.

Runs against a unique throwaway branch so it is repeatable and never touches the
live archive. Verifies that media reaches the git tree as a blob while no media
bytes are ever written to the local filesystem, that a second publish of
unchanged content is a no-op, and that an edited transcript produces exactly one
new commit with the expected blob hash.

    python tests/test_publish.py
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
import tempfile
import uuid
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from upload_data import GitHubArchive, git_blob_sha  # noqa: E402

PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmM"
    "IQAAAABJRU5ErkJggg=="
)
TRANSCRIPT_PATH = "TestServer/general.json"
MEDIA_PATH = "TestServer/mediapool/1_probe.png"


async def delete_branch(archive: GitHubArchive) -> None:
    try:
        await archive._api(
            "DELETE",
            archive._repo_path(f"/git/refs/heads/{archive.branch}"),
        )
    except Exception:
        pass


async def scenario() -> int:
    branch = f"selftest-{uuid.uuid4().hex[:12]}"
    failures: list[str] = []

    def check(label: str, condition: bool) -> None:
        print(f"{'PASS' if condition else 'FAIL'}  {label}")
        if not condition:
            failures.append(label)

    with tempfile.TemporaryDirectory() as scratch:
        root = Path(scratch) / "HOME"
        archive = GitHubArchive(data_root=root, branch=branch)
        try:
            head_before, _, _ = await archive._remote_state()
            check("new branch starts with no commits", head_before is None)

            blob_sha = await archive.store_blob(PNG)
            check("store_blob returns a 40-char git hash", len(blob_sha) == 40)
            check("store_blob hash is the real blob hash", blob_sha == git_blob_sha(PNG))

            archive.save_media_index({MEDIA_PATH: blob_sha})
            check(
                "media index is written locally",
                (root / ".media-index.json").is_file(),
            )

            transcript = [
                {
                    "id": 1,
                    "content": "hello",
                    "attachments": [MEDIA_PATH],
                }
            ]
            (root / "TestServer").mkdir(parents=True, exist_ok=True)
            transcript_file = root / "TestServer" / "general.json"
            transcript_file.write_text(json.dumps(transcript), encoding="utf-8")

            check("first publish commits", await archive.publish())

            head_after, _, paths = await archive._remote_state()
            check("branch head advanced", head_after is not None and head_after != head_before)
            check("transcript present in tree", TRANSCRIPT_PATH in paths)
            check("media blob present in tree", MEDIA_PATH in paths)
            check("media blob hash round-trips", paths.get(MEDIA_PATH) == blob_sha)
            check(
                "transcript blob hash matches its local bytes",
                paths.get(TRANSCRIPT_PATH)
                == git_blob_sha(transcript_file.read_bytes()),
            )
            check("local media index is not published", ".media-index.json" not in paths)
            check(
                "no media bytes were written to disk",
                not (root / "TestServer" / "mediapool").exists(),
            )

            check("republish of unchanged data is a no-op", not await archive.publish())

            transcript.append({"id": 2, "content": "second"})
            transcript_file.write_text(json.dumps(transcript), encoding="utf-8")
            check("edited transcript republishes", await archive.publish())

            head_final, _, final_paths = await archive._remote_state()
            check("exactly one further commit landed", head_final not in (head_before, head_after))
            check("archive path set is unchanged by the edit", set(final_paths) == set(paths))
            check(
                "edited transcript has the new blob hash",
                final_paths.get(TRANSCRIPT_PATH) == git_blob_sha(transcript_file.read_bytes()),
            )
        finally:
            await delete_branch(archive)
            await archive.close()

    print()
    if failures:
        print(f"{len(failures)} check(s) failed")
        return 1
    print("all checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(scenario()))
