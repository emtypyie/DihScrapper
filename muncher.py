"""Merge the archive's inbox into its transcripts. Run by the muncher workflow.

The bot appends batches to ``inbox/`` and never touches a transcript, because
rewriting one costs more every week and the bot has a five minute budget. This
does that work in a CI job instead, where nothing times out.

Each inbox file lands at ``inbox/<batch id>/<server>/<channel>.csv`` and is
merged into ``<server>/<channel>.csv`` keyed on the row id, then deleted. Rows
are only ever added and sorted, so re-running is a no-op and no path is ever
removed from the archive.

Standard library only, deliberately: the workflow checks the repo out and runs
this with nothing installed.

    python muncher.py            # merge and print the commit message
    python muncher.py --dry-run  # report what would be merged
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

from formatter import key_for, read_rows, rewrite, sort_key

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"

logger = logging.getLogger("DihScrapper.muncher")


@dataclass
class Summary:
    batches: int = 0
    files: int = 0
    rows: int = 0

    def message(self) -> str:
        return f"Munch {self.rows} row(s) from {self.batches} batch(es)"


def munch(inbox: Path, archive: Path, dry_run: bool = False) -> Summary:
    """Merge every batch in the inbox, then delete the batches.

    Grouped by target first so that a transcript spread over several batches is
    read and rewritten once, and so that the summary counts rows that were
    genuinely new rather than rows that arrived twice.
    """
    batches = sorted(path for path in inbox.iterdir() if path.is_dir()) if inbox.is_dir() else []
    by_target: dict[Path, list[Path]] = {}
    for batch in batches:
        for source in sorted(batch.rglob("*.csv")):
            # inbox/<batch id>/<server>/<channel>.csv -> <server>/<channel>.csv
            rel = source.relative_to(batch)
            by_target.setdefault(archive / rel, []).append(source)

    summary = Summary(batches=len(batches), files=sum(len(v) for v in by_target.values()))
    for target, sources in sorted(by_target.items()):
        key = key_for(target)
        merged = {row[key]: row for row in read_rows(target)}
        before = len(merged)
        for source in sources:
            for row in read_rows(source):
                merged.setdefault(row[key], row)
        added = len(merged) - before
        summary.rows += added
        logger.info("%s: %d new row(s), %d total", target.as_posix(), added, len(merged))
        # nothing new means the batch was a re-delivery; leave the transcript
        # untouched so git sees no change and only the deletion lands
        if added and not dry_run:
            rewrite(target, sorted(merged.values(), key=lambda row: sort_key(row[key])))

    if not dry_run:
        for batch in batches:
            shutil.rmtree(batch)
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Merge the inbox into the archive transcripts.")
    parser.add_argument("--inbox", default="inbox", help="the inbox directory to drain")
    parser.add_argument("--archive", default=".", help="root the transcripts live under")
    parser.add_argument("--dry-run", action="store_true", help="report without writing")
    args = parser.parse_args()

    inbox = Path(args.inbox).resolve()
    archive = Path(args.archive).resolve()
    # batches are merged to <archive>/<server>/<channel>.csv and then deleted, so
    # the inbox has to sit inside the archive root for those targets to stay in it
    if archive not in inbox.parents:
        parser.error("--inbox must sit inside --archive, e.g. inbox/ under the archive root")

    # logging to stderr: stdout is the commit message the workflow commits
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stderr)
    summary = munch(inbox, archive, dry_run=args.dry_run)
    print(summary.message())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
