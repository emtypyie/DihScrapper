# The archive's side of the pipeline

The bot pushes batches into `ScrapedDih/inbox/`. That push is what starts the
muncher. This directory is the copy of record for the three files `ScrapedDih`
needs; the workflow cannot live in `DihScrapper`, because a workflow only fires
for pushes to its own repository and the push it has to react to happens in the
archive.

| File here | Goes to `ScrapedDih` as |
| --- | --- |
| `.github/workflows/muncher.yml` | `.github/workflows/muncher.yml` |
| — | `muncher.py` (copied from the repo root, never edited here) |
| — | `formatter.py` (copied from the repo root, never edited here) |

`muncher.py` and `formatter.py` are copied, not duplicated, so the merge logic
has exactly one source: this repository. `tests/test_muncher.py` is what proves
that copy is correct, so it passes before the copy is worth making.

## Installing

From the code repo, against a `ScrapedDih` checkout:

```powershell
.\archive-repo\sync.ps1 -Target ..\ScrapedDih
```

It reports each file that differs, so run it with `-Check` first to see what is
out of date. Commit and push the result yourself — RULEBOOK rule 2 applies to
the archive's workflows too.

## What the job does

1. `python muncher.py` merges every batch in `inbox/` into the transcripts it
   names, keyed on the row id, sorted numerically, and prints a commit message.
2. The batches are deleted in the same commit, so `inbox/` is empty afterwards.
3. The result is rebased onto `main` and pushed. It pushes even when a pass has
   nothing to merge, because an earlier pass can leave a rebased commit
   unpushed — exiting on an empty inbox first is how a merge gets lost.

An empty inbox produces no commit, which is what stops the job re-triggering
itself through its own deletions; the one extra run it does cause finds nothing
and pushes an empty tree.

Idempotent, append-only, and serialised by one `concurrency` group: a re-run
changes nothing, no archive path is ever removed, and two merges cannot
interleave. Because the bot also commits to `main`, the job rebases and munches
again rather than losing that race.