#!/usr/bin/env python3
"""Pre-merge guard: block changes that must never arrive through a pull request.

Three rules, all blocking:

  env-file          a real .env, or any *.env, committed by a PR
  protected-path    a change under .github/workflows or .github/scripts
  potential-secret  a high-confidence credential signature, or a
                    credential-shaped assignment whose value is not a placeholder

Two entry points, one rule set:

  --base REF    file list from git, content from the worktree. BloatWare uses
                this: it fails the required check and gives the author feedback
                inside the run.
  --input FILE  file list and unified diffs from JSON, as the pull request API
                returns them. PR Status uses this: it runs trusted base-branch
                code against API data.

The second mode is not redundant. BloatWare runs whatever repo_guard.py the PR's
merge ref contains, so a PR that edits or deletes this script defeats the first
mode by definition. PR Status never checks out PR code, so it is not defeatable
that way. It needs its own copy of the rules, and it gets one for free here.
"""

from __future__ import annotations

import argparse
import json
import posixpath
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Iterable, Iterator

REPO_RULEBOOK = "https://github.com/emtypyie/DihScrapper/blob/main/RULEBOOK.md"

# The CI trust boundary. Anything that decides whether a PR passes lives here,
# so a PR must not be able to edit it. Rulebook section 2.
PROTECTED_PREFIXES = (".github/workflows/", ".github/scripts/")

# Committed templates are the point of .env.example, so they are not violations.
ENV_TEMPLATES = frozenset(
    {".env.example", ".env.sample", ".env.template", ".env.dist", ".env.ci.example"}
)

RULES: dict[str, tuple[str, str]] = {
    "env-file": ("#1-never-commit-env-files", "Never commit an env file"),
    "protected-path": (
        "#2-ci-files-are-maintainer-only",
        "CI files are maintainer-only",
    ),
    "potential-secret": (
        "#3-no-secrets-in-pull-requests",
        "No secrets in pull requests",
    ),
}

# High confidence: a hit here is a real credential, not a coincidence.
SIGNATURE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (label, re.compile(pattern))
    for label, pattern in (
        ("AWS access key id", r"\b(?:AKIA|ASIA|ABIA|ACCA|AGPA|AIDA|AIPA|ANPA|AROA)[0-9A-Z]{16}\b"),
        ("GitHub token", r"\bgh[pousr]_[A-Za-z0-9]{36,255}\b"),
        ("GitHub fine-grained token", r"\bgithub_pat_[A-Za-z0-9_]{40,}\b"),
        ("private key block", r"-----BEGIN (?:[A-Z]+ )*PRIVATE KEY-----"),
        ("Slack token", r"\bxox[baprse]-[A-Za-z0-9-]{10,}\b"),
        ("Google API key", r"\bAIza[0-9A-Za-z_-]{35}\b"),
        ("Google OAuth client secret", r"\bGOCSPX-[0-9A-Za-z_-]{20,}\b"),
        ("Stripe secret key", r"\b[rs]k_(?:live|test)_[A-Za-z0-9]{16,}\b"),
        ("Supabase service key", r"\bsbp_[0-9a-fA-F]{40}\b"),
        ("GitLab personal access token", r"\bglpat-[A-Za-z0-9_-]{20,}\b"),
        ("npm access token", r"\bnpm_[A-Za-z0-9]{36}\b"),
        ("OpenAI-style API key", r"\bsk-(?:proj-)?[A-Za-z0-9_-]{32,}\b"),
        ("JSON Web Token", r"\beyJ[\w-]{6,}\.eyJ[\w-]{6,}\.[\w-]{6,}\b"),
    )
)

# Lower confidence: fires on any credential-shaped assignment, so it needs a
# placeholder filter to stay quiet on .env.example, docs and test fixtures.
# The value is captured three ways because a leaked secret is quoted in source
# but bare in a .env file, and the two need different treatment.
#
# The key is bounded on both sides by non-alphanumerics on purpose. Without the
# lookbehind, `pass` inside the pattern also matches PASS, PASSED and bypass, and
# `const PASS = 'PIPELINE PASSED'` reads as a leaked password.
ASSIGNMENT = re.compile(
    r"""
    (?<![A-Za-z0-9])
    [A-Za-z0-9_.\-]*
    (?P<key>
        pass(?:word|wd|phrase)
      | secrets?
      | tokens?
      | api[_-]?key
      | access[_-]?key
      | auth[_-]?key
      | private[_-]?key
      | credentials?
    )
    (?![A-Za-z0-9])
    \s* [:=] \s*
    (?: "(?P<double>[^"]{8,})"
      | '(?P<single>[^']{8,})'
      | (?P<bare>[^\s"'`#,;]{8,}) )
    """,
    re.IGNORECASE | re.VERBOSE,
)

# A bare SCREAMING_CASE name is a reference to a variable, not a credential.
# Without this, `token=GITHUB_TOKEN` reads as a leak, which is every call site
# in logger.py and main.py.
BARE_REFERENCE = re.compile(r"^[A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*$")

PLACEHOLDERS = frozenset(
    """
    your example placeholder changeme change-me change_me dummy sample template
    test fake todo insert here replace redacted secret none null true false xxx
    os. getenv environ dotenv env. process. localhost 127.0.0.1 0.0.0.0
    < > $ % * .
    """.split()
)

# The generic rule is suppressed here: prose about secrets is not a secret.
GENERIC_EXEMPT_SUFFIXES = (".md", ".rst", ".txt")
GENERIC_EXEMPT_TAGS = ("example", "sample", "template", ".dist")


def _finding(rule: str, path: str, line: int | None, detail: str) -> dict[str, Any]:
    anchor, title = RULES[rule]
    return {
        "rule": rule,
        "title": title,
        "file": path,
        "line": line,
        "detail": detail,
        "rulebook": f"{REPO_RULEBOOK}{anchor}",
    }


def _normalise(path: str) -> str:
    """POSIX, relative, no leading ./ — but never strip a leading dot directory."""
    clean = path.replace("\\", "/")
    while clean.startswith("./"):
        clean = clean[2:]
    return clean


def is_env_file(path: str) -> bool:
    name = posixpath.basename(_normalise(path))
    if name in ENV_TEMPLATES:
        return False
    return name == ".env" or name.startswith(".env.") or name.endswith(".env")


def is_protected(path: str) -> bool:
    clean = _normalise(path)
    return any(clean.startswith(prefix) for prefix in PROTECTED_PREFIXES)


def _skip_generic(path: str) -> bool:
    low = path.lower()
    if low.endswith(GENERIC_EXEMPT_SUFFIXES):
        return True
    return any(tag in low for tag in GENERIC_EXEMPT_TAGS)


def _looks_like_placeholder(value: str) -> bool:
    low = value.lower()
    if any(token in low for token in PLACEHOLDERS):
        return True
    return len(set(low)) <= 3


def _scan_line(
    path: str, line_no: int, line: str, findings: list[dict[str, Any]], allow_generic: bool
) -> None:
    for label, pattern in SIGNATURE_PATTERNS:
        if pattern.search(line):
            findings.append(_finding("potential-secret", path, line_no, f"looks like a {label}"))
    if not allow_generic:
        return
    match = ASSIGNMENT.search(line)
    if not match:
        return
    quoted = match.group("double") is not None or match.group("single") is not None
    value = match.group("double") or match.group("single") or match.group("bare")
    if not quoted and BARE_REFERENCE.match(value):
        return
    if _looks_like_placeholder(value):
        return
    key = match.group("key").strip()
    findings.append(
        _finding(
            "potential-secret",
            path,
            line_no,
            f"`{key}` is assigned a value that is not a placeholder",
        )
    )


def _iter_patch_lines(patch: str) -> Iterator[tuple[int, str]]:
    """Yield (new-file line number, line) for added lines in a unified diff."""
    line_no = 0
    for raw in patch.splitlines():
        if raw.startswith("@@"):
            match = re.search(r"\+(\d+)", raw)
            line_no = int(match.group(1)) if match else 0
        elif raw.startswith("+"):
            yield line_no, raw[1:]
            line_no += 1
        elif raw.startswith(" "):
            line_no += 1


def _entries_from_worktree(paths: Iterable[str]) -> Iterator[tuple[str, list[tuple[int, str]]]]:
    for path in paths:
        target = Path(path)
        if not target.is_file():
            continue
        data = target.read_bytes()
        if b"\x00" in data[:8192]:
            continue
        text = data.decode("utf-8", errors="replace")
        yield path, list(enumerate(text.splitlines(), start=1))


def _entries_from_payload(payload: dict[str, Any]) -> Iterator[tuple[str, list[tuple[int, str]]]]:
    for item in payload.get("files", []):
        path = item.get("filename") or ""
        if not path:
            continue
        yield path, list(_iter_patch_lines(item.get("patch") or ""))


def _git_changed_files(base: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=ACMR", f"{base}...HEAD"],
        check=True,
        capture_output=True,
        text=True,
    )
    return sorted({line.strip() for line in result.stdout.splitlines() if line.strip()})


def build_report(entries: Iterable[tuple[str, list[tuple[int, str]]]], mode: str) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    scanned = 0
    for path, lines in entries:
        clean = _normalise(path)
        if is_env_file(clean):
            findings.append(
                _finding(
                    "env-file",
                    clean,
                    None,
                    "commits a real env file; ship a `.env.example` with empty values instead",
                )
            )
        if is_protected(clean):
            findings.append(
                _finding(
                    "protected-path",
                    clean,
                    None,
                    "changes a maintainer-only CI path; push it to main yourself",
                )
            )
        allow_generic = not _skip_generic(clean)
        for line_no, line in lines:
            _scan_line(clean, line_no, line, findings, allow_generic)
        scanned += 1
    return {
        "ok": not findings,
        "mode": mode,
        "rulebook": REPO_RULEBOOK,
        "files_scanned": scanned,
        "findings": findings,
    }


def _render(report: dict[str, Any]) -> str:
    if report["ok"]:
        return f"Repository rules OK — {report['files_scanned']} file(s) scanned."
    lines = [f"Repository rules violated — {len(report['findings'])} finding(s):"]
    for item in report["findings"]:
        where = item["file"]
        if item["line"]:
            where = f"{where}:{item['line']}"
        lines.append(f"  [{item['title']}] {where} — {item['detail']}")
    lines.append(f"Rulebook: {report['rulebook']}")
    return "\n".join(lines)


def _markdown(report: dict[str, Any]) -> str:
    if report["ok"]:
        return (
            "### Repository rules\n\n"
            f"Passed — {report['files_scanned']} file(s) scanned, no violations.\n"
        )
    rows = [
        "### Repository rules\n",
        f"**{len(report['findings'])} violation(s). This PR cannot be merged until they are fixed.**\n",
        "| Rule | Where | Detail |",
        "| --- | --- | --- |",
    ]
    for item in report["findings"]:
        where = item["file"]
        if item["line"]:
            where = f"{where}:{item['line']}"
        rows.append(f"| {item['title']} | `{where}` | {item['detail']} |")
    rows.append(f"\nRulebook: {report['rulebook']}\n")
    return "\n".join(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--base", help="git ref to diff against, e.g. origin/main")
    group.add_argument("--input", type=Path, help="JSON of {files: [{filename, patch}]}")
    parser.add_argument("--report", type=Path, help="where to write the JSON report")
    parser.add_argument("--summary", type=Path, help="GitHub step summary file to append to")
    args = parser.parse_args(argv)

    if args.base:
        mode = "worktree"
        entries = _entries_from_worktree(_git_changed_files(args.base))
    else:
        mode = "api"
        entries = _entries_from_payload(json.loads(args.input.read_text(encoding="utf-8")))

    report = build_report(entries, mode)
    payload = json.dumps(report, indent=2, sort_keys=True)

    if args.report:
        args.report.write_text(payload + "\n", encoding="utf-8")
    if args.summary:
        with args.summary.open("a", encoding="utf-8") as handle:
            handle.write(_markdown(report))
    print(_render(report))

    return 1 if report["findings"] else 0


if __name__ == "__main__":
    sys.exit(main())
