# Rulebook

The rules a pull request has to follow to be merged into this repository.

**Every rule below is enforced automatically.** A violation fails the
`🦞 BloatWare` check, adds the `PIPELINE FAILED` label, and posts a comment on
the PR naming the exact file and line. There is no way to merge around it.

Enforced by [`.github/scripts/repo_guard.py`](.github/scripts/repo_guard.py),
which runs twice: once inside `bloatware.yml` as the merge gate, and once
inside `pr-status.yml` from the base branch, so that editing the guard itself
does not disable it.

---

## 1. Never commit env files

**Rule:** no pull request may add or modify a real environment file.

Blocked: `.env`, `.env.local`, `.env.production`, anything named `*.env`, at
any depth, including `config/.env`.

**Do this instead:** change [`.env.example`](.env.example). It is the committed
template, its values are meant to be empty, and it is the one file in this
family the guard allows. If you need to document a new setting, add the key
with an empty value and say what it does.

If you genuinely need a local `.env` for testing, it stays out of git —
[`.gitignore`](.gitignore) already lists it. Nothing else to do.

If you ever commit a secret by accident, **do not just delete it in a follow-up
commit.** Git keeps it in history forever. Rotate the credential first, then
clean the history.

## 2. CI files are maintainer only

**Rule:** no pull request may add, modify or delete anything under
`.github/workflows/` or `.github/scripts/`.

These files decide whether a pull request passes. A contributor who can edit
them can edit the judge, so they are outside the contributor's remit by design.

**Do this instead:** open an issue describing the change you want, and a
maintainer will push it. That is not a formality — it is the entire control.
A workflow edit that arrives via PR is exactly the thing rule 2 exists to
stop, including a PR that appears to "only" tweak a job name.

Everything outside those two directories is fair game through normal PRs.

## 3. No secrets in pull requests

**Rule:** no pull request may introduce anything that looks like a live
credential.

Detected outright, anywhere in a changed file:

- AWS access key ids (`AKIA…`, `ASIA…`)
- GitHub tokens, classic (`ghp_`, `gho_`, `ghu_`, `ghs_`, `ghr_`) and
  fine-grained (`github_pat_…`)
- private key blocks, in any PEM armour
- Slack tokens (`xoxb-`, `xoxp-`, …)
- Google API keys (`AIza…`) and OAuth client secrets (`GOCSPX-…`)
- Stripe, GitLab, npm, Supabase and OpenAI-style tokens
- JSON Web Tokens

And separately, any variable named like a credential — `*_TOKEN`,
`*_SECRET`, `PASSWORD`, `*_API_KEY`, `*_PRIVATE_KEY`, `*_CREDENTIAL` —
assigned a value that is not a placeholder.

**What counts as a placeholder,** so the rule does not fire on documentation:
`your_key_here`, `changeme`, `os.environ[...]`, `getenv(...)`, an empty value,
or a bare `SCREAMING_CASE` name that refers to a variable. Prose in `.md` and
`.rst` files, and any path containing `example`, `sample`, `template` or
`.dist`, is exempt from the loose rule. The high-confidence signatures above are
checked everywhere with no exemptions.

**Do this instead:** read the value from the environment. The codebase already
does this — see how `logger.py` builds its GitHub client from `GITHUB_TOKEN`.

Never put a real token in `.env.example` either. It is committed, so it is
public, and the exemption above is a courtesy, not a secret locker.

---

## How the guard sees your changes

Only files you actually added or modified are scanned, so a pre-existing issue
somewhere else in the tree cannot fail an unrelated PR. For the same reason
ruff only formats the Python your PR touched.

## When the guard is wrong

False positives are bugs. Open an issue with the file, the line and what it
matched. A genuine secret is never going to be waved through — the pattern that
caught it stays, and the file gets a narrower exemption instead.

## Review

`REVIEW REQUIRED` means the pipeline is green and a human still has to look at
it. Nothing merges without a maintainer.
