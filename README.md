# DihScrapper

by emtypyie

Active Discord Chat Scraper to train ML MODELS
lol.

---

DihScrapper is a Discord bot that mirrors messages into a private GitHub
repository as they arrive. Point it at a server, invite it, and it builds a timestamped
CSV transcript of the conversation — text, replies, and the user directory that ties
them together. Point a training pipeline at the repo and you have a dataset that keeps
itself current.

Three properties shape the design:

- **The bot captures the live stream only.** `main.py` never looks back: nothing said
  before it connected is read by it. History is a separate program you run on purpose,
  because a backfill costs rate limit and *how much* should be a decision rather than a
  default — see [Backfilling history](#backfilling-history).
- **Media is recorded by reference, never downloaded.** Images, videos, and files are all
  stored as their CDN URL. Nothing is fetched, and nothing but a URL is kept.
- **The archive only ever grows.** The bot commits new files and never rewrites one; the
  workflow that merges them adds rows, sorts, and rewrites. No row is dropped and no path is
  ever deleted, so the local disk is disposable staging and the repository is the record.

Because the archive lives in Git, the scraper can be killed, redeployed, or moved to
another host and pick up exactly where it left off.

## How it works

The bot runs a single event loop over the channels it has been invited to.

1. **Listen.** `on_message` fires for every new message. Messages authored by the bot
   itself are dropped so it never archives its own output.
2. **Buffer a row.** Each message becomes one CSV row appended to that channel's local
   buffer: snowflake ID, ISO timestamp, author name and ID, content, the space-separated
   attachment URLs, and four `reply_to_*` columns when the message answers another.
3. **Track the directory.** Each server keeps a `user_map.csv` of `username,user_id`, keyed
   by ID so a renamed user keeps their history.
4. **Stage, push, verify, clear.** Every `PUSH_INTERVAL` seconds the buffer is copied into a
   batch under `inbox/`, the batch is committed to the archive, its presence on the branch is
   confirmed, and only then is the local buffer cleared.

Rows are keyed on message ID throughout, so the whole cycle is idempotent: a retry after a
failed push re-commits the same rows and changes nothing.

## The push cycle

Each interval runs three steps, and the third is gated on the second.

1. **Stage.** The buffered rows are copied into `inbox/<batch id>/`, one file per channel. The
   batch id is the newest row's timestamp plus a hash of the batch's own bytes, so staging an
   unchanged buffer twice produces the same path — which is how a push that lost its reply
   recognises itself.
2. **Push.** Each batch file is a brand new path, so it is committed as a new blob and a tree
   entry. No archive file is read, rewritten, or even known to the bot. Every path is new, so
   this costs a fixed handful of small calls whatever the archive weighs.
3. **Verify, then clear.** The branch's tree is walked — metadata, not content — to confirm the
   batch is on it, and only then are exactly those rows removed from the buffer by key. A
   message that arrives mid-cycle is not in the cleared set, so it goes out next interval.

If step 1 or 2 fails the buffer is left completely intact and the next interval retries.

## Why an inbox

Merging new messages into a transcript means rewriting the whole file, because a git blob is
content-addressed and immutable: there is no "append eighty bytes to this file" in the object
model or in GitHub's API. So a scraper that merged inline re-downloaded and re-uploaded every
channel it touched, every cycle, forever, and a busy channel eventually took longer than any
sane request timeout.

Splitting the work fixes it without changing what the archive is. The bot only ever adds new
files, so its cost tracks what just arrived. The rewrite moves to `muncher.py`, run by a
GitHub Actions job where nothing times out.

## The muncher

`.github/workflows/muncher.yml` runs on any push that touches `inbox/`. It merges every batch
into the transcripts it names, sorts by ID, deletes the batches, and commits the result. The
only writer of a transcript is this job.

The workflow lives in the **archive** repository, because that is where the push it reacts to
happens: a workflow only fires for pushes to its own repository. `archive-repo/` is the copy
of record for that workflow, and `archive-repo/sync.ps1` installs it — along with `muncher.py`
and `formatter.py` — into a `ScrapedDih` checkout. Those two scripts are copied rather than
duplicated so the merge logic has one source; `tests/test_muncher.py` is what proves the copy
is correct.

- **Idempotent.** Merging by key means a re-run or a manual re-trigger changes nothing.
- **Append-only.** Rows are only ever added, sorted, and rewritten. No archive path is ever
  removed, so the archive can only grow.
- **Serializable.** One `concurrency` group, so two merges can never interleave. The bot also
  commits to `main`, so the job rebases and re-munches rather than losing that race.

An empty inbox produces no commit, which is what stops the workflow re-triggering itself.

## Backfilling history

`main.py` archives what arrives after it connects. `backfill.py` reads what came before,
and it runs only when you ask for it: **Actions → backfill → Run workflow**, or from a
shell.

The bot needs no special permission for this and no user token — a bot with **Read
Message History** can read a channel's history over the REST API. This is the bot's own
token reading a channel it is already a member of; there is no user account automation
anywhere in this project.

```bash
python backfill.py --channel 1234567890                # one channel
python backfill.py --server 123456789 --list           # what would it cover
python backfill.py --server 123456789 --limit 500      # newest 500 per channel
python backfill.py --channel 1234567890 --dry-run      # read and stage, publish nothing
```

It writes through the same inbox the bot does, which is what makes it safe to point at a
live archive:

- It never rewrites a transcript, so a backfill costs what it read rather than what the
  archive weighs.
- Rows it fetches that the bot already captured are deduplicated by the muncher on
  message id, not duplicated. Overlapping the live capture is free, so a backfill can
  run while the bot is going.
- Re-running the same backfill adds nothing. The transcript comes out byte-identical,
  because a merge with no new rows does not rewrite the file.
- History is sorted into place, so a transcript still reads oldest-first afterwards. Read
  by the `id` column either way.

History is read 100 messages at a time and staged in batches of `BACKFILL_CHUNK` rows.
Every batch is one push, and that push is what makes the muncher merge those rows into the
transcripts on the archive's `main` — so **the archive is whole every 10k rows**, not just
at the end of a run, and a run that dies halfway leaves its earlier batches merged. There
is no cap on the read: `--limit` is there for a deliberate partial read, not as a safety
stop.

### Running it from Actions

The `backfill` workflow is manual-only (`workflow_dispatch`) and needs, once:

| | |
| --- | --- |
| Secret `DISCORD_API` | the bot token |
| Secret `ARCHIVE_TOKEN` | PAT with `repo` scope on the archive account |
| Variable `ARCHIVE_OWNER` | archive repo owner |
| Variable `ARCHIVE_REPO` | archive repo name, e.g. `ScrapedDih` |
| Variable `ARCHIVE_BRANCH` | archive branch, default `main` |

Inputs: `channel_id` (comma separated, threads welcome), `server_id` (every readable text
channel), `limit` (0 for all), `resolve_replies`, `delay`, `list_only`.

It publishes over the API with the PAT, so the workflow itself needs no write permission on
this repository. It is serialized with the bot only by GitHub's per-repo limits and the
ref check in `pusher.py`: two writers cannot lose each other's rows, and a run that finds
the ref moved re-reads it rather than forcing.

### What a backfill will not do

- **Reply columns are best-effort.** A reply whose parent is in the page being read is
  resolved with no extra request. One whose parent is further back is left empty unless
  `--resolve-replies` is set, which costs one API call per such reply.
- **Threads only by id.** A thread id works in `channel_id`. Listing every thread in a
  server needs the archived-threads API, which needs admin, so `--server` covers text
  channels.
- **It cannot read what Discord will not serve.** A guild that restricts the bot, or a
  channel it cannot see, is refused with the reason; in a server-wide run that channel is
  skipped and the run continues.
- **Deleted messages stay gone.** History before the bot joined is as good as Discord's
  retention, no more.
- **An unreadable channel does not fail the run**, unless nothing at all could be read —
  otherwise every server with one private channel would paint each run red.

## Publishing model

Commits are assembled through the GitHub git data API: blobs, then a tree layered on the
current remote tree, then a commit, then a ref update. Files are hashed the way git hashes
them, so unchanged content is skipped.

### Starting from an empty repository

GitHub will not create a blob in a repository that has no commits, so a brand-new archive
is seeded before its first push. If the repository already has commits but the configured
branch does not, the branch is pointed at the default branch's head instead.

## Data layout

In the archive repository:

```text
inbox/                              # batches, each one deleted by the muncher
└── 20261001T155501Z-ab12cd34/
    └── TestServer/
        ├── general.csv
        └── user_map.csv
TestServer/                         # transcripts, written only by the muncher
├── general.csv
└── user_map.csv
```

A batch id is `<newest row, UTC>-<8 hex of the batch's content hash>`. Server and channel names
are stripped to `[A-Za-z0-9_-]` and capped at 64 characters.

If you ever find batches piling up in `inbox/`, the muncher is not keeping up — check the
Actions tab for a red run, and re-run the job. Nothing is lost while it is down: the bot keeps
buffering and the batches keep the rows. The bot also logs a warning once more than
`GITHUB_INBOX_WARN` batches are waiting.

**Reading the archive:** a channel is one CSV, but sort by the `id` column rather than trusting
file order. Consumers should concatenate a channel's files and sort by `id`; within a file it
is already sorted.

Locally, `DATA_ROOT` holds only unsent rows plus the batches staged from them:

```text
HOME/
├── [Sanitized_Server_Name]/
│   ├── [Sanitized_Channel_Name].csv
│   └── user_map.csv
└── inbox/
    └── 20261001T155501Z-ab12cd34/
        └── [Sanitized_Server_Name]/
            └── [Sanitized_Channel_Name].csv
```

The local `inbox/` is deleted once its batch is confirmed on the branch.

A channel transcript row:

```csv
id,timestamp,author,author_id,content,attachments,reply_to_id,reply_to_author_id,reply_to_author,reply_to_content
1234567890,2026-09-27T20:18:00+00:00,username_here,9876543210,"Message string, with a comma","https://cdn.discordapp.com/attachments/1/2/pic.png https://cdn.discordapp.com/attachments/1/3/clip.mp4",1122334455,5544332211,original_poster,"This was the text being replied to..."
```

`attachments` is a space-separated list of CDN URLs — images, videos, and files alike. It
is empty when the message had none. The four `reply_to_*` columns are empty when the message
was not a reply.

## Repositories

| Repository | Visibility | Contents |
| --- | --- | --- |
| `DihScrapper` | Public | This code. `HOME/` is gitignored and never enters it. |
| `ScrapedDih` | Private | The archive. CSV transcripts, user maps, and `inbox/` batches in flight. |

`HOME/` belongs exclusively to the private archive and is gitignored in the code repo.
The code lives on the main account; the archive lives on whichever account `GITHUB_OWNER`
points at.

## Setup

Requires Python 3.12+.

```bash
pip install -r requirements.txt
cp .env.example .env    # then fill it in
python main.py
```

`.env`:

```ini
DISCORD_API=<your bot token>
GITHUB_TOKEN=<pat with repo scope on the archive account>
GITHUB_OWNER=myrachane
GITHUB_DATA_REPO=ScrapedDih
GITHUB_BRANCH=main
```

The token is only ever sent as a bearer header from the publishing process. It is never
written to a git config, a remote URL, or a file.

### Discord setup

In the Developer Portal, enable both **Message Content Intent** and **Server Members
Intent**, then invite the bot with permission to view channels and read message history. A
bot without the Message Content Intent receives empty `content` fields and the archive will
be worthless.

### Running in Docker

```bash
docker compose up -d --build
docker compose logs -f
```

The `home` volume is mounted at `/app/HOME` so transcripts survive container rebuilds.
`restart: unless-stopped` brings the bot back after a host reboot, and `SIGTERM` triggers a
clean gateway shutdown rather than a kill.

## Configuration

All optional — the defaults are fine for most servers.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATA_ROOT` | `HOME` | Root of the local transcript tree. |
| `DISCORD_API` | — | Bot token. Required. `DISCORD_TOKEN` also accepted. |
| `GITHUB_TOKEN` | — | PAT with `repo` scope. Publishing is skipped if absent. |
| `GITHUB_OWNER` | `myrachane` | Archive repo owner. |
| `GITHUB_DATA_REPO` | `ScrapedDih` | Archive repo name. |
| `GITHUB_BRANCH` | `main` | Archive branch. |
| `PUSH_INTERVAL` | `300` | Seconds between publishes. |
| `GITHUB_CONNECT_TIMEOUT` | `20` | Seconds to establish a GitHub connection. |
| `GITHUB_READ_TIMEOUT` | `120` | Seconds to wait between chunks of a response, not per request. |
| `GITHUB_API_ATTEMPTS` | `3` | Attempts per GitHub call. Only timeouts, 408/429 and 5xx are retried. |
| `GITHUB_RETRY_BACKOFF` | `1` | Seconds before the first retry; doubles from there, capped at 15. |
| `GITHUB_INBOX_WARN` | `20` | Batches allowed to wait in `inbox/` before the bot warns the muncher is behind. |
| `BACKFILL_LIMIT` | `0` | Messages per channel for a backfill; 0 means the whole channel, uncapped. |
| `BACKFILL_CHUNK` | `10000` | Rows per inbox batch during a backfill, and so the merge cadence. |
| `BACKFILL_DELAY` | `0` | Seconds between history pages. Raise it if Discord throttles. |

The bot publishes once on connect and then every `PUSH_INTERVAL` seconds, so a run shorter
than the interval still leaves its messages in the archive.

### Why the read timeout is not a total

A per-request aggregate timeout fails a slow-but-healthy call for a reason that has nothing to
do with the network, and retrying it changes nothing. So a call is bounded by a connect
timeout and a per-chunk read timeout instead: a stalled socket still trips `sock_read`, while
a large response that is merely slow keeps arriving. Timeouts, 408, 429 and 5xx are retried
with backoff, and a cycle that still fails keeps the buffer and says which call gave up.

The inbox is what keeps that honest — the bot's requests are all small now, so there is no
long read left to be patient about.

## Publishing on demand

```bash
python pusher.py             # stage, push, verify, clear, then exit
python pusher.py --dry-run   # show what would be staged
python muncher.py            # merge the inbox locally, against a checkout
python backfill.py --help    # read history and append it (see above)
```

Safe to run from anywhere: pushing only ever adds new files, so an empty local buffer cannot
damage the archive, and a batch that is already there is skipped rather than duplicated. It
still needs to point at the same `DATA_ROOT` holding the unsent rows, so from the container
that is:

```bash
docker compose exec dihscrapper python pusher.py
```

`muncher.py` normally runs in CI, not locally: it needs the committed batches and the
transcripts, which only a checkout has both of.

## Tests

```bash
python tests/test_muncher.py     # merge, dedupe, sort, idempotence
python tests/test_pusher.py      # stage, push, verify, clear, retries, 404s
python tests/test_backfill.py    # history paging, chunking, replies, append-not-duplicate
python tests/test_pusher.py --live  # the same cycle against a throwaway real repo
```

Everything runs offline against a local fake GitHub and a temporary directory. `--live` is the
exception: it creates a throwaway private repository, drives the real cycle against it, and
deletes it afterwards, so the live archive is never touched.

`tests/test_muncher.py` is where the interesting assertions now live, because the merge is
plain filesystem work and no longer needs a network to be tested: a re-delivered row is not
duplicated, rows sort numerically, `user_map.csv` merges on user id, a second run changes
nothing, and no archive path ever disappears.

## Notes

- The bot archives only channels it can read, and only from the moment it connects.
- Media is intentionally not downloaded. If you need the bytes, this is the wrong tool —
  point a real downloader at the same channels and join on message ID.
- Deleted Discord messages leave orphaned `reply_to_id` values, since the parent can no
  longer be fetched. The row is still kept.
- Because the archive is a git repository, GitHub's hard size cap eventually applies to a
  long-lived deployment of a busy server. A retention policy or a larger host is the
  remedy.
