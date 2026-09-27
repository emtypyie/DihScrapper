# DihScrapper

by emtypyie

Active Discord Chat Scraper to train ML MODELS
lol.

---

DihScrapper is a self-hosted Discord bot that mirrors messages into a private GitHub
repository as they arrive. Point it at a server, invite it, and it builds a timestamped
JSON transcript of the conversation — text, replies, and the user directory that ties
them together. Point a training pipeline at the repo and you have a dataset that keeps
itself current.

Three properties shape the design:

- **It captures the live stream only.** There is no backfill. Nothing that was said before
  the bot connected is ever read. The archive begins when the bot starts.
- **Media is recorded by reference, never downloaded.** Images, videos, and files are all
  stored as their CDN URL. Nothing is fetched, and nothing but a URL is kept.
- **The archive is append-only.** Every cycle appends newly captured messages, verifies they
  landed, and only then clears the local buffer. Nothing in the archive is ever rewritten
  or deleted, so the local disk is disposable staging and the repository is the record.

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
4. **Push, verify, clear.** Every `PUSH_INTERVAL` seconds the buffer is appended to the
   archive, read back to confirm, and only then cleared.

Rows are keyed on message ID throughout, so the whole cycle is idempotent: a retry after a
failed push re-appends the same rows and changes nothing.

## The publish cycle

Each interval runs three steps, and the third is gated on the second.

1. **Append.** For every local CSV, read the published copy from the archive, drop any row
   whose key is already there, sort by ID, and commit the result as one tree and one
   commit. No deletion entry is ever emitted, so the archive can only grow.
2. **Verify.** Re-read the archive from GitHub and confirm every key just committed is
   actually present. Anything missing aborts the cycle.
3. **Clear.** Remove exactly the rows that were confirmed, by key. A message that arrives
   mid-cycle is therefore not lost — it is simply not in the cleared set and goes out on
   the next interval.

If step 1 or 2 fails the buffer is left completely intact and the next interval retries.

## Publishing model

Commits are assembled through the GitHub git data API: blobs, then a tree layered on the
current remote tree, then a commit, then a ref update. Files are hashed the way git hashes
them, so unchanged content is skipped and a steady-state cycle costs a couple of API calls
no matter how large the archive grows.

### Starting from an empty repository

GitHub will not create a blob in a repository that has no commits, so a brand-new archive
is seeded before its first append. If the repository already has commits but the configured
branch does not, the branch is pointed at the default branch's head instead.

## Data layout

In the archive repository:

```text
TestServer/
├── general.csv
└── user_map.csv
```

Server and channel names are stripped to `[A-Za-z0-9_-]` and capped at 64 characters.

Locally, the same layout, holding only unsent rows:

```text
HOME/
└── [Sanitized_Server_Name]/
    ├── [Sanitized_Channel_Name].csv
    └── user_map.csv
```

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
| `ScrapedDih` | Private | The archive. CSV transcripts and user maps. |

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

The bot publishes once on connect and then every `PUSH_INTERVAL` seconds, so a run shorter
than the interval still leaves its messages in the archive.

## Publishing on demand

```bash
python upload_data.py             # append, verify, clear, then exit
python upload_data.py --dry-run   # show what would be appended
```

Safe to run from anywhere: publishing only ever appends, so an empty local buffer cannot
damage the archive. It still needs to point at the same `DATA_ROOT` holding the unsent
rows, so from the container that is:

```bash
docker compose exec dihscrapper python upload_data.py
```

## Tests

```bash
python tests/test_publish.py
```

Creates a throwaway private repository and drives the real append → verify → clear cycle
against it, asserting that image and video attachments are stored as their URLs, that
nothing is ever deleted, that a row arriving mid-publish survives the clear, and that a
re-push of identical rows is a no-op. The repository is deleted afterwards, so the live
archive is never touched.

## Notes

- The bot archives only channels it can read, and only from the moment it connects.
- Media is intentionally not downloaded. If you need the bytes, this is the wrong tool —
  point a real downloader at the same channels and join on message ID.
- Deleted Discord messages leave orphaned `reply_to_id` values, since the parent can no
  longer be fetched. The row is still kept.
- Because the archive is a git repository, GitHub's hard size cap eventually applies to a
  long-lived deployment of a busy server. A retention policy or a larger host is the
  remedy.
