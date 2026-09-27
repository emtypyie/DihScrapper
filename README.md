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
- **Media is never stored.** Attachments are not downloaded at all. An image is recorded
  as the literal string `[potential image]`, so the archive stays pure text and the scraper
  stays cheap to run.
- **Text only, no binaries.** Every file in the archive is a small JSON document. There are
  no images, videos, or blobs to bloat the repository.

Because the archive lives in Git, the scraper can be killed, redeployed, or moved to
another host and pick up exactly where it left off.

## How it works

The bot runs a single event loop over the channels it has been invited to.

1. **Listen.** `on_message` fires for every new message. Messages authored by the bot
   itself are dropped so it never archives its own output.
2. **Build a record.** Each message becomes one JSON object: snowflake ID, ISO timestamp,
   author name and ID, content, an `attachments` list, and a `reply_to` block when the
   message answers another.
3. **Note attachments, not bytes.** An image attachment contributes the placeholder
   `[potential image]` to `attachments`. Any other kind of file contributes its CDN URL, so
   the reference survives even though the bytes were never fetched.
4. **Track the directory.** Each server keeps a `user_map.json` of `username → user_id`, so
   author names stay resolvable to stable identities after display names change.
5. **Publish.** Every `PUSH_INTERVAL` seconds the local JSON is committed to the archive
   repository.

Transcript writes are atomic — staged to a temporary file and renamed into place — so a
crash mid-write cannot leave a half-written log behind.

## Publishing model

The archive is written through the GitHub git data API rather than a local git worktree. A
publish run:

1. Reads the current branch ref and walks the remote tree to learn every tracked path and
   its blob hash.
2. Hashes each local file the same way git does. Files whose hash already matches the
   remote are skipped, so a steady-state sync costs two API calls no matter how large the
   archive grows.
3. Uploads blobs for anything new or changed.
4. Posts a tree layered on the current remote tree, then a commit, then a ref update.

Because the local `HOME` tree is treated as the source of truth, a path that disappears
locally is deleted from the archive on the next publish.

### Starting from an empty repository

GitHub will not create a blob in a repository that has no commits, so a brand-new archive
is seeded before its first publish. If the repository already has commits but the
configured branch does not, the branch is simply pointed at the default branch's head. If
there are no commits at all, a seed file is committed through the contents API first, and
only then are blobs created.

## Data layout

In the archive repository:

```text
TestServer/
├── general.json
└── user_map.json
```

Server and channel names are stripped to `[A-Za-z0-9_-]` and capped at 64 characters.

Locally, the same layout minus the repository:

```text
HOME/
└── [Sanitized_Server_Name]/
    ├── [Sanitized_Channel_Name].json
    └── user_map.json
```

Each entry in a channel transcript:

```json
{
  "id": 1234567890,
  "timestamp": "2026-09-27T20:18:00+00:00",
  "author": "username_here",
  "author_id": 9876543210,
  "content": "Message string text here",
  "attachments": ["[potential image]"],
  "reply_to": {
    "message_id": 1122334455,
    "author_id": 5544332211,
    "author_username": "original_poster",
    "content_snippet": "This was the text being replied to..."
  }
}
```

`attachments` is always present and is empty when the message had none. `[potential image]`
is a fixed marker, not a path — nothing is fetched behind it.

## Repositories

| Repository | Visibility | Contents |
| --- | --- | --- |
| `DihScrapper` | Public | This code. `HOME/` is gitignored and never enters it. |
| `ScrapedDih` | Private | The archive. JSON transcripts only. |

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
python upload_data.py             # publish once, then exit
python upload_data.py --dry-run   # show what would change
```

`--dry-run` reports pending additions, updates, and removals without committing.

## Tests

```bash
python tests/test_publish.py
```

Creates a throwaway private repository, publishes to it, asserts the transcript reaches the
tree with the correct blob hash, that no media or local bookkeeping file is committed, that
unchanged data is a no-op, and that an edited transcript produces exactly one new commit.
The repository is deleted afterwards, so the live archive is never touched.

## Notes

- The bot archives only channels it can read, and only from the moment it connects.
- Media is intentionally dropped. If you need the bytes, this is the wrong tool — point a
  real downloader at the same channels and join on message ID.
- Deleted Discord messages leave orphaned `reply_to` IDs. Those references resolve to
  `null` rather than dropping the message.
- Because the archive is a git repository, GitHub's hard size cap eventually applies to a
  long-lived deployment of a busy server. A retention policy or a larger host is the
  remedy.
