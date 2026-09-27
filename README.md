# DihScrapper

by emtypyie

Active Discord Chat Scraper to train ML MODELS
lol.

---

DihScrapper is a self-hosted Discord bot that mirrors messages into a private GitHub
repository as they arrive. Point it at a server, invite it, and it builds a timestamped
JSON transcript of the conversation — text, replies, attachments, and the user directory
that ties them together. Point a training pipeline at the repo and you have a dataset that
keeps itself current.

Two properties shape the design:

- **It captures the live stream only.** There is no backfill. Nothing that was said before
  the bot connected is ever read. The archive begins when the bot starts.
- **Media never touches the local disk.** Attachments are uploaded straight into the
  archive repository as git blobs. The container's filesystem holds JSON and nothing else,
  so the volume stays small no matter how much media flows through.

Because the archive lives in Git, the scraper can be killed, redeployed, or moved to
another host and pick up exactly where it left off.

## How it works

The bot runs a single event loop over the channels it has been invited to.

1. **Listen.** `on_message` fires for every new message. Messages authored by the bot
   itself are dropped so it never archives its own output.
2. **Build a record.** Each message becomes one JSON object: snowflake ID, ISO timestamp,
   author name and ID, content, archived attachment paths, and a `reply_to` block when the
   message answers another.
3. **Archive media.** Images stream into memory and are committed to the archive as git
   blobs, then discarded. Nothing is written to disk. Anything over `MEDIA_MAX_SIZE` is
   never uploaded — its CDN URL is recorded instead and the message is tagged
   `media_size_exceeded`.
4. **Track the directory.** Each server keeps a `user_map.json` of `username → user_id`, so
   author names stay resolvable to stable identities after display names change.
5. **Publish.** Every `PUSH_INTERVAL` seconds the local JSON is committed to the archive
   repository, alongside every media blob recorded so far.

Transcript writes are atomic — staged to a temporary file and renamed into place — so a
crash mid-write cannot leave a half-written log behind.

## Publishing model

The archive is written through the GitHub git data API rather than a local git worktree.
A publish run:

1. Reads the current branch ref and walks the remote tree to learn every tracked path and
   its blob hash.
2. Hashes each local file the same way git does. Files whose hash already matches the
   remote are skipped, so a steady-state sync costs two API calls no matter how large the
   archive grows.
3. Uploads blobs for anything new or changed, including media blobs recorded earlier.
4. Posts a tree layered on the current remote tree, then a commit, then a ref update.

Media blobs are recorded in `HOME/.media-index.json` as path-to-hash pairs. That index is
local bookkeeping and is never published, which is what lets a later commit reference media
whose bytes are long gone.

Two GitHub quirks are handled explicitly, both of which only appear on a brand-new archive:
an empty repository answers ref lookups with `409` rather than `404`, and refuses blob
creation outright, so the first blob bootstraps the repository with an initial commit
through the contents API.

## Data layout

In the archive repository:

```text
TestServer/
├── general.json
├── user_map.json
└── mediapool/
    ├── 1234567890_image.png
    └── 1234567891_photo.jpg
```

Server and channel names are stripped to `[A-Za-z0-9_-]` and capped at 64 characters.

On disk, only the JSON exists:

```text
HOME/
└── [Sanitized_Server_Name]/
    ├── [Sanitized_Channel_Name].json
    ├── user_map.json
    └── .media-index.json
```

Each entry in a channel transcript:

```json
{
  "id": 1234567890,
  "timestamp": "2026-09-27T20:18:00+00:00",
  "author": "username_here",
  "author_id": 9876543210,
  "content": "Message string text here",
  "attachments": ["Otaku_Valley/mediapool/1234567890_image.png"],
  "reply_to": {
    "message_id": 1122334455,
    "author_id": 5544332211,
    "author_username": "original_poster",
    "content_snippet": "This was the text being replied to..."
  },
  "media_size_exceeded": ["https://cdn.discordapp.com/attachments/.../huge.png"]
}
```

`media_size_exceeded` only appears when at least one attachment was too large to archive.

## Repositories

| Repository | Visibility | Contents |
| --- | --- | --- |
| `DihScrapper` | Public | This code. `HOME/` is gitignored and never enters it. |
| `ScrapedDih` | Private | The archive. JSON transcripts plus media blobs. |

`HOME/` belongs exclusively to the private archive and is gitignored in the code repo.
The code lives on the main account; the archive lives on whichever account
`GITHUB_OWNER` points at.

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
| `MEDIA_MAX_SIZE` | `25` | Max archived attachment size, in MiB. |
| `PUSH_INTERVAL` | `300` | Seconds between publishes. |

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

Creates a throwaway branch, asserts media reaches the tree as a blob while no media bytes
land on disk, that unchanged data is a no-op, and that an edited transcript produces
exactly one new commit with the expected blob hash. The branch is deleted afterwards.

## Notes

- The bot archives only channels it can read, and only from the moment it connects.
- Attachment downloads are capped in memory as they stream, so an oversized file is
  abandoned without ever being fully buffered.
- Deleted Discord messages leave orphaned `reply_to` IDs. Those references resolve to
  `null` rather than dropping the message.
- Because the archive is a git repository, GitHub's hard size cap eventually applies to a
  long-lived deployment of a busy server. A retention policy or a larger host is the
  remedy.
