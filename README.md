# DihScrapper

by emtypyie

Active Discord Chat Scraper to train ML MODELS
lol.

---

DihScrapper is a self-hosted Discord bot that mirrors every message it can see into a
private GitHub repository. Point it at a server, invite it, and it quietly builds a
timestamped JSON archive of the conversation — text, replies, attachments, and the user
directory that ties them together. Point a training pipeline at the repo and you have a
dataset that keeps itself current.

Because the archive lives in Git rather than on a disk you have to babysit, the scraper
can be killed, redeployed, or moved to another host at any time and pick up exactly where
it left off.

## How it works

The bot runs an event-driven loop over the channels it has been invited to.

1. **Listen.** `on_message` fires for every new message. Messages authored by the bot
   itself are dropped so it never archives its own output.
2. **Backfill.** On startup and after any gateway resume, each channel is walked from the
   highest message ID already in the local log. Channels with no local history start from
   the most recent `CATCHUP_LIMIT` messages. An outage of any length closes itself.
3. **Build a record.** Each message becomes one JSON object: snowflake ID, ISO timestamp,
   author name and ID, content, archived attachment paths, and a `reply_to` block when the
   message answers another.
4. **Archive media.** Images stream into the server's `mediapool/` under a filename
   prefixed with the Discord attachment ID, which makes collisions impossible. Anything
   over `MEDIA_MAX_SIZE` is never written to disk — its CDN URL is recorded instead and
   the message is tagged `media_size_exceeded`.
5. **Track the directory.** Each server keeps a `user_map.json` of `username → user_id`,
   so author names stay resolvable to stable identities after display names change.
6. **Push.** Every `PUSH_INTERVAL` seconds the data directory is committed and pushed to
   the private archive repository.

Writes are atomic — a log is staged to a temporary file and renamed into place — so a
crash mid-write cannot leave a half-written archive behind.

## Data layout

```text
HOME/
└── [Sanitized_Server_Name]/
    ├── [Sanitized_Channel_Name].json
    ├── user_map.json
    └── mediapool/
        ├── [Attachment_ID]_[Filename].png
        └── [Attachment_ID]_[Filename].jpg
```

Server and channel names are stripped to `[A-Za-z0-9_-]` and capped at 64 characters.

Each entry in a channel log:

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
| `ScrapedDih` | Private | The archive. `HOME/` is its root and git worktree. |

The two are fully separate repositories. The archive is pushed by a dedicated sync routine
that treats the data directory as its own git worktree with `ScrapedDih` as `origin`, so
media is never copied twice and pushes stay incremental. Credentials are supplied to git
through an askpass helper, which keeps the token out of `.git/config`, out of process
arguments, and off disk.

Before syncing, the routine fetches the remote and rebases local commits onto it. If the
rebase conflicts it resets to the remote state instead — the archive is reconstructible
from Discord, so remote history always wins.

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
GITHUB_TOKEN=<pat with repo scope>
GITHUB_OWNER=emtypyie
GITHUB_DATA_REPO=ScrapedDih
GITHUB_BRANCH=main
```

### Discord setup

In the Developer Portal, enable both **Message Content Intent** and **Server Members
Intent**, then invite the bot with permission to view channels, read message history, and
read message content. A bot without the Message Content Intent receives empty `content`
fields and the archive will be worthless.

### Running in Docker

```bash
docker compose up -d --build
docker compose logs -f
```

Data persists in the named `home` volume mounted at `/app/HOME`, so the archive survives
container rebuilds. `restart: unless-stopped` brings the bot back after a host reboot, and
`SIGTERM` triggers a clean gateway shutdown rather than a kill.

## Configuration

All optional — the defaults are fine for most servers.

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATA_ROOT` | `HOME` | Root of the archive on disk. |
| `DISCORD_API` | — | Bot token. Required. `DISCORD_TOKEN` also accepted. |
| `GITHUB_TOKEN` | — | PAT with `repo` scope. Sync is skipped if absent. |
| `GITHUB_OWNER` | `emtypyie` | Archive repo owner. |
| `GITHUB_DATA_REPO` | `ScrapedDih` | Archive repo name. |
| `GITHUB_BRANCH` | `main` | Archive branch. |
| `MEDIA_MAX_SIZE` | `25` | Max archived attachment size, in MiB. |
| `CATCHUP_LIMIT` | `200` | Messages pulled when a channel has no local history. |
| `CATCHUP_MAX` | `5000` | Ceiling on a single backfill pass. |
| `PUSH_INTERVAL` | `300` | Seconds between archive pushes. |
| `REPO_SOFT_SIZE_LIMIT` | `1073741824` | Warn when the archive exceeds this many bytes. |

`REPO_SOFT_SIZE_LIMIT` is only a warning. GitHub rejects repositories past its hard size
cap, so a long-lived deployment of a busy server will eventually need a larger host or a
retention policy.

## Syncing on demand

```bash
python upload_data.py             # one sync, then exit
python upload_data.py --dry-run   # show what would be pushed
python upload_data.py --loop      # sync every PUSH_INTERVAL seconds
```

`--dry-run` reports pending changes without committing or pushing.

## Notes

- The bot archives only channels it can read. It skips any channel where it lacks
  permission to read history rather than failing.
- Attachment downloads stream to disk in chunks and abort the moment they cross the size
  cap, so an oversized file never lands in the archive.
- Deleted Discord messages leave orphaned `reply_to` IDs. Those references resolve to
  `null` rather than dropping the message.
