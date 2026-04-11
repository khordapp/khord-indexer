# khord-indexer

AT Protocol firehose indexer for [Khord](https://github.com/khordapp/khord).

Subscribes to the AT Protocol firehose, filters `app.khord.*` records, and writes them to a shared SQLite database. This enables the Khord app to serve a fast AppView feed and batch vote counts without querying every followed user's PDS on each page load.

Without the indexer, Khord still works — it falls back to fetching records directly from each user's PDS.

---

## What it indexes

| Collection | Action |
|---|---|
| `app.khord.song` | Upsert / delete songs |
| `app.khord.vote` | Upsert / delete votes |
| `app.khord.setlist.proposal` | Upsert / delete setlist proposals |

The firehose cursor is persisted in the database so restarts resume from where they left off.

## Schema

```
actors           — DID → handle/display name cache
songs            — shared songs with all platform URLs
votes            — up/down votes; subject_uri references a song URI
proposals        — setlist proposals from non-owners, keyed by setlist URI
registered_users — every DID that has signed in (for MAX_USERS enforcement)
banned_users     — DIDs blocked from signing in (live ban support)
instance_settings — dynamic per-instance config; overrides env var defaults
cursor           — singleton row tracking the firehose sequence number
```

See [`schema.sql`](schema.sql) for the full definition.

## Running

### Docker (recommended)

The indexer image is published to GitHub Container Registry:

```bash
docker run -d \
  --name khord-indexer \
  -v /path/to/data:/data \
  -e INDEXER_DB_PATH=/data/khord.db \
  ghcr.io/khordapp/khord-indexer:latest
```

### Docker Compose

The [khord](https://github.com/khordapp/khord) repo includes a `docker-compose.yml` that runs the app, indexer, and Caddy together:

```bash
git clone https://github.com/khordapp/khord && cd khord
cp .env.example .env   # fill in values
docker compose up -d
```

The indexer and app share a `sqlite_data` Docker volume.

### Standalone (Node.js)

```bash
npm install
INDEXER_DB_PATH=/path/to/khord.db node index.js
```

Node.js 22 or later recommended. `better-sqlite3` requires native build tools (`python3`, `make`, `g++`) — on Debian/Ubuntu: `apt install python3 make g++`.

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `INDEXER_DB_PATH` | `/data/khord.db` | Path to the SQLite database file |
| `FIREHOSE_RELAY` | `wss://bsky.network` | AT Protocol firehose relay URL |

## License

GNU Affero General Public License v3.0. See [khordapp/khord](https://github.com/khordapp/khord/blob/main/LICENSE) for details.
