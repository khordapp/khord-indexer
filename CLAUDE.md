# khord-indexer

AT Protocol firehose subscriber for [Khord](https://github.com/khordapp/khord). Subscribes to the relay firehose, filters `app.khord.*` records, and writes them to a shared SQLite database so the Khord app can serve a fast AppView feed and batch vote counts.

The Khord app falls back to direct PDS fetches when the DB is unavailable (API routes return 503 as the signal).

## Files

```
index.js    # Firehose subscriber — handles create/update/delete for songs, votes, proposals
schema.sql  # SQLite schema — applied at startup via db.exec(); idempotent (IF NOT EXISTS)
Dockerfile  # Node 22 alpine; installs python3/make/g++ for better-sqlite3 native bindings
```

## What gets indexed

| Collection | Tables written |
|---|---|
| `app.khord.song` | `actors`, `songs` |
| `app.khord.vote` | `actors`, `votes` |
| `app.khord.setlist.proposal` | `actors`, `proposals` |

## Schema tables

- `actors` — DID cache; populated on first seen record from a DID
- `songs` — all platform URLs + metadata; `listed = 0` rows excluded from feed queries
- `votes` — direction (`up`/`down`), subject song URI
- `proposals` — setlist proposals with embedded song snapshot; keyed by `setlist_uri`
- `registered_users` — every DID that has signed in; used for `MAX_USERS` enforcement
- `banned_users` — DIDs blocked from signing in; written by the admin UI, read by the app
- `instance_settings` — dynamic per-instance config (album art, registration, user cap); overrides env var defaults without a restart
- `cursor` — singleton row; firehose sequence number; persisted so restarts resume cleanly

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `INDEXER_DB_PATH` | `/data/khord.db` | Path to the SQLite database file — must match `INDEXER_DB_PATH` in the Khord app |
| `FIREHOSE_RELAY` | `wss://bsky.network` | AT Protocol firehose relay URL |

## Key decisions

- `better-sqlite3` (synchronous) used deliberately — firehose events are processed one at a time; async SQLite would add complexity with no benefit here
- Schema applied via `db.exec(schema.sql)` at startup — all `CREATE TABLE IF NOT EXISTS`, so safe to run against an existing DB
- Inline migrations for `instance_url` and `listed` columns (ALTER TABLE wrapped in try/catch) handle upgrading existing DBs without a migration framework
- Cursor persisted after every event so a crash loses at most one record, not the entire backlog
- `excludeIdentity`, `excludeAccount`, `excludeSync` flags on the Firehose client — only commit events reach `handleEvent`
- Graceful shutdown on `SIGTERM`: destroys firehose connection, closes DB, exits 0
