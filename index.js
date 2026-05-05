// Khord AppView — AT Protocol firehose indexer
// Subscribes to the relay firehose, filters app.khord.* records,
// and writes them to SQLite for fast feed + vote-count queries.

import Database from 'better-sqlite3';
import { readFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { Firehose } from '@atproto/sync';

const __dirname = dirname(fileURLToPath(import.meta.url));

const DB_PATH  = process.env.INDEXER_DB_PATH ?? join(__dirname, '../data/khord.db');
const RELAY    = process.env.FIREHOSE_RELAY   ?? 'wss://bsky.network';

const SONG_NSID     = 'app.khord.song';
const VOTE_NSID     = 'app.khord.vote';
const PROPOSAL_NSID = 'app.khord.setlist.proposal';

// ── Database setup ────────────────────────────────────────────────────────────

const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');

const schema = readFileSync(join(__dirname, 'schema.sql'), 'utf8');
db.exec(schema);

// Schema sync — adds any columns present in schema.sql but missing from the live DB.
// Extend each array when new columns are added; no manual migration lines needed.
(function syncSchema() {
  const sync = (table, columns) => {
    const existing = new Set(
      db.prepare(`PRAGMA table_info(${table})`).all().map(r => r.name)
    );
    for (const [name, def] of columns) {
      if (!existing.has(name)) {
        console.log(`[migrate] Adding column ${table}.${name}`);
        db.exec(`ALTER TABLE ${table} ADD COLUMN ${name} ${def}`);
      }
    }
  };

  sync('songs', [
    ['album',               'TEXT'],
    ['isrc',                'TEXT'],
    ['odesli_key',          'TEXT'],
    ['spotify_url',         'TEXT'],
    ['apple_music_url',     'TEXT'],
    ['youtube_music_url',   'TEXT'],
    ['tidal_url',           'TEXT'],
    ['deezer_url',          'TEXT'],
    ['amazon_music_url',    'TEXT'],
    ['soundcloud_url',      'TEXT'],
    ['songlink_url',        'TEXT'],
    ['note',                'TEXT'],
    ['listed',              'INTEGER NOT NULL DEFAULT 1'],
    ['instance_url',        'TEXT'],
    ['thumbnail_url',       'TEXT'],
  ]);

  sync('proposals', [
    ['album',               'TEXT'],
    ['thumbnail_url',       'TEXT'],
    ['spotify_url',         'TEXT'],
    ['apple_music_url',     'TEXT'],
    ['youtube_music_url',   'TEXT'],
    ['tidal_url',           'TEXT'],
    ['deezer_url',          'TEXT'],
    ['amazon_music_url',    'TEXT'],
    ['soundcloud_url',      'TEXT'],
    ['songlink_url',        'TEXT'],
    ['note',                'TEXT'],
  ]);

  sync('actors', [
    ['handle',              'TEXT'],
    ['display_name',        'TEXT'],
    ['avatar',              'TEXT'],
  ]);
})();

// ── Prepared statements ───────────────────────────────────────────────────────

const upsertActor = db.prepare(`
  INSERT INTO actors(did) VALUES(@did)
  ON CONFLICT(did) DO NOTHING
`);

const setActorHandle = db.prepare(`
  UPDATE actors SET handle = @handle WHERE did = @did
`);

const upsertSong = db.prepare(`
  INSERT INTO songs(
    uri, cid, actor_did, title, artist, album, isrc, odesli_key,
    thumbnail_url,
    spotify_url, apple_music_url, youtube_music_url, tidal_url,
    deezer_url, amazon_music_url, soundcloud_url, songlink_url,
    note, listed, instance_url, created_at
  ) VALUES(
    @uri, @cid, @actor_did, @title, @artist, @album, @isrc, @odesli_key,
    @thumbnail_url,
    @spotify_url, @apple_music_url, @youtube_music_url, @tidal_url,
    @deezer_url, @amazon_music_url, @soundcloud_url, @songlink_url,
    @note, @listed, @instance_url, @created_at
  )
  ON CONFLICT(uri) DO UPDATE SET
    cid             = excluded.cid,
    title           = excluded.title,
    artist          = excluded.artist,
    album           = excluded.album,
    thumbnail_url   = excluded.thumbnail_url,
    spotify_url     = excluded.spotify_url,
    apple_music_url = excluded.apple_music_url,
    songlink_url    = excluded.songlink_url,
    listed          = excluded.listed,
    instance_url    = excluded.instance_url,
    indexed_at      = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
`);

const deleteSong = db.prepare(`DELETE FROM songs WHERE uri = @uri`);

const upsertVote = db.prepare(`
  INSERT INTO votes(uri, cid, actor_did, subject_uri, direction, created_at)
  VALUES(@uri, @cid, @actor_did, @subject_uri, @direction, @created_at)
  ON CONFLICT(uri) DO UPDATE SET
    cid        = excluded.cid,
    direction  = excluded.direction,
    indexed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
`);

const deleteVote = db.prepare(`DELETE FROM votes WHERE uri = @uri`);

const upsertProposal = db.prepare(`
  INSERT INTO proposals(
    uri, cid, proposer_did, setlist_uri, setlist_cid,
    title, artist, album, thumbnail_url,
    spotify_url, apple_music_url, youtube_music_url, tidal_url,
    deezer_url, amazon_music_url, soundcloud_url, songlink_url,
    note, created_at
  ) VALUES(
    @uri, @cid, @proposer_did, @setlist_uri, @setlist_cid,
    @title, @artist, @album, @thumbnail_url,
    @spotify_url, @apple_music_url, @youtube_music_url, @tidal_url,
    @deezer_url, @amazon_music_url, @soundcloud_url, @songlink_url,
    @note, @created_at
  )
  ON CONFLICT(uri) DO UPDATE SET
    cid        = excluded.cid,
    note       = excluded.note,
    indexed_at = strftime('%Y-%m-%dT%H:%M:%SZ', 'now')
`);

const deleteProposal = db.prepare(`DELETE FROM proposals WHERE uri = @uri`);

const getCursor  = db.prepare(`SELECT seq FROM cursor WHERE id = 1`);
const setCursor  = db.prepare(`UPDATE cursor SET seq = @seq WHERE id = 1`);

// ── Firehose handler ──────────────────────────────────────────────────────────

function handleEvent(evt) {
  const { did, collection, event: action, cid, record, seq } = evt;
  const uri = evt.uri.toString();

  if (collection === SONG_NSID) {
    if (action === 'create' || action === 'update') {
      const r = record;
      upsertActor.run({ did });
      upsertSong.run({
        uri,
        cid:               cid?.toString() ?? '',
        actor_did:         did,
        title:             r.title        ?? '',
        artist:            r.artist       ?? '',
        album:             r.album        ?? null,
        isrc:              r.isrc         ?? null,
        odesli_key:        r.odesliKey    ?? null,
        spotify_url:       r.spotifyUrl   ?? null,
        apple_music_url:   r.appleMusicUrl ?? null,
        youtube_music_url: r.youtubeMusicUrl ?? null,
        tidal_url:         r.tidalUrl     ?? null,
        deezer_url:        r.deezerUrl    ?? null,
        amazon_music_url:  r.amazonMusicUrl ?? null,
        soundcloud_url:    r.soundcloudUrl ?? null,
        songlink_url:      r.songlinkUrl   ?? null,
        thumbnail_url:     r.thumbnailUrl  ?? null,
        note:              r.note          ?? null,
        listed:            r.listed === false ? 0 : 1,
        instance_url:      r.instanceUrl   ?? null,
        created_at:        r.createdAt     ?? new Date().toISOString(),
      });
    } else if (action === 'delete') {
      deleteSong.run({ uri });
    }
  }

  if (collection === PROPOSAL_NSID) {
    if (action === 'create' || action === 'update') {
      const r = record;
      const s = r.snapshot ?? {};
      if (!r.setlistUri) return;
      upsertActor.run({ did });
      upsertProposal.run({
        uri,
        cid:               cid?.toString() ?? '',
        proposer_did:      did,
        setlist_uri:       r.setlistUri,
        setlist_cid:       r.setlistCid ?? '',
        title:             s.title      ?? '',
        artist:            s.artist     ?? '',
        album:             s.album      ?? null,
        thumbnail_url:     s.thumbnailUrl   ?? null,
        spotify_url:       s.spotifyUrl     ?? null,
        apple_music_url:   s.appleMusicUrl  ?? null,
        youtube_music_url: s.youtubeMusicUrl ?? null,
        tidal_url:         s.tidalUrl        ?? null,
        deezer_url:        s.deezerUrl       ?? null,
        amazon_music_url:  s.amazonMusicUrl  ?? null,
        soundcloud_url:    s.soundcloudUrl   ?? null,
        songlink_url:      s.songlinkUrl     ?? null,
        note:              r.note            ?? null,
        created_at:        r.createdAt       ?? new Date().toISOString(),
      });
    } else if (action === 'delete') {
      deleteProposal.run({ uri });
    }
  }

  if (collection === VOTE_NSID) {
    if (action === 'create' || action === 'update') {
      const r = record;
      const subject_uri = r.subject?.uri ?? null;
      if (!subject_uri) return;
      upsertActor.run({ did });
      upsertVote.run({
        uri,
        cid:         cid?.toString() ?? '',
        actor_did:   did,
        subject_uri,
        direction:   r.direction ?? 'up',
        created_at:  r.createdAt ?? new Date().toISOString(),
      });
    } else if (action === 'delete') {
      deleteVote.run({ uri });
    }
  }

  if (seq != null) setCursor.run({ seq });
}

// ── PDS backfill + handle resolution ─────────────────────────────────────────
// On startup:
//   resolveHandles()      — fills actors.handle for every DID that has none.
//                           Handles aren't in firehose commit events, so this
//                           is the only way to populate them.
//   backfillMissingUsers() — fetches up to 50 recent songs from the PDS of any
//                           registered user with no songs in the DB. Covers
//                           history outside the firehose retention window.

const BACKFILL_LIMIT = 50;
const BACKFILL_DELAY = 200; // ms between outbound requests

async function resolveDidDoc(did) {
  try {
    if (did.startsWith('did:plc:')) {
      const res = await fetch(`https://plc.directory/${did}`);
      if (!res.ok) return null;
      return await res.json();
    }
    if (did.startsWith('did:web:')) {
      const domain = did.slice('did:web:'.length);
      const res = await fetch(`https://${domain}/.well-known/did.json`);
      if (!res.ok) return null;
      return await res.json();
    }
  } catch { /* unreachable */ }
  return null;
}

async function resolvePds(did) {
  const doc = await resolveDidDoc(did);
  const svc = doc?.service?.find(s => s.type === 'AtprotoPersonalDataServer');
  return svc?.serviceEndpoint ?? null;
}

async function backfillUser(did) {
  const pds = await resolvePds(did);
  if (!pds) return 0;

  const url = `${pds}/xrpc/com.atproto.repo.listRecords?repo=${encodeURIComponent(did)}&collection=app.khord.song&limit=${BACKFILL_LIMIT}`;
  const res = await fetch(url);
  if (!res.ok) return 0;

  const { records } = await res.json();
  if (!records?.length) return 0;

  let count = 0;
  for (const rec of records) {
    const r = rec.value;
    upsertActor.run({ did });
    upsertSong.run({
      uri:               rec.uri,
      cid:               rec.cid,
      actor_did:         did,
      title:             r.title           ?? '',
      artist:            r.artist          ?? '',
      album:             r.album           ?? null,
      isrc:              r.isrc            ?? null,
      odesli_key:        r.odesliKey       ?? null,
      thumbnail_url:     r.thumbnailUrl    ?? null,
      spotify_url:       r.spotifyUrl      ?? null,
      apple_music_url:   r.appleMusicUrl   ?? null,
      youtube_music_url: r.youtubeMusicUrl ?? null,
      tidal_url:         r.tidalUrl        ?? null,
      deezer_url:        r.deezerUrl       ?? null,
      amazon_music_url:  r.amazonMusicUrl  ?? null,
      soundcloud_url:    r.soundcloudUrl   ?? null,
      songlink_url:      r.songlinkUrl     ?? null,
      note:              r.note            ?? null,
      listed:            r.listed === false ? 0 : 1,
      instance_url:      r.instanceUrl     ?? null,
      created_at:        r.createdAt       ?? new Date().toISOString(),
    });
    count++;
  }
  return count;
}

async function resolveHandles() {
  const actors = db.prepare(`SELECT did FROM actors WHERE handle IS NULL`).all();
  if (actors.length === 0) return;

  console.log(`[handles] ${actors.length} actor(s) missing handle — resolving…`);
  let resolved = 0;

  for (const { did } of actors) {
    try {
      const doc = await resolveDidDoc(did);
      const aka = doc?.alsoKnownAs?.[0];
      const handle = aka?.startsWith('at://') ? aka.slice(5) : null;
      if (handle) {
        setActorHandle.run({ handle, did });
        resolved++;
      }
    } catch (e) {
      console.warn(`[handles] ${did}: ${e.message}`);
    }
    await new Promise(r => setTimeout(r, BACKFILL_DELAY));
  }

  console.log(`[handles] resolved ${resolved}/${actors.length}`);
}

async function backfillAllUsers() {
  const users = db.prepare(`SELECT did FROM registered_users`).all();

  if (users.length === 0) return;

  console.log(`[backfill] ${users.length} registered user(s) — fetching up to ${BACKFILL_LIMIT} recent songs each`);

  let total = 0;
  for (const { did } of users) {
    try {
      const count = await backfillUser(did);
      if (count > 0) {
        console.log(`[backfill] ${did}: upserted ${count} song(s)`);
        total += count;
      }
    } catch (e) {
      console.warn(`[backfill] ${did}: failed — ${e.message}`);
    }
    await new Promise(r => setTimeout(r, BACKFILL_DELAY));
  }

  console.log(`[backfill] done — ${total} song(s) upserted across ${users.length} user(s)`);
}

// ── Start ─────────────────────────────────────────────────────────────────────

const { seq } = getCursor.get();
console.log(`[indexer] starting from cursor seq=${seq}, relay=${RELAY}`);

const firehose = new Firehose({
  relay: RELAY,
  cursor: seq > 0 ? seq : undefined,
  unauthenticatedCommits: true,
  excludeIdentity: true,
  excludeAccount: true,
  excludeSync: true,
  async handleEvent(evt) {
    try {
      handleEvent(evt);
    } catch (e) {
      console.error('[indexer] error handling event:', e);
    }
  },
  onError(err) {
    console.error('[indexer] firehose error:', err);
  },
});

await resolveHandles();
await backfillAllUsers();

firehose.start();
console.log('[indexer] firehose connected');

// Re-run handle resolution every 6 hours to pick up handle changes
setInterval(resolveHandles, 6 * 60 * 60 * 1000);

// Graceful shutdown
process.on('SIGTERM', () => {
  console.log('[indexer] shutting down');
  firehose.destroy();
  db.close();
  process.exit(0);
});
