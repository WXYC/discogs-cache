-- Enrich Discogs cache: restore dropped columns, add artist detail tables
-- Run once against the existing database (post-migration 01).
-- Idempotent: safe to re-run.
--
-- Usage:
--   psql -U postgres -d discogs -f 02_enrich_data.sql

BEGIN;

-- ============================================
-- 1. Restore columns dropped by migration 01
-- ============================================

-- Full release date string (e.g. "2024-03-15")
ALTER TABLE release ADD COLUMN IF NOT EXISTS released text;

-- Discogs artist ID on release_artist (nullable for API-fetched releases)
ALTER TABLE release_artist ADD COLUMN IF NOT EXISTS artist_id integer;

-- Role for extra artists (e.g. "Producer", "Mixed By")
ALTER TABLE release_artist ADD COLUMN IF NOT EXISTS role text;

-- Restore country column on release (used by dedup ranking)
ALTER TABLE release ADD COLUMN IF NOT EXISTS country text;

-- ============================================
-- 2. Enrich release_label table
-- ============================================

ALTER TABLE release_label ADD COLUMN IF NOT EXISTS label_id integer;
ALTER TABLE release_label ADD COLUMN IF NOT EXISTS catno text;

-- ============================================
-- 3. Artist detail tables (new)
-- ============================================

CREATE TABLE IF NOT EXISTS artist (
    id         integer PRIMARY KEY,
    name       text NOT NULL,
    profile    text,
    image_url  text,
    fetched_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS artist_alias (
    artist_id  integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    alias_id   integer,
    alias_name text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artist_alias_artist_id ON artist_alias(artist_id);

CREATE TABLE IF NOT EXISTS artist_name_variation (
    artist_id  integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    name       text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artist_name_variation_artist_id ON artist_name_variation(artist_id);

CREATE TABLE IF NOT EXISTS artist_member (
    artist_id   integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    member_id   integer NOT NULL,
    member_name text NOT NULL,
    active      boolean DEFAULT true
);
CREATE INDEX IF NOT EXISTS idx_artist_member_artist_id ON artist_member(artist_id);

CREATE TABLE IF NOT EXISTS artist_url (
    artist_id integer NOT NULL REFERENCES artist(id) ON DELETE CASCADE,
    url       text NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_artist_url_artist_id ON artist_url(artist_id);

COMMIT;
