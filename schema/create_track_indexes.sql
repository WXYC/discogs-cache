-- Create track-related FK constraints, FK indexes, and trigram indexes.
-- Run AFTER track import (release_track, release_track_artist).
--
-- Base indexes are in create_indexes.sql (run after base import).
-- This file is idempotent: safe to run on resume.

-- Ensure extension is loaded
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================
-- FK constraints (idempotent via DO blocks)
-- ============================================

DO $$
BEGIN
    ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release
        FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$
BEGIN
    ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release
        FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE;
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- ============================================
-- FK indexes
-- ============================================

CREATE INDEX IF NOT EXISTS idx_release_track_release_id
ON release_track(release_id);

CREATE INDEX IF NOT EXISTS idx_release_track_artist_release_id
ON release_track_artist(release_id);

-- ============================================
-- Trigram indexes for fuzzy text search
-- ============================================

-- Track title search: "Find releases containing track 'Blue Monday'"
--    Used by: search_releases_by_track()
--    Query pattern: WHERE lower(f_unaccent(title)) % $1
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_title_trgm
ON release_track USING GIN (lower(f_unaccent(title)) gin_trgm_ops);

-- Track artist search: "Find compilation tracks by 'Joy Division'"
--    Used by: validate_track_on_release() for compilations
--    Query pattern: WHERE lower(f_unaccent(artist_name)) % $1
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_track_artist_name_trgm
ON release_track_artist USING GIN (lower(f_unaccent(artist_name)) gin_trgm_ops);
