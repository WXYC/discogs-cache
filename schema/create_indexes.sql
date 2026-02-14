-- Create base trigram indexes for fuzzy text search
-- Run AFTER base data import (release, release_artist).
-- Track-related indexes are in create_track_indexes.sql (run after track import).
--
-- These indexes enable fast fuzzy matching using pg_trgm extension.

-- Ensure extension is loaded
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ============================================
-- Base trigram indexes for fuzzy text search
-- ============================================

-- 1. Artist name search on releases: "Find releases by 'New Order'"
--    Used by: search_releases_by_track() artist filter
--    Query pattern: WHERE lower(f_unaccent(artist_name)) % $1
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_artist_name_trgm
ON release_artist USING GIN (lower(f_unaccent(artist_name)) gin_trgm_ops);

-- 2. Release title search: "Find releases named 'Power, Corruption & Lies'"
--    Used by: get_release searches
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_release_title_trgm
ON release USING GIN (lower(f_unaccent(title)) gin_trgm_ops);

-- ============================================
-- Verification queries
-- ============================================

-- Check index sizes
-- SELECT
--     indexrelname AS index_name,
--     pg_size_pretty(pg_relation_size(indexrelid)) AS index_size
-- FROM pg_stat_user_indexes
-- WHERE schemaname = 'public'
-- ORDER BY pg_relation_size(indexrelid) DESC;

-- Test trigram search (should use index)
-- EXPLAIN ANALYZE
-- SELECT * FROM release_artist
-- WHERE lower(f_unaccent(artist_name)) % 'new order'
-- LIMIT 10;
