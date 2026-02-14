-- Create immutable wrapper for unaccent() to allow use in index expressions.
--
-- PostgreSQL's built-in unaccent() is STABLE (depends on search_path), so it
-- can't be used directly in index expressions which require IMMUTABLE functions.
-- This wrapper pins the dictionary to public.unaccent, removing the search_path
-- variability.
--
-- Run AFTER create_database.sql (which creates the unaccent extension).
-- Run BEFORE create_indexes.sql (which uses f_unaccent in index expressions).

CREATE OR REPLACE FUNCTION f_unaccent(text) RETURNS text AS $$
  SELECT public.unaccent('public.unaccent', $1)
$$ LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT;
