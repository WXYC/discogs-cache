# Claude Code Instructions for discogs-cache

## Project Overview

ETL pipeline for building and maintaining a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. The cache database is a shared resource consumed by multiple services:

- **request-parser** (Python/FastAPI) - `discogs/cache_service.py` queries the cache for album lookups
- **Backend-Service** (TypeScript/Node.js) - future consumer for Discogs data

## Architecture

### Pipeline Steps

1. **Download** Discogs monthly data dumps (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html
2. **Convert** XML to CSV using [discogs-xml2db](https://github.com/philipmat/discogs-xml2db) (not a PyPI package; must be cloned separately)
3. **Fix newlines** in CSV fields (`scripts/fix_csv_newlines.py`)
4. **Enrich** `library_artists.txt` with WXYC cross-references (`scripts/enrich_library_artists.py`, optional)
5. **Filter** CSVs to library-matching artists only (`scripts/filter_csv.py`) -- ~70% data reduction
6. **Create schema** (`schema/create_database.sql`) and **functions** (`schema/create_functions.sql`)
7. **Import** filtered CSVs into PostgreSQL (`scripts/import_csv.py`)
8. **Create indexes** including accent-insensitive trigram GIN indexes (`schema/create_indexes.sql`)
9. **Deduplicate** by master_id (`scripts/dedup_releases.py`) -- prefers US releases, then most tracks, then lowest ID
10. **Prune or Copy-to** -- one of:
    - `--prune`: delete non-matching releases in place (~89% data reduction, 3 GB -> 340 MB)
    - `--copy-to`/`--target-db-url`: copy matched releases to a separate database, preserving the full import
11. **Vacuum** to reclaim disk space (`VACUUM FULL`)

`scripts/run_pipeline.py` supports two modes:
- `--xml` mode: runs steps 2-11 (XML conversion through vacuum)
- `--csv-dir` mode: runs steps 6-11 (database build from pre-filtered CSVs)

Both modes support `--target-db-url` to copy matched releases to a separate database instead of pruning in place, and `--resume` (csv-dir only) to skip already-completed steps.

Step 1 (download) is always manual.

### master_id Column Lifecycle

The `release` table includes a `master_id` column used during import and dedup. The dedup copy-swap strategy (`CREATE TABLE AS SELECT ...` without `master_id`) drops the column automatically. After dedup, `master_id` no longer exists in the schema.

The `country` column, by contrast, is permanent -- it is included in the dedup copy-swap SELECT list and persists in the final schema for consumers.

### Database Schema (Shared Contract)

The SQL files in `schema/` define the contract between this ETL pipeline and all consumers:

- `schema/create_database.sql` -- Tables: `release`, `release_artist`, `release_track`, `release_track_artist`, `cache_metadata`; extensions: pg_trgm, unaccent
- `schema/create_functions.sql` -- `f_unaccent()` immutable wrapper for accent-insensitive index expressions
- `schema/create_indexes.sql` -- Trigram GIN indexes for accent-insensitive fuzzy text search (pg_trgm + unaccent)

Consumers connect via `DATABASE_URL_DISCOGS` environment variable.

### Docker Compose

`docker-compose.yml` provides a self-contained environment:
- **`db`** service: PostgreSQL 16 with pg_trgm + unaccent, port 5433:5432
- **`pipeline`** service: runs `scripts/run_pipeline.py` against the db

```bash
docker compose up --build   # full pipeline (needs data/ and discogs-xml2db/)
docker compose up db -d     # just the database (for tests)
```

### Key Files

- `scripts/run_pipeline.py` -- Pipeline orchestrator (--xml for steps 2-11, --csv-dir for steps 6-11)
- `scripts/enrich_library_artists.py` -- Enrich artist list with WXYC cross-references (pymysql)
- `scripts/filter_csv.py` -- Filter Discogs CSVs to library artists
- `scripts/import_csv.py` -- Import CSVs into PostgreSQL (psycopg COPY)
- `scripts/dedup_releases.py` -- Deduplicate releases by master_id, preferring US releases (copy-swap with `DROP CASCADE`)
- `scripts/verify_cache.py` -- Multi-index fuzzy matching for KEEP/PRUNE classification; `--copy-to` streams matches to a target DB
- `scripts/csv_to_tsv.py` -- CSV to TSV conversion utility
- `scripts/fix_csv_newlines.py` -- Fix multiline CSV fields
- `lib/matching.py` -- Compilation detection utility
- `lib/pipeline_state.py` -- Pipeline state tracking for resumable runs
- `lib/db_introspect.py` -- Database introspection for inferring pipeline state on resume

### External Inputs

Two files are inputs to the ETL but produced by request-parser:

1. **`library_artists.txt`** -- One artist name per line, used by `filter_csv.py`
2. **`library.db`** -- SQLite database, used by `verify_cache.py` for KEEP/PRUNE classification

Both are produced by request-parser's library sync (`scripts/sync-library.sh`).

## Development

### Testing

Three test layers with pytest markers:

```bash
# Unit tests (no external dependencies, run by default)
pytest tests/unit/ -v

# Integration tests (needs PostgreSQL on port 5433)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m postgres -v

# MySQL integration tests (needs WXYC MySQL on port 3307)
pytest -m mysql -v

# E2E tests (runs full pipeline as subprocess against test Postgres)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m e2e -v
```

Markers: `postgres` (needs PostgreSQL), `mysql` (needs WXYC MySQL), `e2e` (full pipeline), `integration` (needs library.db). Integration and E2E tests are excluded from the default `pytest` run via `addopts` in `pyproject.toml`.

Test fixtures are in `tests/fixtures/` (CSV files, library.db, library_artists.txt). Regenerate with `python tests/fixtures/create_fixtures.py`.

### Code Style

- Line length: 100 chars
- Use `ruff` for linting
- Python 3.11+

## Pipeline Lifecycle

This pipeline runs monthly (or when Discogs publishes new data dumps). It has a completely different lifecycle from the request-handling services that consume its output.

## Development Practices

### TDD (Required)

All code changes in this repo follow test-driven development. This is not optional.

1. **Red**: Write a failing test that describes the desired behavior. Run the test and confirm it fails for the right reason.
2. **Green**: Write the minimum implementation to make the test pass. Run the test and confirm it passes.
3. **Refactor**: Look for opportunities to improve the code while keeping tests green. Re-run tests after each change.
4. **Repeat**: Continue the cycle until the feature is complete.

**Key principle**: No production code without a failing test first.
