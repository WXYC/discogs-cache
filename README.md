# discogs-cache

ETL pipeline for building a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. Reduces Discogs API calls by providing a local cache for album lookups, track validation, and artwork URLs.

## Overview

The pipeline processes monthly Discogs data dumps (~40 GB XML) into a focused PostgreSQL database (~3 GB) containing only releases by artists in the WXYC library catalog. This provides:

- Fast local lookups instead of rate-limited Discogs API calls
- Trigram fuzzy text search via pg_trgm
- Shared data resource for multiple consuming services

## Prerequisites

- Python 3.11+
- PostgreSQL with the `pg_trgm` extension
- Discogs monthly data dump (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html
- [discogs-xml2db](https://github.com/philipmat/discogs-xml2db) to convert XML to CSV
- `library_artists.txt` (one artist name per line, from request-parser's library)

Or use Docker (see [Docker Compose](#docker-compose) below).

## Setup

```bash
pip install -e ".[dev]"
```

## Running the Pipeline

### Docker Compose

The easiest way to run the full pipeline. Place your filtered CSVs and `library.db` in the `data/` directory:

```bash
# Copy .env.example and adjust if needed
cp .env.example .env

# Place filtered CSVs in data/csv/, library.db in data/
mkdir -p data/csv
cp /path/to/filtered/*.csv data/csv/
cp /path/to/library.db data/

# Run the pipeline
docker compose up --build
```

The pipeline service waits for Postgres to be ready, then runs all steps automatically.

### Orchestration Script

Run all pipeline steps with a single command (steps 4-9 below):

```bash
python scripts/run_pipeline.py <csv_dir> [library_db] [database_url]
```

- `csv_dir` - Directory containing filtered Discogs CSV files
- `library_db` - Path to library.db (optional; if omitted, prune step is skipped)
- `database_url` - PostgreSQL URL (default: `DATABASE_URL` env var or `postgresql://localhost:5432/discogs`)

### Manual Pipeline Steps

#### 1. Convert XML to CSV

Use discogs-xml2db to convert the Discogs XML dump to CSV files:

```bash
python -m discogs_xml2db releases.xml --output csv
```

#### 2. Filter to library artists

Reduces data volume by ~70%, keeping only releases by artists in the library catalog:

```bash
python scripts/filter_csv.py /path/to/library_artists.txt /path/to/csv_output/ /path/to/filtered_output/
```

#### 3. Fix CSV newlines (if needed)

```bash
python scripts/fix_csv_newlines.py /path/to/filtered_output/
```

#### 4. Create database and schema

```bash
createdb discogs
psql -d discogs -f schema/create_database.sql
```

#### 5. Import filtered CSVs

```bash
python scripts/import_csv.py /path/to/filtered_output/ [database_url]
```

`database_url` defaults to `postgresql:///discogs`.

#### 6. Create trigram indexes

Run after data import (takes 10-30 minutes on large datasets):

```bash
psql -d discogs -f schema/create_indexes.sql
```

#### 7. Deduplicate by master_id

Removes duplicate releases sharing the same master release, keeping the one with the most tracks:

```bash
python scripts/dedup_releases.py [database_url]
```

#### 8. Prune to library matches

Classifies each release as KEEP/PRUNE/REVIEW by fuzzy-matching (artist, title) pairs against the library catalog, then deletes PRUNE releases. This typically removes ~89% of data (e.g., 3 GB -> 340 MB).

```bash
# Dry run first to review the report
python scripts/verify_cache.py /path/to/library.db [database_url]

# Prune (REVIEW releases are never deleted)
python scripts/verify_cache.py --prune /path/to/library.db [database_url]
```

#### 9. Reclaim disk space

After pruning, reclaim space with VACUUM FULL (locks tables, run during downtime):

```bash
psql -d discogs -c "VACUUM FULL;"
```

## Database Schema

The schema files in `schema/` define the shared contract between this ETL pipeline and all consumers.

### Tables

| Table | Description |
|-------|-------------|
| `release` | Release metadata: id, title, release_year, artwork_url |
| `release_artist` | Artists on releases (main + extra credits) |
| `release_track` | Tracks on releases with position and duration |
| `release_track_artist` | Artists on specific tracks (for compilations) |
| `cache_metadata` | Data freshness tracking (cached_at, source) |

### Indexes

- Foreign key indexes on all child tables
- Trigram GIN indexes (`pg_trgm`) on `title` and `artist_name` columns for fuzzy text search
- Cache metadata indexes for freshness queries

### Consumer Integration

Consumers connect via the `DATABASE_URL_DISCOGS` environment variable:

```
DATABASE_URL_DISCOGS=postgresql://user:pass@host:5432/discogs
```

Current consumers:
- **request-parser** (`discogs/cache_service.py`) - Python/asyncpg
- **Backend-Service** - TypeScript/Node.js (planned)

## Utility Scripts

| Script | Purpose |
|--------|---------|
| `scripts/run_pipeline.py` | Pipeline orchestrator (steps 4-9) |
| `scripts/csv_to_tsv.py` | Convert CSV to TSV with PostgreSQL escaping |
| `scripts/fix_csv_newlines.py` | Replace embedded newlines in CSV fields |
| `scripts/import_csv.sh` | Shell-based CSV import (legacy, use `import_csv.py` instead) |

## Testing

Tests are organized into three layers:

```bash
# Unit tests (no external dependencies, run by default)
pytest tests/unit/ -v

# Integration tests (needs PostgreSQL)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m postgres -v

# E2E tests (needs PostgreSQL, runs full pipeline as subprocess)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m e2e -v

# All tests requiring Postgres
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m 'postgres or e2e' -v
```

Integration and E2E tests are excluded by default (`pytest` with no args runs only unit tests). Start the test database with:

```bash
docker compose up db -d
```

## Migrations

The `migrations/` directory contains historical one-time migrations:

- `01_optimize_schema.sql` - Initial schema optimization (drops unused tables/columns, adds artwork_url and release_year, deduplicates by master_id). Already applied to the production database.
