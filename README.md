# discogs-cache

ETL pipeline for building a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. Reduces Discogs API calls by providing a local cache for album lookups, track validation, and artwork URLs.

## Overview

The pipeline processes monthly Discogs data dumps (~40 GB XML) into a focused PostgreSQL database (~3 GB) containing only releases by artists in the WXYC library catalog. This provides:

- Fast local lookups instead of rate-limited Discogs API calls
- Trigram fuzzy text search via pg_trgm
- Shared data resource for multiple consuming services

## Prerequisites

- Python 3.11+
- PostgreSQL with the `pg_trgm` extension (or use [Docker Compose](#docker-compose))
- Discogs monthly data dump (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html
- [discogs-xml2db](https://github.com/philipmat/discogs-xml2db) -- clone separately; not a PyPI package
- `library_artists.txt` and `library.db` (produced by request-parser's library sync)

## Setup

```bash
pip install -e ".[dev]"
```

## Pipeline

All 9 steps are automated by `run_pipeline.py` (or Docker Compose). The script supports two modes: full pipeline from XML, or database build from pre-filtered CSVs.

| Step | Script | Description |
|------|--------|-------------|
| 1. Convert | discogs-xml2db | XML data dump to CSV |
| 2. Fix newlines | `scripts/fix_csv_newlines.py` | Clean embedded newlines in CSV fields |
| 2.5. Enrich | `scripts/enrich_library_artists.py` | Enrich artist list with cross-references (optional) |
| 3. Filter | `scripts/filter_csv.py` | Keep only library artists (~70% reduction) |
| 4. Create schema | `schema/create_database.sql` | Set up tables and constraints |
| 5. Import | `scripts/import_csv.py` | Bulk load CSVs via psycopg COPY |
| 6. Create indexes | `schema/create_indexes.sql` | Trigram GIN indexes for fuzzy search |
| 7. Deduplicate | `scripts/dedup_releases.py` | Keep best release per master_id (most tracks) |
| 8. Prune | `scripts/verify_cache.py --prune` | Remove non-library releases (~89% reduction) |
| 9. Vacuum | `VACUUM FULL` | Reclaim disk space |

Step 2.5 generates `library_artists.txt` from `library.db` and optionally enriches it with alternate artist names and cross-references from the WXYC MySQL catalog database. This reduces false negatives at the filtering stage for artists known by multiple names (e.g., "Body Count" filed under Ice-T).

### Docker Compose

The easiest way to run the full pipeline:

```bash
# Clone discogs-xml2db (one-time setup)
git clone https://github.com/philipmat/discogs-xml2db.git

# Place input files in data/
mkdir -p data
cp /path/to/releases.xml.gz data/
cp /path/to/library_artists.txt data/
cp /path/to/library.db data/

docker compose up --build
```

### Orchestration Script

Full pipeline from XML (steps 1-9):

```bash
python scripts/run_pipeline.py \
  --xml /path/to/releases.xml.gz \
  --xml2db /path/to/discogs-xml2db/ \
  --library-artists /path/to/library_artists.txt \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs
```

To enrich `library_artists.txt` with alternate names and cross-references from the WXYC catalog database, add `--wxyc-db-url`:

```bash
python scripts/run_pipeline.py \
  --xml /path/to/releases.xml.gz \
  --xml2db /path/to/discogs-xml2db/ \
  --library-artists /path/to/library_artists.txt \
  --library-db /path/to/library.db \
  --wxyc-db-url mysql://user:pass@host:port/wxycmusic \
  --database-url postgresql://localhost:5432/discogs
```

Database build from pre-filtered CSVs (steps 4-9):

```bash
python scripts/run_pipeline.py \
  --csv-dir /path/to/filtered/ \
  --library-db /path/to/library.db \
  --database-url postgresql://localhost:5432/discogs
```

- `--library-db` is optional; if omitted, the prune step is skipped
- `--database-url` defaults to `DATABASE_URL` env var or `postgresql://localhost:5432/discogs`

### Running Steps Manually

Individual steps can also be run directly:

```bash
# 1. Convert XML to CSV (run from discogs-xml2db directory)
cd /path/to/discogs-xml2db
python run.py --export release --output /path/to/raw/ /path/to/releases.xml.gz

# 2. Fix CSV newlines
python scripts/fix_csv_newlines.py /path/to/raw/release.csv /path/to/cleaned/release.csv

# 3. Filter to library artists
python scripts/filter_csv.py /path/to/library_artists.txt /path/to/cleaned/ /path/to/filtered/

# 4. Create schema
psql -d discogs -f schema/create_database.sql

# 5. Import CSVs
python scripts/import_csv.py /path/to/filtered/ [database_url]

# 6. Create indexes (10-30 min on large datasets)
psql -d discogs -f schema/create_indexes.sql

# 7. Deduplicate
python scripts/dedup_releases.py [database_url]

# 8. Prune (dry run first, then with --prune)
python scripts/verify_cache.py /path/to/library.db [database_url]
python scripts/verify_cache.py --prune /path/to/library.db [database_url]

# 9. Vacuum
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

## Testing

Tests are organized into three layers:

```bash
# Unit tests (no external dependencies, run by default)
pytest tests/unit/ -v

# Integration tests (needs PostgreSQL)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m postgres -v

# MySQL integration tests (needs WXYC MySQL on port 3307)
pytest -m mysql -v

# E2E tests (needs PostgreSQL, runs full pipeline as subprocess)
DATABASE_URL_TEST=postgresql://discogs:discogs@localhost:5433/postgres \
  pytest -m e2e -v
```

Integration and E2E tests are excluded by default (`pytest` with no args runs only unit tests). Start the test database with:

```bash
docker compose up db -d
```

## Migrations

The `migrations/` directory contains historical one-time migrations:

- `01_optimize_schema.sql` - Initial schema optimization (drops unused tables/columns, adds artwork_url and release_year, deduplicates by master_id). Already applied to the production database.
