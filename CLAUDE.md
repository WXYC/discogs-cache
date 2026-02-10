# Claude Code Instructions for discogs-cache

## Project Overview

ETL pipeline for building and maintaining a PostgreSQL cache of Discogs release data, filtered to artists in the WXYC radio library catalog. The cache database is a shared resource consumed by multiple services:

- **request-parser** (Python/FastAPI) - `discogs/cache_service.py` queries the cache for album lookups
- **Backend-Service** (TypeScript/Node.js) - future consumer for Discogs data

## Architecture

### ETL Pipeline

1. **Download** Discogs monthly data dumps (XML) from https://discogs-data-dumps.s3.us-west-2.amazonaws.com/index.html
2. **Convert** XML to CSV using [discogs-xml2db](https://github.com/philipmat/discogs-xml2db)
3. **Filter** CSVs to library-matching artists only (`scripts/filter_csv.py`) - ~70% data reduction
4. **Create schema** (`schema/create_database.sql`)
5. **Import** filtered CSVs into PostgreSQL (`scripts/import_csv.py`)
6. **Create indexes** including trigram GIN indexes (`schema/create_indexes.sql`)
7. **Deduplicate** by master_id (`scripts/dedup_releases.py`)
8. **Verify** cache against library catalog (`scripts/verify_cache.py`) - classifies releases as KEEP/PRUNE/REVIEW

### Database Schema (Shared Contract)

The SQL files in `schema/` define the contract between this ETL pipeline and all consumers:

- `schema/create_database.sql` - Tables: `release`, `release_artist`, `release_track`, `release_track_artist`, `cache_metadata`
- `schema/create_indexes.sql` - Trigram GIN indexes for fuzzy text search (pg_trgm)

Consumers connect via `DATABASE_URL_DISCOGS` environment variable.

### Key Files

- `scripts/filter_csv.py` - Filter Discogs CSVs to library artists
- `scripts/import_csv.py` - Import CSVs into PostgreSQL (psycopg COPY)
- `scripts/dedup_releases.py` - Deduplicate releases by master_id (copy-swap strategy)
- `scripts/verify_cache.py` - Multi-index matching pipeline for KEEP/PRUNE classification
- `scripts/csv_to_tsv.py` - CSV to TSV conversion utility
- `scripts/fix_csv_newlines.py` - Fix multiline CSV fields
- `scripts/import_csv.sh` - Shell orchestration for CSV import
- `lib/matching.py` - Compilation detection utility

### External Inputs

Two files are inputs to the ETL but produced by request-parser:

1. **`library_artists.txt`** - One artist name per line, used by `filter_csv.py`
2. **`library.db`** - SQLite database, used by `verify_cache.py` for KEEP/PRUNE classification

Both are produced by request-parser's library sync (`scripts/sync-library.sh`).

## Development

### Testing

```bash
# Unit tests (no external dependencies)
pytest tests/unit/ -v

# Integration tests (needs library.db)
LIBRARY_DB=/path/to/library.db pytest tests/integration/ -v -m integration
```

### Code Style

- Line length: 100 chars
- Use `ruff` for linting
- Python 3.11+

## Pipeline Lifecycle

This pipeline runs monthly (or when Discogs publishes new data dumps). It has a completely different lifecycle from the request-handling services that consume its output.
