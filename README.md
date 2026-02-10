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

## Setup

```bash
pip install -e ".[dev]"
```

## Pipeline Steps

### 1. Convert XML to CSV

Use discogs-xml2db to convert the Discogs XML dump to CSV files:

```bash
python -m discogs_xml2db releases.xml --output csv
```

### 2. Filter to library artists

Reduces data volume by ~70%, keeping only releases by artists in the library catalog:

```bash
python scripts/filter_csv.py /path/to/library_artists.txt /path/to/csv_output/ /path/to/filtered_output/
```

### 3. Create database and schema

```bash
createdb discogs
psql -d discogs -f schema/create_database.sql
```

### 4. Import filtered CSVs

```bash
python scripts/import_csv.py /path/to/filtered_output/ [database_url]
```

`database_url` defaults to `postgresql:///discogs`.

### 5. Create trigram indexes

Run after data import (takes 10-30 minutes on large datasets):

```bash
psql -d discogs -f schema/create_indexes.sql
```

### 6. Deduplicate by master_id

Removes duplicate releases sharing the same master release, keeping the one with the most tracks:

```bash
python scripts/dedup_releases.py [database_url]
```

### 7. Verify and prune

Classifies each release as KEEP/PRUNE/REVIEW by fuzzy-matching against the library catalog:

```bash
# Dry run (default): report what would be pruned
python scripts/verify_cache.py /path/to/library.db [database_url]

# Actually prune (REVIEW releases are never deleted)
python scripts/verify_cache.py --prune /path/to/library.db [database_url]
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
| `scripts/csv_to_tsv.py` | Convert CSV to TSV with PostgreSQL escaping |
| `scripts/fix_csv_newlines.py` | Replace embedded newlines in CSV fields |
| `scripts/import_csv.sh` | Shell-based CSV import (legacy, use `import_csv.py` instead) |

## Testing

```bash
# Unit tests (no external dependencies)
pytest tests/unit/ -v

# Integration tests (needs library.db)
LIBRARY_DB=/path/to/library.db pytest tests/integration/ -v -m integration
```

## Migrations

The `migrations/` directory contains historical one-time migrations:

- `01_optimize_schema.sql` - Initial schema optimization (drops unused tables/columns, adds artwork_url and release_year, deduplicates by master_id). Already applied to the production database.
