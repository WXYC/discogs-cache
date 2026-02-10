#!/usr/bin/env python3
"""Orchestrate the Discogs cache ETL pipeline.

Runs all pipeline steps in order: schema creation, CSV import, index creation,
deduplication, pruning, and vacuum.  Each step is run as a subprocess for
isolation; the script aborts on the first failure.

Usage:
    python scripts/run_pipeline.py <csv_dir> [library_db] [database_url]

    csv_dir      Directory containing filtered Discogs CSV files.
    library_db   Path to library.db for KEEP/PRUNE classification (optional;
                 if omitted, the prune step is skipped).
    database_url PostgreSQL connection URL (default: from DATABASE_URL env var,
                 or postgresql://localhost:5432/discogs).

Environment variables:
    DATABASE_URL  Default database URL when not passed on the command line.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
SCHEMA_DIR = SCRIPT_DIR.parent / "schema"

# Maximum seconds to wait for Postgres to become ready.
PG_CONNECT_TIMEOUT = 30


def wait_for_postgres(db_url: str) -> None:
    """Poll Postgres until a connection succeeds or timeout is reached."""
    logger.info("Waiting for PostgreSQL at %s ...", db_url)
    deadline = time.monotonic() + PG_CONNECT_TIMEOUT
    delay = 0.5
    while True:
        try:
            conn = psycopg.connect(db_url, connect_timeout=5)
            conn.close()
            logger.info("PostgreSQL is ready.")
            return
        except psycopg.OperationalError:
            if time.monotonic() >= deadline:
                logger.error("Timed out waiting for PostgreSQL after %ds", PG_CONNECT_TIMEOUT)
                sys.exit(1)
            time.sleep(delay)
            delay = min(delay * 2, 3)


def run_sql_file(db_url: str, sql_file: Path, *, strip_concurrently: bool = False) -> None:
    """Execute a SQL file against the database using psycopg.

    Args:
        db_url: PostgreSQL connection URL.
        sql_file: Path to the .sql file.
        strip_concurrently: If True, remove CONCURRENTLY from CREATE INDEX
            statements (safe on a fresh database with no concurrent queries).
    """
    logger.info("Running %s ...", sql_file.name)

    sql = sql_file.read_text()
    if strip_concurrently:
        sql = sql.replace(" CONCURRENTLY", "")

    conn = psycopg.connect(db_url, autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
    except psycopg.Error as exc:
        logger.error("SQL execution failed for %s: %s", sql_file.name, exc)
        conn.close()
        sys.exit(1)
    conn.close()
    logger.info("  done.")


def run_step(description: str, cmd: list[str]) -> None:
    """Run a subprocess, logging and aborting on failure."""
    logger.info("Step: %s", description)
    start = time.monotonic()
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.stdout:
        for line in result.stdout.strip().splitlines():
            logger.info("  %s", line)
    if result.stderr:
        for line in result.stderr.strip().splitlines():
            if result.returncode != 0:
                logger.error("  %s", line)
            else:
                logger.info("  %s", line)

    elapsed = time.monotonic() - start
    if result.returncode != 0:
        logger.error("Step failed (exit %d) after %.1fs", result.returncode, elapsed)
        sys.exit(1)
    logger.info("  completed in %.1fs", elapsed)


def run_vacuum(db_url: str) -> None:
    """Run VACUUM FULL on all pipeline tables."""
    logger.info("Running VACUUM FULL ...")
    tables = ["release", "release_artist", "release_track", "release_track_artist", "cache_metadata"]
    conn = psycopg.connect(db_url, autocommit=True)
    for table in tables:
        logger.info("  VACUUM FULL %s ...", table)
        try:
            with conn.cursor() as cur:
                cur.execute(f"VACUUM FULL {table}")
        except psycopg.Error as exc:
            logger.warning("  VACUUM FULL %s failed: %s", table, exc)
    conn.close()
    logger.info("  VACUUM complete.")


def report_sizes(db_url: str) -> None:
    """Log final table row counts and sizes."""
    logger.info("Final database state:")
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT relname,
                   n_live_tup::bigint as row_count,
                   pg_size_pretty(pg_total_relation_size(relid)) as total_size
            FROM pg_stat_user_tables
            WHERE relname IN (
                'release', 'release_artist', 'release_track',
                'release_track_artist', 'cache_metadata'
            )
            ORDER BY pg_total_relation_size(relid) DESC
        """)
        for row in cur.fetchall():
            logger.info("  %-25s %10s rows   %s", row[0], f"{row[1]:,}", row[2])
    conn.close()


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    csv_dir = Path(sys.argv[1])
    library_db = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    db_url = (
        sys.argv[3]
        if len(sys.argv) > 3
        else os.environ.get("DATABASE_URL", "postgresql://localhost:5432/discogs")
    )

    if not csv_dir.exists():
        logger.error("CSV directory not found: %s", csv_dir)
        sys.exit(1)
    if library_db and not library_db.exists():
        logger.error("library.db not found: %s", library_db)
        sys.exit(1)

    python = sys.executable
    pipeline_start = time.monotonic()

    # Step 1: Wait for Postgres
    wait_for_postgres(db_url)

    # Step 2: Create schema
    run_sql_file(db_url, SCHEMA_DIR / "create_database.sql")

    # Step 3: Import CSVs
    run_step(
        "Import CSVs",
        [python, str(SCRIPT_DIR / "import_csv.py"), str(csv_dir), db_url],
    )

    # Step 4: Create trigram indexes (strip CONCURRENTLY for fresh DB)
    run_sql_file(db_url, SCHEMA_DIR / "create_indexes.sql", strip_concurrently=True)

    # Step 5: Deduplicate by master_id
    run_step(
        "Deduplicate releases",
        [python, str(SCRIPT_DIR / "dedup_releases.py"), db_url],
    )

    # Step 6: Prune to library matches (optional)
    if library_db:
        run_step(
            "Prune to library matches",
            [python, str(SCRIPT_DIR / "verify_cache.py"), "--prune", str(library_db), db_url],
        )
    else:
        logger.info("Skipping prune step (no library.db provided)")

    # Step 7: Vacuum
    run_vacuum(db_url)

    # Step 8: Report
    report_sizes(db_url)

    total = time.monotonic() - pipeline_start
    logger.info("Pipeline complete in %.1f minutes.", total / 60)


if __name__ == "__main__":
    main()
