#!/usr/bin/env python3
"""Orchestrate the Discogs cache ETL pipeline.

Two modes of operation:

  Full pipeline from XML (steps 1-9):
    python scripts/run_pipeline.py \\
      --xml <releases.xml.gz> \\
      --xml2db <path/to/discogs-xml2db/> \\
      --library-artists <library_artists.txt> \\
      [--library-db <library.db>] \\
      [--wxyc-db-url <mysql://user:pass@host:port/db>] \\
      [--database-url <url>]

  Database build from pre-filtered CSVs (steps 4-9):
    python scripts/run_pipeline.py \\
      --csv-dir <path/to/filtered/> \\
      [--library-db <library.db>] \\
      [--database-url <url>]

Environment variables:
    DATABASE_URL  Default database URL when --database-url is not specified.
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import tempfile
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--xml",
        type=Path,
        metavar="FILE",
        help="Path to Discogs releases XML dump (e.g. releases.xml.gz). "
        "Requires --xml2db and --library-artists.",
    )
    source.add_argument(
        "--csv-dir",
        type=Path,
        metavar="DIR",
        help="Directory containing pre-filtered Discogs CSV files (skips steps 1-3).",
    )

    parser.add_argument(
        "--xml2db",
        type=Path,
        metavar="DIR",
        help="Path to cloned discogs-xml2db repository. Required with --xml.",
    )
    parser.add_argument(
        "--library-artists",
        type=Path,
        metavar="FILE",
        help="Path to library_artists.txt. Required with --xml.",
    )
    parser.add_argument(
        "--library-db",
        type=Path,
        metavar="FILE",
        help="Path to library.db for KEEP/PRUNE classification "
        "(optional; if omitted, the prune step is skipped).",
    )
    parser.add_argument(
        "--wxyc-db-url",
        type=str,
        default=None,
        metavar="URL",
        help="MySQL connection URL for WXYC catalog database "
        "(e.g. mysql://user:pass@host:port/dbname). "
        "Enriches library_artists.txt with alternate names and cross-references. "
        "Requires --library-db.",
    )
    parser.add_argument(
        "--database-url",
        type=str,
        default=os.environ.get("DATABASE_URL", "postgresql://localhost:5432/discogs"),
        help="PostgreSQL connection URL "
        "(default: DATABASE_URL env var or postgresql://localhost:5432/discogs).",
    )

    args = parser.parse_args(argv)

    # Validate --xml mode dependencies
    if args.xml is not None:
        if args.xml2db is None:
            parser.error("--xml2db is required when using --xml")
        if args.library_artists is None:
            parser.error("--library-artists is required when using --xml")

    if args.wxyc_db_url and not args.library_db:
        parser.error("--library-db is required when using --wxyc-db-url")

    return args


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


def run_step(description: str, cmd: list[str], **kwargs) -> None:
    """Run a subprocess, logging and aborting on failure."""
    logger.info("Step: %s", description)
    start = time.monotonic()
    result = subprocess.run(cmd, capture_output=True, text=True, **kwargs)

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
    tables = [
        "release",
        "release_artist",
        "release_track",
        "release_track_artist",
        "cache_metadata",
    ]
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


def convert_xml_to_csv(xml_file: Path, xml2db_dir: Path, output_dir: Path) -> None:
    """Step 1: Convert Discogs XML dump to CSV using discogs-xml2db."""
    run_step(
        "Convert XML to CSV",
        [
            sys.executable,
            "run.py",
            "--export",
            "release",
            "--output",
            str(output_dir),
            str(xml_file.resolve()),
        ],
        cwd=str(xml2db_dir),
    )


def fix_csv_newlines(input_dir: Path, output_dir: Path) -> None:
    """Step 2: Fix embedded newlines in CSV fields."""
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "fix_csv_newlines", SCRIPT_DIR / "fix_csv_newlines.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    logger.info("Step: Fix CSV newlines")
    mod.fix_csv_dir(input_dir, output_dir)


def enrich_library_artists(
    library_db: Path,
    library_artists_out: Path,
    wxyc_db_url: str | None = None,
) -> None:
    """Step 2.5: Enrich library_artists.txt with WXYC cross-reference data."""
    cmd = [
        sys.executable,
        str(SCRIPT_DIR / "enrich_library_artists.py"),
        "--library-db",
        str(library_db),
        "--output",
        str(library_artists_out),
    ]
    if wxyc_db_url:
        cmd.extend(["--wxyc-db-url", wxyc_db_url])
    run_step("Enrich library artists", cmd)


def filter_to_library_artists(library_artists: Path, input_dir: Path, output_dir: Path) -> None:
    """Step 3: Filter CSVs to only releases by library artists."""
    run_step(
        "Filter to library artists",
        [
            sys.executable,
            str(SCRIPT_DIR / "filter_csv.py"),
            str(library_artists),
            str(input_dir),
            str(output_dir),
        ],
    )


def main() -> None:
    args = parse_args()

    python = sys.executable
    db_url = args.database_url
    pipeline_start = time.monotonic()

    # Validate paths
    if args.xml is not None:
        if not args.xml.exists():
            logger.error("XML file not found: %s", args.xml)
            sys.exit(1)
        if not args.xml2db.exists():
            logger.error("discogs-xml2db directory not found: %s", args.xml2db)
            sys.exit(1)
        if not args.library_artists.exists():
            logger.error("library_artists.txt not found: %s", args.library_artists)
            sys.exit(1)
    else:
        if not args.csv_dir.exists():
            logger.error("CSV directory not found: %s", args.csv_dir)
            sys.exit(1)

    if args.library_db and not args.library_db.exists():
        logger.error("library.db not found: %s", args.library_db)
        sys.exit(1)

    # Steps 1-3: XML conversion, newline fix, filtering (only in --xml mode)
    if args.xml is not None:
        with tempfile.TemporaryDirectory(prefix="discogs_pipeline_") as tmpdir:
            tmp = Path(tmpdir)
            raw_csv_dir = tmp / "raw"
            cleaned_csv_dir = tmp / "cleaned"
            filtered_csv_dir = tmp / "filtered"

            # Step 1: Convert XML to CSV
            convert_xml_to_csv(args.xml, args.xml2db, raw_csv_dir)

            # Step 2: Fix CSV newlines
            fix_csv_newlines(raw_csv_dir, cleaned_csv_dir)

            # Step 2.5: Enrich library_artists.txt (optional)
            if args.library_db:
                enriched_artists = tmp / "enriched_library_artists.txt"
                enrich_library_artists(args.library_db, enriched_artists, args.wxyc_db_url)
                library_artists_path = enriched_artists
            else:
                library_artists_path = args.library_artists

            # Step 3: Filter to library artists
            filter_to_library_artists(library_artists_path, cleaned_csv_dir, filtered_csv_dir)

            # Steps 4-9: Database build
            _run_database_build(db_url, filtered_csv_dir, args.library_db, python)
    else:
        # Steps 4-9 only (--csv-dir mode)
        _run_database_build(db_url, args.csv_dir, args.library_db, python)

    total = time.monotonic() - pipeline_start
    logger.info("Pipeline complete in %.1f minutes.", total / 60)


def _run_database_build(db_url: str, csv_dir: Path, library_db: Path | None, python: str) -> None:
    """Steps 4-9: database schema, import, indexes, dedup, prune, vacuum."""
    # Step 4: Wait for Postgres
    wait_for_postgres(db_url)

    # Step 5: Create schema
    run_sql_file(db_url, SCHEMA_DIR / "create_database.sql")

    # Step 6: Import CSVs
    run_step(
        "Import CSVs",
        [python, str(SCRIPT_DIR / "import_csv.py"), str(csv_dir), db_url],
    )

    # Step 7: Create trigram indexes (strip CONCURRENTLY for fresh DB)
    run_sql_file(db_url, SCHEMA_DIR / "create_indexes.sql", strip_concurrently=True)

    # Step 8: Deduplicate by master_id
    run_step(
        "Deduplicate releases",
        [python, str(SCRIPT_DIR / "dedup_releases.py"), db_url],
    )

    # Step 9: Prune to library matches (optional)
    if library_db:
        run_step(
            "Prune to library matches",
            [python, str(SCRIPT_DIR / "verify_cache.py"), "--prune", str(library_db), db_url],
        )
    else:
        logger.info("Skipping prune step (no library.db provided)")

    # Step 10: Vacuum
    run_vacuum(db_url)

    # Step 11: Report
    report_sizes(db_url)


if __name__ == "__main__":
    main()
