#!/usr/bin/env python3
"""Deduplicate releases by master_id using CREATE TABLE AS + swap.

Instead of deleting 88% of rows (slow, huge WAL), copies the 12% we want
to keep into fresh tables, then swaps them in. Much faster for high
delete ratios.

Expects dedup_delete_ids table to already exist (from a previous run).
If not, creates it from the ROW_NUMBER query.

Usage:
    python dedup_releases.py [database_url]

    database_url defaults to postgresql:///discogs
"""

import logging
import sys
import time

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def _track_count_table_exists(conn) -> bool:
    """Return True if the release_track_count pre-computed table exists."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'release_track_count'
            )
        """)
        return cur.fetchone()[0]


def ensure_dedup_ids(conn) -> int:
    """Ensure dedup_delete_ids table exists. Create if needed.

    Uses release_track_count table for track counts if available (v2 pipeline),
    falling back to counting from release_track directly (v1 / standalone usage).

    Returns number of IDs to delete.
    """
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'dedup_delete_ids'
            )
        """)
        exists = cur.fetchone()[0]

    if exists:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM dedup_delete_ids")
            count = int(cur.fetchone()[0])
        logger.info(f"dedup_delete_ids already exists with {count:,} IDs")
        return count

    # Choose track count source: pre-computed table or live count from release_track
    use_precomputed = _track_count_table_exists(conn)

    if use_precomputed:
        logger.info(
            "Creating dedup_delete_ids from ROW_NUMBER query (using pre-computed track counts)..."
        )
        track_count_join = "JOIN release_track_count tc ON tc.release_id = r.id"
    else:
        logger.info(
            "Creating dedup_delete_ids from ROW_NUMBER query (counting from release_track)..."
        )
        track_count_join = (
            "JOIN ("
            "    SELECT release_id, COUNT(*) as track_count"
            "    FROM release_track"
            "    GROUP BY release_id"
            ") tc ON tc.release_id = r.id"
        )

    with conn.cursor() as cur:
        # track_count_join is built from trusted internal constants, not user input
        cur.execute(f"""
            CREATE UNLOGGED TABLE dedup_delete_ids AS
            SELECT id AS release_id FROM (
                SELECT r.id, r.master_id,
                       ROW_NUMBER() OVER (
                           PARTITION BY r.master_id
                           ORDER BY tc.track_count DESC, r.id ASC
                       ) as rn
                FROM release r
                {track_count_join}
                WHERE r.master_id IS NOT NULL
            ) ranked
            WHERE rn > 1
        """)
        cur.execute("ALTER TABLE dedup_delete_ids ADD PRIMARY KEY (release_id)")
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM dedup_delete_ids")
        count = int(cur.fetchone()[0])
    logger.info(f"Created dedup_delete_ids with {count:,} IDs")
    return count


def copy_table(conn, old_table: str, new_table: str, columns: str, id_col: str) -> int:
    """Copy rows NOT in dedup_delete_ids to a new table.

    Returns row count of new table.
    """
    start = time.time()
    logger.info(f"Copying {old_table} -> {new_table} (keeping non-duplicate rows)...")

    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS {new_table}")
        cur.execute(f"""
            CREATE TABLE {new_table} AS
            SELECT {columns} FROM {old_table} t
            WHERE NOT EXISTS (
                SELECT 1 FROM dedup_delete_ids d WHERE d.release_id = t.{id_col}
            )
        """)
        cur.execute(f"SELECT count(*) FROM {new_table}")
        count = int(cur.fetchone()[0])
    conn.commit()

    elapsed = time.time() - start
    logger.info(f"  {new_table}: {count:,} rows in {elapsed:.1f}s")
    return count


def swap_tables(conn, old_table: str, new_table: str) -> None:
    """Swap old and new tables atomically.

    Uses CASCADE on DROP to remove FK constraints that reference the old table.
    Constraints are recreated by add_constraints_and_indexes() after all swaps.
    """
    bak = f"{old_table}_old"
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE {old_table} RENAME TO {bak}")
        cur.execute(f"ALTER TABLE {new_table} RENAME TO {old_table}")
        cur.execute(f"DROP TABLE {bak} CASCADE")
    conn.commit()
    logger.info(f"  Swapped {new_table} -> {old_table}")


def add_base_constraints_and_indexes(conn) -> None:
    """Add PK, FK constraints and indexes to base tables (no track tables).

    Called after dedup copy-swap. Track constraints are added separately
    by create_track_indexes.sql after track import.
    """
    logger.info("Adding base constraints and indexes...")
    start = time.time()

    statements = [
        # Primary key on release
        "ALTER TABLE release ADD PRIMARY KEY (id)",
        # FK constraints with CASCADE (base tables only)
        "ALTER TABLE release_artist ADD CONSTRAINT fk_release_artist_release "
        "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
        "ALTER TABLE cache_metadata ADD CONSTRAINT fk_cache_metadata_release "
        "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
        "ALTER TABLE cache_metadata ADD PRIMARY KEY (release_id)",
        # FK indexes (base tables only)
        "CREATE INDEX idx_release_artist_release_id ON release_artist(release_id)",
        # Base trigram indexes for fuzzy search (accent-insensitive via f_unaccent)
        "CREATE INDEX idx_release_artist_name_trgm ON release_artist "
        "USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
        "CREATE INDEX idx_release_title_trgm ON release "
        "USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
        # Cache metadata indexes
        "CREATE INDEX idx_cache_metadata_cached_at ON cache_metadata(cached_at)",
        "CREATE INDEX idx_cache_metadata_source ON cache_metadata(source)",
    ]

    with conn.cursor() as cur:
        for i, stmt in enumerate(statements):
            label = stmt.split("(")[0].strip() if "(" in stmt else stmt[:60]
            logger.info(f"  [{i + 1}/{len(statements)}] {label}...")
            stmt_start = time.time()
            cur.execute(stmt)
            conn.commit()
            logger.info(f"    done in {time.time() - stmt_start:.1f}s")

    elapsed = time.time() - start
    logger.info(f"Base constraints and indexes added in {elapsed:.1f}s")


def add_track_constraints_and_indexes(conn) -> None:
    """Add FK constraints and indexes to track tables.

    Called after track import (post-dedup). Equivalent to running
    create_track_indexes.sql.
    """
    logger.info("Adding track constraints and indexes...")
    start = time.time()

    statements = [
        # FK constraints with CASCADE
        "ALTER TABLE release_track ADD CONSTRAINT fk_release_track_release "
        "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
        "ALTER TABLE release_track_artist ADD CONSTRAINT fk_release_track_artist_release "
        "FOREIGN KEY (release_id) REFERENCES release(id) ON DELETE CASCADE",
        # FK indexes
        "CREATE INDEX idx_release_track_release_id ON release_track(release_id)",
        "CREATE INDEX idx_release_track_artist_release_id ON release_track_artist(release_id)",
        # Track trigram indexes for fuzzy search
        "CREATE INDEX idx_release_track_title_trgm ON release_track "
        "USING gin (lower(f_unaccent(title)) gin_trgm_ops)",
        "CREATE INDEX idx_release_track_artist_name_trgm ON release_track_artist "
        "USING gin (lower(f_unaccent(artist_name)) gin_trgm_ops)",
    ]

    with conn.cursor() as cur:
        for i, stmt in enumerate(statements):
            label = stmt.split("(")[0].strip() if "(" in stmt else stmt[:60]
            logger.info(f"  [{i + 1}/{len(statements)}] {label}...")
            stmt_start = time.time()
            cur.execute(stmt)
            conn.commit()
            logger.info(f"    done in {time.time() - stmt_start:.1f}s")

    elapsed = time.time() - start
    logger.info(f"Track constraints and indexes added in {elapsed:.1f}s")


def add_constraints_and_indexes(conn) -> None:
    """Add PK, FK constraints and indexes to all tables.

    Convenience function that calls both base and track versions.
    Used for backward compatibility (standalone dedup with all tables present).
    """
    add_base_constraints_and_indexes(conn)
    add_track_constraints_and_indexes(conn)


def main():
    db_url = sys.argv[1] if len(sys.argv) > 1 else "postgresql:///discogs"

    logger.info(f"Connecting to {db_url}")
    conn = psycopg.connect(db_url, autocommit=True)

    # Step 1: Ensure dedup IDs exist
    delete_count = ensure_dedup_ids(conn)
    if delete_count == 0:
        logger.info("No duplicates found, nothing to do")
        # Clean up release_track_count if it exists
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS release_track_count")
        conn.close()
        return

    total_start = time.time()

    # Step 2: Copy each table (keeping only non-duplicate rows)
    # Only base tables + cache_metadata (tracks are imported after dedup)
    tables = [
        ("release", "new_release", "id, title, release_year, artwork_url", "id"),
        ("release_artist", "new_release_artist", "release_id, artist_name, extra", "release_id"),
        (
            "cache_metadata",
            "new_cache_metadata",
            "release_id, cached_at, source, last_validated",
            "release_id",
        ),
    ]

    for old, new, cols, id_col in tables:
        copy_table(conn, old, new, cols, id_col)

    # Step 3: Drop old FK constraints before swap
    logger.info("Dropping FK constraints on old tables...")
    with conn.cursor() as cur:
        for stmt in [
            "ALTER TABLE release_artist DROP CONSTRAINT IF EXISTS fk_release_artist_release",
            "ALTER TABLE cache_metadata DROP CONSTRAINT IF EXISTS fk_cache_metadata_release",
        ]:
            cur.execute(stmt)

    # Step 4: Swap tables
    logger.info("Swapping tables...")
    for old, new, _, _ in tables:
        swap_tables(conn, old, new)

    # Step 5: Add base constraints and indexes
    add_base_constraints_and_indexes(conn)

    # Step 6: Cleanup
    logger.info("Cleaning up...")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        cur.execute("DROP TABLE IF EXISTS release_track_count")

    # Step 7: Report
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM release")
        count = cur.fetchone()[0]

    total_elapsed = time.time() - total_start
    logger.info(f"Done! Final release count: {count:,} ({total_elapsed / 60:.1f} min total)")

    # Table sizes
    with conn.cursor() as cur:
        cur.execute("""
            SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) as total_size
            FROM pg_stat_user_tables
            WHERE relname IN ('release', 'release_artist', 'release_track',
                              'release_track_artist', 'cache_metadata')
            ORDER BY pg_total_relation_size(relid) DESC
        """)
        logger.info("Table sizes:")
        for row in cur.fetchall():
            logger.info(f"  {row[0]}: {row[1]}")

    conn.close()


if __name__ == "__main__":
    main()
