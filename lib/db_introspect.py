"""Database introspection for inferring pipeline state.

When --resume is used but no state file exists, these functions inspect the
database to infer which pipeline steps have already completed.
"""

from __future__ import annotations

import psycopg

from lib.pipeline_state import PipelineState


def table_exists(db_url: str, table_name: str) -> bool:
    """Return True if the table exists in the public schema."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.tables"
            "  WHERE table_schema = 'public' AND table_name = %s"
            ")",
            (table_name,),
        )
        result = cur.fetchone()[0]
    conn.close()
    return result


def table_has_rows(db_url: str, table_name: str) -> bool:
    """Return True if the table has at least one row."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {table_name} LIMIT 1)")
        result = cur.fetchone()[0]
    conn.close()
    return result


def column_exists(db_url: str, table_name: str, column_name: str) -> bool:
    """Return True if the column exists on the table."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT EXISTS ("
            "  SELECT 1 FROM information_schema.columns"
            "  WHERE table_name = %s AND column_name = %s"
            ")",
            (table_name, column_name),
        )
        result = cur.fetchone()[0]
    conn.close()
    return result


def trigram_indexes_exist(db_url: str) -> bool:
    """Return True if trigram GIN indexes exist on the expected tables."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT indexname FROM pg_indexes"
            " WHERE schemaname = 'public' AND indexname LIKE '%trgm%'"
        )
        indexes = {row[0] for row in cur.fetchall()}
    conn.close()
    expected = {
        "idx_release_track_title_trgm",
        "idx_release_artist_name_trgm",
        "idx_release_track_artist_name_trgm",
        "idx_release_title_trgm",
    }
    return expected.issubset(indexes)


def infer_pipeline_state(db_url: str) -> PipelineState:
    """Infer pipeline state from database structure.

    Useful when --resume is used but no state file exists. Inspects the
    database to determine which steps have already completed.

    Steps that cannot be inferred (prune, vacuum) are left as pending
    since they are safe to re-run.
    """
    state = PipelineState(db_url=db_url, csv_dir="")

    if not table_exists(db_url, "release"):
        return state
    state.mark_completed("create_schema")

    if not table_has_rows(db_url, "release"):
        return state
    state.mark_completed("import_csv")

    if not trigram_indexes_exist(db_url):
        return state
    state.mark_completed("create_indexes")

    if column_exists(db_url, "release", "master_id"):
        return state
    state.mark_completed("dedup")

    # prune and vacuum cannot be inferred from database state
    return state
