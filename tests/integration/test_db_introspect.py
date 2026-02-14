"""Integration tests for lib/db_introspect — database introspection utilities.

These tests run against a real PostgreSQL instance and verify that we can
infer pipeline state from database structure.
"""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from lib.db_introspect import (
    base_trigram_indexes_exist,
    column_exists,
    infer_pipeline_state,
    table_exists,
    table_has_rows,
    track_trigram_indexes_exist,
    trigram_indexes_exist,
)

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

ALL_TABLES = (
    "cache_metadata",
    "release_track_artist",
    "release_track",
    "release_artist",
    "release",
)

pytestmark = pytest.mark.postgres


def _clean_db(db_url: str) -> None:
    """Drop all pipeline tables so the database is empty."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
    conn.close()


class TestTableExists:
    """table_exists() detects table presence."""

    def test_false_on_empty_db(self, db_url) -> None:
        _clean_db(db_url)
        assert not table_exists(db_url, "release")

    def test_true_after_schema_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        assert table_exists(db_url, "release")


class TestTableHasRows:
    """table_has_rows() detects non-empty tables."""

    def test_false_on_empty_table(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        assert not table_has_rows(db_url, "release")

    def test_true_after_insert(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test')")
        conn.close()
        assert table_has_rows(db_url, "release")


class TestColumnExists:
    """column_exists() detects column presence."""

    def test_true_for_master_id_before_dedup(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        assert column_exists(db_url, "release", "master_id")

    def test_false_for_nonexistent_column(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        assert not column_exists(db_url, "release", "nonexistent")

    def test_false_after_column_dropped(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute("ALTER TABLE release DROP COLUMN master_id")
        conn.close()
        assert not column_exists(db_url, "release", "master_id")


class TestBaseTrigramIndexesExist:
    """base_trigram_indexes_exist() detects base GIN trigram indexes."""

    def test_false_before_index_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        assert not base_trigram_indexes_exist(db_url)

    def test_true_after_base_index_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()
        assert base_trigram_indexes_exist(db_url)


class TestTrackTrigramIndexesExist:
    """track_trigram_indexes_exist() detects track GIN trigram indexes."""

    def test_false_before_index_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        assert not track_trigram_indexes_exist(db_url)

    def test_true_after_track_index_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()
        assert track_trigram_indexes_exist(db_url)


class TestTrigramIndexesExist:
    """trigram_indexes_exist() detects all GIN trigram indexes (backward compat)."""

    def test_false_before_index_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()
        assert not trigram_indexes_exist(db_url)

    def test_true_after_index_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()
        assert trigram_indexes_exist(db_url)


class TestInferPipelineState:
    """infer_pipeline_state() returns correct status at each stage."""

    def test_empty_db(self, db_url) -> None:
        _clean_db(db_url)
        state = infer_pipeline_state(db_url)
        for step in [
            "create_schema",
            "import_csv",
            "create_indexes",
            "dedup",
            "import_tracks",
            "create_track_indexes",
            "prune",
            "vacuum",
        ]:
            assert not state.is_completed(step)

    def test_after_schema_creation(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

        state = infer_pipeline_state(db_url)
        assert state.is_completed("create_schema")
        assert not state.is_completed("import_csv")

    def test_after_import(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test')")
        conn.close()

        state = infer_pipeline_state(db_url)
        assert state.is_completed("create_schema")
        assert state.is_completed("import_csv")
        assert not state.is_completed("create_indexes")

    def test_after_base_indexes(self, db_url) -> None:
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test')")
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()

        state = infer_pipeline_state(db_url)
        assert state.is_completed("create_schema")
        assert state.is_completed("import_csv")
        assert state.is_completed("create_indexes")
        assert not state.is_completed("dedup")

    def test_after_dedup(self, db_url) -> None:
        """After dedup, master_id column is absent."""
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test')")
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
            cur.execute("ALTER TABLE release DROP COLUMN master_id")
        conn.close()

        state = infer_pipeline_state(db_url)
        assert state.is_completed("dedup")
        assert not state.is_completed("import_tracks")
        assert not state.is_completed("create_track_indexes")

    def test_after_track_import(self, db_url) -> None:
        """After track import, release_track has rows."""
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test')")
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
            cur.execute("ALTER TABLE release DROP COLUMN master_id")
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (1, 1, 'T1')"
            )
        conn.close()

        state = infer_pipeline_state(db_url)
        assert state.is_completed("dedup")
        assert state.is_completed("import_tracks")
        assert not state.is_completed("create_track_indexes")

    def test_after_track_indexes(self, db_url) -> None:
        """After track indexes, all track trigram indexes exist."""
        _clean_db(db_url)
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            cur.execute("INSERT INTO release (id, title) VALUES (1, 'Test')")
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
            cur.execute("ALTER TABLE release DROP COLUMN master_id")
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) VALUES (1, 1, 'T1')"
            )
            sql = SCHEMA_DIR.joinpath("create_track_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()

        state = infer_pipeline_state(db_url)
        assert state.is_completed("dedup")
        assert state.is_completed("import_tracks")
        assert state.is_completed("create_track_indexes")
        # prune and vacuum are never inferred
        assert not state.is_completed("prune")
        assert not state.is_completed("vacuum")
