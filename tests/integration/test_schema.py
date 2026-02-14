"""Integration tests for schema creation (create_database.sql, create_indexes.sql)."""

from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"

pytestmark = pytest.mark.postgres


class TestCreateDatabase:
    """Verify create_database.sql produces the expected schema."""

    @pytest.fixture(autouse=True)
    def _apply_schema(self, db_url):
        """Run create_database.sql and create_functions.sql against the test database."""
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
        conn.close()

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_schema_executes_without_error(self) -> None:
        """Schema can be applied to a fresh database."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone()[0] == 1
        conn.close()

    def test_all_tables_exist(self) -> None:
        expected = {
            "release",
            "release_artist",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = {row[0] for row in cur.fetchall()}
        conn.close()
        assert expected.issubset(tables)

    @pytest.mark.parametrize(
        "table, expected_columns",
        [
            ("release", {"id", "title", "release_year", "artwork_url", "master_id"}),
            ("release_artist", {"release_id", "artist_name", "extra"}),
            ("release_track", {"release_id", "sequence", "position", "title", "duration"}),
            ("release_track_artist", {"release_id", "track_sequence", "artist_name"}),
            ("cache_metadata", {"release_id", "cached_at", "source", "last_validated"}),
        ],
        ids=[
            "release",
            "release_artist",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        ],
    )
    def test_table_columns(self, table: str, expected_columns: set[str]) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = %s",
                (table,),
            )
            columns = {row[0] for row in cur.fetchall()}
        conn.close()
        assert expected_columns.issubset(columns), (
            f"Missing columns in {table}: {expected_columns - columns}"
        )

    def test_pg_trgm_extension_enabled(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT extname FROM pg_extension WHERE extname = 'pg_trgm'")
            result = cur.fetchone()
        conn.close()
        assert result is not None, "pg_trgm extension not installed"

    def test_unaccent_extension_enabled(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT extname FROM pg_extension WHERE extname = 'unaccent'")
            result = cur.fetchone()
        conn.close()
        assert result is not None, "unaccent extension not installed"

    def test_f_unaccent_function_exists(self) -> None:
        """Immutable f_unaccent() wrapper is available and strips diacritics."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT f_unaccent('Björk')")
            result = cur.fetchone()[0]
        conn.close()
        assert result == "Bjork"

    def test_fk_constraints_with_cascade(self) -> None:
        """Child tables have ON DELETE CASCADE foreign keys to release."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    tc.table_name,
                    rc.delete_rule
                FROM information_schema.table_constraints tc
                JOIN information_schema.referential_constraints rc
                    ON tc.constraint_name = rc.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND rc.delete_rule = 'CASCADE'
            """)
            fk_tables = {row[0] for row in cur.fetchall()}
        conn.close()
        expected_fk_tables = {
            "release_artist",
            "release_track",
            "release_track_artist",
            "cache_metadata",
        }
        assert expected_fk_tables.issubset(fk_tables)

    def test_no_unique_constraints_on_child_tables(self) -> None:
        """Child tables must not have UNIQUE constraints (Python-level dedup handles this).

        UNIQUE constraints on text columns cause btree overflow when artist_name
        exceeds ~900 bytes. Dedup is handled by import_csv.py's unique_key filtering.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tc.table_name, tc.constraint_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'UNIQUE'
                  AND tc.table_name IN ('release_artist', 'release_track_artist')
            """)
            unique_constraints = cur.fetchall()
        conn.close()
        assert unique_constraints == [], f"Unexpected UNIQUE constraints: {unique_constraints}"

    def test_schema_is_idempotent(self) -> None:
        """Running the schema twice doesn't error (IF NOT EXISTS)."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()


class TestCreateIndexes:
    """Verify create_indexes.sql can be applied after data import."""

    @pytest.fixture(autouse=True)
    def _apply_schema_and_data(self, db_url):
        """Set up schema, functions, and insert minimal sample data for index creation."""
        self.db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(SCHEMA_DIR.joinpath("create_functions.sql").read_text())
            # Insert minimal data so indexes have something to work with
            cur.execute(
                "INSERT INTO release (id, title) VALUES (1, 'Test Album') ON CONFLICT DO NOTHING"
            )
            cur.execute(
                "INSERT INTO release_artist (release_id, artist_name, extra) "
                "SELECT 1, 'Test Artist', 0 WHERE NOT EXISTS "
                "(SELECT 1 FROM release_artist WHERE release_id = 1)"
            )
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, position, title) "
                "SELECT 1, 1, 'A1', 'Test Track' WHERE NOT EXISTS "
                "(SELECT 1 FROM release_track WHERE release_id = 1 AND sequence = 1)"
            )
            cur.execute(
                "INSERT INTO release_track_artist (release_id, track_sequence, artist_name) "
                "SELECT 1, 1, 'Track Artist' WHERE NOT EXISTS "
                "(SELECT 1 FROM release_track_artist WHERE release_id = 1 AND track_sequence = 1)"
            )
        conn.close()

    def test_indexes_execute_without_error(self) -> None:
        """Trigram indexes can be created after data is loaded."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            # Strip CONCURRENTLY since we're in a test with autocommit
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)
        conn.close()

    def test_trigram_indexes_exist(self) -> None:
        """All four trigram indexes are created."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)

            cur.execute("""
                SELECT indexname FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
            """)
            indexes = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {
            "idx_release_track_title_trgm",
            "idx_release_artist_name_trgm",
            "idx_release_track_artist_name_trgm",
            "idx_release_title_trgm",
        }
        assert expected.issubset(indexes)

    def test_trigram_indexes_use_unaccent(self) -> None:
        """All four trigram indexes use f_unaccent() for accent-insensitive matching."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        with conn.cursor() as cur:
            sql = SCHEMA_DIR.joinpath("create_indexes.sql").read_text()
            sql = sql.replace(" CONCURRENTLY", "")
            cur.execute(sql)

            cur.execute("""
                SELECT indexname, indexdef FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname LIKE '%trgm%'
            """)
            rows = cur.fetchall()
        conn.close()
        for indexname, indexdef in rows:
            assert "f_unaccent" in indexdef, (
                f"Index {indexname} should use f_unaccent(): {indexdef}"
            )
