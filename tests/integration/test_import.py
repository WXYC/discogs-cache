"""Integration tests for scripts/import_csv.py against a real PostgreSQL database."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"

# Load import_csv module
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

import_csv_func = _ic.import_csv
import_artwork = _ic.import_artwork
TABLES = _ic.TABLES

pytestmark = pytest.mark.postgres


class TestImportCsv:
    """Import fixture CSVs into a fresh schema and verify results."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_database(self, db_url):
        """Apply schema and import all fixture CSVs (once per test class)."""
        conn = psycopg.connect(db_url, autocommit=True)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
        conn.close()

        conn = psycopg.connect(db_url)
        for table_config in TABLES:
            csv_path = CSV_DIR / table_config["csv_file"]
            if csv_path.exists():
                import_csv_func(
                    conn,
                    csv_path,
                    table_config["table"],
                    table_config["csv_columns"],
                    table_config["db_columns"],
                    table_config["required"],
                    table_config["transforms"],
                )
        import_artwork(conn, CSV_DIR)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cache_metadata (release_id, source)
                SELECT id, 'bulk_import' FROM release
                ON CONFLICT (release_id) DO NOTHING
            """)
        conn.commit()
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self, db_url):
        self.db_url = db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_release_row_count(self) -> None:
        """Correct number of releases imported (skipping empty-title row)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        # 16 rows in fixture CSV, minus 1 with empty title (release 7001)
        assert count == 15

    def test_release_artist_row_count(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_artist")
            count = cur.fetchone()[0]
        conn.close()
        # 16 rows in fixture CSV (all have required fields)
        assert count == 16

    def test_release_track_row_count(self) -> None:
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release_track")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 30

    def test_extract_year_applied(self) -> None:
        """Dates are transformed to 4-digit years."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 1001 has released="1997-06-16", should become 1997
            cur.execute("SELECT release_year FROM release WHERE id = 1001")
            year = cur.fetchone()[0]
        conn.close()
        assert year == 1997

    def test_unknown_date_yields_null(self) -> None:
        """Non-date strings in released field produce NULL release_year."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 6001 has released="Unknown"
            cur.execute("SELECT release_year FROM release WHERE id = 6001")
            year = cur.fetchone()[0]
        conn.close()
        assert year is None

    def test_empty_date_yields_null(self) -> None:
        """Empty released field produces NULL release_year."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 5002 has released=""
            cur.execute("SELECT release_year FROM release WHERE id = 5002")
            year = cur.fetchone()[0]
        conn.close()
        assert year is None

    def test_null_required_fields_skipped(self) -> None:
        """Rows with null required fields are not imported."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Release 7001 has empty title (required)
            cur.execute("SELECT count(*) FROM release WHERE id = 7001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_master_id_imported(self) -> None:
        """master_id column is populated for releases that have one."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT master_id FROM release WHERE id = 1001")
            master_id = cur.fetchone()[0]
        conn.close()
        assert master_id == 500

    def test_null_master_id(self) -> None:
        """Releases without master_id have NULL."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT master_id FROM release WHERE id = 4001")
            master_id = cur.fetchone()[0]
        conn.close()
        assert master_id is None

    def test_artwork_url_primary(self) -> None:
        """Primary artwork image is preferred."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 1001")
            url = cur.fetchone()[0]
        conn.close()
        assert url is not None
        assert "release-1001" in url

    def test_artwork_url_fallback(self) -> None:
        """Secondary image used as fallback when no primary exists."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 2001")
            url = cur.fetchone()[0]
        conn.close()
        assert url is not None
        assert "release-2001" in url

    def test_artwork_url_missing(self) -> None:
        """Releases without images have NULL artwork_url."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT artwork_url FROM release WHERE id = 5001")
            url = cur.fetchone()[0]
        conn.close()
        assert url is None

    def test_cache_metadata_populated(self) -> None:
        """All imported releases have cache_metadata entries."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM cache_metadata")
            meta_count = cur.fetchone()[0]
            cur.execute("SELECT count(*) FROM release")
            release_count = cur.fetchone()[0]
        conn.close()
        assert meta_count == release_count

    def test_cache_metadata_source(self) -> None:
        """Cache metadata source is 'bulk_import'."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT source FROM cache_metadata")
            sources = {row[0] for row in cur.fetchall()}
        conn.close()
        assert sources == {"bulk_import"}
