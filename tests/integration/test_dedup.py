"""Integration tests for scripts/dedup_releases.py against a real PostgreSQL database."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"

# Load modules
_IMPORT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _IMPORT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

_DEDUP_PATH = Path(__file__).parent.parent.parent / "scripts" / "dedup_releases.py"
_dspec = importlib.util.spec_from_file_location("dedup_releases", _DEDUP_PATH)
assert _dspec is not None and _dspec.loader is not None
_dd = importlib.util.module_from_spec(_dspec)
_dspec.loader.exec_module(_dd)

import_csv_func = _ic.import_csv
import_artwork = _ic.import_artwork
TABLES = _ic.TABLES
ensure_dedup_ids = _dd.ensure_dedup_ids
copy_table = _dd.copy_table
swap_tables = _dd.swap_tables
add_constraints_and_indexes = _dd.add_constraints_and_indexes

pytestmark = pytest.mark.postgres

ALL_TABLES = ("cache_metadata", "release_track_artist",
              "release_track", "release_artist", "release")


def _drop_all_tables(conn) -> None:
    """Drop all pipeline tables with CASCADE to clear any state."""
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        # Also drop dedup artifacts
        cur.execute("DROP TABLE IF EXISTS dedup_delete_ids CASCADE")
        for prefix in ("new_", ""):
            for table in ALL_TABLES:
                cur.execute(f"DROP TABLE IF EXISTS {prefix}{table}_old CASCADE")


def _fresh_import(db_url: str) -> None:
    """Drop everything, apply schema, and import fixture CSVs."""
    conn = psycopg.connect(db_url, autocommit=True)
    _drop_all_tables(conn)
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



def _run_dedup(db_url: str) -> None:
    """Run the full dedup pipeline against the database."""
    conn = psycopg.connect(db_url, autocommit=True)
    delete_count = ensure_dedup_ids(conn)
    if delete_count > 0:
        tables = [
            ("release", "new_release", "id, title, release_year, artwork_url", "id"),
            ("release_artist", "new_release_artist",
             "release_id, artist_name, extra", "release_id"),
            ("release_track", "new_release_track",
             "release_id, sequence, position, title, duration", "release_id"),
            ("release_track_artist", "new_release_track_artist",
             "release_id, track_sequence, artist_name", "release_id"),
            ("cache_metadata", "new_cache_metadata",
             "release_id, cached_at, source, last_validated", "release_id"),
        ]

        for old, new, cols, id_col in tables:
            copy_table(conn, old, new, cols, id_col)

        for old, new, _, _ in tables:
            swap_tables(conn, old, new)
        add_constraints_and_indexes(conn)

        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
    conn.close()


class TestDedup:
    """Deduplicate releases by master_id using the copy-swap strategy."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_dedup(self, db_url):
        """Import fixtures and run dedup (once per class)."""
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        _run_dedup(db_url)

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_correct_release_kept_for_master_500(self) -> None:
        """Release 1002 (5 tracks) kept over 1001 (3 tracks) and 1003 (1 track)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id"
            )
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1002]

    def test_correct_release_kept_for_master_600(self) -> None:
        """Release 2002 (4 tracks) kept over 2001 (2 tracks)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM release WHERE id IN (2001, 2002) ORDER BY id"
            )
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [2002]

    def test_unique_master_id_release_untouched(self) -> None:
        """Release 3001 (unique master_id 700) is not removed."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 3001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 1

    def test_null_master_id_release_untouched(self) -> None:
        """Release 4001 (no master_id) is not removed."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 4001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 1

    def test_child_table_rows_cleaned(self) -> None:
        """Deduped releases have their child table rows removed."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM release_artist WHERE release_id = 1001"
            )
            artist_count = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM release_track WHERE release_id = 1001"
            )
            track_count = cur.fetchone()[0]
        conn.close()
        assert artist_count == 0
        assert track_count == 0

    def test_kept_release_tracks_preserved(self) -> None:
        """The kept release still has its tracks."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM release_track WHERE release_id = 1002"
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 5

    def test_master_id_column_dropped(self) -> None:
        """master_id column no longer exists after copy-swap (not in SELECT list)."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'release' AND column_name = 'master_id'"
            )
            result = cur.fetchone()
        conn.close()
        assert result is None

    def test_primary_key_recreated(self) -> None:
        """Primary key on release(id) exists after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT constraint_name FROM information_schema.table_constraints
                WHERE table_name = 'release' AND constraint_type = 'PRIMARY KEY'
            """)
            result = cur.fetchone()
        conn.close()
        assert result is not None

    def test_fk_constraints_recreated(self) -> None:
        """FK constraints on child tables exist after dedup."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tc.table_name
                FROM information_schema.table_constraints tc
                WHERE tc.constraint_type = 'FOREIGN KEY'
            """)
            fk_tables = {row[0] for row in cur.fetchall()}
        conn.close()
        expected = {"release_artist", "release_track",
                    "release_track_artist", "cache_metadata"}
        assert expected.issubset(fk_tables)

    def test_total_release_count_after_dedup(self) -> None:
        """Total releases: 15 imported - 3 duplicates = 12."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        # 15 imported (7001 skipped), 1001+1003 removed (master 500), 2001 removed (master 600)
        assert count == 12


class TestDedupNoop:
    """Verify dedup is a no-op when there are no duplicates."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        conn = psycopg.connect(db_url, autocommit=True)
        _drop_all_tables(conn)
        with conn.cursor() as cur:
            cur.execute(SCHEMA_DIR.joinpath("create_database.sql").read_text())
            cur.execute(
                "INSERT INTO release (id, title, master_id) VALUES (1, 'A', 100)"
            )
            cur.execute(
                "INSERT INTO release (id, title, master_id) VALUES (2, 'B', 200)"
            )
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) "
                "VALUES (1, 1, 'Track A')"
            )
            cur.execute(
                "INSERT INTO release_track (release_id, sequence, title) "
                "VALUES (2, 1, 'Track B')"
            )
        conn.close()

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_no_duplicates_found(self) -> None:
        """ensure_dedup_ids returns 0 when no duplicates exist."""
        conn = psycopg.connect(self.db_url, autocommit=True)
        count = ensure_dedup_ids(conn)
        # Clean up dedup_delete_ids if created
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS dedup_delete_ids")
        conn.close()
        assert count == 0
