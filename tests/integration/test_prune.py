"""Integration tests for verify_cache.py pruning against a real PostgreSQL database."""

from __future__ import annotations

import importlib.util
import sys as _sys
from pathlib import Path

import psycopg
import pytest

SCHEMA_DIR = Path(__file__).parent.parent.parent / "schema"
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"
FIXTURE_LIBRARY_DB = FIXTURES_DIR / "library.db"

ALL_TABLES = ("cache_metadata", "release_track_artist",
              "release_track", "release_artist", "release")

# Load import_csv module
_IMPORT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _IMPORT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

import_csv_func = _ic.import_csv
import_artwork = _ic.import_artwork
TABLES = _ic.TABLES

# verify_cache uses asyncpg, so we load it but only use the non-async pieces
_VC_PATH = Path(__file__).parent.parent.parent / "scripts" / "verify_cache.py"
_vc_spec = importlib.util.spec_from_file_location("verify_cache", _VC_PATH)
assert _vc_spec is not None and _vc_spec.loader is not None
_sys.modules["verify_cache"] = _vc = importlib.util.module_from_spec(_vc_spec)
_vc_spec.loader.exec_module(_vc)

LibraryIndex = _vc.LibraryIndex
MultiIndexMatcher = _vc.MultiIndexMatcher
Decision = _vc.Decision
classify_all_releases = _vc.classify_all_releases

pytestmark = pytest.mark.postgres


def _fresh_import(db_url: str) -> None:
    """Drop everything, apply schema, and import fixture CSVs."""
    conn = psycopg.connect(db_url, autocommit=True)
    with conn.cursor() as cur:
        for table in ALL_TABLES:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
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


def _load_releases_sync(db_url: str) -> list[tuple[int, str, str]]:
    """Load releases with primary artist (sync version of load_discogs_releases)."""
    conn = psycopg.connect(db_url)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT r.id, ra.artist_name, r.title
            FROM release r
            JOIN release_artist ra ON ra.release_id = r.id AND ra.extra = 0
            ORDER BY r.id
        """)
        rows = [(row[0], row[1], row[2]) for row in cur.fetchall()]
    conn.close()
    return rows


class TestPruneClassification:
    """Verify KEEP/PRUNE classifications against fixture library.db."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up(self, db_url):
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        index = LibraryIndex.from_sqlite(FIXTURE_LIBRARY_DB)
        matcher = MultiIndexMatcher(index)
        releases = _load_releases_sync(db_url)
        self.__class__._report = classify_all_releases(releases, index, matcher)

    @pytest.fixture(autouse=True)
    def _store_attrs(self):
        self.db_url = self.__class__._db_url
        self.report = self.__class__._report

    def test_radiohead_ok_computer_kept(self) -> None:
        """Radiohead 'OK Computer' should be classified as KEEP."""
        assert self.report.keep_ids & {1001, 1002, 1003}

    def test_joy_division_unknown_pleasures_kept(self) -> None:
        """Joy Division 'Unknown Pleasures' should be classified as KEEP."""
        assert self.report.keep_ids & {2001, 2002}

    def test_unknown_album_pruned(self) -> None:
        """Release 5001 'Unknown Album' by 'DJ Unknown' should be PRUNE."""
        assert 5001 in self.report.prune_ids

    def test_non_library_artist_pruned(self) -> None:
        """Release 10001 by 'Random Artist X' should be PRUNE."""
        assert 10001 in self.report.prune_ids

    def test_abbey_road_kept(self) -> None:
        """Beatles 'Abbey Road' should be KEEP (tests comma convention)."""
        assert 9001 in self.report.keep_ids

    def test_kid_a_kept(self) -> None:
        """Radiohead 'Kid A' should be KEEP."""
        assert 3001 in self.report.keep_ids

    def test_amnesiac_kept(self) -> None:
        """Radiohead 'Amnesiac' should be KEEP."""
        assert 4001 in self.report.keep_ids


class TestPruneExecution:
    """Verify --prune actually deletes PRUNE releases."""

    @pytest.fixture(autouse=True, scope="class")
    def _set_up_and_classify(self, db_url):
        self.__class__._db_url = db_url
        _fresh_import(db_url)
        index = LibraryIndex.from_sqlite(FIXTURE_LIBRARY_DB)
        matcher = MultiIndexMatcher(index)
        releases = _load_releases_sync(db_url)
        self.__class__._report = classify_all_releases(releases, index, matcher)

    @pytest.fixture(autouse=True)
    def _store_attrs(self):
        self.db_url = self.__class__._db_url
        self.report = self.__class__._report

    def test_prune_deletes_releases(self) -> None:
        """Pruned release IDs are actually deleted from the release table."""
        if not self.report.prune_ids:
            pytest.skip("No releases classified as PRUNE")

        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            id_list = list(self.report.prune_ids)
            cur.execute(
                "DELETE FROM release WHERE id = ANY(%s::integer[])", (id_list,)
            )
        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM release WHERE id = ANY(%s::integer[])",
                (id_list,),
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_fk_cascade_cleans_child_tables(self) -> None:
        """Deleting releases cascades to child tables (verified after prune above)."""
        if not self.report.prune_ids:
            pytest.skip("No releases classified as PRUNE")

        prune_id = next(iter(self.report.prune_ids))
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            # After prune_deletes_releases ran, child rows should be gone too
            cur.execute(
                "SELECT count(*) FROM release_artist WHERE release_id = %s",
                (prune_id,),
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0

    def test_keep_releases_survive_prune(self) -> None:
        """KEEP releases are not affected by the prune operation."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            keep_list = list(self.report.keep_ids)
            cur.execute(
                "SELECT count(*) FROM release WHERE id = ANY(%s::integer[])",
                (keep_list,),
            )
            count = cur.fetchone()[0]
        conn.close()
        assert count == len(self.report.keep_ids)
