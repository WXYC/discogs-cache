"""End-to-end test for the full pipeline orchestration script.

Runs scripts/run_pipeline.py as a subprocess against a test PostgreSQL database
using fixture CSVs and fixture library.db, then verifies the final database state.
"""

from __future__ import annotations

import os
import subprocess
import sys
import uuid
from pathlib import Path

import psycopg
from psycopg import sql
import pytest

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
CSV_DIR = FIXTURES_DIR / "csv"
FIXTURE_LIBRARY_DB = FIXTURES_DIR / "library.db"
RUN_PIPELINE = Path(__file__).parent.parent.parent / "scripts" / "run_pipeline.py"

ADMIN_URL = os.environ.get("DATABASE_URL_TEST", "postgresql://localhost:5433/postgres")

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="class")
def e2e_db_url():
    """Create a fresh database for each E2E test class.

    Each test class gets its own database so that one pipeline run
    (which modifies schema via dedup) does not interfere with another.
    """
    db_name = f"discogs_e2e_{uuid.uuid4().hex[:8]}"
    admin_conn = psycopg.connect(ADMIN_URL, autocommit=True)

    with admin_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))

    if "@" in ADMIN_URL:
        base = ADMIN_URL.rsplit("/", 1)[0]
    else:
        base = ADMIN_URL.rsplit("/", 1)[0]
    test_url = f"{base}/{db_name}"

    yield test_url

    with admin_conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = {} AND pid <> pg_backend_pid()"
            ).format(sql.Literal(db_name))
        )
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
    admin_conn.close()


class TestPipeline:
    """Run the full pipeline and verify final database state."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url):
        """Run run_pipeline.py as a subprocess against the test database."""
        self.__class__._db_url = e2e_db_url

        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                str(CSV_DIR),
                str(FIXTURE_LIBRARY_DB),
                e2e_db_url,
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )

        self.__class__._stdout = result.stdout
        self.__class__._stderr = result.stderr
        self.__class__._returncode = result.returncode

        if result.returncode != 0:
            # Print output for debugging
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def _connect(self):
        return psycopg.connect(self.db_url)

    def test_tables_populated(self) -> None:
        """Core tables have rows after pipeline completion.

        release_track_artist is excluded because it only contains rows for
        compilation releases, which may be pruned depending on matching.
        """
        conn = self._connect()
        for table in ("release", "release_artist", "release_track",
                       "cache_metadata"):
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*) FROM {table}")
                count = cur.fetchone()[0]
            assert count > 0, f"Table {table} is empty"
        conn.close()

    def test_duplicates_removed(self) -> None:
        """Duplicate releases (same master_id) have been removed.

        In the fixture data, releases 1001, 1002, 1003 share master_id 500.
        After dedup, only release 1002 (5 tracks, the most) should remain.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM release WHERE id IN (1001, 1002, 1003) ORDER BY id"
            )
            ids = [row[0] for row in cur.fetchall()]
        conn.close()
        assert ids == [1002], f"Expected only 1002 after dedup, got {ids}"

    def test_prune_releases_gone(self) -> None:
        """Releases not matching the library have been pruned.

        Release 10001 ('Some Random Album' by 'Random Artist X') should be
        pruned as it doesn't match any library entry.
        """
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 10001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0, "Release 10001 should have been pruned"

    def test_keep_releases_present(self) -> None:
        """Releases matching the library are still present after pruning."""
        conn = self._connect()
        with conn.cursor() as cur:
            # Kid A (3001) should survive both dedup and prune
            cur.execute("SELECT count(*) FROM release WHERE id = 3001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 1, "Release 3001 (Kid A) should still exist"

    def test_master_id_column_absent(self) -> None:
        """master_id column is dropped by the dedup copy-swap."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'release' AND column_name = 'master_id'"
            )
            result = cur.fetchone()
        conn.close()
        assert result is None, "master_id column should not exist after dedup"

    def test_indexes_exist(self) -> None:
        """Trigram indexes exist on the final database."""
        conn = self._connect()
        with conn.cursor() as cur:
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
        assert expected.issubset(indexes), f"Missing indexes: {expected - indexes}"

    def test_fk_constraints_exist(self) -> None:
        """FK constraints exist on all child tables."""
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

    def test_null_title_release_not_imported(self) -> None:
        """Release 7001 (empty title) should not exist."""
        conn = self._connect()
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release WHERE id = 7001")
            count = cur.fetchone()[0]
        conn.close()
        assert count == 0


class TestPipelineWithoutLibrary:
    """Run pipeline without library.db (skips prune step)."""

    @pytest.fixture(autouse=True, scope="class")
    def _run_pipeline(self, e2e_db_url):
        """Run run_pipeline.py without library.db."""
        self.__class__._db_url = e2e_db_url

        result = subprocess.run(
            [
                sys.executable,
                str(RUN_PIPELINE),
                str(CSV_DIR),
                # No library_db argument — prune should be skipped
            ],
            capture_output=True,
            text=True,
            timeout=120,
            env={
                **os.environ,
                "DATABASE_URL": e2e_db_url,
            },
        )

        self.__class__._returncode = result.returncode
        self.__class__._stderr = result.stderr

        if result.returncode != 0:
            print("STDOUT:", result.stdout)
            print("STDERR:", result.stderr)

        assert result.returncode == 0, (
            f"Pipeline failed (exit {result.returncode}):\n{result.stderr}"
        )

    @pytest.fixture(autouse=True)
    def _store_url(self):
        self.db_url = self.__class__._db_url

    def test_tables_populated(self) -> None:
        """Tables should still be populated when prune is skipped."""
        conn = psycopg.connect(self.db_url)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM release")
            count = cur.fetchone()[0]
        conn.close()
        assert count > 0

    def test_prune_skipped_message(self) -> None:
        """Log should indicate prune was skipped."""
        assert "Skipping prune step" in self.__class__._stderr
