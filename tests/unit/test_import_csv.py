"""Unit tests for scripts/import_csv.py."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

# Load import_csv module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "import_csv.py"
_spec = importlib.util.spec_from_file_location("import_csv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_ic = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_ic)

extract_year = _ic.extract_year
TABLES = _ic.TABLES
TableConfig = _ic.TableConfig


# ---------------------------------------------------------------------------
# extract_year
# ---------------------------------------------------------------------------


class TestExtractYear:
    """Extracting a 4-digit year from Discogs 'released' field."""

    @pytest.mark.parametrize(
        "input_val, expected",
        [
            ("2023-01-15", "2023"),
            ("1997-06-16", "1997"),
            ("1969-09-26", "1969"),
            ("2000", "2000"),
            ("1979", "1979"),
            ("", None),
            (None, None),
            ("Unknown", None),
            ("TBD", None),
            ("0000", "0000"),
            ("2023-00-00", "2023"),
        ],
        ids=[
            "full-date", "full-date-1997", "full-date-1969",
            "year-only", "year-only-1979",
            "empty", "none", "unknown-text", "tbd-text",
            "zeros", "partial-date",
        ],
    )
    def test_extract_year(self, input_val: str | None, expected: str | None) -> None:
        assert extract_year(input_val) == expected


# ---------------------------------------------------------------------------
# TABLES config validation
# ---------------------------------------------------------------------------


class TestTablesConfig:
    """Validate the TABLES configuration for CSV import."""

    def test_all_tables_have_matching_column_lengths(self) -> None:
        """csv_columns and db_columns must be the same length."""
        for table_config in TABLES:
            assert len(table_config["csv_columns"]) == len(table_config["db_columns"]), (
                f"Column length mismatch in {table_config['table']}: "
                f"csv_columns={len(table_config['csv_columns'])}, "
                f"db_columns={len(table_config['db_columns'])}"
            )

    def test_required_columns_are_subset_of_csv_columns(self) -> None:
        """Required columns must exist in csv_columns."""
        for table_config in TABLES:
            csv_set = set(table_config["csv_columns"])
            required_set = set(table_config["required"])
            assert required_set.issubset(csv_set), (
                f"Required columns not in csv_columns for {table_config['table']}: "
                f"{required_set - csv_set}"
            )

    def test_release_table_includes_master_id(self) -> None:
        """The release table must import master_id for dedup."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "master_id" in release_config["csv_columns"]
        assert "master_id" in release_config["db_columns"]

    def test_release_table_transforms_released_to_year(self) -> None:
        """The released field should be transformed via extract_year."""
        release_config = next(t for t in TABLES if t["table"] == "release")
        assert "released" in release_config["transforms"]
        assert release_config["transforms"]["released"] is extract_year

    @pytest.mark.parametrize(
        "table_name",
        ["release", "release_artist", "release_track", "release_track_artist"],
    )
    def test_table_has_csv_file(self, table_name: str) -> None:
        """Each table config specifies a CSV file."""
        table_config = next(t for t in TABLES if t["table"] == table_name)
        assert table_config["csv_file"].endswith(".csv")

    def test_all_tables_have_required_keys(self) -> None:
        """Each table config must have all required TypedDict keys."""
        required_keys = {"csv_file", "table", "csv_columns", "db_columns", "required", "transforms"}
        for table_config in TABLES:
            assert required_keys.issubset(table_config.keys()), (
                f"Missing keys in {table_config.get('table', '?')}: "
                f"{required_keys - table_config.keys()}"
            )


# ---------------------------------------------------------------------------
# Column header detection
# ---------------------------------------------------------------------------


class TestColumnHeaderDetection:
    """Verify CSV column expectations match fixture data."""

    def test_release_csv_has_expected_columns(self) -> None:
        import csv as csv_mod

        csv_path = Path(__file__).parent.parent / "fixtures" / "csv" / "release.csv"
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            headers = reader.fieldnames
        assert headers is not None
        release_config = next(t for t in TABLES if t["table"] == "release")
        for col in release_config["csv_columns"]:
            assert col in headers, f"Expected column {col!r} not in release.csv headers: {headers}"

    def test_release_artist_csv_has_expected_columns(self) -> None:
        import csv as csv_mod

        csv_path = Path(__file__).parent.parent / "fixtures" / "csv" / "release_artist.csv"
        with open(csv_path) as f:
            reader = csv_mod.DictReader(f)
            headers = reader.fieldnames
        assert headers is not None
        ra_config = next(t for t in TABLES if t["table"] == "release_artist")
        for col in ra_config["csv_columns"]:
            assert col in headers, (
                f"Expected column {col!r} not in release_artist.csv headers: {headers}"
            )
