"""Unit tests for scripts/enrich_library_artists.py."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

# Load enrich_library_artists module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "enrich_library_artists.py"
_spec = importlib.util.spec_from_file_location("enrich_library_artists", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_mod = importlib.util.module_from_spec(_spec)
sys.modules["enrich_library_artists"] = _mod
_spec.loader.exec_module(_mod)

extract_base_artists = _mod.extract_base_artists
merge_and_write = _mod.merge_and_write
parse_args = _mod.parse_args

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# extract_base_artists
# ---------------------------------------------------------------------------


class TestExtractBaseArtists:
    """Extracting unique artist names from library.db."""

    def test_returns_nonempty_set(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        assert isinstance(artists, set)
        assert len(artists) > 0

    def test_contains_known_artist(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        # library.db has "Radiohead" as an artist
        assert "Radiohead" in artists

    def test_excludes_compilation_artists(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        for name in artists:
            name_lower = name.lower()
            assert "various" not in name_lower, f"Compilation artist not excluded: {name}"

    def test_no_empty_strings(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        assert "" not in artists
        assert all(name.strip() for name in artists)

    def test_preserves_original_case(self) -> None:
        artists = extract_base_artists(FIXTURES_DIR / "library.db")
        # Should have mixed case, not all lowercase
        assert "Radiohead" in artists
        assert "radiohead" not in artists


# ---------------------------------------------------------------------------
# merge_and_write
# ---------------------------------------------------------------------------


class TestMergeAndWrite:
    """Merging artist sets and writing output file."""

    def test_merges_all_sources(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha", "Beta"},
            alternates={"Gamma"},
            cross_refs={"Delta"},
            release_cross_refs={"Epsilon"},
            output=output,
        )
        lines = output.read_text().splitlines()
        assert set(lines) == {"Alpha", "Beta", "Gamma", "Delta", "Epsilon"}

    def test_no_duplicates(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha", "Beta"},
            alternates={"Beta", "Gamma"},
            cross_refs={"Gamma", "Delta"},
            release_cross_refs={"Alpha"},
            output=output,
        )
        lines = output.read_text().splitlines()
        assert len(lines) == len(set(lines))
        assert set(lines) == {"Alpha", "Beta", "Gamma", "Delta"}

    def test_sorted_output(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Zebra", "Apple", "Mango"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert lines == sorted(lines)

    def test_excludes_empty_strings(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha", ""},
            alternates={"  ", "Beta"},
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert "" not in lines
        assert "  " not in lines
        assert set(lines) == {"Alpha", "Beta"}

    def test_excludes_compilation_artists(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha"},
            alternates={"Various Artists", "Soundtrack Orchestra"},
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert "Various Artists" not in lines
        assert "Soundtrack Orchestra" not in lines
        assert "Alpha" in lines

    def test_preserves_original_case(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"The Beatles", "RZA", "dj shadow"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        lines = output.read_text().splitlines()
        assert "The Beatles" in lines
        assert "RZA" in lines
        assert "dj shadow" in lines

    def test_trailing_newline(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base={"Alpha"},
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        content = output.read_text()
        assert content.endswith("\n")

    def test_empty_sets_produce_empty_file(self, tmp_path: Path) -> None:
        output = tmp_path / "artists.txt"
        merge_and_write(
            base=set(),
            alternates=set(),
            cross_refs=set(),
            release_cross_refs=set(),
            output=output,
        )
        content = output.read_text()
        assert content == ""


# ---------------------------------------------------------------------------
# parse_args
# ---------------------------------------------------------------------------


class TestParseArgs:
    """CLI argument parsing."""

    def test_required_args(self) -> None:
        args = parse_args(
            [
                "--library-db",
                "library.db",
                "--output",
                "artists.txt",
            ]
        )
        assert args.library_db == Path("library.db")
        assert args.output == Path("artists.txt")
        assert args.wxyc_db_url is None

    def test_with_wxyc_db_url(self) -> None:
        args = parse_args(
            [
                "--library-db",
                "library.db",
                "--output",
                "artists.txt",
                "--wxyc-db-url",
                "mysql://wxyc:wxyc@localhost:3307/wxycmusic",
            ]
        )
        assert args.wxyc_db_url == "mysql://wxyc:wxyc@localhost:3307/wxycmusic"

    def test_missing_required_args_exits(self) -> None:
        with pytest.raises(SystemExit):
            parse_args(["--library-db", "library.db"])  # missing --output
