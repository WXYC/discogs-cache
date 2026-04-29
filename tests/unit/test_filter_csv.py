"""Unit tests for scripts/filter_csv.py."""

from __future__ import annotations

import csv
import importlib.util
from pathlib import Path

import pytest

# Load filter_csv module from scripts directory
_SCRIPT_PATH = Path(__file__).parent.parent.parent / "scripts" / "filter_csv.py"
_spec = importlib.util.spec_from_file_location("filter_csv", _SCRIPT_PATH)
assert _spec is not None and _spec.loader is not None
_fc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_fc)

normalize_artist = _fc.normalize_artist
load_library_artists = _fc.load_library_artists
find_matching_release_ids = _fc.find_matching_release_ids
filter_csv_file = _fc.filter_csv_file
get_release_id_column = _fc.get_release_id_column
main = _fc.main
normalize_title = _fc.normalize_title
load_library_pairs = _fc.load_library_pairs
find_matching_release_ids_pairwise = _fc.find_matching_release_ids_pairwise
filter_csvs_by_pairs = _fc.filter_csvs_by_pairs

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# normalize_artist
# ---------------------------------------------------------------------------


class TestNormalizeArtist:
    """Artist normalization for matching."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Autechre", "autechre"),
            ("  Autechre  ", "autechre"),
            ("AUTECHRE", "autechre"),
            ("  Mixed Case  ", "mixed case"),
            ("", ""),
            # Diacritic regression cases. Inputs are drawn from the WXYC
            # canonical artist pool where possible (Csillagrablók, Hermanos
            # Gutiérrez, Nilüfer Yanya), then supplemented with synthetic
            # cases for diacritic categories the canonical pool doesn't
            # cover (e.g., a-grave, n-tilde) so the normalizer's Unicode
            # handling stays exercised across the Latin block.
            ("Csillagrablók", "csillagrablok"),
            ("Hermanos Gutiérrez", "hermanos gutierrez"),
            ("Nilüfer Yanya", "nilufer yanya"),
            ("Père Ubu", "pere ubu"),
            ("Señor Coconut", "senor coconut"),
            ("Façade", "facade"),
        ],
        ids=[
            "lowercase",
            "strip-spaces",
            "all-caps",
            "mixed-case-strip",
            "empty",
            "csillagrablok",
            "hermanos-gutierrez",
            "nilufer-yanya",
            "pere-ubu",
            "senor-coconut",
            "facade",
        ],
    )
    def test_normalize(self, raw: str, expected: str) -> None:
        assert normalize_artist(raw) == expected


# ---------------------------------------------------------------------------
# load_library_artists
# ---------------------------------------------------------------------------


class TestLoadLibraryArtists:
    """Loading artist names from library_artists.txt."""

    def test_loads_fixture_file(self) -> None:
        path = FIXTURES_DIR / "library_artists.txt"
        artists = load_library_artists(path)
        assert isinstance(artists, set)
        assert len(artists) > 0

    def test_names_are_normalized(self) -> None:
        path = FIXTURES_DIR / "library_artists.txt"
        artists = load_library_artists(path)
        # All names should be lowercase and stripped
        for name in artists:
            assert name == name.lower().strip()

    def test_canonical_artist_in_set(self) -> None:
        path = FIXTURES_DIR / "library_artists.txt"
        artists = load_library_artists(path)
        assert "autechre" in artists

    def test_blank_lines_excluded(self, tmp_path: Path) -> None:
        txt = tmp_path / "artists.txt"
        txt.write_text("Alpha\n\n  \nBeta\n")
        artists = load_library_artists(txt)
        assert artists == {"alpha", "beta"}


# ---------------------------------------------------------------------------
# find_matching_release_ids
# ---------------------------------------------------------------------------


class TestFindMatchingReleaseIds:
    """Finding release IDs with matching artists from release_artist.csv."""

    def test_finds_matching_ids(self) -> None:
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"autechre"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        # Autechre is on releases 1001, 1002, 1003, 3001, 4001
        assert ids == {1001, 1002, 1003, 3001, 4001}

    def test_no_matches(self) -> None:
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"nonexistent artist xyz"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        assert ids == set()

    def test_multiple_artists(self) -> None:
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"autechre", "stereolab"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        assert {1001, 1002, 1003, 3001, 4001, 2001, 2002}.issubset(ids)

    def test_extra_artists_not_matched_for_id(self) -> None:
        """Extra artists (credit=1) still use their artist_name for matching."""
        release_artist_path = FIXTURES_DIR / "csv" / "release_artist.csv"
        library_artists = {"some producer"}
        ids = find_matching_release_ids(release_artist_path, library_artists)
        # "Some Producer" is an extra artist on release 1001
        assert 1001 in ids

    def test_normalize_cache_avoids_redundant_calls(self, tmp_path: Path) -> None:
        """Duplicate artist names should only be normalized once (via cache)."""
        csv_path = tmp_path / "release_artist.csv"
        # Write a CSV with the same artist name repeated many times
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["release_id", "artist_id", "artist_name", "extra", "anv", "position"])
            for i in range(1, 101):
                writer.writerow([i, 1, "Juana Molina", 0, "", 1])

        from unittest.mock import patch

        call_count = 0
        original_normalize = normalize_artist

        def counting_normalize(name):
            nonlocal call_count
            call_count += 1
            return original_normalize(name)

        with patch.object(_fc, "normalize_artist", side_effect=counting_normalize):
            find_matching_release_ids(csv_path, {"juana molina"})

        # With caching, normalize should be called once for the unique name,
        # not 100 times for every row.
        assert call_count == 1


# ---------------------------------------------------------------------------
# get_release_id_column
# ---------------------------------------------------------------------------


class TestGetReleaseIdColumn:
    """Column name detection for different CSV files."""

    @pytest.mark.parametrize(
        "filename, expected",
        [
            ("release.csv", "id"),
            ("release_artist.csv", "release_id"),
            ("release_track.csv", "release_id"),
            ("release_track_artist.csv", "release_id"),
            ("release_image.csv", "release_id"),
        ],
        ids=["release", "release_artist", "release_track", "release_track_artist", "release_image"],
    )
    def test_column_name(self, filename: str, expected: str) -> None:
        assert get_release_id_column(filename) == expected


# ---------------------------------------------------------------------------
# filter_csv_file
# ---------------------------------------------------------------------------


class TestFilterCsvFile:
    """Filtering a CSV file to only matching release IDs."""

    def test_filters_to_matching_ids(self, tmp_path: Path) -> None:
        matching_ids = {1001, 3001}
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "release_filtered.csv"

        input_count, output_count = filter_csv_file(input_path, output_path, matching_ids, "id")
        assert input_count > 0
        assert output_count == 2

        # Verify output contains only matching IDs
        with open(output_path) as f:
            reader = csv.DictReader(f)
            ids = {int(row["id"]) for row in reader}
        assert ids == {1001, 3001}

    def test_preserves_all_columns(self, tmp_path: Path) -> None:
        matching_ids = {1001}
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "release_filtered.csv"

        filter_csv_file(input_path, output_path, matching_ids, "id")

        with open(input_path) as f:
            original_headers = csv.DictReader(f).fieldnames

        with open(output_path) as f:
            filtered_headers = csv.DictReader(f).fieldnames

        assert original_headers == filtered_headers

    def test_empty_matching_set(self, tmp_path: Path) -> None:
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "release_filtered.csv"

        input_count, output_count = filter_csv_file(input_path, output_path, set(), "id")
        assert input_count > 0
        assert output_count == 0

    def test_filters_child_table(self, tmp_path: Path) -> None:
        matching_ids = {1001}
        input_path = FIXTURES_DIR / "csv" / "release_track.csv"
        output_path = tmp_path / "release_track_filtered.csv"

        _, output_count = filter_csv_file(input_path, output_path, matching_ids, "release_id")
        assert output_count == 5  # Release 1001 has 5 tracks

    def test_missing_id_column_raises_clear_error(self, tmp_path: Path) -> None:
        """When id_column is not in the CSV header, a ValueError is raised
        with a message listing the available columns."""
        input_path = FIXTURES_DIR / "csv" / "release.csv"
        output_path = tmp_path / "out.csv"

        with pytest.raises(ValueError, match="Column 'nonexistent'.*not found"):
            filter_csv_file(input_path, output_path, {1001}, "nonexistent")

    def test_row_with_invalid_release_id_skipped(self, tmp_path: Path) -> None:
        """Rows where the release_id is not a valid integer are silently skipped."""
        csv_path = tmp_path / "release.csv"
        output_path = tmp_path / "out.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title"])
            writer.writerow(["abc", "Bad ID"])
            writer.writerow(["1001", "Good ID"])

        input_count, output_count = filter_csv_file(csv_path, output_path, {1001}, "id")
        assert input_count == 2
        assert output_count == 1

    def test_short_row_skipped(self, tmp_path: Path) -> None:
        """Rows shorter than expected (IndexError on id column) are silently skipped."""
        csv_path = tmp_path / "release.csv"
        output_path = tmp_path / "out.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "title", "country"])
            # Normal row
            writer.writerow(["1001", "DOGA", "AR"])
            # Write a short row manually (fewer columns than header)
            f.write('"short"\n')

        input_count, output_count = filter_csv_file(csv_path, output_path, {1001}, "id")
        assert input_count == 2
        assert output_count == 1


class TestFindMatchingReleaseIdsEdgeCases:
    """Edge cases for find_matching_release_ids."""

    def test_short_row_skipped(self, tmp_path: Path) -> None:
        """Rows missing the artist_name column are silently skipped."""
        csv_path = tmp_path / "release_artist.csv"
        with open(csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["release_id", "artist_id", "artist_name", "extra", "anv", "position"])
            writer.writerow(["1001", "101", "Juana Molina", "0", "", "1"])
            # Short row missing artist_name
            f.write('"2001","201"\n')

        ids = find_matching_release_ids(csv_path, {"juana molina"})
        assert ids == {1001}


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMain:
    """Tests for the main() entry point."""

    def test_wrong_arg_count_exits(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("sys.argv", ["filter_csv.py"])
        with pytest.raises(SystemExit) as exc_info:
            main()
        # argparse uses exit code 2 for usage errors; the legacy 3-positional
        # CLI used exit 1. The exit-on-bad-input contract still holds.
        assert exc_info.value.code in (1, 2)

    def test_missing_library_artists_exits(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            "sys.argv",
            [
                "filter_csv.py",
                str(tmp_path / "nonexistent.txt"),
                str(tmp_path),
                str(tmp_path / "out"),
            ],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_missing_csv_dir_exits(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        artists_file = tmp_path / "artists.txt"
        artists_file.write_text("Juana Molina\n")
        monkeypatch.setattr(
            "sys.argv",
            ["filter_csv.py", str(artists_file), str(tmp_path / "nope"), str(tmp_path / "out")],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_missing_release_artist_csv_exits(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        artists_file = tmp_path / "artists.txt"
        artists_file.write_text("Juana Molina\n")
        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        monkeypatch.setattr(
            "sys.argv",
            ["filter_csv.py", str(artists_file), str(csv_dir), str(tmp_path / "out")],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_no_matches_exits(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        artists_file = tmp_path / "artists.txt"
        artists_file.write_text("Nonexistent Artist XYZ\n")

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        with open(csv_dir / "release_artist.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["release_id", "artist_id", "artist_name", "extra", "anv", "position"])
            writer.writerow(["1001", "101", "Juana Molina", "0", "", "1"])

        monkeypatch.setattr(
            "sys.argv",
            ["filter_csv.py", str(artists_file), str(csv_dir), str(tmp_path / "out")],
        )
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == 1

    def test_happy_path(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        artists_file = tmp_path / "artists.txt"
        artists_file.write_text("Juana Molina\n")

        csv_dir = tmp_path / "csv"
        csv_dir.mkdir()
        out_dir = tmp_path / "out"

        with open(csv_dir / "release_artist.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["release_id", "artist_id", "artist_name", "extra", "anv", "position"])
            writer.writerow(["5001", "101", "Juana Molina", "0", "", "1"])
            writer.writerow(["5002", "102", "Stereolab", "0", "", "1"])

        with open(csv_dir / "release.csv", "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    "id",
                    "status",
                    "title",
                    "country",
                    "released",
                    "notes",
                    "data_quality",
                    "master_id",
                    "format",
                ]
            )
            writer.writerow(
                ["5001", "Accepted", "DOGA", "AR", "2024-05-10", "", "Correct", "8001", "LP"]
            )
            writer.writerow(
                [
                    "5002",
                    "Accepted",
                    "Aluminum Tunes",
                    "UK",
                    "1998-09-01",
                    "",
                    "Correct",
                    "8002",
                    "CD",
                ]
            )

        monkeypatch.setattr(
            "sys.argv",
            ["filter_csv.py", str(artists_file), str(csv_dir), str(out_dir)],
        )
        main()

        assert out_dir.exists()
        with open(out_dir / "release.csv") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["id"] == "5001"


# ---------------------------------------------------------------------------
# Pair-wise (artist, title) filter — closes the OOM gap on Railway-sized DBs
# (#128). The artist-only filter passes ~4.2M releases through; pair-wise
# narrows to ~58K so import doesn't overflow the destination volume.
# ---------------------------------------------------------------------------


import sqlite3  # noqa: E402


class TestNormalizeTitle:
    """Release-title normalization for pair matching."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("Confield", "confield"),
            ("  Confield  ", "confield"),
            ("CONFIELD", "confield"),
            ("On Your Own Love Again", "on your own love again"),
            ("", ""),
            # Diacritic regressions: the title's normalization must be the
            # same shape as artist normalization so a Discogs "PAINLESS"
            # against library "PAINLESS" matches even after the canonical
            # diacritic-bearing artist (Nilüfer Yanya) routes them together.
            ("Pequeña Vertigem de Amor", "pequena vertigem de amor"),
            ("Père Ubu's Métal Box", "pere ubu's metal box"),
        ],
    )
    def test_normalize_title(self, raw: str, expected: str) -> None:
        assert normalize_title(raw) == expected


class TestLoadLibraryPairs:
    """Loading (artist, title) pairs from the library.db SQLite file."""

    def _make_library_db(self, tmp_path: Path, rows: list[tuple[str, str]]) -> Path:
        path = tmp_path / "library.db"
        conn = sqlite3.connect(str(path))
        conn.execute(
            "CREATE TABLE library (id INTEGER PRIMARY KEY AUTOINCREMENT, "
            "artist TEXT NOT NULL, title TEXT NOT NULL, format TEXT)"
        )
        conn.executemany("INSERT INTO library (artist, title) VALUES (?, ?)", rows)
        conn.commit()
        conn.close()
        return path

    def test_returns_inverted_index_keyed_by_title(self, tmp_path: Path) -> None:
        db = self._make_library_db(
            tmp_path,
            [
                ("Autechre", "Confield"),
                ("Autechre", "Amber"),
                ("Stereolab", "Aluminum Tunes"),
            ],
        )
        pairs = load_library_pairs(db)
        assert pairs == {
            "confield": {"autechre"},
            "amber": {"autechre"},
            "aluminum tunes": {"stereolab"},
        }

    def test_collapses_duplicate_rows(self, tmp_path: Path) -> None:
        # The fixture library.db has duplicate (artist, title) rows from
        # multiple library copies of the same album. The set-valued index
        # collapses these.
        db = self._make_library_db(
            tmp_path,
            [
                ("Stereolab", "Aluminum Tunes"),
                ("Stereolab", "Aluminum Tunes"),
                ("Stereolab", "Aluminum Tunes"),
            ],
        )
        pairs = load_library_pairs(db)
        assert pairs == {"aluminum tunes": {"stereolab"}}

    def test_groups_multiple_artists_under_same_title(self, tmp_path: Path) -> None:
        db = self._make_library_db(
            tmp_path,
            [
                ("Various Artists", "Compilation"),
                ("Stereolab", "Compilation"),
            ],
        )
        pairs = load_library_pairs(db)
        assert pairs == {"compilation": {"various artists", "stereolab"}}

    def test_normalizes_diacritics_on_load(self, tmp_path: Path) -> None:
        db = self._make_library_db(
            tmp_path,
            [("Nilüfer Yanya", "PAINLESS")],
        )
        pairs = load_library_pairs(db)
        assert pairs == {"painless": {"nilufer yanya"}}


class TestFindMatchingReleaseIdsPairwise:
    """Two-pass pair-wise scan: keep only release_ids whose (artist, title)
    matches a library pair."""

    def _write_csv(self, path: Path, header: list[str], rows: list[list[str]]) -> None:
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)

    def test_exact_pair_match(self, tmp_path: Path) -> None:
        release = tmp_path / "release.csv"
        release_artist = tmp_path / "release_artist.csv"
        self._write_csv(release, ["id", "title"], [["1001", "Confield"]])
        self._write_csv(release_artist, ["release_id", "artist_name"], [["1001", "Autechre"]])
        pairs = {"confield": {"autechre"}}
        ids = find_matching_release_ids_pairwise(release, release_artist, pairs)
        assert ids == {1001}

    def test_title_in_library_but_artist_isnt_excluded(self, tmp_path: Path) -> None:
        release = tmp_path / "release.csv"
        release_artist = tmp_path / "release_artist.csv"
        self._write_csv(release, ["id", "title"], [["1001", "Confield"]])
        # release_artist has the right title-keyed candidate but artist is wrong
        self._write_csv(
            release_artist, ["release_id", "artist_name"], [["1001", "Some Other Band"]]
        )
        pairs = {"confield": {"autechre"}}
        ids = find_matching_release_ids_pairwise(release, release_artist, pairs)
        assert ids == set()

    def test_title_not_in_library_excludes_release(self, tmp_path: Path) -> None:
        release = tmp_path / "release.csv"
        release_artist = tmp_path / "release_artist.csv"
        self._write_csv(release, ["id", "title"], [["1001", "Some Other Album"]])
        self._write_csv(release_artist, ["release_id", "artist_name"], [["1001", "Autechre"]])
        pairs = {"confield": {"autechre"}}
        ids = find_matching_release_ids_pairwise(release, release_artist, pairs)
        assert ids == set()

    def test_multi_artist_release_kept_when_one_matches(self, tmp_path: Path) -> None:
        # A release with several featured artists is kept if ANY one of them
        # forms a library pair with the release's title.
        release = tmp_path / "release.csv"
        release_artist = tmp_path / "release_artist.csv"
        self._write_csv(release, ["id", "title"], [["9001", "From Here We Go Sublime"]])
        self._write_csv(
            release_artist,
            ["release_id", "artist_name"],
            [
                ["9001", "Some Producer"],
                ["9001", "Field, The"],
            ],
        )
        pairs = {"from here we go sublime": {"field, the"}}
        ids = find_matching_release_ids_pairwise(release, release_artist, pairs)
        assert ids == {9001}

    def test_diacritics_normalized_on_both_sides(self, tmp_path: Path) -> None:
        release = tmp_path / "release.csv"
        release_artist = tmp_path / "release_artist.csv"
        self._write_csv(release, ["id", "title"], [["6001", "PAINLESS"]])
        self._write_csv(release_artist, ["release_id", "artist_name"], [["6001", "Nilüfer Yanya"]])
        # Library entries are pre-normalized by load_library_pairs, but we
        # build the pairs-set the same way here to keep the test honest.
        pairs = {"painless": {"nilufer yanya"}}
        ids = find_matching_release_ids_pairwise(release, release_artist, pairs)
        assert ids == {6001}

    def test_handles_malformed_rows(self, tmp_path: Path) -> None:
        # Defensive: short rows / non-numeric IDs should be skipped without
        # taking the whole pass down.
        release = tmp_path / "release.csv"
        release_artist = tmp_path / "release_artist.csv"
        self._write_csv(
            release,
            ["id", "title"],
            [
                ["not-an-int", "Confield"],
                ["1001", "Confield"],
            ],
        )
        self._write_csv(
            release_artist,
            ["release_id", "artist_name"],
            [
                ["1001", "Autechre"],
                ["bad-row"],  # short row
            ],
        )
        pairs = {"confield": {"autechre"}}
        ids = find_matching_release_ids_pairwise(release, release_artist, pairs)
        assert ids == {1001}


class TestFilterCsvsByPairs:
    """End-to-end orchestrator: library.db + CSV input dir → filtered CSVs."""

    def test_filters_against_real_fixtures(self, tmp_path: Path) -> None:
        # Reuses tests/fixtures/library.db + tests/fixtures/csv/. Combined,
        # they contain six (artist, title) pairs that match the fixture
        # release rows: (Autechre, Confield) covers 1001/1002/1003;
        # (Autechre, Amber) → 3001; (Autechre, Tri Repetae) → 4001;
        # (Stereolab, Aluminum Tunes) → 2001/2002; (Field, The, From Here
        # We Go Sublime) → 9001; (Nilüfer Yanya, PAINLESS) → 6001 via
        # diacritic normalization. Compound-artist release 9002 (Duke
        # Ellington & John Coltrane) is a known false negative — the
        # split artist rows don't match the combined library string.
        out_dir = tmp_path / "filtered"
        stats = filter_csvs_by_pairs(
            FIXTURES_DIR / "library.db",
            FIXTURES_DIR / "csv",
            out_dir,
        )

        with open(out_dir / "release.csv") as f:
            kept_ids = {int(row["id"]) for row in csv.DictReader(f)}

        assert kept_ids == {1001, 1002, 1003, 2001, 2002, 3001, 4001, 6001, 9001}

        # Sanity: the filtered CSV is a strict subset of the fixture, and
        # release_artist.csv has been narrowed to the surviving release_ids.
        with open(out_dir / "release_artist.csv") as f:
            ra_release_ids = {int(row["release_id"]) for row in csv.DictReader(f)}
        assert ra_release_ids <= kept_ids
        assert ra_release_ids == kept_ids  # every survivor still has its artists

        # Stats are populated for each file actually present in the input dir.
        assert "release.csv" in stats
        assert "release_artist.csv" in stats
        in_count, out_count = stats["release.csv"]
        assert out_count == len(kept_ids)
        assert in_count > out_count  # some releases were dropped

    def test_overwrites_input_when_input_and_output_dir_match(self, tmp_path: Path) -> None:
        # Deployment pattern: rebuild-cache.yml runs the pair-wise filter
        # in-place over the converter's output dir to keep runner disk small.
        # Copy the fixture CSVs into a writable scratch dir first.
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        for src in (FIXTURES_DIR / "csv").iterdir():
            if src.is_file():
                (scratch / src.name).write_bytes(src.read_bytes())

        filter_csvs_by_pairs(
            FIXTURES_DIR / "library.db",
            scratch,
            scratch,
        )

        with open(scratch / "release.csv") as f:
            kept_ids = {int(row["id"]) for row in csv.DictReader(f)}
        # Same result as the separate-output case.
        assert 1001 in kept_ids and 9002 not in kept_ids
