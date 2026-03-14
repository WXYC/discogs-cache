"""Tests for lib/format_normalization.py — format category mapping."""

import importlib.util
import sys
from pathlib import Path

import pytest

# Load format_normalization module from lib directory
_LIB_PATH = Path(__file__).parent.parent.parent / "lib" / "format_normalization.py"
_spec = importlib.util.spec_from_file_location("format_normalization", _LIB_PATH)
assert _spec is not None and _spec.loader is not None
_fn = importlib.util.module_from_spec(_spec)
sys.modules["format_normalization"] = _fn
_spec.loader.exec_module(_fn)

normalize_format = _fn.normalize_format
normalize_library_format = _fn.normalize_library_format
format_matches = _fn.format_matches


# ---------------------------------------------------------------------------
# normalize_format (Discogs format strings)
# ---------------------------------------------------------------------------


class TestNormalizeFormat:
    """Map raw Discogs format strings to broad categories."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            # Vinyl family
            ("Vinyl", "Vinyl"),
            ("LP", "Vinyl"),
            ("2xLP", "Vinyl"),
            ("3xLP", "Vinyl"),
            ('12"', "Vinyl"),
            ('10"', "Vinyl"),
            # CD family
            ("CD", "CD"),
            ("2xCD", "CD"),
            ("3xCD", "CD"),
            ("CD-R", "CD"),
            ("CDr", "CD"),
            # Cassette
            ("Cassette", "Cassette"),
            # 7" singles
            ('7"', '7"'),
            # Digital
            ("File", "Digital"),
            ("FLAC", "Digital"),
            ("MP3", "Digital"),
            ("WAV", "Digital"),
            # Unknown / empty
            (None, None),
            ("", None),
            ("Box Set", None),
            ("Laserdisc", None),
        ],
        ids=[
            "vinyl",
            "lp",
            "2xlp",
            "3xlp",
            "12_inch",
            "10_inch",
            "cd",
            "2xcd",
            "3xcd",
            "cd-r",
            "cdr",
            "cassette",
            "7_inch",
            "file",
            "flac",
            "mp3",
            "wav",
            "none",
            "empty",
            "box_set",
            "laserdisc",
        ],
    )
    def test_normalize_format(self, raw, expected):
        assert normalize_format(raw) == expected

    def test_multi_format_takes_first(self):
        """Multi-format strings separated by comma use the first format."""
        assert normalize_format("CD, DVD") == "CD"
        assert normalize_format("LP, CD") == "Vinyl"

    def test_case_insensitive(self):
        """Format matching is case-insensitive."""
        assert normalize_format("cd") == "CD"
        assert normalize_format("vinyl") == "Vinyl"
        assert normalize_format("lp") == "Vinyl"
        assert normalize_format("cassette") == "Cassette"

    def test_whitespace_stripped(self):
        """Leading/trailing whitespace is stripped."""
        assert normalize_format("  CD  ") == "CD"
        assert normalize_format(" LP ") == "Vinyl"


# ---------------------------------------------------------------------------
# normalize_library_format (WXYC library format strings)
# ---------------------------------------------------------------------------


class TestNormalizeLibraryFormat:
    """Map WXYC library format strings to the same categories."""

    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("LP", "Vinyl"),
            ("CD", "CD"),
            ("Cassette", "Cassette"),
            ('7"', '7"'),
            ("Vinyl", "Vinyl"),
            (None, None),
            ("", None),
            # Quantity suffix
            ("cd x 2", "CD"),
            ("cd x 3", "CD"),
            ("cd x 4", "CD"),
            ("cd x 2 box", "CD"),
            # Vinyl with size
            ('vinyl - 12"', "Vinyl"),
            ('vinyl - 7"', '7"'),
            ("vinyl - LP", "Vinyl"),
            ('vinyl - 10"', "Vinyl"),
            # Vinyl with size + quantity
            ("vinyl - LP x 2", "Vinyl"),
            ('vinyl - 7" x 2', '7"'),
            ('vinyl - 12" x 2', "Vinyl"),
            ('vinyl - 10" x 2', "Vinyl"),
        ],
        ids=[
            "lp",
            "cd",
            "cassette",
            "7_inch",
            "vinyl",
            "none",
            "empty",
            "cd_x_2",
            "cd_x_3",
            "cd_x_4",
            "cd_x_2_box",
            "vinyl_12_inch",
            "vinyl_7_inch",
            "vinyl_lp",
            "vinyl_10_inch",
            "vinyl_lp_x_2",
            "vinyl_7_inch_x_2",
            "vinyl_12_inch_x_2",
            "vinyl_10_inch_x_2",
        ],
    )
    def test_normalize_library_format(self, raw, expected):
        assert normalize_library_format(raw) == expected

    def test_case_insensitive(self):
        assert normalize_library_format("lp") == "Vinyl"
        assert normalize_library_format("cd") == "CD"


# ---------------------------------------------------------------------------
# format_matches
# ---------------------------------------------------------------------------


class TestFormatMatches:
    """Test format compatibility between release and library formats."""

    def test_matching_format(self):
        assert format_matches("CD", {"CD", "Vinyl"}) is True

    def test_non_matching_format(self):
        assert format_matches("Cassette", {"CD", "Vinyl"}) is False

    def test_none_release_format_matches_anything(self):
        """NULL release format matches any library format set."""
        assert format_matches(None, {"CD", "Vinyl"}) is True

    def test_none_in_library_formats_matches_anything(self):
        """NULL in library formats means match anything."""
        assert format_matches("CD", {None}) is True

    def test_empty_library_formats_matches_anything(self):
        """Empty library format set means no format data — match anything."""
        assert format_matches("CD", set()) is True

    def test_both_none(self):
        assert format_matches(None, {None}) is True

    def test_none_release_empty_library(self):
        assert format_matches(None, set()) is True
