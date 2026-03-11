"""Format normalization for Discogs and WXYC library format strings.

Maps raw format strings to broad categories where track listings are typically
identical within the category. Used by dedup (partition by format) and
verify_cache (format-aware KEEP/PRUNE decisions).

Categories:
    "Vinyl"    — LP, Vinyl, 2xLP, 3xLP, 12", 10"
    "CD"       — CD, 2xCD, 3xCD, CD-R, CDr
    "Cassette" — Cassette
    "7\""      — 7" singles (distinct track listings from LPs)
    "Digital"  — File, FLAC, MP3, WAV
    None       — unknown, empty, unrecognized
"""

from __future__ import annotations

import re

# Quantity prefix pattern: "2x", "3x", etc.
_QUANTITY_RE = re.compile(r"^\d+x", re.IGNORECASE)

# Mapping from lowercase format string to category.
_FORMAT_MAP: dict[str, str] = {
    "vinyl": "Vinyl",
    "lp": "Vinyl",
    '12"': "Vinyl",
    '10"': "Vinyl",
    "cd": "CD",
    "cd-r": "CD",
    "cdr": "CD",
    "cassette": "Cassette",
    '7"': '7"',
    "file": "Digital",
    "flac": "Digital",
    "mp3": "Digital",
    "wav": "Digital",
}


def normalize_format(raw: str | None) -> str | None:
    """Normalize a Discogs format string to a broad category.

    Splits multi-format on comma (takes first), strips quantity prefix ("2x"),
    and maps to a category. Returns None for unknown/empty/unrecognized formats.

    Args:
        raw: Raw Discogs format string, e.g. "2xLP", "CD, DVD", "Vinyl".

    Returns:
        Normalized category string or None.
    """
    if not raw:
        return None

    # Take the first format from multi-format strings
    fmt = raw.split(",")[0].strip()
    if not fmt:
        return None

    # Strip quantity prefix (e.g. "2x" from "2xLP")
    fmt = _QUANTITY_RE.sub("", fmt)

    return _FORMAT_MAP.get(fmt.lower())


def normalize_library_format(raw: str | None) -> str | None:
    """Normalize a WXYC library format string to the same category space.

    WXYC library uses simpler format names (LP, CD, Cassette, 7", Vinyl).

    Args:
        raw: Raw library format string.

    Returns:
        Normalized category string or None.
    """
    if not raw:
        return None

    fmt = raw.strip()
    if not fmt:
        return None

    return _FORMAT_MAP.get(fmt.lower())


def format_matches(release_format: str | None, library_formats: set[str | None]) -> bool:
    """Check if a release's format is compatible with the library's format set.

    Returns True if the release format is in the library's format set, or if
    either side has no format data (graceful degradation).

    Args:
        release_format: Normalized release format category (or None).
        library_formats: Set of normalized library format categories for a
            specific (artist, title) pair. May contain None.

    Returns:
        True if the formats are compatible.
    """
    # No library format data — match anything (backward-compatible)
    if not library_formats:
        return True

    # NULL release format — match anything (graceful degradation for direct-PG mode)
    if release_format is None:
        return True

    # NULL in library formats means "match anything"
    if None in library_formats:
        return True

    return release_format in library_formats
