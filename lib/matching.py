"""Matching utilities for compilation detection."""

COMPILATION_KEYWORDS = frozenset(
    {
        "various",
        "soundtrack",
        "compilation",
        "v/a",
        "v.a.",
    }
)
"""Keywords indicating a compilation/soundtrack album (case-insensitive substring match)."""


def is_compilation_artist(artist: str) -> bool:
    """Check if an artist name indicates a compilation/soundtrack album.

    Args:
        artist: Artist name to check

    Returns:
        True if artist contains compilation keywords (various, soundtrack, etc.)
    """
    if not artist:
        return False
    artist_lower = artist.lower()
    return any(keyword in artist_lower for keyword in COMPILATION_KEYWORDS)
