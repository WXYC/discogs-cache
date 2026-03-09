"""Split combined multi-artist library entries into individual components.

The WXYC library stores multi-artist collaborations as combined strings
(e.g., "Mike Vainio, Ryoji, Alva Noto"), but Discogs models them as separate
release_artist rows. This module provides functions to split combined strings
into components for matching at the filter and KEEP/PRUNE stages.
"""

from __future__ import annotations

import re
import unicodedata


def _strip_accents(s: str) -> str:
    """Strip accents via NFKD decomposition (matching verify_cache.py's strip_accents)."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_for_lookup(name: str) -> str:
    """Normalize a name for known_artists lookup (lowercase + accent-stripped)."""
    return _strip_accents(name.strip()).lower()


def _is_numeric(s: str) -> bool:
    """Check if a string is purely numeric (digits, possibly with commas)."""
    return bool(re.fullmatch(r"[\d,]+", s))


def _valid_component(s: str) -> bool:
    """Check if a split component is meaningful (not too short or numeric)."""
    s = s.strip()
    if len(s) <= 1:
        return False
    return True


def _comma_guard(components: list[str]) -> bool:
    """Return True if the comma split looks like a real multi-artist delimiter.

    Returns False (blocking the split) if any component is purely numeric,
    which catches cases like "10,000 Maniacs" -> ["10", "000 Maniacs"].
    """
    for c in components:
        stripped = c.strip()
        if _is_numeric(stripped):
            return False
    return True


def _split_trailing_and(components: list[str]) -> list[str]:
    """Handle trailing 'and X' in the last comma-split component.

    "Emerson, Lake, and Palmer" -> ["Emerson", "Lake", "Palmer"]

    Does NOT handle '&' -- that's deferred to contextual splitting.
    """
    if len(components) < 2:
        return components

    last = components[-1].strip()

    # "and Palmer" -> "Palmer"
    and_match = re.match(r"^and\s+(.+)$", last, re.IGNORECASE)
    if and_match:
        return components[:-1] + [and_match.group(1)]

    return components


def split_artist_name(name: str) -> list[str]:
    """Split a combined artist name into individual components.

    Returns only the individual components, never the original combined name.
    Returns an empty list if the name doesn't appear to be a multi-artist entry.

    Splits on ``, `` (comma-space), `` / `` (slash), and `` + `` (plus).
    Does NOT split on `` & `` or `` and `` -- those are handled by
    split_artist_name_contextual() which can check against known artists.

    Comma guard: skip the split entirely if any component after splitting is
    purely numeric. This prevents splitting "10,000 Maniacs".
    """
    name = name.strip()
    if not name:
        return []

    components: list[str] | None = None

    # Try comma split first (most specific multi-artist pattern)
    if ", " in name:
        parts = [p.strip() for p in name.split(", ")]
        parts = _split_trailing_and(parts)
        if len(parts) >= 2 and _comma_guard(parts):
            components = parts

    # Try slash split
    if components is None and " / " in name:
        parts = [p.strip() for p in name.split(" / ")]
        if len(parts) >= 2:
            components = parts

    # Try plus split
    if components is None and " + " in name:
        parts = [p.strip() for p in name.split(" + ")]
        if len(parts) >= 2:
            components = parts

    if components is None:
        return []

    # Filter out invalid components and deduplicate (preserving order)
    seen: set[str] = set()
    result: list[str] = []
    for c in components:
        if _valid_component(c) and c not in seen:
            seen.add(c)
            result.append(c)

    # Only return components if we have at least 2 meaningful parts
    # (or 1 after dedup, which means there were duplicates)
    if len(result) < 1 or (len(result) == 1 and len(components) == 1):
        return []

    return result


def split_artist_name_contextual(name: str, known_artists: set[str]) -> list[str]:
    """Split a combined artist name, using context from the full artist set.

    First applies all context-free splits from split_artist_name(). Then,
    for any remaining unsplit components (or the original name if no context-free
    split applied), tries splitting on `` & `` when at least one resulting
    component (after normalization) exists in known_artists.

    Returns only the individual components, never the original combined name.
    Returns an empty list if no splitting applies.

    The known_artists set should contain normalized artist names (lowercase,
    accent-stripped) from the full library.
    """
    # First try context-free splitting
    components = split_artist_name(name)

    if components:
        # Re-check each component for contextual & splitting
        expanded: list[str] = []
        for c in components:
            sub = _try_ampersand_split(c, known_artists)
            if sub:
                expanded.extend(sub)
            else:
                expanded.append(c)
        return _dedupe(expanded)

    # No context-free split applied; try ampersand split on the whole name
    amp_result = _try_ampersand_split(name.strip(), known_artists)
    return amp_result if amp_result else []


def _try_ampersand_split(name: str, known_artists: set[str]) -> list[str] | None:
    """Try splitting on ' & ' if at least one component is a known artist.

    Returns the components if the split is valid, None otherwise.
    """
    if " & " not in name:
        return None

    parts = [p.strip() for p in name.split(" & ")]
    if len(parts) < 2:
        return None

    # Check if any component is a known artist
    for p in parts:
        if _normalize_for_lookup(p) in known_artists:
            # Valid split -- return all components that pass the filter
            valid = [p for p in parts if _valid_component(p)]
            return valid if len(valid) >= 2 else None

    return None


def _dedupe(items: list[str]) -> list[str]:
    """Deduplicate while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result
