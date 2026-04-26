"""Verify test fixtures only contain canonical WXYC artists.

The canonical source is the `canonicalArtistNames` array in
wxyc-shared/src/test-utils/wxyc-example-data.json. This guards against
regressions where contributors add mainstream artists (Radiohead, Beatles,
etc.) to the fixture data.

WXYC is a freeform college radio station — fixtures should reflect the
artists the station actually plays.
"""

from __future__ import annotations

import json
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LIBRARY_ARTISTS_TXT = REPO_ROOT / "tests" / "fixtures" / "library_artists.txt"

_CANONICAL_RELATIVE = "wxyc-shared/src/test-utils/wxyc-example-data.json"

# Allowed pseudonyms that don't appear in canonicalArtistNames but are
# legitimate fixture entries:
#   - "Field, The" — testing the "X, The" → "The X" inversion the
#     parser handles. The canonical form "The Field" is in the canonical
#     list; the comma-suffix form is the test variant.
#
# Note: production tooling (`wxyc-enrich-library-artists`) filters all
# compilation-marker artists out of library_artists.txt via
# `is_compilation_artist()`, so "Various Artists" (and its catalog variants
# like "Various Artists - Hiphop") never appear in this file in production
# and are not allowed here either.
ALLOWED_NON_CANONICAL = {"Field, The"}


def _find_canonical() -> Path:
    d = REPO_ROOT
    while d != d.parent:
        candidate = d / _CANONICAL_RELATIVE
        if candidate.exists():
            return candidate
        d = d.parent
    raise FileNotFoundError(f"could not locate {_CANONICAL_RELATIVE} above {REPO_ROOT}")


def _canonical_artist_names() -> set[str]:
    with _find_canonical().open() as f:
        return set(json.load(f)["canonicalArtistNames"])


def test_library_artists_txt_only_canonical() -> None:
    canonical = _canonical_artist_names() | ALLOWED_NON_CANONICAL
    listed = [
        line.strip()
        for line in LIBRARY_ARTISTS_TXT.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    non_canonical = [name for name in listed if name not in canonical]
    assert not non_canonical, (
        f"library_artists.txt contains non-canonical artists: {non_canonical}. "
        f"Use artists from {_CANONICAL_RELATIVE} (canonicalArtistNames array)."
    )
