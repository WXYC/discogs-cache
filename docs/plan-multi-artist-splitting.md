# Plan: Multi-Artist Splitting for Library Entries

## Problem

The WXYC library stores multi-artist collaborations as combined strings (e.g., "Mike Vainio, Ryoji, Alva Noto"), but Discogs models them as separate `release_artist` rows ("Mika Vainio", "Ryoji Ikeda", "Alva Noto"). This mismatch causes two failures:

1. **Filter stage**: The converter's `--library-artists` filter checks each individual Discogs artist against `library_artists.txt`. None of the individual names match the combined string, so the release is excluded entirely.
2. **KEEP/PRUNE stage**: `verify_cache.py` groups releases by individual artist name and matches against the `LibraryIndex`. Individual artist names like "Alva Noto" won't match the combined library entry "Mike Vainio, Ryoji, Alva Noto".

~2,042 library entries (3.16% of 64,597) contain multi-artist delimiters. Not all are true multi-artist entries (many are band names), so splitting must be applied carefully.

## Scope

This plan covers changes to `enrich_library_artists.py` (filter stage) and `verify_cache.py` (KEEP/PRUNE stage), plus a shared splitting utility in `lib/`. No changes to the Rust converter or database schema are needed.

## Approach: Conservative Delimiter-Based Splitting

Split combined artist strings into components and index them alongside the original. The pipeline's existing two-stage design (broad filter, then precise KEEP/PRUNE) means false positives from over-splitting at the filter stage are acceptable -- they'll be pruned later.

### Delimiter Strategy

The library uses these delimiters in combined artist entries:

| Delimiter | Count | Split? | Rationale |
|-----------|-------|--------|-----------|
| `, ` | 212 | Yes, with guard | Most reliable multi-artist signal. Guard: skip if component is a single word <= 3 chars or looks numeric (handles "10,000 Maniacs", "Emerson, Lake, and Palmer" edge cases -- but note ELP components would still be useful individually). |
| ` / ` | 45 | Yes | Almost always a multi-artist delimiter ("J Dilla / Jay Dee", "Fred Hopkins / Dierdre Murray"). |
| ` + ` | 19 | Yes | Discogs uses `+` as a join_field for collaborations ("Mika Vainio + Ryoji Ikeda + Alva Noto"). |
| ` & ` | 582 | Yes, with guard | Common in both band names ("Simon & Garfunkel") and collaborations ("Duke Ellington & John Coltrane"). Guard: only split if at least one component matches a known library artist individually. This requires a two-pass approach. |
| ` and ` | 1,201 | No (initial) | Too ambiguous -- "Andy Human and the Reptoids", "Sly and the Family Stone". Overwhelmingly band names with "and the" pattern. Could revisit with heuristics in a future phase. |
| ` with ` | 54 | No | Almost always band names ("Nurse with Wound", "My Life with the Thrill Kill Kult"). |

### Two-Pass Approach for ` & ` Splitting

To avoid splitting band names like "Simon & Garfunkel":

1. **Pass 1**: Collect all unique individual artist names from the library (the full set of `SELECT DISTINCT artist` values).
2. **Pass 2**: For combined entries containing ` & `, split into components. Only emit the components if at least one component already exists as a standalone artist in the library. This leverages the fact that WXYC typically catalogs major artists individually.

Example:
- "Duke Ellington & John Coltrane" → "Duke Ellington" exists independently → split ✓
- "Simon & Garfunkel" → neither "Simon" nor "Garfunkel" exists independently → don't split ✓
- "13 & God" → "God" exists independently (different artist, but the heuristic is imperfect) → split, but "13" is unlikely to cause filter problems

This heuristic will have some false positives/negatives but is a reasonable starting point.

## Implementation

### Phase 1: Shared Splitting Utility (`lib/artist_splitting.py`)

Create a new module with:

```python
def split_artist_name(name: str) -> list[str]:
    """Split a combined artist name into individual components.

    Returns only the individual components, never the original combined name.
    Returns an empty list if the name doesn't appear to be a multi-artist entry.
    Callers are responsible for emitting both the original and the components
    when building library_artists.txt or the LibraryIndex.

    Splits unconditionally on `, `, ` / `, ` + `.
    Comma guard: skip the split entirely if ANY component after splitting is
    purely numeric or a single alphabetic character. This prevents splitting
    "10,000 Maniacs" (component "10" is numeric) while allowing "Emerson, Lake,
    and Palmer" (all components are multi-character alphabetic).
    """
```

- Split on `, `, ` / `, ` + ` (unconditionally, with comma guard)
- Strip trailing " and ..." from comma-split components (handles "Emerson, Lake, and Palmer" → ["Emerson", "Lake", "Palmer"])
- Filter out components that are a single character or empty after stripping

```python
def split_artist_name_contextual(name: str, known_artists: set[str]) -> list[str]:
    """Split a combined artist name, using context from the full artist set.

    First applies all splits from split_artist_name(). Then, additionally splits
    on ` & ` when at least one resulting component (after normalization) exists
    in known_artists.

    Returns only the individual components, never the original combined name.
    Returns an empty list if no splitting applies.

    The known_artists set should contain normalized artist names from the full
    library (the complete set of DISTINCT artists, normalized for comparison).
    """
```

Tests first (TDD):

**`split_artist_name()` (context-free):**
- `, ` splitting: "Mike Vainio, Ryoji, Alva Noto" → ["Mike Vainio", "Ryoji", "Alva Noto"]
- ` + ` splitting: "David + David" → ["David"] (deduplicated)
- ` / ` splitting: "J Dilla / Jay Dee" → ["J Dilla", "Jay Dee"]
- Numeric comma guard: "10,000 Maniacs" → [] (no split -- "10" and "000 Maniacs" fail guard)
- Trailing "and": "Emerson, Lake, and Palmer" → ["Emerson", "Lake", "Palmer"]
- No split on " and ": "Andy Human and the Reptoids" → []
- No split on " with ": "Nurse with Wound" → []
- Single-char filter: component "X" after splitting → filtered out
- No delimiter: "Autechre" → []

**`split_artist_name_contextual()` (with known_artists):**
- ` & ` with known artist: "Duke Ellington & John Coltrane", known={"duke ellington"} → ["Duke Ellington", "John Coltrane"]
- ` & ` without known artist: "Simon & Garfunkel", known={} → [] (no split)
- ` & ` with unrelated known: "13 & God", known={"god"} → ["13", "God"] (imperfect but acceptable)
- Mixed delimiters: "Crosby, Stills, Nash & Young", known={"neil young"} → comma split first gives ["Crosby", "Stills", "Nash & Young"], then "&" in "Nash & Young" checked contextually; if "Young" not in known but "Neil Young" is, no further split → ["Crosby", "Stills", "Nash & Young"]. But if known={"crosby"}, comma split already succeeded → ["Crosby", "Stills", "Nash & Young"] (` & ` within a component is re-checked contextually)
- Passes through to split_artist_name first: "J Dilla / Jay Dee" with any known set → ["J Dilla", "Jay Dee"] (context-free split takes priority)

### Phase 2: Enrich Library Artists (`scripts/enrich_library_artists.py`)

Modify the script to:
1. Load all unique artist names (pass 1)
2. For each combined entry, compute split components using `split_artist_name_contextual()`
3. Add components to the output alongside the originals
4. Log statistics: how many entries were split, how many new artist names added

This ensures `library_artists.txt` includes individual components so the converter's filter catches multi-artist releases.

### Phase 3: Library Index Splitting (`scripts/verify_cache.py`)

Modify `LibraryIndex.from_sqlite()` to:
1. After loading all (artist, title) pairs, identify combined artist entries
2. Split them using `split_artist_name_contextual()` (using the full artist set as context)
3. For each component, add synthetic entries to `exact_pairs` and `artist_to_titles` / `artist_to_titles_list`, mapping the component artist to the same titles
4. This allows individual Discogs artist rows to match against component artists

**Index invariant**: Synthetic entries from splitting are added to `exact_pairs`, `artist_to_titles`, and `artist_to_titles_list` only. They are NOT added to `all_artists` or `compilation_titles`, which remain authoritative for the library's actual artist set. This prevents component names from polluting fuzzy scorer inputs that iterate the full artist list.

Example: Library has ("Mike Vainio, Ryoji, Alva Noto", "Live 2002"). After splitting, the index also contains:
- ("Mike Vainio", "Live 2002") in exact_pairs, artist_to_titles
- ("Ryoji", "Live 2002") in exact_pairs, artist_to_titles
- ("Alva Noto", "Live 2002") in exact_pairs, artist_to_titles

Now when `verify_cache.py` processes the Discogs release with artist "Alva Noto" and title "Live 2002", it finds a match via the fast path (`classify_known_artist`).

### Phase 4: Integration Testing

Add integration tests that exercise the full flow:
- A multi-artist Discogs release where the library has a combined string
- Verify the release survives filtering and is classified as KEEP
- Use the existing test infrastructure (fixture CSVs + test library.db)

## Files Changed

| File | Change |
|------|--------|
| `lib/artist_splitting.py` | New: splitting utility functions |
| `tests/unit/test_artist_splitting.py` | New: unit tests for splitting |
| `scripts/enrich_library_artists.py` | Modify: use splitting when generating library_artists.txt |
| `tests/unit/test_enrich_library_artists.py` | New or modify: test splitting integration |
| `scripts/verify_cache.py` | Modify: split combined entries when building LibraryIndex |
| `tests/unit/test_verify_cache.py` | Modify: add tests for split-aware index building |
| `tests/integration/test_prune.py` | Modify: add multi-artist test case |

## Out of Scope

- Changes to the Rust converter (it already handles multi-artist releases correctly at the data level)
- Splitting on ` and ` or ` with ` (too many false positives; revisit later with better heuristics or a manual blocklist)
- Handling the "Mike" vs "Mika" typo problem (that's a data quality issue in the library, orthogonal to splitting)
- Fuzzy splitting (e.g., matching "Ryoji" to "Ryoji Ikeda") -- the existing fuzzy matchers in verify_cache.py may partially handle this already

## Risks

1. **Over-splitting band names**: The two-pass heuristic for ` & ` mitigates this, but some false splits are possible. The pipeline's two-stage design (filter broadly, prune precisely) limits the impact.
2. **Ambiguous commas**: "Emerson, Lake, and Palmer" is a band name but also three individual artists. Splitting it adds valid individual entries that may cause some extra releases to be kept. This seems acceptable.
3. **Performance**: Splitting adds entries to both `library_artists.txt` and the `LibraryIndex`. The number of additional entries should be small (~2K split into ~5K components) relative to the 23K+ existing artists.
