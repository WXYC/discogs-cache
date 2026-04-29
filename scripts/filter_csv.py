#!/usr/bin/env python3
"""Filter Discogs CSV exports to only include releases by library artists.

This script significantly reduces the data size by only keeping releases
that have at least one artist matching the library catalog.

Usage:
    python filter_discogs_csv.py /path/to/library_artists.txt /path/to/csv_output/ /path/to/filtered_output/
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lib.observability import init_logger  # noqa: E402

logger = logging.getLogger(__name__)

# CSV files that need to be filtered by release_id.
RELEASE_ID_FILES = [
    "release.csv",
    "release_artist.csv",
    "release_label.csv",
    "release_genre.csv",
    "release_style.csv",
    "release_track.csv",
    "release_track_artist.csv",
    "release_image.csv",  # for artwork_url extraction during import
]


def normalize_artist(name: str) -> str:
    """Normalize artist name for matching.

    Strips diacritics so that Discogs "Björk" matches library "Bjork".
    """
    nfkd = unicodedata.normalize("NFKD", name)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()


def normalize_title(title: str) -> str:
    """Normalize release title for matching.

    Same shape as ``normalize_artist`` so a Discogs title with diacritics
    matches a library title without them (and vice versa).
    """
    nfkd = unicodedata.normalize("NFKD", title)
    stripped = "".join(c for c in nfkd if not unicodedata.combining(c))
    return stripped.lower().strip()


def load_library_artists(path: Path) -> set[str]:
    """Load library artists into a normalized set."""
    logger.info(f"Loading library artists from {path}")
    artists = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            name = line.strip()
            if name:
                artists.add(normalize_artist(name))
    logger.info(f"Loaded {len(artists):,} unique library artists")
    return artists


def find_matching_release_ids(release_artist_path: Path, library_artists: set[str]) -> set[int]:
    """Find all release IDs that have at least one matching library artist.

    Uses csv.reader with positional indexing instead of csv.DictReader
    to avoid dict creation overhead on 100M+ row files.
    """
    logger.info(f"Scanning {release_artist_path} for matching artists...")
    matching_ids = set()
    total_rows = 0
    matched_rows = 0

    normalize_cache: dict[str, str] = {}

    with open(release_artist_path, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        release_id_idx = header.index("release_id")
        artist_name_idx = header.index("artist_name")
        for row in reader:
            total_rows += 1
            try:
                raw_name = row[artist_name_idx]
            except IndexError:
                continue
            artist_name = normalize_cache.get(raw_name)
            if artist_name is None:
                artist_name = normalize_artist(raw_name)
                normalize_cache[raw_name] = artist_name
            if artist_name in library_artists:
                release_id = int(row[release_id_idx])
                matching_ids.add(release_id)
                matched_rows += 1

            if total_rows % 500000 == 0:
                logger.info(
                    f"  Processed {total_rows:,} rows, found {len(matching_ids):,} matching releases"
                )

    logger.info(
        f"Finished: {matched_rows:,} artist matches across {len(matching_ids):,} releases "
        f"(from {total_rows:,} total rows)"
    )
    return matching_ids


def get_release_id_column(filename: str) -> str:
    """Get the column name containing release_id for each file type."""
    if filename == "release.csv":
        return "id"
    return "release_id"


def filter_csv_file(
    input_path: Path, output_path: Path, matching_ids: set[int], id_column: str
) -> tuple[int, int]:
    """Filter a CSV file to only include rows with matching release IDs.

    Uses csv.reader with positional indexing instead of csv.DictReader
    to avoid dict creation overhead on large files.
    """
    input_count = 0
    output_count = 0

    with open(input_path, encoding="utf-8", errors="replace") as infile:
        reader = csv.reader(infile)
        header = next(reader)
        try:
            id_idx = header.index(id_column)
        except ValueError:
            raise ValueError(
                f"Column '{id_column}' not found in {input_path}. Available columns: {header}"
            ) from None

        with open(output_path, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)

            for row in reader:
                input_count += 1
                try:
                    release_id = int(row[id_idx])
                    if release_id in matching_ids:
                        writer.writerow(row)
                        output_count += 1
                except (ValueError, IndexError):
                    # Skip rows with invalid release IDs or short rows
                    pass

                if input_count % 1000000 == 0:
                    logger.info(f"  Processed {input_count:,} rows, kept {output_count:,}")

    return input_count, output_count


def load_library_pairs(library_db: Path) -> dict[str, set[str]]:
    """Load (artist, title) pairs from a SQLite ``library.db`` file.

    Returns an inverted index ``{normalized_title: set of normalized_artists}``.
    Built this way so the pair-wise scan over release_artist.csv can do a
    single dict lookup keyed by the candidate release's title.
    """
    logger.info("Loading library pairs from %s", library_db)
    pairs: dict[str, set[str]] = {}
    conn = sqlite3.connect(str(library_db))
    try:
        for artist, title in conn.execute("SELECT artist, title FROM library"):
            if not artist or not title:
                continue
            n_title = normalize_title(title)
            n_artist = normalize_artist(artist)
            pairs.setdefault(n_title, set()).add(n_artist)
    finally:
        conn.close()
    logger.info(
        "Loaded %d distinct library titles spanning %d (artist, title) pairs",
        len(pairs),
        sum(len(v) for v in pairs.values()),
    )
    return pairs


def find_matching_release_ids_pairwise(
    release_csv: Path,
    release_artist_csv: Path,
    library_pairs: dict[str, set[str]],
) -> set[int]:
    """Find release IDs whose (any artist, title) matches a library pair.

    Two passes: pass 1 builds a ``{release_id: normalized_title}`` map for
    only those releases whose title is in ``library_pairs`` (an inverted
    index keyed by title). Pass 2 walks ``release_artist.csv`` and for each
    candidate release checks whether the artist is in the library's set
    for that title. Memory bounded by the size of the title intersection,
    which is small even on a 4M-release dump (~200 K candidates worst case).
    """
    logger.info("Pair-wise pass 1: indexing release titles in %s", release_csv)
    candidate_titles: dict[int, str] = {}
    title_normalize_cache: dict[str, str] = {}

    with open(release_csv, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        try:
            id_idx = header.index("id")
            title_idx = header.index("title")
        except ValueError as exc:
            raise ValueError(f"release.csv missing expected columns. Header: {header}") from exc
        total_rows = 0
        for row in reader:
            total_rows += 1
            try:
                raw_title = row[title_idx]
                raw_id = row[id_idx]
            except IndexError:
                continue
            n_title = title_normalize_cache.get(raw_title)
            if n_title is None:
                n_title = normalize_title(raw_title)
                title_normalize_cache[raw_title] = n_title
            if n_title not in library_pairs:
                continue
            try:
                rid = int(raw_id)
            except ValueError:
                continue
            candidate_titles[rid] = n_title

            if total_rows % 1000000 == 0:
                logger.info(
                    "  Pass 1: scanned %d release rows, %d title candidates so far",
                    total_rows,
                    len(candidate_titles),
                )

    logger.info(
        "Pair-wise pass 1: %d candidate release titles match the library", len(candidate_titles)
    )

    logger.info("Pair-wise pass 2: scanning %s for matching artists", release_artist_csv)
    matching: set[int] = set()
    artist_normalize_cache: dict[str, str] = {}

    with open(release_artist_csv, encoding="utf-8", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)
        try:
            rid_idx = header.index("release_id")
            artist_idx = header.index("artist_name")
        except ValueError as exc:
            raise ValueError(
                f"release_artist.csv missing expected columns. Header: {header}"
            ) from exc
        total_rows = 0
        for row in reader:
            total_rows += 1
            try:
                raw_rid = row[rid_idx]
                raw_artist = row[artist_idx]
            except IndexError:
                continue
            try:
                rid = int(raw_rid)
            except ValueError:
                continue
            n_title = candidate_titles.get(rid)
            if n_title is None:
                continue
            n_artist = artist_normalize_cache.get(raw_artist)
            if n_artist is None:
                n_artist = normalize_artist(raw_artist)
                artist_normalize_cache[raw_artist] = n_artist
            if n_artist in library_pairs[n_title]:
                matching.add(rid)

            if total_rows % 1000000 == 0:
                logger.info(
                    "  Pass 2: scanned %d release_artist rows, %d matching releases so far",
                    total_rows,
                    len(matching),
                )

    logger.info(
        "Pair-wise filter kept %d releases (down from %d candidate titles)",
        len(matching),
        len(candidate_titles),
    )
    return matching


def filter_csvs_by_pairs(
    library_db: Path,
    csv_input_dir: Path,
    csv_output_dir: Path,
) -> dict[str, tuple[int, int]]:
    """Filter every release-id-keyed CSV in ``csv_input_dir`` to only the
    releases whose (artist, title) pair matches the WXYC library.

    Designed to run between the converter's artist-only filter (~4 M releases)
    and the import step. The pair-wise narrowing brings it down to ~50 K, which
    fits on a small Postgres host (Railway-sized) where the artist-only output
    overflows the volume during ``COPY release_artist`` (#128).

    ``csv_input_dir`` and ``csv_output_dir`` may point at the same path; in
    that case the original CSVs are overwritten in place. The CI workflow
    uses this to keep runner disk small.

    Returns ``{filename: (input_count, output_count)}``.
    """
    library_pairs = load_library_pairs(library_db)

    release_csv = csv_input_dir / "release.csv"
    release_artist_csv = csv_input_dir / "release_artist.csv"
    if not release_csv.exists() or not release_artist_csv.exists():
        raise FileNotFoundError(
            f"release.csv and release_artist.csv must both exist in {csv_input_dir}"
        )

    matching_ids = find_matching_release_ids_pairwise(
        release_csv, release_artist_csv, library_pairs
    )

    csv_output_dir.mkdir(parents=True, exist_ok=True)

    # When input and output dirs match, write each filtered CSV to a sibling
    # ``.tmp`` first and atomically replace, so a partial failure leaves the
    # original intact.
    in_place = csv_input_dir.resolve() == csv_output_dir.resolve()
    stats: dict[str, tuple[int, int]] = {}

    for filename in RELEASE_ID_FILES:
        input_path = csv_input_dir / filename
        if not input_path.exists():
            continue
        output_path = csv_output_dir / filename
        id_column = get_release_id_column(filename)
        if in_place:
            tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
            input_count, output_count = filter_csv_file(
                input_path, tmp_path, matching_ids, id_column
            )
            tmp_path.replace(output_path)
        else:
            input_count, output_count = filter_csv_file(
                input_path, output_path, matching_ids, id_column
            )
        stats[filename] = (input_count, output_count)
        reduction = (1 - output_count / input_count) * 100 if input_count > 0 else 0.0
        logger.info(
            "  %s: %d → %d rows (%.1f%% reduction)",
            filename,
            input_count,
            output_count,
            reduction,
        )

    return stats


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Filter Discogs CSVs to a smaller subset. Two modes:\n"
            "  1. Artist-only (default): pass library_artists.txt; keep releases\n"
            "     with at least one matching artist.\n"
            "  2. Pair-wise: pass --library-db; keep releases whose (artist, title)\n"
            "     matches a library entry. Used to narrow the converter's\n"
            "     ~4M-release output to ~50K so import doesn't OOM small\n"
            "     destination DBs (#128)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--library-db",
        type=Path,
        help="SQLite library.db for pair-wise (artist, title) filtering.",
    )
    parser.add_argument(
        "--library-artists",
        type=Path,
        help="library_artists.txt for artist-only filtering (default mode).",
    )
    parser.add_argument(
        "csv_input_dir", type=Path, help="Directory containing the converter's CSVs."
    )
    parser.add_argument(
        "csv_output_dir",
        type=Path,
        help="Output directory for filtered CSVs. May equal csv_input_dir for in-place rewrite.",
    )
    args = parser.parse_args(argv)
    if not args.library_db and not args.library_artists:
        parser.error("one of --library-db or --library-artists is required")
    if args.library_db and args.library_artists:
        parser.error("--library-db and --library-artists are mutually exclusive")
    return args


def main(argv: list[str] | None = None) -> None:
    init_logger(repo="discogs-etl", tool="discogs-etl filter_csv")

    # Backwards-compat: the original positional CLI was
    #   filter_csv.py <library_artists> <csv_in> <csv_out>
    # Detect that shape and rewrite into the argparse-friendly form so
    # existing callers (scripts, docs) keep working.
    raw_argv = list(sys.argv[1:]) if argv is None else list(argv)
    if len(raw_argv) == 3 and not any(a.startswith("--") for a in raw_argv):
        raw_argv = ["--library-artists", raw_argv[0], raw_argv[1], raw_argv[2]]

    args = _parse_args(raw_argv)

    if args.library_db:
        if not args.library_db.exists():
            logger.error("library.db not found: %s", args.library_db)
            sys.exit(1)
        if not args.csv_input_dir.exists():
            logger.error("CSV input directory not found: %s", args.csv_input_dir)
            sys.exit(1)
        stats = filter_csvs_by_pairs(args.library_db, args.csv_input_dir, args.csv_output_dir)
        logger.info("=== Pair-wise filter summary ===")
        for filename, (inp, out) in stats.items():
            pct = (1 - out / inp) * 100 if inp > 0 else 0.0
            logger.info("  %s: %d → %d (%.1f%% reduction)", filename, inp, out, pct)
        return

    # Artist-only mode (legacy default).
    if not args.library_artists.exists():
        logger.error("Library artists file not found: %s", args.library_artists)
        sys.exit(1)
    if not args.csv_input_dir.exists():
        logger.error("CSV input directory not found: %s", args.csv_input_dir)
        sys.exit(1)

    args.csv_output_dir.mkdir(parents=True, exist_ok=True)

    library_artists = load_library_artists(args.library_artists)

    release_artist_path = args.csv_input_dir / "release_artist.csv"
    if not release_artist_path.exists():
        logger.error("release_artist.csv not found in %s", args.csv_input_dir)
        sys.exit(1)

    matching_ids = find_matching_release_ids(release_artist_path, library_artists)

    if not matching_ids:
        logger.warning("No matching releases found! Check artist name normalization.")
        sys.exit(1)

    logger.info("Found %d releases to keep", len(matching_ids))

    stats: dict[str, tuple[int, int, float]] = {}
    for filename in RELEASE_ID_FILES:
        input_path = args.csv_input_dir / filename
        if not input_path.exists():
            logger.warning("Skipping %s (not found)", filename)
            continue

        output_path = args.csv_output_dir / filename
        id_column = get_release_id_column(filename)

        logger.info("Filtering %s...", filename)
        input_count, output_count = filter_csv_file(
            input_path, output_path, matching_ids, id_column
        )

        reduction_pct = (1 - output_count / input_count) * 100 if input_count > 0 else 0
        stats[filename] = (input_count, output_count, reduction_pct)
        logger.info("  %d → %d rows (%.1f%% reduction)", input_count, output_count, reduction_pct)

    logger.info("=== Summary ===")
    logger.info("Library artists: %d", len(library_artists))
    logger.info("Matching releases: %d", len(matching_ids))
    for filename, (inp, out, pct) in stats.items():
        logger.info("  %s: %d → %d (%.1f%% reduction)", filename, inp, out, pct)


if __name__ == "__main__":
    main()
