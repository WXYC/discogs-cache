#!/usr/bin/env python3
"""WXYC genre misfiling analysis.

Cross-references WXYC library catalog genre assignments against Discogs release
genres and MusicBrainz community tags. Produces a CSV of misfilings and a
markdown report with bias analysis.

The CSV output is consumed by resolve_collisions.py for name collision resolution.

Usage:
    python scripts/genre_analysis.py \
        --library-db data/library.db \
        --discogs-url postgresql://discogs:discogs@localhost:5433/discogs \
        --mb-url postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz \
        --output-csv ../docs/genre-analysis-results.csv \
        --output-report ../docs/genre-misfiling-report.md
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import time
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import psycopg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Genre mapping: WXYC genre -> set of accepted Discogs genres
# ---------------------------------------------------------------------------

WXYC_TO_DISCOGS: dict[str, set[str]] = {
    "Rock": {"Rock", "Pop"},
    "Jazz": {"Jazz", "Funk / Soul"},
    "Blues": {"Blues", "Funk / Soul"},
    "Electronic": {"Electronic"},
    "Hiphop": {"Hip Hop"},
    "Classical": {"Classical"},
    "OCS": {"Folk, World, & Country"},
    "Africa": {"Folk, World, & Country"},
    "Asia": {"Folk, World, & Country"},
    "Latin": {"Latin"},
    "Reggae": {"Reggae"},
    "Soundtracks": {"Stage & Screen"},
    "Comedy": {"Non-Music"},
    "Spoken": {"Non-Music"},
}

# MusicBrainz tag -> WXYC-equivalent genre (lowercase tags)
MB_TAG_TO_WXYC: dict[str, str] = {}
_rock_tags = [
    "rock",
    "indie rock",
    "alternative rock",
    "punk",
    "punk rock",
    "post-punk",
    "shoegaze",
    "noise rock",
    "post-rock",
    "psychedelic rock",
    "garage rock",
    "grunge",
    "new wave",
    "art rock",
    "krautrock",
    "lo-fi",
    "dream pop",
    "indie pop",
    "power pop",
    "pop",
    "pop rock",
    "britpop",
    "twee pop",
    "noise pop",
    "math rock",
    "emo",
    "hardcore",
    "post-hardcore",
    "screamo",
    "metal",
    "heavy metal",
    "black metal",
    "death metal",
    "doom metal",
    "stoner rock",
    "sludge metal",
    "progressive rock",
    "progressive metal",
    "experimental rock",
    "industrial",
    "gothic rock",
    "darkwave",
]
_electronic_tags = [
    "electronic",
    "ambient",
    "electronica",
    "idm",
    "techno",
    "house",
    "drum and bass",
    "dubstep",
    "trip hop",
    "downtempo",
    "glitch",
    "minimal",
    "noise",
    "dark ambient",
    "drone",
    "experimental",
    "experimental electronic",
    "electro",
    "trance",
    "breaks",
    "uk garage",
    "jungle",
    "gabber",
    "industrial music",
    "ebm",
    "synthwave",
    "vaporwave",
    "chillwave",
    "future bass",
    "footwork",
    "grime",
    "uk bass",
    "electroacoustic",
    "musique concrète",
    "tape music",
    "sound art",
    "field recording",
]
_hiphop_tags = [
    "hip hop",
    "hip-hop",
    "rap",
    "gangsta rap",
    "conscious hip hop",
    "underground hip hop",
    "boom bap",
    "trap",
    "southern hip hop",
    "west coast hip hop",
    "east coast hip hop",
    "abstract hip hop",
    "instrumental hip hop",
]
_jazz_tags = [
    "jazz",
    "free jazz",
    "bebop",
    "hard bop",
    "cool jazz",
    "modal jazz",
    "fusion",
    "jazz fusion",
    "avant-garde jazz",
    "spiritual jazz",
    "jazz-funk",
    "nu jazz",
    "acid jazz",
    "big band",
    "swing",
    "contemporary jazz",
    "free improvisation",
]
_blues_tags = [
    "blues",
    "electric blues",
    "chicago blues",
    "delta blues",
    "blues rock",
    "country blues",
    "rhythm and blues",
]
_soul_tags = [
    "soul",
    "r&b",
    "funk",
    "neo soul",
    "motown",
    "northern soul",
    "contemporary r&b",
    "gospel",
    "disco",
    "boogie",
]
_folk_tags = [
    "folk",
    "singer-songwriter",
    "country",
    "americana",
    "bluegrass",
    "folk rock",
    "traditional folk",
    "celtic",
    "old-time",
    "acoustic",
    "freak folk",
    "neofolk",
    "indie folk",
]
_reggae_tags = [
    "reggae",
    "dub",
    "ska",
    "dancehall",
    "roots reggae",
    "lovers rock",
]
_classical_tags = [
    "classical",
    "contemporary classical",
    "modern classical",
    "opera",
    "orchestral",
    "chamber music",
    "choral",
    "baroque",
    "romantic",
    "minimalism",
    "20th century classical",
]
_african_tags = [
    "african",
    "afrobeat",
    "highlife",
    "afropop",
    "world",
    "mbalax",
    "soukous",
    "jùjú",
    "fuji music",
    "ethio-jazz",
    "gnawa",
    "desert blues",
]
_latin_tags = [
    "latin",
    "bossa nova",
    "samba",
    "mpb",
    "tropicália",
    "cumbia",
    "salsa",
    "latin jazz",
    "tango",
    "nueva canción",
    "forró",
]
for tag in _rock_tags:
    MB_TAG_TO_WXYC[tag] = "Rock"
for tag in _electronic_tags:
    MB_TAG_TO_WXYC[tag] = "Electronic"
for tag in _hiphop_tags:
    MB_TAG_TO_WXYC[tag] = "Hiphop"
for tag in _jazz_tags:
    MB_TAG_TO_WXYC[tag] = "Jazz"
for tag in _blues_tags:
    MB_TAG_TO_WXYC[tag] = "Blues"
for tag in _soul_tags:
    MB_TAG_TO_WXYC[tag] = "Soul/R&B"
for tag in _folk_tags:
    MB_TAG_TO_WXYC[tag] = "OCS"
for tag in _reggae_tags:
    MB_TAG_TO_WXYC[tag] = "Reggae"
for tag in _classical_tags:
    MB_TAG_TO_WXYC[tag] = "Classical"
for tag in _african_tags:
    MB_TAG_TO_WXYC[tag] = "Africa"
for tag in _latin_tags:
    MB_TAG_TO_WXYC[tag] = "Latin"

# Countries by region for geographic analysis
AFRICAN_COUNTRIES = {
    "Algeria",
    "Angola",
    "Benin",
    "Botswana",
    "Burkina Faso",
    "Burundi",
    "Cameroon",
    "Cape Verde",
    "Central African Republic",
    "Chad",
    "Comoros",
    "Democratic Republic of the Congo",
    "Republic of the Congo",
    "Côte d'Ivoire",
    "Djibouti",
    "Egypt",
    "Equatorial Guinea",
    "Eritrea",
    "Eswatini",
    "Ethiopia",
    "Gabon",
    "Gambia",
    "Ghana",
    "Guinea",
    "Guinea-Bissau",
    "Kenya",
    "Lesotho",
    "Liberia",
    "Libya",
    "Madagascar",
    "Malawi",
    "Mali",
    "Mauritania",
    "Mauritius",
    "Morocco",
    "Mozambique",
    "Namibia",
    "Niger",
    "Nigeria",
    "Rwanda",
    "São Tomé and Príncipe",
    "Senegal",
    "Seychelles",
    "Sierra Leone",
    "Somalia",
    "South Africa",
    "South Sudan",
    "Sudan",
    "Tanzania",
    "Togo",
    "Tunisia",
    "Uganda",
    "Zambia",
    "Zimbabwe",
    "Réunion",
}
ASIAN_COUNTRIES = {
    "Afghanistan",
    "Armenia",
    "Azerbaijan",
    "Bahrain",
    "Bangladesh",
    "Bhutan",
    "Brunei",
    "Cambodia",
    "China",
    "Cyprus",
    "Georgia",
    "India",
    "Indonesia",
    "Iran",
    "Iraq",
    "Israel",
    "Japan",
    "Jordan",
    "Kazakhstan",
    "Kuwait",
    "Kyrgyzstan",
    "Laos",
    "Lebanon",
    "Malaysia",
    "Maldives",
    "Mongolia",
    "Myanmar",
    "Nepal",
    "North Korea",
    "Oman",
    "Pakistan",
    "Palestine",
    "Philippines",
    "Qatar",
    "Saudi Arabia",
    "Singapore",
    "South Korea",
    "Sri Lanka",
    "Syria",
    "Taiwan",
    "Tajikistan",
    "Thailand",
    "Timor-Leste",
    "Turkey",
    "Turkmenistan",
    "United Arab Emirates",
    "Uzbekistan",
    "Vietnam",
    "Yemen",
}
LATIN_COUNTRIES = {
    "Argentina",
    "Bolivia",
    "Brazil",
    "Chile",
    "Colombia",
    "Costa Rica",
    "Cuba",
    "Dominican Republic",
    "Ecuador",
    "El Salvador",
    "Guatemala",
    "Haiti",
    "Honduras",
    "Jamaica",
    "Mexico",
    "Nicaragua",
    "Panama",
    "Paraguay",
    "Peru",
    "Puerto Rico",
    "Trinidad and Tobago",
    "Uruguay",
    "Venezuela",
    "Guadeloupe",
    "Martinique",
}
GEOGRAPHIC_BINS = {"Africa", "Asia", "Latin"}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class LibraryArtist:
    """A unique WXYC library artist with genre and catalog info."""

    library_code_id: int  # Synthetic ID from ROW_NUMBER
    artist_name: str
    wxyc_genre: str
    call_letters: str
    call_numbers: int
    titles: list[str] = field(default_factory=list)


@dataclass
class GenreResult:
    """Genre analysis result for one WXYC artist."""

    artist: LibraryArtist
    discogs_genre: str  # Dominant Discogs genre
    pct: float  # Disagreement percentage
    title_overlap: int  # Number of matching titles
    discogs_titles: int  # Total Discogs titles checked
    library_titles: int  # Total WXYC titles
    diagnosis: str  # MISFILED or WRONG_PERSON
    priority: str  # HIGH, MEDIUM, LOW, or empty


@dataclass
class MbTagResult:
    """MusicBrainz tag analysis for one artist."""

    artist_name: str
    wxyc_genre: str
    dominant_tag: str
    mapped_genre: str
    agrees: bool


@dataclass
class GeoResult:
    """Geographic analysis for one artist."""

    artist_name: str
    wxyc_genre: str
    country: str
    region: str  # "Africa", "Asia", "Latin", "Western", "Other"
    in_geographic_bin: bool
    gender: str | None


# ---------------------------------------------------------------------------
# Library loading
# ---------------------------------------------------------------------------


def load_library_artists(library_db: Path) -> list[LibraryArtist]:
    """Load unique WXYC artists from library.db."""
    conn = sqlite3.connect(str(library_db))
    rows = conn.execute("""
        SELECT DISTINCT artist, genre, call_letters, artist_call_number
        FROM library
        WHERE artist NOT LIKE 'Various Artists%'
        ORDER BY genre, call_letters, artist_call_number
    """).fetchall()

    artists = []
    for i, (name, genre, cl, cn) in enumerate(rows, 1):
        artists.append(
            LibraryArtist(
                library_code_id=i,
                artist_name=name,
                wxyc_genre=genre,
                call_letters=cl,
                call_numbers=cn,
            )
        )

    # Load titles for each artist
    for artist in artists:
        title_rows = conn.execute(
            "SELECT DISTINCT title FROM library WHERE artist = ? AND genre = ?",
            (artist.artist_name, artist.wxyc_genre),
        ).fetchall()
        artist.titles = [r[0] for r in title_rows if r[0]]

    conn.close()
    logger.info("Loaded %d unique library artists", len(artists))
    return artists


# ---------------------------------------------------------------------------
# Discogs genre analysis
# ---------------------------------------------------------------------------


def match_discogs_genres(
    conn: psycopg.Connection, artists: list[LibraryArtist]
) -> list[GenreResult]:
    """For each WXYC artist, query Discogs for release genres and compare."""
    results = []
    total = len(artists)

    for i, artist in enumerate(artists, 1):
        if i % 1000 == 0:
            logger.info("  Discogs genre check: %d/%d", i, total)

        accepted = WXYC_TO_DISCOGS.get(artist.wxyc_genre, set())
        if not accepted:
            continue

        # Get all releases by this artist with their genres
        rows = conn.execute(
            """
            SELECT r.id, r.title, array_agg(DISTINCT rg.genre) as genres
            FROM release_artist ra
            JOIN release r ON ra.release_id = r.id
            JOIN release_genre rg ON r.id = rg.release_id
            WHERE ra.artist_name = %s AND ra.extra = 0
            GROUP BY r.id, r.title
        """,
            (artist.artist_name,),
        ).fetchall()

        if not rows:
            continue

        # Count releases that disagree with WXYC genre
        disagree_count = 0
        genre_counter: Counter[str] = Counter()
        discogs_titles = set()

        for _release_id, title, genres in rows:
            discogs_titles.add(title.lower().strip() if title else "")
            for g in genres:
                if g:
                    genre_counter[g] += 1
            # A release disagrees if NONE of its genres are in the accepted set
            release_genres = {g for g in genres if g}
            if not release_genres & accepted:
                disagree_count += 1

        total_releases = len(rows)
        pct = disagree_count / total_releases if total_releases > 0 else 0

        if pct < 0.50:
            continue  # Not a misfiling candidate

        # Determine dominant Discogs genre (most common genre that ISN'T accepted)
        dominant = ""
        for genre, _count in genre_counter.most_common():
            if genre not in accepted:
                dominant = genre
                break

        # Title overlap for collision detection
        library_titles_lower = {t.lower().strip() for t in artist.titles}
        overlap = len(library_titles_lower & discogs_titles)

        diagnosis = "MISFILED" if overlap > 0 else "WRONG_PERSON"
        priority = ""
        if diagnosis == "MISFILED":
            if pct >= 0.75:
                priority = "HIGH"
            elif pct >= 0.60:
                priority = "MEDIUM"
            else:
                priority = "LOW"

        results.append(
            GenreResult(
                artist=artist,
                discogs_genre=dominant,
                pct=round(pct * 100, 1),
                title_overlap=overlap,
                discogs_titles=total_releases,
                library_titles=len(artist.titles),
                diagnosis=diagnosis,
                priority=priority,
            )
        )

    logger.info(
        "Discogs analysis: %d results (%d MISFILED, %d WRONG_PERSON)",
        len(results),
        sum(1 for r in results if r.diagnosis == "MISFILED"),
        sum(1 for r in results if r.diagnosis == "WRONG_PERSON"),
    )
    return results


# ---------------------------------------------------------------------------
# MusicBrainz tag analysis
# ---------------------------------------------------------------------------


def mb_tag_analysis(conn: psycopg.Connection, artists: list[LibraryArtist]) -> list[MbTagResult]:
    """Cross-validate WXYC genres against MusicBrainz community tags."""
    results = []
    total = len(artists)

    for i, artist in enumerate(artists, 1):
        if i % 2000 == 0:
            logger.info("  MB tag check: %d/%d", i, total)

        # Match by name (case-insensitive)
        rows = conn.execute(
            """
            SELECT t.name, at.count
            FROM mb_artist a
            JOIN mb_artist_tag at ON a.id = at.artist
            JOIN mb_tag t ON at.tag = t.id
            WHERE lower(a.name) = lower(%s) AND at.count >= 1
            ORDER BY at.count DESC
        """,
            (artist.artist_name,),
        ).fetchall()

        if not rows:
            continue

        # Find dominant tag that maps to a WXYC genre
        dominant_tag = ""
        mapped_genre = ""
        for tag_name, _count in rows:
            tag_lower = tag_name.lower()
            if tag_lower in MB_TAG_TO_WXYC:
                dominant_tag = tag_name
                mapped_genre = MB_TAG_TO_WXYC[tag_lower]
                break

        if not mapped_genre:
            continue

        # Check agreement: does the mapped genre match WXYC genre?
        # Soul/R&B maps to Blues or Jazz in WXYC (no dedicated bin)
        agrees = False
        if mapped_genre == artist.wxyc_genre:
            agrees = True
        elif mapped_genre == "Soul/R&B" and artist.wxyc_genre in (
            "Blues",
            "Jazz",
            "Hiphop",
            "Rock",
        ):
            agrees = True  # No WXYC bin for Soul/R&B, so scattered is expected
        elif mapped_genre == "OCS" and artist.wxyc_genre in ("Africa", "Asia", "Latin"):
            agrees = True  # Geographic bins accept Folk/World
        elif mapped_genre == "Africa" and artist.wxyc_genre in ("OCS", "Jazz", "Rock"):
            agrees = True  # African music in genre bins is fine

        results.append(
            MbTagResult(
                artist_name=artist.artist_name,
                wxyc_genre=artist.wxyc_genre,
                dominant_tag=dominant_tag,
                mapped_genre=mapped_genre,
                agrees=agrees,
            )
        )

    logger.info("MB tag analysis: %d artists with tags", len(results))
    return results


# ---------------------------------------------------------------------------
# Geographic / bias analysis
# ---------------------------------------------------------------------------


def mb_geographic_analysis(
    conn: psycopg.Connection, artists: list[LibraryArtist]
) -> list[GeoResult]:
    """Analyze geographic distribution and bias in WXYC genre classification."""
    results = []
    total = len(artists)

    for i, artist in enumerate(artists, 1):
        if i % 2000 == 0:
            logger.info("  MB geo check: %d/%d", i, total)

        row = conn.execute(
            """
            SELECT a.name, ar.name as area_name, g.name as gender
            FROM mb_artist a
            LEFT JOIN mb_area ar ON a.area = ar.id
            LEFT JOIN mb_gender g ON a.gender = g.id
            WHERE lower(a.name) = lower(%s)
            LIMIT 1
        """,
            (artist.artist_name,),
        ).fetchone()

        if not row or not row[1]:
            continue

        _mb_name, country, gender = row

        # Classify region
        if country in AFRICAN_COUNTRIES:
            region = "Africa"
        elif country in ASIAN_COUNTRIES:
            region = "Asia"
        elif country in LATIN_COUNTRIES:
            region = "Latin"
        elif country in (
            "United States",
            "United Kingdom",
            "Canada",
            "Australia",
            "New Zealand",
            "Ireland",
        ) or country in (
            "Germany",
            "France",
            "Italy",
            "Spain",
            "Netherlands",
            "Belgium",
            "Sweden",
            "Norway",
            "Denmark",
            "Finland",
            "Austria",
            "Switzerland",
            "Portugal",
            "Greece",
            "Poland",
            "Czech Republic",
            "Hungary",
            "Romania",
            "Bulgaria",
            "Croatia",
            "Serbia",
            "Slovakia",
            "Slovenia",
            "Estonia",
            "Latvia",
            "Lithuania",
            "Iceland",
            "Luxembourg",
        ):
            region = "Western"
        else:
            region = "Other"

        in_geographic_bin = artist.wxyc_genre in GEOGRAPHIC_BINS

        results.append(
            GeoResult(
                artist_name=artist.artist_name,
                wxyc_genre=artist.wxyc_genre,
                country=country,
                region=region,
                in_geographic_bin=in_geographic_bin,
                gender=gender,
            )
        )

    logger.info("MB geographic analysis: %d artists with area data", len(results))
    return results


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------


def write_csv(results: list[GenreResult], path: Path) -> None:
    """Write genre-analysis-results.csv."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "library_code_id",
        "artist_name",
        "wxyc_genre",
        "call_letters",
        "call_numbers",
        "discogs_genre",
        "pct",
        "title_overlap",
        "discogs_titles",
        "library_titles",
        "diagnosis",
        "priority",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow(
                {
                    "library_code_id": r.artist.library_code_id,
                    "artist_name": r.artist.artist_name,
                    "wxyc_genre": r.artist.wxyc_genre,
                    "call_letters": r.artist.call_letters,
                    "call_numbers": r.artist.call_numbers,
                    "discogs_genre": r.discogs_genre,
                    "pct": r.pct,
                    "title_overlap": r.title_overlap,
                    "discogs_titles": r.discogs_titles,
                    "library_titles": r.library_titles,
                    "diagnosis": r.diagnosis,
                    "priority": r.priority,
                }
            )
    logger.info("Wrote %d rows to %s", len(results), path)


# ---------------------------------------------------------------------------
# Markdown report
# ---------------------------------------------------------------------------


def write_report(
    results: list[GenreResult],
    mb_results: list[MbTagResult],
    geo_results: list[GeoResult],
    path: Path,
) -> None:
    """Write the genre misfiling report as markdown."""
    path.parent.mkdir(parents=True, exist_ok=True)

    misfiled = [r for r in results if r.diagnosis == "MISFILED"]
    wrong_person = [r for r in results if r.diagnosis == "WRONG_PERSON"]
    high = [r for r in misfiled if r.priority == "HIGH"]
    medium = [r for r in misfiled if r.priority == "MEDIUM"]
    low = [r for r in misfiled if r.priority == "LOW"]

    lines = []
    lines.append("# WXYC Genre Misfiling Report")
    lines.append("")
    lines.append(
        "Generated from cross-referencing WXYC library catalog against Discogs and MusicBrainz metadata."
    )
    lines.append("")

    # Summary
    lines.append("## Summary")
    lines.append("")
    lines.append("| Category | Count | Description |")
    lines.append("|----------|-------|-------------|")
    lines.append(
        f"| Confirmed misfilings | {len(misfiled):,} | Correct artist matched, genre doesn't align with Discogs |"
    )
    lines.append(
        f"| Name collisions | {len(wrong_person):,} | Wrong Discogs artist matched (different person, same name) |"
    )
    lines.append("")

    # Priority breakdown
    lines.append("## Misfiling Priority Breakdown")
    lines.append("")
    lines.append("| Priority | Criteria | Count |")
    lines.append("|----------|----------|-------|")
    lines.append(f"| HIGH | >=75% of Discogs releases disagree with WXYC genre | {len(high):,} |")
    lines.append(f"| MEDIUM | 60-74% disagreement | {len(medium):,} |")
    lines.append(f"| LOW | 50-59% disagreement | {len(low):,} |")
    lines.append("")

    # Top migration paths
    migration: Counter[tuple[str, str]] = Counter()
    for r in misfiled:
        if r.priority in ("HIGH", "MEDIUM"):
            # Map Discogs genre back to WXYC-equivalent
            target = _discogs_to_wxyc_target(r.discogs_genre)
            if target and target != r.artist.wxyc_genre:
                migration[(r.artist.wxyc_genre, target)] += 1

    lines.append("## Top Migration Paths")
    lines.append("")
    lines.append("| From | To | Count (HIGH+MED) |")
    lines.append("|------|-----|-----------------|")
    for (from_g, to_g), count in migration.most_common(15):
        lines.append(f"| {from_g} | {to_g} | {count} |")
    lines.append("")

    # MusicBrainz tag agreement
    if mb_results:
        lines.append("## MusicBrainz Tag Agreement Rates by WXYC Genre")
        lines.append("")
        genre_stats: dict[str, dict[str, int]] = defaultdict(
            lambda: {"total": 0, "agree": 0, "disagree": 0}
        )
        for r in mb_results:
            genre_stats[r.wxyc_genre]["total"] += 1
            if r.agrees:
                genre_stats[r.wxyc_genre]["agree"] += 1
            else:
                genre_stats[r.wxyc_genre]["disagree"] += 1

        lines.append("| WXYC Genre | Artists with tags | Agree | Disagree | Disagree % |")
        lines.append("|-----------|-------------------|-------|----------|-----------|")
        for genre in sorted(
            genre_stats,
            key=lambda g: genre_stats[g]["disagree"] / max(genre_stats[g]["total"], 1),
            reverse=True,
        ):
            s = genre_stats[genre]
            pct = s["disagree"] / s["total"] * 100 if s["total"] else 0
            lines.append(
                f"| {genre} | {s['total']:,} | {s['agree']:,} | {s['disagree']:,} | {pct:.1f}% |"
            )
        lines.append("")

    # Geographic/racial bias
    if geo_results:
        lines.append("## Racial/Geographic Bias Analysis")
        lines.append("")

        # Artists by region: geographic bin vs genre bin
        region_stats: dict[str, dict[str, int]] = defaultdict(
            lambda: {"geo_bin": 0, "genre_bin": 0}
        )
        for r in geo_results:
            if r.region in ("Africa", "Asia", "Latin"):
                if r.in_geographic_bin:
                    region_stats[r.region]["geo_bin"] += 1
                else:
                    region_stats[r.region]["genre_bin"] += 1

        lines.append("### Geographic Bins vs Genre Bins")
        lines.append("")
        lines.append("| Region | In geographic bin | In genre bins | % in genre bins |")
        lines.append("|--------|------------------|---------------|----------------|")
        for region in ("Africa", "Asia", "Latin"):
            s = region_stats[region]
            total = s["geo_bin"] + s["genre_bin"]
            pct = s["genre_bin"] / total * 100 if total else 0
            region_label = f"{region}n" if region != "Latin" else "Latin American"
            lines.append(
                f"| {region_label} artists | {s['geo_bin']} | {s['genre_bin']} | {pct:.1f}% |"
            )
        lines.append("")

        # Funk/Soul gap
        soul_results = [r for r in mb_results if r.mapped_genre == "Soul/R&B"]
        if soul_results:
            lines.append("### The Funk/Soul Gap")
            lines.append("")
            soul_by_genre: Counter[str] = Counter()
            for r in soul_results:
                soul_by_genre[r.wxyc_genre] += 1
            lines.append(
                f"{len(soul_results)} artists tagged 'soul', 'r&b', or 'funk' on MusicBrainz are scattered across WXYC's taxonomy:"
            )
            lines.append("")
            lines.append("| WXYC Genre | Soul/Funk/R&B Artists |")
            lines.append("|-----------|---------------------|")
            for genre, count in soul_by_genre.most_common():
                lines.append(f"| {genre} | {count} ({count / len(soul_results) * 100:.1f}%) |")
            lines.append("")

        # Gender
        gender_geo = [r for r in geo_results if r.in_geographic_bin and r.gender]
        gender_genre = [r for r in geo_results if not r.in_geographic_bin and r.gender]
        if gender_geo and gender_genre:
            lines.append("### Gender in Geographic vs Genre Bins")
            lines.append("")
            geo_gender = Counter(r.gender for r in gender_geo)
            genre_gender = Counter(r.gender for r in gender_genre)
            geo_total = len(gender_geo)
            genre_total = len(gender_genre)
            lines.append("| Bin type | Male | Female | Unknown/Other |")
            lines.append("|---------|------|--------|---------------|")
            lines.append(
                f"| Genre bins | {genre_gender.get('Male', 0) / genre_total * 100:.1f}% | {genre_gender.get('Female', 0) / genre_total * 100:.1f}% | {(genre_total - genre_gender.get('Male', 0) - genre_gender.get('Female', 0)) / genre_total * 100:.1f}% |"
            )
            lines.append(
                f"| Geographic bins | {geo_gender.get('Male', 0) / geo_total * 100:.1f}% | {geo_gender.get('Female', 0) / geo_total * 100:.1f}% | {(geo_total - geo_gender.get('Male', 0) - geo_gender.get('Female', 0)) / geo_total * 100:.1f}% |"
            )
            lines.append("")

    # Per-genre analysis sections for major bins
    for genre_name in ("Hiphop", "Rock", "Classical", "Blues"):
        genre_misfiled = [
            r
            for r in misfiled
            if r.artist.wxyc_genre == genre_name and r.priority in ("HIGH", "MEDIUM")
        ]
        if genre_misfiled:
            lines.append(f"## {genre_name} Bin Analysis")
            lines.append("")
            target_counts: Counter[str] = Counter()
            for r in genre_misfiled:
                target = _discogs_to_wxyc_target(r.discogs_genre)
                if target:
                    target_counts[target] += 1
            for target, count in target_counts.most_common(5):
                lines.append(f"- {count} HIGH+MEDIUM priority artists should be in {target}")
            if len(genre_misfiled) <= 20:
                lines.append("")
                lines.append(
                    "Artists: " + ", ".join(r.artist.artist_name for r in genre_misfiled[:20])
                )
            lines.append("")

    text = "\n".join(lines)
    path.write_text(text)
    logger.info("Wrote report to %s", path)


def _discogs_to_wxyc_target(discogs_genre: str) -> str:
    """Map a Discogs genre to its most likely WXYC target genre."""
    mapping = {
        "Electronic": "Electronic",
        "Hip Hop": "Hiphop",
        "Rock": "Rock",
        "Pop": "Rock",
        "Jazz": "Jazz",
        "Funk / Soul": "Blues/Soul",
        "Blues": "Blues",
        "Folk, World, & Country": "OCS/Folk",
        "Classical": "Classical",
        "Reggae": "Reggae",
        "Latin": "Latin",
        "Stage & Screen": "Soundtracks",
        "Non-Music": "Comedy/Spoken",
    }
    return mapping.get(discogs_genre, "")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description="WXYC genre misfiling analysis")
    parser.add_argument("--library-db", required=True, help="Path to library.db")
    parser.add_argument(
        "--discogs-url",
        default="postgresql://discogs:discogs@localhost:5433/discogs",
        help="Discogs PostgreSQL URL",
    )
    parser.add_argument(
        "--mb-url",
        default="postgresql://musicbrainz:musicbrainz@localhost:5434/musicbrainz",
        help="MusicBrainz PostgreSQL URL",
    )
    parser.add_argument(
        "--output-csv",
        default="../docs/genre-analysis-results.csv",
        help="Output CSV path",
    )
    parser.add_argument(
        "--output-report",
        default="../docs/genre-misfiling-report.md",
        help="Output markdown report path",
    )
    args = parser.parse_args()

    library_db = Path(args.library_db)
    if not library_db.exists():
        logger.error("library.db not found: %s", library_db)
        return

    # Load library artists
    artists = load_library_artists(library_db)

    # Discogs genre analysis
    logger.info("Connecting to Discogs database...")
    t0 = time.time()
    with psycopg.connect(args.discogs_url) as discogs_conn:
        results = match_discogs_genres(discogs_conn, artists)
    logger.info("Discogs analysis took %.1fs", time.time() - t0)

    # Write CSV (before MB analysis, so resolve_collisions.py can run in parallel)
    write_csv(results, Path(args.output_csv))

    # MusicBrainz analysis
    mb_results: list[MbTagResult] = []
    geo_results: list[GeoResult] = []
    try:
        logger.info("Connecting to MusicBrainz database...")
        t0 = time.time()
        with psycopg.connect(args.mb_url) as mb_conn:
            mb_results = mb_tag_analysis(mb_conn, artists)
            geo_results = mb_geographic_analysis(mb_conn, artists)
        logger.info("MusicBrainz analysis took %.1fs", time.time() - t0)
    except psycopg.OperationalError as e:
        logger.warning("MusicBrainz database unavailable, skipping: %s", e)

    # Write report
    write_report(results, mb_results, geo_results, Path(args.output_report))

    # Print summary
    misfiled = sum(1 for r in results if r.diagnosis == "MISFILED")
    wrong = sum(1 for r in results if r.diagnosis == "WRONG_PERSON")
    high = sum(1 for r in results if r.priority == "HIGH")
    print(f"\nResults: {len(results)} total ({misfiled} MISFILED, {wrong} WRONG_PERSON)")
    print(
        f"Priority: {high} HIGH, {sum(1 for r in results if r.priority == 'MEDIUM')} MEDIUM, {sum(1 for r in results if r.priority == 'LOW')} LOW"
    )
    if mb_results:
        mb_disagree = sum(1 for r in mb_results if not r.agrees)
        print(
            f"MusicBrainz: {len(mb_results)} artists with tags, {mb_disagree} disagree ({mb_disagree / len(mb_results) * 100:.1f}%)"
        )
    if geo_results:
        print(f"Geographic: {len(geo_results)} artists with area data")


if __name__ == "__main__":
    main()
