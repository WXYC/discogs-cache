#!/usr/bin/env python3
"""Fix CSVs with embedded newlines in fields by replacing them with spaces."""

import csv
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def fix_csv(input_path: Path, output_path: Path) -> int:
    """Read CSV, replace newlines in fields with spaces, write cleaned CSV."""
    count = 0
    with open(input_path, encoding="utf-8", errors="replace") as infile:
        reader = csv.reader(infile)
        header = next(reader)

        with open(output_path, "w", encoding="utf-8", newline="") as outfile:
            writer = csv.writer(outfile)
            writer.writerow(header)

            for row in reader:
                # Replace newlines in each field
                cleaned = [field.replace("\n", " ").replace("\r", "") for field in row]
                writer.writerow(cleaned)
                count += 1

                if count % 500000 == 0:
                    print(f"  {count:,} rows...")

    return count


def fix_csv_dir(input_dir: Path, output_dir: Path) -> None:
    """Apply fix_csv() to all .csv files in input_dir, writing to output_dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_files = sorted(input_dir.glob("*.csv"))
    if not csv_files:
        logger.warning("No .csv files found in %s", input_dir)
        return
    for csv_file in csv_files:
        logger.info("Fixing newlines in %s ...", csv_file.name)
        count = fix_csv(csv_file, output_dir / csv_file.name)
        logger.info("  %s: %s rows", csv_file.name, f"{count:,}")


def main():
    if len(sys.argv) != 3:
        print("Usage: fix_csv_newlines.py <input.csv> <output.csv>")
        sys.exit(1)

    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    print(f"Fixing {input_path.name}...")
    count = fix_csv(input_path, output_path)
    print(f"Done: {count:,} rows")


if __name__ == "__main__":
    main()
