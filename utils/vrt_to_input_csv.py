#!/usr/bin/env python3
"""
vrt_to_csv.py

Parses a GDAL VRT file (given as a URL or local path) whose <SourceFilename>
entries point to yearly raster tiles such as:

    /vsicurl/https://aquainfra-fgi.a3s.fi/europe/clcplus_3035/2018/clcplus3035_2018_3900000_2500000.tif

and writes a ';'-separated CSV with the columns:

    file;start-date;end-date;collection;asset;item_id

- file        : the source URL, with the leading /vsicurl/ removed
- start-date  : 1.1.<year>   (year parsed from the filename)
- end-date    : 31.12.<year>
- collection  : fixed value given on the command line
- asset       : fixed value given on the command line
- item_id     : <item-id-prefix><year>_<x>_<y>
                e.g. for clcplus3035_2018_3900000_2500000.tif and prefix
                "clcplus3035_" this becomes "clcplus3035_2018_3900000_2500000"

Usage:
    python vrt_to_csv.py <vrt_url_or_path> \
        --collection clc_plus_backbone \
        --asset classification \
        --item-id-prefix clcplus3035_ \
        --output clcplus3035.csv

If --item-id-prefix is omitted, the item_id column will just be
"<year>_<x>_<y>".
"""

import argparse
import csv
import re
import sys
import urllib.request
import xml.etree.ElementTree as ET

# Matches e.g. "clcplus3035_2018_3900000_2500000.tif"
# -> year=2018, x=3900000, y=2500000
FILENAME_RE = re.compile(r"_(?P<year>\d{4})_(?P<x>-?\d+)_(?P<y>-?\d+)\.tif$", re.IGNORECASE)

VSICURL_PREFIX = "/vsicurl/"


def read_vrt(source: str) -> str:
    """Read VRT content from a URL or a local file path."""
    if re.match(r"^https?://", source, re.IGNORECASE):
        with urllib.request.urlopen(source) as resp:
            return resp.read().decode("utf-8")
    with open(source, "r", encoding="utf-8") as f:
        return f.read()


def strip_vsicurl(path: str) -> str:
    if path.startswith(VSICURL_PREFIX):
        return path[len(VSICURL_PREFIX):]
    return path


def extract_source_filenames(vrt_text: str):
    """Return a list of raw <SourceFilename> text values from the VRT, in order."""
    root = ET.fromstring(vrt_text)
    filenames = []
    for elem in root.iter("SourceFilename"):
        if elem.text:
            filenames.append(elem.text.strip())
    return filenames


def build_rows(filenames, item_id_prefix: str):
    rows = []
    seen = set()
    for raw in filenames:
        clean = strip_vsicurl(raw)
        if clean in seen:
            continue  # VRT may reference the same file more than once (e.g. overviews)
        seen.add(clean)

        m = FILENAME_RE.search(clean)
        if not m:
            print(f"Warning: could not parse year/x/y from '{clean}', skipping.", file=sys.stderr)
            continue

        year = m.group("year")
        x = m.group("x")
        y = m.group("y")

        start_date = f"1.1.{year}"
        end_date = f"31.12.{year}"
        item_id = f"{item_id_prefix}{year}_{x}_{y}"

        rows.append((clean, start_date, end_date, item_id))
    return rows


def main():
    parser = argparse.ArgumentParser(
        description="Create a ';'-separated CSV inventory from a GDAL VRT file."
    )
    parser.add_argument("vrt", help="URL or local path to the .vrt file")
    parser.add_argument("--collection", required=True, help="Value for the 'collection' column")
    parser.add_argument("--asset", required=True, help="Value for the 'asset' column")
    parser.add_argument(
        "--item-id-prefix",
        default="",
        help="Prefix prepended to '<year>_<x>_<y>' to build item_id (e.g. 'clcplus3035_')",
    )
    parser.add_argument(
        "--output", "-o", default="output.csv", help="Path to the output CSV file"
    )
    args = parser.parse_args()

    vrt_text = read_vrt(args.vrt)
    filenames = extract_source_filenames(vrt_text)

    if not filenames:
        print("No <SourceFilename> entries found in VRT.", file=sys.stderr)
        sys.exit(1)

    rows = build_rows(filenames, args.item_id_prefix)

    with open(args.output, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["file", "start-date", "end-date", "collection", "asset", "item_id"])
        for clean, start_date, end_date, item_id in rows:
            writer.writerow([clean, start_date, end_date, args.collection, args.asset, item_id])

    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()