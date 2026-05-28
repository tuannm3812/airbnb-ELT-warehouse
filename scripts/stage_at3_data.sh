#!/usr/bin/env bash
set -euo pipefail

DEFAULT_SOURCE_DIR="${SOURCE_ARCHIVE_DIR:-data/source_zips}"
SOURCE_DIR="${1:-$DEFAULT_SOURCE_DIR}"
TARGET_DIR="${2:-data}"

LISTINGS_ZIP="$SOURCE_DIR/listings.zip"
CENSUS_ZIP="$SOURCE_DIR/Census LGA.zip"
NSW_LGA_ZIP="$SOURCE_DIR/NSW_LGA.zip"

for zip_file in "$LISTINGS_ZIP" "$CENSUS_ZIP" "$NSW_LGA_ZIP"; do
  if [ ! -f "$zip_file" ]; then
    echo "Missing required ZIP: $zip_file" >&2
    exit 1
  fi
done

mkdir -p "$TARGET_DIR/airbnb" "$TARGET_DIR/census/G01" "$TARGET_DIR/census/G02" "$TARGET_DIR/mappings"

unzip -jo "$LISTINGS_ZIP" "*.csv" -d "$TARGET_DIR/airbnb"
unzip -jo "$CENSUS_ZIP" "Census LGA/*.csv" -d "$TARGET_DIR/census"
unzip -jo "$NSW_LGA_ZIP" "*.csv" -d "$TARGET_DIR/mappings"

mv -f "$TARGET_DIR/census/2016Census_G01_NSW_LGA.csv" \
  "$TARGET_DIR/census/G01/2016Census_G01_NSW_LGA.csv"
mv -f "$TARGET_DIR/census/2016Census_G02_NSW_LGA.csv" \
  "$TARGET_DIR/census/G02/2016Census_G02_NSW_LGA.csv"

find "$TARGET_DIR" -name "._*" -type f -delete

echo "Staged data in: $TARGET_DIR"
find "$TARGET_DIR" -maxdepth 3 -type f | sort
