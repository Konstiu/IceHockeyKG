#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RAW_ZIP="${1:-$ROOT_DIR/exports/raw_data.zip}"
PROCESSED_ZIP="${2:-$ROOT_DIR/exports/processed_outputs.zip}"

RAW_DIR="$ROOT_DIR/data/raw"
PROCESSED_DIR="$ROOT_DIR/data/processed"

if ! command -v unzip >/dev/null 2>&1; then
  echo "ERROR: 'unzip' ist nicht installiert."
  exit 1
fi

if [[ ! -f "$RAW_ZIP" ]]; then
  echo "ERROR: raw ZIP nicht gefunden: $RAW_ZIP"
  exit 1
fi

if [[ ! -f "$PROCESSED_ZIP" ]]; then
  echo "ERROR: processed ZIP nicht gefunden: $PROCESSED_ZIP"
  exit 1
fi

mkdir -p "$RAW_DIR" "$PROCESSED_DIR"

echo "Entpacke raw data: $RAW_ZIP"
unzip -o "$RAW_ZIP" -d "$ROOT_DIR"

echo "Entpacke processed outputs: $PROCESSED_ZIP"
unzip -o "$PROCESSED_ZIP" -d "$ROOT_DIR"

echo "Fertig."
echo "Raw: $RAW_DIR"
echo "Processed: $PROCESSED_DIR"
