#!/usr/bin/env bash
set -euo pipefail

# Simple local helper to publish kernel artifacts and run the cross-spark publish tests.
# Usage: scripts/dev-kernel-publish-and-test.sh

root_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root_dir"

echo "Publishing storage (root) then kernel artifacts locally..."
./build/sbt "++ 2.13.16" "storage/publishM2"
cd "$root_dir/kernel"
../build/sbt ";+kernelApi/publishM2; +kernelDefaults/publishM2; +kernelUnityCatalog/publishM2"
cd "$root_dir"

echo "Running cross-spark publish test with recent python..."
# Prefer newer python if available
PY_BIN="$(command -v python3.10 || command -v python3.9 || command -v python3)"
if [ -z "$PY_BIN" ]; then
  echo "No suitable python3 found" >&2
  exit 1
fi
"$PY_BIN" -m venv .venv || true
VENV_PY="$root_dir/.venv/bin/python"
"$VENV_PY" -m pip install --upgrade pip dataclasses || true
"$VENV_PY" project/tests/test_cross_spark_publish.py

echo "Done."
