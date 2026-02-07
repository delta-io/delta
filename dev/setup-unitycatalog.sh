#!/usr/bin/env bash

#
# Clones and builds Unity Catalog, publishing artifacts to ~/.m2.
# Retries up to MAX_ATTEMPTS times, clearing the Coursier cache between
# attempts to recover from corrupted partial downloads (HTTP 416).
#

set -euo pipefail

UC_REPO="https://github.com/openinx/unitycatalog.git"
UC_BRANCH="staging-table-catalog"
UC_DIR="/tmp/unitycatalog"
MAX_ATTEMPTS=3
RETRY_DELAY=15

echo "=== Setting up Unity Catalog (${UC_BRANCH}) ==="

rm -rf "${UC_DIR}"
git clone --branch "${UC_BRANCH}" --single-branch "${UC_REPO}" "${UC_DIR}"
cd "${UC_DIR}"

for attempt in $(seq 1 "${MAX_ATTEMPTS}"); do
  echo ""
  echo "=== Build attempt ${attempt}/${MAX_ATTEMPTS} ==="

  if ./build/sbt -Dsbt.boot.lock=false clean package publishM2; then
    echo "=== Build succeeded ==="
    exit 0
  fi

  if [ "${attempt}" -eq "${MAX_ATTEMPTS}" ]; then
    echo "=== Build failed after ${MAX_ATTEMPTS} attempts ==="
    exit 1
  fi

  echo "Build failed — clearing Coursier cache and retrying in ${RETRY_DELAY}s..."
  rm -rf "${HOME}/.cache/coursier" "${HOME}/.coursier/cache"
  sleep "${RETRY_DELAY}"
done
