#!/usr/bin/env bash

#
# Clones and builds Unity Catalog from a custom fork, then publishes artifacts
# to the local Maven repository (~/.m2). This is required because the Delta
# build depends on io.unitycatalog:unitycatalog-client:0.3.0-SNAPSHOT which is
# not published to any remote Maven repository.
#
# The build is retried up to 3 times to handle transient failures such as
# coursier OverlappingFileLockException during parallel dependency resolution.
#
# Usage:
#   dev/setup-unitycatalog.sh
#

set -euo pipefail

UC_REPO="https://github.com/openinx/unitycatalog.git"
UC_BRANCH="staging-table-catalog"
UC_DIR="/tmp/unitycatalog"
MAX_ATTEMPTS=3
RETRY_DELAY=10

echo "=== Setting up Unity Catalog ==="
echo "Repository: ${UC_REPO}"
echo "Branch:     ${UC_BRANCH}"
echo "Directory:  ${UC_DIR}"

# Clone if not already present
if [ -d "${UC_DIR}" ]; then
  echo "Unity Catalog directory already exists, removing..."
  rm -rf "${UC_DIR}"
fi

git clone "${UC_REPO}" "${UC_DIR}"
cd "${UC_DIR}"
git checkout "${UC_BRANCH}"

# Build with retry logic
for attempt in $(seq 1 ${MAX_ATTEMPTS}); do
  echo ""
  echo "=== Unity Catalog build attempt ${attempt} of ${MAX_ATTEMPTS} ==="
  if ./build/sbt clean package publishM2; then
    echo "=== Unity Catalog build succeeded on attempt ${attempt} ==="
    exit 0
  fi

  if [ "${attempt}" -eq "${MAX_ATTEMPTS}" ]; then
    echo "=== Unity Catalog build failed after ${MAX_ATTEMPTS} attempts ==="
    exit 1
  fi

  echo "Build failed, retrying in ${RETRY_DELAY} seconds..."
  sleep "${RETRY_DELAY}"
done
