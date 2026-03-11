#!/usr/bin/env bash

set -euo pipefail

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/unitycatalog/unitycatalog.git}"
UC_REF="${UC_REF:-7c8453b7b21a2c4a7bdb03894592baefb3a04efd}"

rm -rf "$UC_DIR"
git clone "$UC_REPO" "$UC_DIR"
cd "$UC_DIR"
git checkout "$UC_REF"

./build/sbt \
  "set client / Compile / packageDoc / publishArtifact := false" \
  clean \
  client/generate \
  client/publishLocal \
  client/publishM2 \
  server/publishLocal \
  server/publishM2

# spark/publishM2 occasionally loses a transient dependency race in CI. A short
# retry loop is enough once the partially-downloaded artifacts are in cache.
for attempt in 1 2 3; do
  if ./build/sbt \
    "set client / Compile / packageDoc / publishArtifact := false" \
    spark/publishLocal \
    spark/publishM2; then
    exit 0
  fi

  if [[ "$attempt" -eq 3 ]]; then
    exit 1
  fi

  echo "spark/publishM2 failed on attempt $attempt; retrying after a short backoff"
  sleep 5
done
