#!/usr/bin/env bash
#
# Builds Unity Catalog from source and publishes jars to the local Maven/Ivy
# cache so that Delta tests can run against a UC version newer than the last
# release. Intended to be called from CI before the test step.
#
# Environment variables (all optional):
#   UC_DIR   — directory to clone into        (default: /tmp/unitycatalog)
#   UC_REPO  — git remote URL                 (default: upstream unitycatalog)
#   UC_REF   — commit / branch / tag to build (default: main)

set -euo pipefail

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/unitycatalog/unitycatalog.git}"
UC_REF="${UC_REF:-main}"

echo ">>> Cloning Unity Catalog from $UC_REPO at ref $UC_REF"
rm -rf "$UC_DIR"
git clone --depth 50 "$UC_REPO" "$UC_DIR"
cd "$UC_DIR"
git checkout "$UC_REF"

# Extract the version UC will publish (e.g. "0.5.0-SNAPSHOT")
UC_VERSION=$(grep 'ThisBuild / version' version.sbt | sed 's/.*:= *"\(.*\)"/\1/')
if [[ -z "$UC_VERSION" ]]; then
  echo "ERROR: Could not extract UC version from version.sbt" >&2
  exit 1
fi
echo ">>> UC version: $UC_VERSION"
echo "$UC_VERSION" > "$UC_DIR/.uc-version"

echo ">>> Building and publishing UC client + server to local Maven repo"
./build/sbt \
  "set client / Compile / packageDoc / publishArtifact := false" \
  clean \
  client/generate \
  client/publishLocal \
  client/publishM2 \
  server/publishLocal \
  server/publishM2

# spark/publishM2 can hit a transient coursier lock race — retry up to 3 times.
echo ">>> Building and publishing UC spark module to local Maven repo"
for attempt in 1 2 3; do
  if ./build/sbt \
    "set client / Compile / packageDoc / publishArtifact := false" \
    spark/publishLocal \
    spark/publishM2; then
    echo ">>> UC build complete"
    exit 0
  fi

  if [[ "$attempt" -eq 3 ]]; then
    echo ">>> spark/publishM2 failed after 3 attempts"
    exit 1
  fi

  echo ">>> spark/publishM2 failed on attempt $attempt; retrying..."
  sleep 5
done
