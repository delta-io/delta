#!/usr/bin/env bash
#
# Builds Unity Catalog from source at the commit pinned in
# project/unitycatalog-pin.sha and publishes jars to the local Maven/Ivy
# cache so that Delta tests can exercise an unreleased UC version.
#
# Intended to be called from CI before the UC test step, and optionally
# by local developers who want to test against UC master. See
# UC_MASTER_TESTING.md for the full local-dev workflow.
#
# Environment variable overrides (all optional):
#   UC_DIR   — directory to clone into   (default: /tmp/unitycatalog)
#   UC_REPO  — git remote URL            (default: upstream unitycatalog)
#   UC_REF   — commit / branch / tag     (default: SHA from unitycatalog-pin.sha)
#
# Setting UC_REF explicitly is useful for local experimentation; CI should
# always build the pinned SHA so CI signal is reproducible.
#
# Outputs:
#   $UC_DIR/.uc-version — the version UC published (e.g. "0.5.0-SNAPSHOT"),
#                         which callers pass as -DunityCatalogVersion=<version>.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIN_FILE="$SCRIPT_DIR/../unitycatalog-pin.sha"

# Read the pinned SHA, ignoring comments and blank lines.
DEFAULT_REF=""
if [[ -f "$PIN_FILE" ]]; then
  DEFAULT_REF=$(grep -vE '^\s*(#|$)' "$PIN_FILE" | head -n 1 | tr -d '[:space:]')
fi
if [[ -z "$DEFAULT_REF" ]]; then
  echo "ERROR: Could not read pinned UC SHA from $PIN_FILE" >&2
  exit 1
fi

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/unitycatalog/unitycatalog.git}"
UC_REF="${UC_REF:-$DEFAULT_REF}"

echo ">>> Fetching Unity Catalog from $UC_REPO at ref $UC_REF"
rm -rf "$UC_DIR"
mkdir -p "$UC_DIR"
# `git fetch <sha>` works for any commit (not just refs) because GitHub
# enables uploadpack.allowReachableSHA1InWant. This keeps the fetch shallow
# regardless of how far back on main the pinned SHA is.
git -C "$UC_DIR" init --quiet
git -C "$UC_DIR" remote add origin "$UC_REPO"
git -C "$UC_DIR" fetch --depth 1 --quiet origin "$UC_REF"
git -C "$UC_DIR" checkout --quiet FETCH_HEAD

cd "$UC_DIR"

# Extract the version UC will publish (e.g. "0.5.0-SNAPSHOT").
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
    echo ">>> UC build complete. To run Delta UC tests against this build:"
    echo ">>>   build/sbt -DunityCatalogVersion=$UC_VERSION sparkUnityCatalog/test kernelUnityCatalog/test"
    exit 0
  fi

  if [[ "$attempt" -eq 3 ]]; then
    echo ">>> spark/publishM2 failed after 3 attempts"
    exit 1
  fi

  echo ">>> spark/publishM2 failed on attempt $attempt; retrying..."
  sleep 5
done
