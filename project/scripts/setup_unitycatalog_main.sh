#!/usr/bin/env bash
#
# Builds Unity Catalog from source at the commit pinned in
# project/unitycatalog-pin.sha and publishes the client / server / spark
# jars to the local Ivy and Maven caches so Delta's sbt build can resolve
# them. Delta master depends on APIs that are not yet in a released UC
# version, which is why this step is required — not optional — before
# building any sbt project that transitively touches the UC modules. See
# UC_MASTER_TESTING.md for the full story.
#
# Idempotency: a marker file under ~/.ivy2/local records the SHA the
# cache was last built for. If the marker matches the pinned SHA, the
# script skips the rebuild and just re-emits the UC version to the
# sentinel path. That makes CI and local re-invocations cheap.
#
# Environment variable overrides (all optional):
#   UC_DIR   — directory to clone into   (default: /tmp/unitycatalog)
#   UC_REPO  — git remote URL            (default: upstream unitycatalog)
#   UC_REF   — commit / branch / tag     (default: SHA from unitycatalog-pin.sha)
#   UC_FORCE — set to "1" to force a rebuild even if the marker matches
#
# Setting UC_REF explicitly forces a rebuild (the marker optimization only
# applies when using the pinned SHA). CI should never set UC_REF.
#
# Outputs:
#   $UC_DIR/.uc-version — the UC version that was published, e.g.
#                         "0.5.0-SNAPSHOT". Callers read this when they
#                         want to override -DunityCatalogVersion; the
#                         default in build.sbt is kept in sync with the
#                         pin so the flag isn't normally needed.

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
UC_FORCE="${UC_FORCE:-0}"

# Marker lives under the local Ivy repo so GitHub Actions caches that
# include ~/.ivy2 carry it automatically.
MARKER_FILE="$HOME/.ivy2/local/.unitycatalog-pin"

using_pinned_default=0
if [[ "$UC_REF" == "$DEFAULT_REF" ]]; then
  using_pinned_default=1
fi

# Fast path: pinned default + marker match + not forced → skip rebuild.
if [[ "$using_pinned_default" -eq 1 && "$UC_FORCE" != "1" && -f "$MARKER_FILE" ]]; then
  CACHED_SHA=$(awk -F= '/^sha=/ {print $2}' "$MARKER_FILE" 2>/dev/null)
  CACHED_VERSION=$(awk -F= '/^version=/ {print $2}' "$MARKER_FILE" 2>/dev/null)
  if [[ "$CACHED_SHA" == "$DEFAULT_REF" && -n "$CACHED_VERSION" ]]; then
    echo ">>> UC jars already published for pinned SHA $DEFAULT_REF (version $CACHED_VERSION); skipping rebuild"
    mkdir -p "$UC_DIR"
    echo "$CACHED_VERSION" > "$UC_DIR/.uc-version"
    echo ">>> To force a rebuild, set UC_FORCE=1"
    exit 0
  fi
fi

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

# Extract UC's declared base version from version.sbt (e.g. "0.5.0-SNAPSHOT"),
# then append the pinned SHA so every pin yields a distinct Ivy coordinate.
# This is what eliminates the stale-jar risk on pin bumps where UC itself
# didn't change its version string.
UC_BASE_VERSION=$(grep 'ThisBuild / version' version.sbt | sed 's/.*:= *"\(.*\)"/\1/')
if [[ -z "$UC_BASE_VERSION" ]]; then
  echo "ERROR: Could not extract UC version from version.sbt" >&2
  exit 1
fi
UC_VERSION="$UC_BASE_VERSION-$UC_REF"
echo ">>> UC base version: $UC_BASE_VERSION"
echo ">>> Publishing as:  $UC_VERSION"
echo "$UC_VERSION" > "$UC_DIR/.uc-version"

# Override version.sbt's value via sbt `set` so every publish* command uses
# the composed <base>-<sha> coordinate. Applied as a persistent setting so
# it sticks across the two sbt invocations below.
SET_VERSION_CMD="set ThisBuild / version := \"$UC_VERSION\""

echo ">>> Building and publishing UC client + server to local Maven repo"
./build/sbt \
  "$SET_VERSION_CMD" \
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
    "$SET_VERSION_CMD" \
    "set client / Compile / packageDoc / publishArtifact := false" \
    spark/publishLocal \
    spark/publishM2; then
    if [[ "$using_pinned_default" -eq 1 ]]; then
      mkdir -p "$(dirname "$MARKER_FILE")"
      printf 'sha=%s\nversion=%s\n' "$UC_REF" "$UC_VERSION" > "$MARKER_FILE"
    fi
    echo ">>> UC build complete."
    echo ">>> To run Delta UC tests against this build:"
    echo ">>>   build/sbt sparkUnityCatalog/test kernelUnityCatalog/test"
    echo ">>> (build.sbt's unityCatalogVersion default is kept in sync with the pin,"
    echo ">>>  so -DunityCatalogVersion is only needed when experimenting with a non-pinned ref.)"
    exit 0
  fi

  if [[ "$attempt" -eq 3 ]]; then
    echo ">>> spark/publishM2 failed after 3 attempts"
    exit 1
  fi

  echo ">>> spark/publishM2 failed on attempt $attempt; retrying..."
  sleep 5
done
