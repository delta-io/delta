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
# Idempotency: the script always clones UC (a shallow fetch-by-SHA takes
# ~1s) so we can read its version.sbt to compute the exact Ivy coordinate
# we'd publish. If the `unitycatalog-client` ivy.xml for that coordinate
# already exists under ~/.ivy2/local, the slow sbt publish steps are
# skipped. That's the canonical Ivy artifact path — the one sbt itself
# uses for resolution — so the presence check is exactly the same as "sbt
# can resolve this dep."
#
# Environment variable overrides (all optional):
#   UC_DIR   — directory to clone into   (default: /tmp/unitycatalog)
#   UC_REPO  — git remote URL            (default: upstream unitycatalog)
#   UC_REF   — commit / branch / tag     (default: SHA from unitycatalog-pin.sha)
#   UC_FORCE — set to "1" to rebuild even if the Ivy artifact is present
#
# Setting UC_REF explicitly changes the coordinate the script computes, so
# the idempotency check naturally falls through to a rebuild unless that
# exact override has already been published. CI should never set UC_REF.
#
# Outputs:
#   $UC_DIR/.uc-version — the composed version that was published, e.g.
#                         "0.5.0-SNAPSHOT-a7683a2306...". Callers read this
#                         to pass -DunityCatalogVersion when experimenting
#                         with a non-pinned ref; the default in build.sbt
#                         derives from the pin file so the flag isn't
#                         normally needed.

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

# Compose the Ivy coordinate: <UC base version>-<resolved SHA>. Encoding
# the SHA guarantees every pin yields a distinct coordinate, so a stale
# build from a previous pin can never resolve silently.
UC_BASE_VERSION=$(grep 'ThisBuild / version' version.sbt | sed 's/.*:= *"\(.*\)"/\1/')
if [[ -z "$UC_BASE_VERSION" ]]; then
  echo "ERROR: Could not extract UC version from version.sbt" >&2
  exit 1
fi
UC_VERSION="$UC_BASE_VERSION-$UC_REF"
echo ">>> UC base version: $UC_BASE_VERSION"
echo ">>> Target coordinate: $UC_VERSION"
echo "$UC_VERSION" > "$UC_DIR/.uc-version"

# Canonical Ivy artifact path. If this exists, sbt can already resolve the
# coordinate — no need to re-publish.
IVY_CANARY="$HOME/.ivy2/local/io.unitycatalog/unitycatalog-client/$UC_VERSION/ivys/ivy.xml"

if [[ "$UC_FORCE" != "1" && -f "$IVY_CANARY" ]]; then
  echo ">>> Found $IVY_CANARY"
  echo ">>> UC $UC_VERSION already published to ~/.ivy2/local; skipping sbt build."
  echo ">>> (Set UC_FORCE=1 to rebuild anyway.)"
  exit 0
fi

# Override version.sbt via sbt `set` so every publish* command uses the
# composed <base>-<sha> coordinate. Applied as a persistent setting so it
# sticks across the two sbt invocations below.
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
    if [[ ! -f "$IVY_CANARY" ]]; then
      echo "ERROR: publish succeeded but $IVY_CANARY is missing — the publish target layout may have changed." >&2
      exit 1
    fi
    echo ">>> UC build complete. Published coordinate: $UC_VERSION"
    exit 0
  fi

  if [[ "$attempt" -eq 3 ]]; then
    echo ">>> spark/publishM2 failed after 3 attempts"
    exit 1
  fi

  echo ">>> spark/publishM2 failed on attempt $attempt; retrying..."
  sleep 5
done
