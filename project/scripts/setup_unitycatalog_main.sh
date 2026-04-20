#!/usr/bin/env bash
#
# TEMPORARY scaffolding — delete once UC 0.5 is released and Delta can
# pin a released UC version instead (see `unityCatalogReleaseVersion` in
# build.sbt).
#
# What this does:
#   Clones Unity Catalog at the commit in project/unitycatalog-pin.sha,
#   publishes the client / server / spark jars into ~/.ivy2/local (and
#   ~/.m2) at coordinate <UC version.sbt>-<pinned sha>, e.g.
#   0.5.0-SNAPSHOT-a7683a2306..., so sbt can resolve UC dependencies
#   locally. build.sbt's unityCatalogVersion is derived from the same
#   pin file, so publisher and consumer agree by construction.
#
# Why locally:
#   Delta master depends on UC APIs that aren't in any released UC yet.
#   Encoding the pinned SHA in the Ivy coordinate means a pin bump
#   changes the coordinate even when UC's version.sbt didn't move, so
#   stale jars from a previous pin can't resolve silently.
#
# How to invoke:
#   The first `build/sbt` that touches sparkUnityCatalog or
#   kernelUnityCatalog calls this script automatically via
#   `ensurePinnedUnityCatalog` (build.sbt). You only need to run it
#   directly for debugging or for experimenting with an override ref.
#
# How to bump the pin:
#   1. Edit project/unitycatalog-pin.sha to a newer SHA from
#      https://github.com/unitycatalog/unitycatalog/commits/main
#   2. If UC's version.sbt string changed at the new SHA, also update
#      `unityCatalogBaseVersion` in build.sbt (same commit).
#   3. Run this script locally; then `build/sbt sparkUnityCatalog/test
#      kernelUnityCatalog/test`.
#   4. Open a focused PR.
#
# Idempotency:
#   Always clones UC shallowly to resolve the target coordinate (~1s),
#   then checks ~/.ivy2/local/io.unitycatalog/unitycatalog-client/
#   <coordinate>/ivys/ivy.xml — the canonical path sbt uses for
#   resolution. If it's there, the sbt publish is skipped. UC_FORCE=1
#   bypasses the check.
#
# Environment overrides:
#   UC_DIR   directory to clone into  (default: /tmp/unitycatalog)
#   UC_REPO  git remote URL           (default: upstream unitycatalog)
#   UC_REF   commit / branch / tag    (default: pin file SHA)
#   UC_FORCE set to "1" to rebuild even when the Ivy artifact exists
#
# Overriding UC_REF computes a different coordinate, which naturally
# falls through to a rebuild unless that exact override was already
# published. CI should never set UC_REF.
#
# Output: $UC_DIR/.uc-version contains the coordinate that was
# published, for callers experimenting with an override (pass as
# -DunityCatalogVersion=...).

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
