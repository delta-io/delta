#!/usr/bin/env bash
#
# General-purpose helper to clone Unity Catalog at some ref and publish its client/server/spark
# jars to ~/.ivy2/local (and ~/.m2) so sbt can resolve UC dependencies locally. Useful whenever
# Delta needs a UC build that isn't yet available on Maven Central — most obviously for the
# pinned-master arrangement below, but also for the floating-main canary in
# disabled_spark_test_uc_master.yaml and for ad-hoc dev experimentation against UC master.
#
# What the pinned-master usage adds on top of the generic flow — and which is temporary
# scaffolding to rip out when Delta can use a released UC version again (flip
# `unityCatalogReleaseVersion` in build.sbt) — is:
#   - the UC_PIN_SHA / UC_BASE_VERSION constants below,
#   - the `--print-version` short-circuit that build.sbt uses to discover the coordinate,
#   - the sanity check that UC's version.sbt matches UC_BASE_VERSION.
# Rip those out, keep the generic clone/publish flow.
#
# What this does:
#   Publishes UC (client/server/spark jars) into ~/.ivy2/local at coordinate
#   <UC_BASE_VERSION>-<7-char sha>, e.g. 0.5.0-SNAPSHOT-3b45d34. Idempotent: when the canonical
#   Ivy artifact already exists for the target coordinate, the slow sbt publish is skipped.
#
#   `--print-version` short-circuits before any filesystem work and just echoes the coordinate
#   that would be published. That's how build.sbt discovers the version string in pinned-master
#   mode.
#
# Why local publish:
#   Delta master depends on UC APIs that aren't in any released UC yet. Encoding the pinned SHA
#   in the Ivy coordinate means a pin bump changes the coordinate even when UC's version.sbt
#   didn't move, so stale jars from a previous pin can't resolve silently.
#
# How to invoke:
#   The first `build/sbt` that touches sparkUnityCatalog or kernelUnityCatalog calls this script
#   automatically via `ensurePinnedUnityCatalog` in build.sbt. You only need to run it by hand
#   for debugging or for experimenting with an override ref.
#
# How to bump the pin:
#   1. Replace UC_PIN_SHA below with a newer commit from
#      https://github.com/unitycatalog/unitycatalog/commits/main
#   2. If UC's version.sbt string differs at the new SHA, also update UC_BASE_VERSION below (this
#      script sanity-checks the two match when the slow path runs, so you'll get a loud error
#      if not).
#   3. Run this script locally; then `build/sbt sparkUnityCatalog/test kernelUnityCatalog/test`.
#   4. Open a focused PR.
#
# Environment overrides:
#   UC_DIR   directory to clone into  (default: /tmp/unitycatalog)
#   UC_REPO  git remote URL           (default: upstream unitycatalog)
#   UC_REF   commit / branch / tag    (default: UC_PIN_SHA below)
#   UC_FORCE set to "1" to rebuild even when the Ivy artifact exists
#
# Overriding UC_REF computes a different coordinate, which naturally falls through to a rebuild
# unless that exact override was already published. CI should never set UC_REF.

set -euo pipefail

# ---------------------------------------------------------------------------------------------
# The pin. Bump both lines together if UC's version.sbt changed at the new SHA. build.sbt's
# `unityCatalogVersion` is obtained by running this script with `--print-version`, so these two
# values are the single source of truth.
UC_PIN_SHA=3b45d34d66feb6fe48e8f37dc41acae3018db971
UC_BASE_VERSION=0.5.0-SNAPSHOT
# ---------------------------------------------------------------------------------------------

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/unitycatalog/unitycatalog.git}"
UC_REF="${UC_REF:-$UC_PIN_SHA}"
UC_FORCE="${UC_FORCE:-0}"

# 7-char suffix for the Ivy coordinate (matches git's default abbreviation). For a 40-char hex
# SHA, take the first 7. Anything else (a branch name or tag passed via UC_REF) passes through
# as-is.
if [[ "$UC_REF" =~ ^[0-9a-f]{40}$ ]]; then
  UC_REF_SHORT="${UC_REF:0:7}"
else
  UC_REF_SHORT="$UC_REF"
fi
UC_VERSION="$UC_BASE_VERSION-$UC_REF_SHORT"

# --print-version: discover the coordinate without doing any work. build.sbt uses this at load
# time to populate `unityCatalogVersion`.
if [[ "${1:-}" == "--print-version" ]]; then
  echo "$UC_VERSION"
  exit 0
fi

# Canonical Ivy artifact path. If it exists, sbt can already resolve the coordinate — no fetch,
# no publish, just exit.
IVY_CANARY="$HOME/.ivy2/local/io.unitycatalog/unitycatalog-client/$UC_VERSION/ivys/ivy.xml"
if [[ "$UC_FORCE" != "1" && -f "$IVY_CANARY" ]]; then
  echo ">>> UC $UC_VERSION already published to ~/.ivy2/local; skipping."
  echo ">>> (Set UC_FORCE=1 to rebuild anyway.)"
  exit 0
fi

# Safety check: verify UC_PIN_SHA is actually a commit on UC's main branch, not a stray ref
# (e.g. a fork, a PR branch, a dangling commit). Uses GitHub's compare API; `ahead` or
# `identical` means the SHA is reachable from main. Only runs for the pinned default —
# explicit UC_REF overrides are for experimentation and skip this check.
if [[ "$UC_REF" == "$UC_PIN_SHA" ]]; then
  echo ">>> Verifying $UC_REF is on UC main via GitHub compare API"
  CURL_AUTH=()
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    CURL_AUTH=(-H "Authorization: Bearer $GITHUB_TOKEN")
  fi
  COMPARE_STATUS=$(curl -fsSL "${CURL_AUTH[@]}" \
    "https://api.github.com/repos/unitycatalog/unitycatalog/compare/$UC_REF...main" \
    | grep -m1 '"status"' \
    | sed 's/.*"status":[[:space:]]*"\([^"]*\)".*/\1/' || true)
  case "$COMPARE_STATUS" in
    ahead|identical)
      echo ">>> $UC_REF is on UC main ($COMPARE_STATUS)"
      ;;
    *)
      echo "ERROR: UC_PIN_SHA=$UC_REF is not reachable from unitycatalog/unitycatalog main" >&2
      echo "       (GitHub compare status: ${COMPARE_STATUS:-unknown})." >&2
      echo "       Pin must reference a commit on https://github.com/unitycatalog/unitycatalog/commits/main" >&2
      exit 1
      ;;
  esac
fi

echo ">>> Fetching Unity Catalog from $UC_REPO at ref $UC_REF"
rm -rf "$UC_DIR"
mkdir -p "$UC_DIR"
# `git fetch <sha>` works for any commit (not just refs) because GitHub enables
# uploadpack.allowReachableSHA1InWant. This keeps the fetch shallow regardless of how far back on
# main the pinned SHA is.
git -C "$UC_DIR" init --quiet
git -C "$UC_DIR" remote add origin "$UC_REPO"
git -C "$UC_DIR" fetch --depth 1 --quiet origin "$UC_REF"
git -C "$UC_DIR" checkout --quiet FETCH_HEAD

cd "$UC_DIR"

# Sanity-check UC_BASE_VERSION against what UC actually declares at this commit. If they drift
# (someone bumped UC_PIN_SHA across a UC version.sbt change without also bumping
# UC_BASE_VERSION), the Ivy coordinate wouldn't match what sbt publishes — fail loudly instead of
# silently producing unresolvable coordinates.
ACTUAL_BASE=$(grep 'ThisBuild / version' version.sbt | sed 's/.*:= *"\(.*\)"/\1/')
if [[ "$ACTUAL_BASE" != "$UC_BASE_VERSION" ]]; then
  echo "ERROR: UC at $UC_REF has version.sbt '$ACTUAL_BASE', but this script pins UC_BASE_VERSION='$UC_BASE_VERSION'." >&2
  echo "Bump UC_BASE_VERSION in this script to match." >&2
  exit 1
fi

# Override version.sbt via sbt `set` so every publish* command uses the composed <base>-<sha>
# coordinate. Applied as a persistent setting so it sticks across the two sbt invocations below.
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
