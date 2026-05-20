#!/usr/bin/env bash
#
# Helper to clone Unity Catalog at the pinned SHA (or `main` for the floating canary) and
# publish its client/server/spark jars to ~/.ivy2/local (and ~/.m2) so sbt can resolve UC
# dependencies locally. Used by the pinned arrangement below and by the floating-main canary
# in disabled_spark_test_uc_master.yaml. UC_REF is restricted to `main` or the pinned SHA;
# no other values are accepted.
#
# What the pinned-SHA usage adds on top of the generic flow - and which is temporary
# scaffolding to rip out when Delta can use a released UC version again (flip
# `unityCatalogReleaseVersion` in build.sbt) - is:
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
#   that would be published. That's how build.sbt discovers the version string in pinned mode.
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
#   UC_DIR   directory to clone into       (default: /tmp/unitycatalog)
#   UC_REPO  git remote URL                (default: upstream unitycatalog)
#   UC_REF   must be `main` or UC_PIN_SHA  (default: UC_PIN_SHA below)
#   UC_FORCE set to "1" to rebuild even when the Ivy artifact exists
#
# UC_REF is restricted to exactly two values by design: the pinned SHA (the normal case) or
# `main` (for the floating-main canary flow). Any other value is rejected. CI should never set
# UC_REF.

set -euo pipefail

# ---------------------------------------------------------------------------------------------
# The pin. Bump both lines together if UC's version.sbt changed at the new SHA. build.sbt's
# `unityCatalogVersion` is obtained by running this script with `--print-version`, so these two
# values are the single source of truth.
UC_PIN_SHA=e6deb37e890a0a6fb8ae495b5bec52326731f6a6
UC_BASE_VERSION=0.5.0-SNAPSHOT
# ---------------------------------------------------------------------------------------------

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/unitycatalog/unitycatalog.git}"
UC_REF="${UC_REF:-$UC_PIN_SHA}"
UC_FORCE="${UC_FORCE:-0}"

# Enforce the two-value contract. Anything else is either a typo or a misuse and would bypass the
# safety check below.
if [[ "$UC_REF" != "main" && "$UC_REF" != "$UC_PIN_SHA" ]]; then
  echo "ERROR: UC_REF must be 'main' or the pinned SHA ($UC_PIN_SHA). Got: $UC_REF" >&2
  exit 1
fi

# 7-char suffix for the Ivy coordinate. The pinned SHA gets abbreviated to git's default length;
# the string `main` passes through as-is, yielding coordinates like `0.5.0-SNAPSHOT-main`.
if [[ "$UC_REF" == "main" ]]; then
  UC_REF_SHORT="main"
else
  UC_REF_SHORT="${UC_REF:0:7}"
fi
UC_VERSION="$UC_BASE_VERSION-$UC_REF_SHORT"

# --print-version: discover the coordinate without doing any work. build.sbt uses this at load
# time to populate `unityCatalogVersion`.
if [[ "${1:-}" == "--print-version" ]]; then
  echo "$UC_VERSION"
  exit 0
fi

# Canonical Ivy + Maven artifact paths. Delta depends on all three UC modules; sbt resolves from
# ~/.ivy2/local, mvn (kernel-examples integration tests) resolves from ~/.m2/repository. If any
# is missing in either layout we must re-publish.
IVY_LOCAL="$HOME/.ivy2/local/io.unitycatalog"
IVY_CANARY_CLIENT="$IVY_LOCAL/unitycatalog-client/$UC_VERSION/ivys/ivy.xml"
IVY_CANARY_SERVER="$IVY_LOCAL/unitycatalog-server/$UC_VERSION/ivys/ivy.xml"
IVY_CANARY_SPARK="$IVY_LOCAL/unitycatalog-spark_2.13/$UC_VERSION/ivys/ivy.xml"
IVY_CANARY_HADOOP="$IVY_LOCAL/unitycatalog-hadoop/$UC_VERSION/ivys/ivy.xml"
M2_LOCAL="$HOME/.m2/repository/io/unitycatalog"
M2_CANARY_CLIENT="$M2_LOCAL/unitycatalog-client/$UC_VERSION/unitycatalog-client-$UC_VERSION.pom"
M2_CANARY_SERVER="$M2_LOCAL/unitycatalog-server/$UC_VERSION/unitycatalog-server-$UC_VERSION.pom"
M2_CANARY_SPARK="$M2_LOCAL/unitycatalog-spark_2.13/$UC_VERSION/unitycatalog-spark_2.13-$UC_VERSION.pom"
M2_CANARY_HADOOP="$M2_LOCAL/unitycatalog-hadoop/$UC_VERSION/unitycatalog-hadoop-$UC_VERSION.pom"
ALL_CANARIES=("$IVY_CANARY_CLIENT" "$IVY_CANARY_SERVER" "$IVY_CANARY_SPARK" "$IVY_CANARY_HADOOP"
              "$M2_CANARY_CLIENT" "$M2_CANARY_SERVER" "$M2_CANARY_SPARK" "$M2_CANARY_HADOOP")

all_canaries_present() {
  for c in "${ALL_CANARIES[@]}"; do
    [[ -f "$c" ]] || return 1
  done
  return 0
}

if [[ "$UC_FORCE" != "1" ]] && all_canaries_present; then
  echo ">>> UC $UC_VERSION already published to ~/.ivy2/local; skipping."
  echo ">>> (Set UC_FORCE=1 to rebuild anyway.)"
  exit 0
fi

echo ">>> Fetching Unity Catalog main from $UC_REPO"
rm -rf "$UC_DIR"
mkdir -p "$UC_DIR"
# Fetch main's full history so we can run `git merge-base --is-ancestor` below to verify the
# pinned SHA is actually on main. UC's repo is small; full fetch of one branch is cheap.
git -C "$UC_DIR" init --quiet
git -C "$UC_DIR" remote add origin "$UC_REPO"
git -C "$UC_DIR" fetch --quiet origin main

cd "$UC_DIR"

# Safety check: the pinned SHA must be reachable from UC main. Local `merge-base --is-ancestor`
# on the history we just fetched - no GitHub API, no token needed. Only applies when UC_REF is
# the pinned SHA; UC_REF=main is trivially on main.
if [[ "$UC_REF" == "$UC_PIN_SHA" ]]; then
  if ! git merge-base --is-ancestor "$UC_PIN_SHA" origin/main 2>/dev/null; then
    echo "ERROR: UC_PIN_SHA=$UC_PIN_SHA is not reachable from unitycatalog/unitycatalog main." >&2
    echo "       Pin must reference a commit on https://github.com/unitycatalog/unitycatalog/commits/main" >&2
    exit 1
  fi
fi

if [[ "$UC_REF" == "main" ]]; then
  git checkout --quiet origin/main
else
  git checkout --quiet "$UC_PIN_SHA"
fi

# Sanity-check UC_BASE_VERSION against what UC actually declares at this commit. If they drift
# (someone bumped UC_PIN_SHA across a UC version.sbt change without also bumping
# UC_BASE_VERSION), the Ivy coordinate wouldn't match what sbt publishes - fail loudly instead of
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
  server/publishM2 \
  hadoop/publishLocal \
  hadoop/publishM2

# spark/publishM2 can hit a transient coursier lock race - retry up to 3 times.
echo ">>> Building and publishing UC spark module to local Maven repo"
for attempt in 1 2 3; do
  if ./build/sbt \
    "$SET_VERSION_CMD" \
    "set client / Compile / packageDoc / publishArtifact := false" \
    spark/publishLocal \
    spark/publishM2; then
    for c in "${ALL_CANARIES[@]}"; do
      if [[ ! -f "$c" ]]; then
        echo "ERROR: publish succeeded but $c is missing - the publish target layout may have changed." >&2
        exit 1
      fi
    done
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
