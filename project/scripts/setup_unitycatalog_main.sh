#!/usr/bin/env bash
#
# Helper to clone Unity Catalog at the pinned SHA (or `main` for the floating canary, or an
# arbitrary ref for release pipelines) and publish its client/server/spark jars to ~/.ivy2/local
# (and ~/.m2) so sbt can resolve UC dependencies locally. Used by the pinned arrangement below,
# by the floating-main canary in disabled_spark_test_uc_master.yaml, and by release pipelines
# that need to build UC from source when JFrog hasn't propagated released artifacts yet.
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
#   When UC_VERSION is set from environment, the coordinate is used verbatim (no SHA suffix).
#   This is the release pipeline mode.
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
#   UC_DIR        directory to clone into                 (default: /tmp/unitycatalog)
#   UC_REPO       git remote URL                          (default: upstream unitycatalog)
#   UC_REF        git ref to check out — accepts `main`, the pinned SHA, or any arbitrary ref
#                 (tag, branch, or full SHA) for release pipelines
#                                                        (default: UC_PIN_SHA below)
#   UC_VERSION    explicit version coordinate to publish as (e.g. 0.5.0-rc1). When set, the
#                 script uses it verbatim instead of composing <base>-<sha>. Purely a data
#                 override — does NOT control behavioral gates (see DELTA_RELEASE_MODE).
#   DELTA_RELEASE_MODE
#                 set to "1" to activate release-pipeline behavior: skip rm -rf of stale
#                 artifacts (fresh runners, multiple Spark versions accumulate), skip the
#                 UC_BASE_VERSION sanity check (caller owns the version), and skip the
#                 ancestor-reachability check.                (default: 0)
#   UC_FORCE      set to "1" to rebuild even when the Ivy artifact exists
#   SPARK_VERSION Spark major.minor UC should build for
#                 Forwarded as -DsparkVersion to UC's sbt; also determines the published artifact
#                 name (unitycatalog-spark_${X.Y}_2.13). Delta's build.sbt sets this from
#                 CrossSparkVersions when invoking the script; matrix CI workflows set it from
#                 `matrix.spark_version`. Workflows that don't care which Spark variant UC builds
#                 (kernel/flink/etc.) inherit the in-script fallback below.
#
# Modes of operation:
#   1. Pinned SHA (default, PR CI): UC_REF defaults to UC_PIN_SHA, version is computed as
#      <UC_BASE_VERSION>-<7-char sha>. Sanity checks and rm -rf apply.
#   2. Floating canary: UC_REF=main, version is <UC_BASE_VERSION>-main.
#   3. Release pipeline: UC_REF=v0.5.0 UC_VERSION=0.5.0-rc1 DELTA_RELEASE_MODE=1 — arbitrary
#      ref, explicit version, no sanity checks, no rm -rf (fresh runners, multiple Spark
#      versions accumulate).
#
# Release pipeline examples:
#   UC_REF=v0.5.0 UC_VERSION=0.5.0-rc1 DELTA_RELEASE_MODE=1 SPARK_VERSION=4.0 bash setup_unitycatalog_main.sh
#   UC_REF=v0.5.0 UC_VERSION=0.5.0-rc1 DELTA_RELEASE_MODE=1 SPARK_VERSION=4.1 bash setup_unitycatalog_main.sh
#   UC_VERSION=0.5.0-rc1 bash setup_unitycatalog_main.sh --print-version  # => 0.5.0-rc1

set -euo pipefail

# ---------------------------------------------------------------------------------------------
# The pin. Bump both lines together if UC's version.sbt changed at the new SHA. build.sbt's
# `unityCatalogVersion` is obtained by running this script with `--print-version`, so these two
# values are the single source of truth.
UC_PIN_SHA=5a3b69dd79366e2e48ccff7a22fc735e0317ea8b
UC_BASE_VERSION=0.5.0-SNAPSHOT
# ---------------------------------------------------------------------------------------------

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/unitycatalog/unitycatalog.git}"
UC_REF="${UC_REF:-$UC_PIN_SHA}"
UC_FORCE="${UC_FORCE:-0}"
SPARK_VERSION="${SPARK_VERSION:-4.1}"
DELTA_RELEASE_MODE="${DELTA_RELEASE_MODE:-0}"

# Compose version coordinate. When UC_VERSION is set from env, use it verbatim (no SHA suffix).
# Otherwise compute from UC_BASE_VERSION + abbreviated ref.
if [[ -z "${UC_VERSION:-}" ]]; then
  if [[ "$UC_REF" == "main" ]]; then
    UC_REF_SHORT="main"
  else
    UC_REF_SHORT="${UC_REF:0:7}"
  fi
  UC_VERSION="$UC_BASE_VERSION-$UC_REF_SHORT"
fi

# --print-version: discover the coordinate without doing any work. build.sbt uses this at load
# time to populate `unityCatalogVersion`.
if [[ "${1:-}" == "--print-version" ]]; then
  echo "$UC_VERSION"
  exit 0
fi

# Canonical Ivy + Maven artifact paths. Delta depends on all three UC modules; sbt resolves from
# ~/.ivy2/local, mvn (kernel-examples integration tests) resolves from ~/.m2/repository. If any
# is missing in either layout we must re-publish.
# UC publishes its Spark connector under a per-Spark-version coordinate
# (unitycatalog-spark_${SPARK_VERSION}_2.13). The suffix tracks SPARK_VERSION so the
# canary check matches whatever variant we tell UC to build below.
IVY_LOCAL="$HOME/.ivy2/local/io.unitycatalog"
IVY_CANARY_CLIENT="$IVY_LOCAL/unitycatalog-client/$UC_VERSION/ivys/ivy.xml"
IVY_CANARY_SERVER="$IVY_LOCAL/unitycatalog-server/$UC_VERSION/ivys/ivy.xml"
UC_SPARK_ARTIFACT="unitycatalog-spark_${SPARK_VERSION}_2.13"
IVY_CANARY_SPARK="$IVY_LOCAL/$UC_SPARK_ARTIFACT/$UC_VERSION/ivys/ivy.xml"
IVY_CANARY_HADOOP="$IVY_LOCAL/unitycatalog-hadoop/$UC_VERSION/ivys/ivy.xml"
M2_LOCAL="$HOME/.m2/repository/io/unitycatalog"
M2_CANARY_CLIENT="$M2_LOCAL/unitycatalog-client/$UC_VERSION/unitycatalog-client-$UC_VERSION.pom"
M2_CANARY_SERVER="$M2_LOCAL/unitycatalog-server/$UC_VERSION/unitycatalog-server-$UC_VERSION.pom"
M2_CANARY_SPARK="$M2_LOCAL/$UC_SPARK_ARTIFACT/$UC_VERSION/$UC_SPARK_ARTIFACT-$UC_VERSION.pom"
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

echo ">>> Fetching Unity Catalog from $UC_REPO (ref: $UC_REF)"
rm -rf "$UC_DIR"
mkdir -p "$UC_DIR"
git -C "$UC_DIR" init --quiet
git -C "$UC_DIR" remote add origin "$UC_REPO"
# Full fetch (not --depth=1) so merge-base --is-ancestor can verify the pin.
if [[ "$UC_REF" == "$UC_PIN_SHA" || "$UC_REF" == "main" ]]; then
  git -C "$UC_DIR" fetch --quiet origin main
else
  git -C "$UC_DIR" fetch --quiet origin "$UC_REF"
fi

cd "$UC_DIR"

# Safety check: the pinned SHA must be reachable from UC main. Local `merge-base --is-ancestor`
# on the history we just fetched - no GitHub API, no token needed. Only applies when UC_REF is
# the pinned SHA; UC_REF=main is trivially on main. Skipped in release mode.
if [[ "$DELTA_RELEASE_MODE" != "1" && "$UC_REF" == "$UC_PIN_SHA" ]]; then
  if ! git merge-base --is-ancestor "$UC_PIN_SHA" origin/main 2>/dev/null; then
    echo "ERROR: UC_PIN_SHA=$UC_PIN_SHA is not reachable from unitycatalog/unitycatalog main." >&2
    echo "       Pin must reference a commit on https://github.com/unitycatalog/unitycatalog/commits/main" >&2
    exit 1
  fi
fi

if [[ "$UC_REF" == "main" ]]; then
  git checkout --quiet origin/main
elif [[ "$UC_REF" == "$UC_PIN_SHA" ]]; then
  git checkout --quiet "$UC_PIN_SHA"
else
  # FETCH_HEAD, not $UC_REF: `git fetch origin <tag>` doesn't create a local
  # refs/tags/ entry in a fresh init'd repo, so `git checkout <tag>` fails.
  # FETCH_HEAD works uniformly for branches, tags, and SHAs.
  git checkout --quiet FETCH_HEAD
fi

# Sanity-check UC_BASE_VERSION against what UC actually declares at this commit. If they drift
# (someone bumped UC_PIN_SHA across a UC version.sbt change without also bumping
# UC_BASE_VERSION), the Ivy coordinate wouldn't match what sbt publishes - fail loudly instead of
# silently producing unresolvable coordinates. Skipped in release mode (caller owns the version).
if [[ "$DELTA_RELEASE_MODE" != "1" ]]; then
  ACTUAL_BASE=$(grep 'ThisBuild / version' version.sbt | sed 's/.*:= *"\(.*\)"/\1/')
  if [[ "$ACTUAL_BASE" != "$UC_BASE_VERSION" ]]; then
    echo "ERROR: UC at $UC_REF has version.sbt '$ACTUAL_BASE', but this script pins UC_BASE_VERSION='$UC_BASE_VERSION'." >&2
    echo "Bump UC_BASE_VERSION in this script to match." >&2
    exit 1
  fi
fi

# Override version.sbt via sbt `set` so every publish* command uses the composed <base>-<sha>
# coordinate. Applied as a persistent setting so it sticks across the two sbt invocations below.
SET_VERSION_CMD="set ThisBuild / version := \"$UC_VERSION\""

# Force publishLocal / publishM2 to overwrite existing artifacts. UC artifacts at the same
# coordinate can be left behind from a prior run (e.g. cross-Spark publish re-invokes this
# script for a different sparkVersion while client/server/hadoop are already in ~/.ivy2/local
# and ~/.m2 from the first invocation). publishLocalConfiguration / publishM2Configuration are
# task settings scoped per-project (ThisBuild / Global don't propagate), so we set them on each
# project we publish. Both configs need overriding: publishLocal uses the former, publishM2
# uses the latter.
SET_OVERWRITE_CMDS=()
for p in client server hadoop spark; do
  SET_OVERWRITE_CMDS+=(
    "set $p / publishLocalConfiguration := ($p / publishLocalConfiguration).value.withOverwrite(true)"
    "set $p / publishM2Configuration := ($p / publishM2Configuration).value.withOverwrite(true)"
  )
done

echo ">>> Building and publishing UC client + server to local Maven repo"
# Clear stale UC artifacts — GHA cache may restore jars from a prior run at the same coordinate,
# and SBT's publishM2 refuses to overwrite (ThisBuild / publishM2Configuration is ignored).
# Skip in release mode: fresh runners have no stale artifacts, and multiple Spark versions must
# accumulate (client/server/hadoop published on first invocation, spark on each).
if [[ "$DELTA_RELEASE_MODE" != "1" ]]; then
  rm -rf "$HOME/.ivy2/local/io.unitycatalog" "$HOME/.m2/repository/io/unitycatalog"
fi
./build/sbt \
  "$SET_VERSION_CMD" \
  "${SET_OVERWRITE_CMDS[@]}" \
  "set client / Compile / packageDoc / publishArtifact := false" \
  clean \
  client/generate \
  client/publishLocal \
  client/publishM2 \
  server/publishLocal \
  server/publishM2 \
  hadoop/publishLocal \
  hadoop/publishM2

# Publish the Spark connector for the caller's Spark version. Each CI matrix cell passes its own
# SPARK_VERSION; when auto-triggered by ensurePinnedUnityCatalog (no env), defaults above.
echo ">>> Publishing UC spark connector for Spark $SPARK_VERSION"
for attempt in 1 2 3; do
  if ./build/sbt \
    -DsparkVersion="$SPARK_VERSION" \
    -DskipDeltaSpark=true \
    "$SET_VERSION_CMD" \
    "${SET_OVERWRITE_CMDS[@]}" \
    "set client / Compile / packageDoc / publishArtifact := false" \
    spark/publishLocal \
    spark/publishM2; then
    break
  fi
  if [[ "$attempt" -eq 3 ]]; then
    echo ">>> spark/publishM2 (Spark $SPARK_VERSION) failed after 3 attempts"
    exit 1
  fi
  echo ">>> spark/publishM2 (Spark $SPARK_VERSION) failed on attempt $attempt; retrying..."
  sleep 5
done

echo ">>> Verifying published artifacts"
for c in "${ALL_CANARIES[@]}"; do
  if [[ ! -f "$c" ]]; then
    echo "ERROR: publish succeeded but $c is missing - the publish target layout may have changed." >&2
    exit 1
  fi
done
echo ">>> UC build complete. Published coordinate: $UC_VERSION"
