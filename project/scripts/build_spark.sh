#!/usr/bin/env bash
#
# Build an Apache Spark source ref and publish it to local Maven for Delta tests.
#
# Optional:
#   SPARK_SOURCE_REF=<git-ref-or-sha, default: master>
#   SPARK_COMMIT=<git-sha> compatibility alias for SPARK_SOURCE_REF
#   SPARK_VERSION=<compatibility line, default: 4.2>
#   SPARK_BASE_VERSION=<artifact base version; default read from Spark version.sbt>
#   SPARK_ARTIFACT_VERSION=<exact local Maven version to publish>
#   SPARK_REPO=<git URL, default: https://github.com/apache/spark.git>
#   SPARK_DIR=<checkout dir, default: /tmp/spark>
#   SPARK_MVN_PROFILES=<profiles, default: -Pscala-2.13 -Phive -Phive-thriftserver -Pconnect>
#   SPARK_MVN_GOALS=<maven goals, default: install>
#   SPARK_MVN_EXTRA_ARGS=<additional args>
#
set -euo pipefail

SPARK_SOURCE_REF="${SPARK_SOURCE_REF:-${SPARK_COMMIT:-master}}"
SPARK_REPO="${SPARK_REPO:-https://github.com/apache/spark.git}"
SPARK_DIR="${SPARK_DIR:-/tmp/spark}"
SPARK_VERSION="${SPARK_VERSION:-4.2}"
SPARK_MVN_PROFILES="${SPARK_MVN_PROFILES:--Pscala-2.13 -Phive -Phive-thriftserver -Pconnect}"
SPARK_MVN_GOALS="${SPARK_MVN_GOALS:-install}"
SPARK_MVN_EXTRA_ARGS="${SPARK_MVN_EXTRA_ARGS:-}"

if [[ -d "$SPARK_DIR/.git" ]]; then
  echo "Using existing Spark checkout at $SPARK_DIR"
else
  echo "Initializing Spark checkout at $SPARK_DIR"
  mkdir -p "$SPARK_DIR"
  git -C "$SPARK_DIR" init
fi

if ! git -C "$SPARK_DIR" remote get-url origin > /dev/null 2>&1; then
  git -C "$SPARK_DIR" remote add origin "$SPARK_REPO"
else
  git -C "$SPARK_DIR" remote set-url origin "$SPARK_REPO"
fi

git -C "$SPARK_DIR" fetch --depth 1 origin "$SPARK_SOURCE_REF"
git -C "$SPARK_DIR" checkout --detach FETCH_HEAD
ACTUAL_SHA="$(git -C "$SPARK_DIR" rev-parse HEAD)"
SPARK_SHORT_SHA="$(echo "$ACTUAL_SHA" | cut -c1-12)"

cd "$SPARK_DIR"

read_spark_base_version() {
  python3 - <<'PY'
import re
from pathlib import Path

version_file = Path("version.sbt")
if not version_file.exists():
    raise SystemExit(1)

match = re.search(r'\bversion\s*:?=\s*"([^"]+)"', version_file.read_text())
if not match:
    raise SystemExit(1)

print(match.group(1).removesuffix("-SNAPSHOT"))
PY
}

if [[ -z "${SPARK_BASE_VERSION:-}" ]]; then
  if SPARK_BASE_VERSION="$(read_spark_base_version)"; then
    :
  elif [[ "$SPARK_VERSION" =~ ^[0-9]+\.[0-9]+$ ]]; then
    SPARK_BASE_VERSION="${SPARK_VERSION}.0"
  else
    SPARK_BASE_VERSION="${SPARK_VERSION%-SNAPSHOT}"
  fi
fi

SPARK_ARTIFACT_VERSION="${SPARK_ARTIFACT_VERSION:-${SPARK_BASE_VERSION}-${SPARK_SHORT_SHA}-SNAPSHOT}"

echo "Building Spark ref $SPARK_SOURCE_REF ($ACTUAL_SHA) as Maven version $SPARK_ARTIFACT_VERSION"

# Spark's Maven build does not use the `revision` property for its own module
# versions, so update the checked-out POMs before publishing the local artifacts.
# shellcheck disable=SC2086
./build/mvn \
  $SPARK_MVN_EXTRA_ARGS \
  versions:set \
  -DnewVersion="$SPARK_ARTIFACT_VERSION" \
  -DgenerateBackupPoms=false

# shellcheck disable=SC2086
./build/mvn \
  -DskipTests \
  -Dscala.version=2.13.17 \
  $SPARK_MVN_PROFILES \
  $SPARK_MVN_EXTRA_ARGS \
  $SPARK_MVN_GOALS

emit_output() {
  local key="$1" value="$2"
  if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
    echo "$key=$value" >> "$GITHUB_OUTPUT"
  fi
  echo "$key=$value"
}

emit_output "SPARK_SOURCE_REF" "$SPARK_SOURCE_REF"
emit_output "SPARK_SHA" "$ACTUAL_SHA"
emit_output "SPARK_VERSION" "$SPARK_VERSION"
emit_output "SPARK_BASE_VERSION" "$SPARK_BASE_VERSION"
emit_output "SPARK_ARTIFACT_VERSION" "$SPARK_ARTIFACT_VERSION"
