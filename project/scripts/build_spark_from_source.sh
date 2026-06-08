#!/usr/bin/env bash
#
# Build a specific Apache Spark commit and publish it to local Maven for Delta tests.
#
# Required:
#   SPARK_COMMIT=<git-sha>
#
# Optional:
#   SPARK_VERSION=<compatibility line, default: 4.2>
#   SPARK_BASE_VERSION=<artifact base version, default: ${SPARK_VERSION}.0>
#   SPARK_ARTIFACT_VERSION=<exact local Maven version to publish>
#   SPARK_REPO=<git URL, default: https://github.com/apache/spark.git>
#   SPARK_DIR=<checkout dir, default: /tmp/spark>
#   SPARK_MVN_PROFILES=<profiles, default: -Pscala-2.13 -Phive -Phive-thriftserver -Pconnect>
#   SPARK_MVN_GOALS=<maven goals, default: install>
#   SPARK_MVN_EXTRA_ARGS=<additional args>
#
set -euo pipefail

SPARK_COMMIT="${SPARK_COMMIT:?SPARK_COMMIT is required}"
SPARK_REPO="${SPARK_REPO:-https://github.com/apache/spark.git}"
SPARK_DIR="${SPARK_DIR:-/tmp/spark}"
SPARK_VERSION="${SPARK_VERSION:-4.2}"
if [[ -z "${SPARK_BASE_VERSION:-}" ]]; then
  if [[ "$SPARK_VERSION" =~ ^[0-9]+\.[0-9]+$ ]]; then
    SPARK_BASE_VERSION="${SPARK_VERSION}.0"
  else
    SPARK_BASE_VERSION="${SPARK_VERSION%-SNAPSHOT}"
  fi
fi
SPARK_SHORT_SHA="$(echo "$SPARK_COMMIT" | tr '[:upper:]' '[:lower:]' | cut -c1-12)"
SPARK_ARTIFACT_VERSION="${SPARK_ARTIFACT_VERSION:-${SPARK_BASE_VERSION}-${SPARK_SHORT_SHA}-SNAPSHOT}"
SPARK_MVN_PROFILES="${SPARK_MVN_PROFILES:--Pscala-2.13 -Phive -Phive-thriftserver -Pconnect}"
SPARK_MVN_GOALS="${SPARK_MVN_GOALS:-install}"
SPARK_MVN_EXTRA_ARGS="${SPARK_MVN_EXTRA_ARGS:-}"

if [[ ! "$SPARK_COMMIT" =~ ^[0-9a-fA-F]{7,40}$ ]]; then
  echo "SPARK_COMMIT must be a 7 to 40 character git SHA: $SPARK_COMMIT" >&2
  exit 1
fi

if [[ -d "$SPARK_DIR/.git" ]]; then
  echo "Using existing Spark checkout at $SPARK_DIR"
  git -C "$SPARK_DIR" fetch "$SPARK_REPO" "$SPARK_COMMIT"
else
  echo "Cloning Spark from $SPARK_REPO into $SPARK_DIR"
  git clone "$SPARK_REPO" "$SPARK_DIR"
  git -C "$SPARK_DIR" fetch "$SPARK_REPO" "$SPARK_COMMIT"
fi

git -C "$SPARK_DIR" checkout --detach "$SPARK_COMMIT"
ACTUAL_SHA="$(git -C "$SPARK_DIR" rev-parse HEAD)"
if [[ "$ACTUAL_SHA" != "$(git -C "$SPARK_DIR" rev-parse "$SPARK_COMMIT^{commit}")" ]]; then
  echo "Checked out $ACTUAL_SHA, expected $SPARK_COMMIT" >&2
  exit 1
fi

echo "Building Spark commit $ACTUAL_SHA as Maven version $SPARK_ARTIFACT_VERSION"
cd "$SPARK_DIR"

# Spark's Maven build uses the revision property for artifact versions.
# shellcheck disable=SC2086
./build/mvn \
  -DskipTests \
  -Drevision="$SPARK_ARTIFACT_VERSION" \
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

emit_output "SPARK_SHA" "$ACTUAL_SHA"
emit_output "SPARK_VERSION" "$SPARK_VERSION"
emit_output "SPARK_ARTIFACT_VERSION" "$SPARK_ARTIFACT_VERSION"
