#!/usr/bin/env bash

set -euo pipefail

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/TimothyW553/unitycatalog.git}"
UC_REF="${UC_REF:-7078eb1b9760a89935ef024cc435ae1256cf6229}"
UNITY_CATALOG_VERSION="${UNITY_CATALOG_VERSION:-0.5.0-SNAPSHOT}"

# Keep publishLocal/publishM2 aligned with the shell home directory used by CI.
export SBT_OPTS="${SBT_OPTS:-} -Duser.home=${HOME} -Dsbt.ivy.home=${HOME}/.ivy2 -Dsbt.coursier.home-dir=${HOME}/.cache/coursier"

rm -rf "$UC_DIR"
git clone "$UC_REPO" "$UC_DIR"
cd "$UC_DIR"
git checkout "$UC_REF"

./build/sbt \
  "set client / Compile / packageDoc / publishArtifact := false" \
  clean \
  client/generate \
  client/publishLocal \
  client/publishM2 \
  server/publishLocal \
  server/publishM2

# GitHub Actions occasionally hits a transient coursier structure-lock race while
# fetching the Scala compiler bridge for spark/publishM2. A retry is sufficient
# once the partially downloaded artifacts are in cache.
for attempt in 1 2 3; do
  if ./build/sbt \
    "set client / Compile / packageDoc / publishArtifact := false" \
    spark/publishLocal \
    spark/publishM2; then
    break
  fi

  if [[ "$attempt" -eq 3 ]]; then
    exit 1
  fi

  echo "spark/publishM2 failed on attempt $attempt; retrying after a short backoff"
  sleep 5
done

test -f "${HOME}/.ivy2/local/io.unitycatalog/unitycatalog-client/${UNITY_CATALOG_VERSION}/ivys/ivy.xml"
test -f "${HOME}/.m2/repository/io/unitycatalog/unitycatalog-client/${UNITY_CATALOG_VERSION}/unitycatalog-client-${UNITY_CATALOG_VERSION}.pom"
