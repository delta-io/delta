#!/usr/bin/env bash

#
# Copyright (2026) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Publishes the Unity Catalog snapshot that Delta's DRC integration builds against.
#
# Delta's build.sbt pins `unityCatalogVersion` to a SNAPSHOT that resolves only from
# ~/.ivy2/local. Before the DRC integration stack can compile in clean CI, the UC
# repo must be cloned at a known-stable SHA and publishLocal'd. This script is the
# reproducible source of truth for WHICH UC SHA Delta currently integrates against
# -- do not edit build.sbt's version string without bumping UC_SHA here.
#
# When Yi's shared UC-SHA CI helper lands (tracked in delta-io/delta#6616), this
# script should be replaced by a thin wrapper around it.
#
# Usage:
#   ./dev/publish-uc-snapshot.sh [/path/to/checkout-dir]
#
# Defaults to "$HOME/.cache/delta-uc-snapshot" when no path is supplied.
#

set -euo pipefail

# Pinned UC SHA. Update in lockstep with unityCatalogVersion in Delta's build.sbt.
# Matches the DRC API surface consumed by delta/storage and delta/spark.
UC_SHA="ae4bcf6bf588546593e19e501ca2d68759224991"
UC_EXPECTED_VERSION="0.5.0-SNAPSHOT"
UC_REPO_URL="https://github.com/unitycatalog/unitycatalog.git"
CHECKOUT_DIR="${1:-$HOME/.cache/delta-uc-snapshot}"

echo "==> Ensuring UC checkout at $CHECKOUT_DIR pinned to $UC_SHA"
if [[ ! -d "$CHECKOUT_DIR/.git" ]]; then
  mkdir -p "$(dirname "$CHECKOUT_DIR")"
  git clone "$UC_REPO_URL" "$CHECKOUT_DIR"
fi

(
  cd "$CHECKOUT_DIR"
  git fetch --quiet origin "$UC_SHA" || git fetch --quiet origin
  git checkout --detach "$UC_SHA"
  actual_version="$(grep -E 'ThisBuild / version' version.sbt | sed -E 's/.*"(.*)".*/\1/')"
  if [[ "$actual_version" != "$UC_EXPECTED_VERSION" ]]; then
    echo "ERROR: UC checkout at $UC_SHA declares version $actual_version, expected $UC_EXPECTED_VERSION." >&2
    echo "       If UC bumped its version, update UC_EXPECTED_VERSION and Delta's build.sbt in lockstep." >&2
    exit 1
  fi
  echo "==> Running sbt publishLocal in $CHECKOUT_DIR"
  # System sbt works; the UC-bundled build/sbt sometimes can't download sbt-launch.
  if command -v sbt >/dev/null 2>&1; then
    sbt -mem 3000 publishLocal
  else
    ./build/sbt publishLocal
  fi
)

echo "==> Done. ~/.ivy2/local/io.unitycatalog/ now has $UC_EXPECTED_VERSION for Delta's build.sbt."
