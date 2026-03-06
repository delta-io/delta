#!/usr/bin/env bash

set -euo pipefail

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"
UC_REPO="${UC_REPO:-https://github.com/TimothyW553/unitycatalog.git}"
UC_REF="${UC_REF:-eea7e881c577439e9b0421d1293df630c36e2ef4}"

rm -rf "$UC_DIR"
git clone "$UC_REPO" "$UC_DIR"
cd "$UC_DIR"
git checkout "$UC_REF"
./build/sbt \
  "set client / Compile / packageDoc / publishArtifact := false" \
  clean \
  client/generate \
  client/publishM2 \
  server/publishM2 \
  spark/publishM2
