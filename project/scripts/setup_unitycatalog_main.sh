#!/usr/bin/env bash

set -euo pipefail

UC_DIR="${UC_DIR:-/tmp/unitycatalog}"

rm -rf "$UC_DIR"
git clone https://github.com/unitycatalog/unitycatalog.git "$UC_DIR"
cd "$UC_DIR"
git checkout main
./build/sbt clean package publishM2
