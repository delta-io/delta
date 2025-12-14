#!/bin/bash
# Copyright (2024) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Publish kernel locally for root build development
# Usage: ./scripts/dev-kernel.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

echo "=== Delta Kernel Local Development Build ==="
echo ""

cd "${REPO_ROOT}/kernel"

echo "Building and publishing kernel locally..."
sbt publishLocal

echo ""
echo "=== Success ==="
echo "Kernel artifacts published to ~/.ivy2/local/io.delta/"
echo ""
echo "To use with root build:"
echo "  KERNEL_VERSION=1.0.0-SNAPSHOT build/sbt compile"
echo ""
