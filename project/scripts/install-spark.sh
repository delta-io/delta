#!/bin/sh
# Download and extract a single Spark binary distribution.
# Single source of truth for Spark download logic across CI workflows.
#
# Usage:
#   bash project/scripts/install-spark.sh <spark_full_version>
#
# Arguments:
#   spark_full_version  Full Spark version, e.g. "4.0.1"
#
# The version is extracted to ~/spark-<version>-bin-hadoop3.
set -eu

fullSparkVersion="${1:?Usage: install-spark.sh <spark_full_version>}"

if [ -d ~/spark-${fullSparkVersion}-bin-hadoop3 ]; then
  echo "Spark ${fullSparkVersion} already present, skipping download."
else
  echo "Downloading Spark ${fullSparkVersion}..."
  wget -q "https://archive.apache.org/dist/spark/spark-${fullSparkVersion}/spark-${fullSparkVersion}-bin-hadoop3.tgz"
  tar xzf "spark-${fullSparkVersion}-bin-hadoop3.tgz" -C ~/
  rm "spark-${fullSparkVersion}-bin-hadoop3.tgz"
  echo "Spark ${fullSparkVersion} installed to ~/spark-${fullSparkVersion}-bin-hadoop3."
fi
