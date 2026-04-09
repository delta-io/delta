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

FULL="${1:?Usage: install-spark.sh <spark_full_version>}"

if [ -d ~/spark-${FULL}-bin-hadoop3 ]; then
  echo "Spark ${FULL} already present, skipping download."
else
  echo "Downloading Spark ${FULL}..."
  wget -q "https://archive.apache.org/dist/spark/spark-${FULL}/spark-${FULL}-bin-hadoop3.tgz"
  tar xzf "spark-${FULL}-bin-hadoop3.tgz" -C ~/
  rm "spark-${FULL}-bin-hadoop3.tgz"
  echo "Spark ${FULL} installed to ~/spark-${FULL}-bin-hadoop3."
fi
