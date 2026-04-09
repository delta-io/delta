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

full_spark_version="${1:?Usage: install-spark.sh <spark_full_version>}"

if [ -d ~/spark-${full_spark_version}-bin-hadoop3 ]; then
  echo "Spark ${full_spark_version} already present, skipping download."
else
  echo "Downloading Spark ${full_spark_version}..."
  wget -q "https://archive.apache.org/dist/spark/spark-${full_spark_version}/spark-${full_spark_version}-bin-hadoop3.tgz"
  tar xzf "spark-${full_spark_version}-bin-hadoop3.tgz" -C ~/
  rm "spark-${full_spark_version}-bin-hadoop3.tgz"
  echo "Spark ${full_spark_version} installed to ~/spark-${full_spark_version}-bin-hadoop3."
fi
