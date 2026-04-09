#!/bin/sh
# Download and extract Spark binary distributions.
# Single source of truth for Spark download logic across CI workflows.
#
# Usage:
#   bash project/scripts/install-spark.sh <spark_full_version_list_json>
#
# Arguments:
#   spark_full_version_list_json  JSON array of full Spark versions to install,
#                                 e.g. '["3.5.4","4.0.0"]'
#
# Each version is extracted to ~/spark-<version>-bin-hadoop3 and skipped if
# that directory already exists (cache-friendly).
set -eu

SPARK_FULL_VERSION_LIST="${1:?Usage: install-spark.sh <spark_full_version_list_json>}"

for FULL in $(echo "$SPARK_FULL_VERSION_LIST" | jq -r '.[]'); do
  if [ ! -d ~/spark-${FULL}-bin-hadoop3 ]; then
    echo "Downloading Spark ${FULL}..."
    wget -q "https://archive.apache.org/dist/spark/spark-${FULL}/spark-${FULL}-bin-hadoop3.tgz"
    tar xzf "spark-${FULL}-bin-hadoop3.tgz" -C ~/
    rm "spark-${FULL}-bin-hadoop3.tgz"
  else
    echo "Spark ${FULL} already present, skipping download."
  fi
done
