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
# To add a new version, append its SHA512 from:
#   https://archive.apache.org/dist/spark/spark-<version>/spark-<version>-bin-hadoop3.tgz.sha512
set -eu

full_spark_version="${1:?Usage: install-spark.sh <spark_full_version>}"

# SHA512 checksums from official Apache release assets.
case "$full_spark_version" in
  4.0.1)
    TARBALL_SHA512="9198602c6b931b46686f32a25793b3bb58b522cd98a5b6a94d2484bae32e3e7b520d60f4bffe72ba29ff5c9ecd862443841ee47dde0f2f9e1bf52539f7baef41"
    ;;
  4.1.0)
    TARBALL_SHA512="fff7f929d98779b096a2d2395b1b9db1ce277660f3852dd45e1457c373013f2669074315252181a3cea291d8f3a726c70f7f9b247e723edbf8080f40888edde1"
    ;;
  *)
    echo "ERROR: No hardened SHA512 for Spark ${full_spark_version}." >&2
    echo "Add the checksum from: https://archive.apache.org/dist/spark/spark-${full_spark_version}/spark-${full_spark_version}-bin-hadoop3.tgz.sha512" >&2
    exit 1
    ;;
esac

if [ -d ~/spark-${full_spark_version}-bin-hadoop3 ]; then
  echo "Spark ${full_spark_version} already present, skipping download."
else
  echo "Downloading Spark ${full_spark_version}..."
  TARBALL="spark-${full_spark_version}-bin-hadoop3.tgz"
  wget -q "https://archive.apache.org/dist/spark/spark-${full_spark_version}/${TARBALL}"
  echo "${TARBALL_SHA512}  ${TARBALL}" | sha512sum -c -
  tar xzf "${TARBALL}" -C ~/
  rm "${TARBALL}"
  echo "Spark ${full_spark_version} installed to ~/spark-${full_spark_version}-bin-hadoop3."
fi
