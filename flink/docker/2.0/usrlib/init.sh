#!/usr/bin/env bash
set -e
rm -rf /opt/flink/lib/log4j-*.jar
cp /opt/flink/usrlib/*.jar /opt/flink/lib/
cp /opt/flink/usrlib/core-site.xml /opt/flink/conf/
exec /docker-entrypoint.sh "$@"
