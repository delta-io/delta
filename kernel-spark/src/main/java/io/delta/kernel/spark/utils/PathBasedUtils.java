package io.delta.kernel.spark.utils;

import io.delta.kernel.Snapshot;
import io.delta.kernel.TableManager;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.spark.catalog.SparkTableWithFallback;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

public class PathBasedUtils {
    public static Table wrapWithDsv2Table(Identifier identifier, String path, Table fallback) {
        return new SparkTableWithFallback(identifier, path, fallback);
    }
}
