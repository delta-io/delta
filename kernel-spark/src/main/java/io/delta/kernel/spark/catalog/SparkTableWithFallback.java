package io.delta.kernel.spark.catalog;

import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;

import java.util.Map;

public class SparkTableWithFallback extends SparkTable {

    private final Table fallback;

    public SparkTableWithFallback(Identifier identifier, String tablePath, Map<String, String> options, Table fallback) {
        super(identifier, tablePath, options);
        this.fallback = fallback;
    }

    public SparkTableWithFallback(Identifier identifier, String tablePath, Table fallback) {
        super(identifier, tablePath);
        this.fallback = fallback;

    }
}
