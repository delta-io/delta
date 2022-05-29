package io.delta.flink.source.internal.utils;

import org.apache.flink.table.types.logical.LogicalType;

/**
 * Schema information about column names and their types that should be read from Delta table.
 */
public class SourceSchema {

    /**
     * Delta table column names to read.
     */
    private final String[] columnNames;

    /**
     * Data types for {@link #columnNames}.
     */
    private final LogicalType[] columnTypes;

    /**
     * Delta table {@link io.delta.standalone.Snapshot} version from which this schema (column
     * names and types) was acquired.
     */
    private final long snapshotVersion;

    public SourceSchema(String[] columnNames, LogicalType[] columnTypes, long snapshotVersion) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.snapshotVersion = snapshotVersion;
    }

    /**
     * @return Delta table column names that should be raed from Delta table row.
     */
    public String[] getColumnNames() {
        return columnNames;
    }

    /**
     * @return An array with {@link LogicalType} objects for column names returned by {@link
     * #getColumnNames()}.
     */
    public LogicalType[] getColumnTypes() {
        return columnTypes;
    }

    /**
     * @return a {@link io.delta.standalone.Snapshot} version for which this schema is valid.
     */
    public long getSnapshotVersion() {
        return snapshotVersion;
    }
}
