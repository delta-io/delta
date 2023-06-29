package io.delta.flink.source.internal.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.internal.SchemaConverter;
import org.apache.flink.table.types.logical.LogicalType;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

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

    /**
     * {@link List} with names of partition columns. If empty, then no partition columns were found
     * for given schema version.
     */
    private final List<String> partitionColumns;

    private SourceSchema(
        String[] columnNames,
        LogicalType[] columnTypes,
        long snapshotVersion,
        Collection<String> partitionColumns) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.snapshotVersion = snapshotVersion;
        this.partitionColumns = new ArrayList<>(partitionColumns);
    }

    /**
     * Creates {@link SourceSchema} instance using {@link Metadata} information from Delta's
     * {@link Snapshot}.
     * <p>
     * In case {@code userColumnNames} parameter is defined, this method will extract type
     * information for every provided column. The created {@link SourceSchema} object will
     * contain only columns defined in userColumnNames parameter.
     * <p>
     * If userColumnNames will be empty or null, then created {@link SourceSchema} will contain all
     * table columns from Snapshot's metadata.
     *
     * @param userColumnNames user defined columns that if defined, should be read from Delta
     *                        table.
     * @param snapshot {@link Snapshot} to extract schema information from.
     * @return A {@link SourceSchema} with column names and their {@link LogicalType}.
     */
    public static SourceSchema fromSnapshot(
            Collection<String> userColumnNames,
            Snapshot snapshot) {

        String[] columnNames;
        LogicalType[] columnTypes;

        Metadata metadata = snapshot.getMetadata();
        StructType tableSchema = metadata.getSchema();

        if (tableSchema == null) {
            throw new IllegalArgumentException(
                String.format(
                    "Unable to find Schema information in Delta log for Snapshot version [%d]",
                    snapshot.getVersion()
            ));
        }

        if (userColumnNames != null && !userColumnNames.isEmpty()) {
            columnTypes = new LogicalType[userColumnNames.size()];
            int i = 0;
            for (String columnName : userColumnNames) {
                StructField field = tableSchema.get(columnName);
                columnTypes[i++] = SchemaConverter.toFlinkDataType(
                    field.getDataType(),
                    field.isNullable());
            }
            columnNames = userColumnNames.toArray(new String[0]);
        } else {
            StructField[] fields = tableSchema.getFields();
            columnNames = new String[fields.length];
            columnTypes = new LogicalType[fields.length];
            int i = 0;
            for (StructField field : fields) {
                columnNames[i] = field.getName();
                columnTypes[i] = SchemaConverter.toFlinkDataType(field.getDataType(),
                    field.isNullable());
                i++;
            }
        }

        return new SourceSchema(
            columnNames,
            columnTypes,
            snapshot.getVersion(),
            metadata.getPartitionColumns()
        );
    }

    public List<String> getPartitionColumns() {
        return Collections.unmodifiableList(partitionColumns);
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
