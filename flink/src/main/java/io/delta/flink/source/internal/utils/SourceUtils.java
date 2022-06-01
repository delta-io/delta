package io.delta.flink.source.internal.utils;

import java.util.Collection;

import io.delta.flink.source.internal.SchemaConverter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.types.logical.LogicalType;
import static org.apache.flink.util.Preconditions.checkArgument;

import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

/**
 * A utility class for Source connector
 */
public final class SourceUtils {

    private SourceUtils() {

    }

    /**
     * Converts Flink's {@link Path} to String
     *
     * @param path Flink's {@link Path}
     * @return String representation of {@link Path}
     */
    public static String pathToString(Path path) {
        checkArgument(path != null, "Path argument cannot be be null.");
        return path.toUri().normalize().toString();
    }

    /**
     * Method to extract schema from Delta's table and convert it to {@link SourceSchema}.
     * <p>
     * In case userColumnNames parameter is defined, this method will extract Type information for
     * every provided column. The created {@link SourceSchema} object will contain only columns
     * defined in userColumnNames parameter.
     * <p>
     * If userColumnNames will be empty or null, then created {@link SourceSchema} will contain all
     * Delta table columns.
     *
     * @param userColumnNames user defined columns that if defined, should be read from Delta
     *                        table.
     * @param logSchema       original Delta table schema
     * @param snapshotVersion a {@link io.delta.standalone.Snapshot} version that this schema is
     *                        valid.
     * @return A {@link SourceSchema} with column names and their {@link LogicalType}.
     */
    public static SourceSchema buildSourceSchema(
            Collection<String> userColumnNames,
            StructType logSchema,
            long snapshotVersion) {

        String[] columnNames;
        LogicalType[] columnTypes;

        if (userColumnNames != null && !userColumnNames.isEmpty()) {
            columnTypes = new LogicalType[userColumnNames.size()];
            int i = 0;
            for (String columnName : userColumnNames) {
                StructField field = logSchema.get(columnName);
                columnTypes[i++] = SchemaConverter.toFlinkDataType(
                    field.getDataType(),
                    field.isNullable());
            }
            columnNames = userColumnNames.toArray(new String[0]);
        } else {
            StructField[] fields = logSchema.getFields();
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

        return new SourceSchema(columnNames, columnTypes, snapshotVersion);
    }

}
