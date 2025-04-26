package io.delta.table;

import java.util.List;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

public class DeltaPartitionUtil {
    private DeltaPartitionUtil() {}

    // always set the spec ID to 1 because it cannot change in a table. use 0 for the
    // unpartitioned spec
    private static final int SPEC_ID = 1;

    /**
     * Convert a set of partition columns to an Iceberg partition spec.
     *
     * @param schema current table schema
     * @param partitionColumns a list of column names
     * @return an equivalent Iceberg PartitionSpec
     */
    public static PartitionSpec convert(Schema schema, List<String> partitionColumns) {
        if (partitionColumns.isEmpty()) {
            return PartitionSpec.unpartitioned();
        }

        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema).withSpecId(SPEC_ID);

        for (String column : partitionColumns) {
            builder.identity(column);
        }

        return builder.build();
    }
}
