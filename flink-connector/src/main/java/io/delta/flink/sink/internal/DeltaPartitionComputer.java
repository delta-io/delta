package io.delta.flink.sink.internal;

import java.io.Serializable;
import java.util.LinkedHashMap;

import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

public interface DeltaPartitionComputer<T> extends Serializable {

    /**
     * Compute partition values from record.
     * <p>
     * E.g.
     * If the table has two partitioning columns 'date' and 'country' then this method should
     * return linked hashmap like:
     * LinkedHashMap(
     *     "date" -&gt; "2020-01-01",
     *     "country" -&gt; "x"
     * )
     * <p>
     * for event that should be written to example path of:
     * '/some_path/table_1/date=2020-01-01/country=x'.
     *
     * @param element input record.
     * @param context {@link BucketAssigner.Context} that can be used during partition's
     *                assignment
     * @return partition values.
     */
    LinkedHashMap<String, String> generatePartitionValues(
        T element, BucketAssigner.Context context);

    /**
     * Implementation of {@link DeltaPartitionComputer} for stream of {@link RowData} elements.
     * <p>
     * This partition computer resolves partition values by extracting them from element's fields
     * by provided partitions' names. This behaviour can be overridden by providing static values
     * for partitions' fields.
     */
    class DeltaRowDataPartitionComputer implements DeltaPartitionComputer<RowData> {

        private final LinkedHashMap<String, String> staticPartitionSpec;
        private final RowType rowType;
        String[] partitionColumns;

        /**
         * Creates instance of partition computer for {@link RowData}
         *
         * @param rowType       logical schema of the records in the stream/table
         * @param partitionColumns list of partition column names in the order they should be
         *                         applied when creating a destination path
         */
        public DeltaRowDataPartitionComputer(RowType rowType,
                                             String[] partitionColumns) {
            this(rowType, partitionColumns, new LinkedHashMap<>());
        }

        /**
         * Creates instance of partition computer for {@link RowData}
         *
         * @param rowType             logical schema of the records in the stream/table
         * @param partitionColumns       list of partition column names in the order they should be
         *                            applied when creating a destination path
         * @param staticPartitionSpec static values for partitions that should set explicitly
         *                            instead of being derived from the content of the records
         */
        public DeltaRowDataPartitionComputer(RowType rowType,
                                             String[] partitionColumns,
                                             LinkedHashMap<String, String> staticPartitionSpec) {
            this.rowType = rowType;
            this.partitionColumns = partitionColumns;
            this.staticPartitionSpec = staticPartitionSpec;
        }

        @Override
        public LinkedHashMap<String, String> generatePartitionValues(
            RowData element,
            BucketAssigner.Context context) {
            LinkedHashMap<String, String> partitionValues = new LinkedHashMap<>();

            for (String partitionKey : partitionColumns) {
                int keyIndex = rowType.getFieldIndex(partitionKey);
                LogicalType keyType = rowType.getTypeAt(keyIndex);

                if (staticPartitionSpec.containsKey(partitionKey)) {
                    // We want the output partition values to be String's anyways, so no need
                    // to parse/cast the staticPartitionSpec value
                    partitionValues.put(partitionKey, staticPartitionSpec.get(partitionKey));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
                    partitionValues.put(partitionKey, element.getString(keyIndex).toString());
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.INTEGER) {
                    partitionValues.put(partitionKey, String.valueOf(element.getInt(keyIndex)));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.BIGINT) {
                    partitionValues.put(partitionKey, String.valueOf(element.getLong(keyIndex)));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.SMALLINT) {
                    partitionValues.put(partitionKey, String.valueOf(element.getShort(keyIndex)));
                } else if (keyType.getTypeRoot() == LogicalTypeRoot.TINYINT) {
                    partitionValues.put(partitionKey, String.valueOf(element.getByte(keyIndex)));
                } else {
                    throw new RuntimeException("Type not supported " + keyType.getTypeRoot());
                }
            }
            return partitionValues;
        }
    }
}
