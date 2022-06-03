package io.delta.flink.source.internal;

import java.util.Map;

import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.table.filesystem.PartitionFieldExtractor;
import org.apache.flink.table.filesystem.RowPartitionComputer;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * An implementation of Flink's {@link PartitionFieldExtractor} interface for Delta Lake tables.
 * This implementation extracts partition values from {@link DeltaSourceSplit#getPartitionValues()}.
 * The value is converted to proper {@link LogicalType} provided via column type array in Delta
 * Source definition.
 */
public class DeltaPartitionFieldExtractor<SplitT extends DeltaSourceSplit>
    implements PartitionFieldExtractor<SplitT> {

    /**
     * Extracts Delta's partition value
     *
     * @param split     The {@link DeltaSourceSplit} with partition's value map.
     * @param fieldName The name of Delta's partition column.
     * @param fieldType The {@link LogicalType} that partition value should be converted to.
     * @return {@link Object} that is a converted value of Delta's partition column for provided
     * split.
     */
    @Override
    public Object extract(SplitT split, String fieldName, LogicalType fieldType) {
        Map<String, String> partitionValues = split.getPartitionValues();

        sanityCheck(fieldName, partitionValues);

        return RowPartitionComputer.restorePartValueFromType(partitionValues.get(fieldName),
            fieldType);
    }

    private void sanityCheck(String fieldName, Map<String, String> partitionValues) {
        if (tableHasNoPartitions(partitionValues)) {
            throw DeltaSourceExceptions.notPartitionedTableException(fieldName);
        }

        if (isNotAPartitionColumn(fieldName, partitionValues)) {
            throw DeltaSourceExceptions.missingPartitionValueException(
                fieldName,
                partitionValues.keySet()
            );
        }
    }

    private boolean tableHasNoPartitions(Map<String, String> partitionValues) {
        return partitionValues == null || partitionValues.isEmpty();
    }

    private boolean isNotAPartitionColumn(String fieldName, Map<String, String> partitionValues) {
        return !partitionValues.containsKey(fieldName);
    }
}
