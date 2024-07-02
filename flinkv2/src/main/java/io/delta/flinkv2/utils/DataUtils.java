package io.delta.flinkv2.utils;

import io.delta.kernel.expressions.Literal;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DataUtils {

    private DataUtils() { }

    public static Map<String, Literal> flinkRowToPartitionValues(
            RowType rowType,
            RowData rowData,
            Set<String> partitionColNames) {
        Map<String, Literal> output = new HashMap<>();

        for (String partitionColName : partitionColNames) {
            final int partitionValueColIdx = rowType.getFieldIndex(partitionColName);
            output.put(partitionColName, flinkDataElementToKernelLiteral(rowType, rowData, partitionValueColIdx));
        }

        return output;
    }

    public static Literal flinkDataElementToKernelLiteral(
            RowType rowType,
            RowData rowData,
            int colIdx) {
        final LogicalTypeRoot flinkType = rowType.getTypeAt(colIdx).getTypeRoot();
        switch(flinkType) {
            case INTEGER:
                return Literal.ofInt(rowData.getInt(colIdx));
            default:
                throw new UnsupportedOperationException(
                    String.format("Type not supported: %s", flinkType)
                );
        }
    }
}
