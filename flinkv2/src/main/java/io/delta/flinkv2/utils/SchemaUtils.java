package io.delta.flinkv2.utils;

import io.delta.kernel.types.*;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

public class SchemaUtils {
    private SchemaUtils() { }

    public static StructType toDeltaDataType(RowType rowType) {
        List<StructField> fields = rowType.getFields()
            .stream()
            .map(rowField -> {
                DataType rowFieldType = toDeltaDataType(rowField.getType());
                return new StructField(
                    rowField.getName(),
                    rowFieldType,
                    rowField.getType().isNullable());
            })
            .collect(Collectors.toList());

        return new StructType(fields);
    }

    public static DataType toDeltaDataType(LogicalType flinkType) {
        switch (flinkType.getTypeRoot()) {
            case BIGINT:
                return LongType.LONG;
            case INTEGER:
                return IntegerType.INTEGER;
            default:
                throw new UnsupportedOperationException(
                    "Type not supported: " + flinkType);
        }
    }
}
