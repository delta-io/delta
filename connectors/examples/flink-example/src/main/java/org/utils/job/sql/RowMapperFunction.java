package org.utils.job.sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

/**
 * A Helper Mapper function to convert Stream of Row element to stream of RowDataElements.
 */
public class RowMapperFunction implements MapFunction<Row, RowData> {

    private final DataFormatConverters.DataFormatConverter<RowData, Row> converter;

    public RowMapperFunction(LogicalType logicalType) {
        this.converter =
            DataFormatConverters.getConverterForDataType(
                TypeConversions.fromLogicalToDataType(logicalType)
            );
    }

    @Override
    public RowData map(Row value) {
        return converter.toInternal(value);
    }
}
