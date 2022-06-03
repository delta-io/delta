package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.formats.parquet.ParquetColumnarRowInputFormat;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;

/**
 * Implementation of {@link DeltaBulkFormat} for {@link RowData} type.
 */
public class RowDataFormat extends ParquetColumnarRowInputFormat<DeltaSourceSplit>
    implements DeltaBulkFormat<RowData> {

    // Making this constructor package protected to make sure that only RowDataFormatBuilder can
    // initialize object of this class.
    RowDataFormat(
        Configuration hadoopConfig,
        RowType projectedType,
        int batchSize,
        boolean isUtcTimestamp,
        boolean isCaseSensitive) {
        super(hadoopConfig, projectedType, batchSize, isUtcTimestamp, isCaseSensitive);
    }

    RowDataFormat(
        Configuration hadoopConfig,
        RowType projectedRowType,
        RowType producedRowType,
        ColumnBatchFactory<DeltaSourceSplit> factory,
        int batchSize,
        boolean parquetUtcTimestamp,
        boolean parquetCaseSensitive) {
        super(hadoopConfig, projectedRowType, producedRowType, factory, batchSize,
            parquetUtcTimestamp, parquetCaseSensitive);
    }

    public static RowDataFormatBuilder builder(RowType rowType, Configuration hadoopConfiguration) {
        return new RowDataFormatBuilder(rowType, hadoopConfiguration);
    }

}
