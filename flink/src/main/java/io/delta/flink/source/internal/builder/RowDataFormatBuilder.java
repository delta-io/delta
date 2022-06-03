package io.delta.flink.source.internal.builder;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.delta.flink.source.internal.DeltaPartitionFieldExtractor;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.formats.parquet.vector.ColumnBatchFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builder for {@link RowData} implementation io {@link FormatBuilder}
 */
public class RowDataFormatBuilder implements FormatBuilder<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(RowDataFormatBuilder.class);

    // -------------- Hardcoded Non Public Options ----------
    /**
     * Hardcoded option for {@link RowDataFormat} to threat timestamps as a UTC timestamps.
     */
    private static final boolean PARQUET_UTC_TIMESTAMP = true;

    /**
     * Hardcoded option for {@link RowDataFormat} to use case-sensitive in column name processing
     * for Parquet files.
     */
    private static final boolean PARQUET_CASE_SENSITIVE = true;
    // ------------------------------------------------------

    // TODO PR 12 get this from options.
    private static final int BATCH_SIZE = 2048;

    private final RowType rowType;

    /**
     * An instance of Hadoop configuration used to read Parquet files.
     */
    private final Configuration hadoopConfiguration;

    /**
     * An array with Delta table partition columns.
     */
    private List<String> partitionColumns; // partitionColumn is validated in DeltaSourceBuilder.

    RowDataFormatBuilder(RowType rowType, Configuration hadoopConfiguration) {
        this.rowType = rowType;
        this.hadoopConfiguration = hadoopConfiguration;
        this.partitionColumns = Collections.emptyList();
    }

    @Override
    public RowDataFormatBuilder partitionColumns(List<String> partitionColumns) {
        this.partitionColumns = partitionColumns;
        return this;
    }

    /**
     * Creates an instance of {@link RowDataFormat}.
     *
     * @throws DeltaSourceValidationException if invalid arguments were passed to {@link
     *                                        RowDataFormatBuilder}. For example null arguments.
     */
    public RowDataFormat build() {

        if (partitionColumns.isEmpty()) {
            LOG.info("Building format data for non-partitioned Delta table.");
            return buildFormatWithoutPartitions();
        } else {
            LOG.info("Building format data for partitioned Delta table.");
            return
                buildFormatWithPartitionColumns(
                    rowType,
                    hadoopConfiguration,
                    partitionColumns
                );
        }
    }

    private RowDataFormat buildFormatWithoutPartitions() {

        return new RowDataFormat(
            hadoopConfiguration,
            rowType,
            BATCH_SIZE,
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    private RowDataFormat buildFormatWithPartitionColumns(
        RowType producedRowType,
        Configuration hadoopConfig,
        List<String> partitionColumns) {

        RowType projectedRowType =
            new RowType(
                producedRowType.getFields().stream()
                    .filter(field -> !partitionColumns.contains(field.getName()))
                    .collect(Collectors.toList()));

        List<String> projectedNames = projectedRowType.getFieldNames();

        ColumnBatchFactory<DeltaSourceSplit> factory =
            RowBuilderUtils.createPartitionedColumnFactory(
                producedRowType,
                projectedNames,
                partitionColumns,
                new DeltaPartitionFieldExtractor<>(),
                BATCH_SIZE);

        return new RowDataFormat(
            hadoopConfig,
            projectedRowType,
            producedRowType,
            factory,
            BATCH_SIZE,
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }
}
