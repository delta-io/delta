package io.delta.flink.source.internal.builder;

import java.util.Collections;
import java.util.List;

import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Builder for {@link RowData} implementation io {@link FormatBuilder}
 */
public class RowDataFormatBuilder implements FormatBuilder<RowData> {

    /**
     * Message prefix for validation exceptions.
     */
    private static final String EXCEPTION_PREFIX = "RowDataFormatBuilder - ";

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
    private List<String> partitionColumns;

    RowDataFormatBuilder(RowType rowType, Configuration hadoopConfiguration) {
        this.rowType = rowType;
        this.hadoopConfiguration = hadoopConfiguration;
        this.partitionColumns = Collections.emptyList();
    }

    /**
     * Set list of partition columns.
     */
    public RowDataFormatBuilder partitionColumns(List<String> partitionColumns) {
        checkNotNull(partitionColumns, EXCEPTION_PREFIX + "partition column list cannot be null.");
        checkArgument(partitionColumns.stream().noneMatch(StringUtils::isNullOrWhitespaceOnly),
            EXCEPTION_PREFIX
                + "List with partition columns contains at least one element that is null, "
                + "empty, or contains only whitespace characters.");

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
            return buildFormatWithoutPartitions();
        } else {
            // TODO PR 11
            throw new UnsupportedOperationException("Partition support will be added later.");
            /* return
                buildPartitionedFormat(columnNames, columnTypes, configuration, partitions,
                    sourceConfiguration);*/
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
}
