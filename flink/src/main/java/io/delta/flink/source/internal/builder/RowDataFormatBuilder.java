package io.delta.flink.source.internal.builder;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
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

    // TODO PR 9.1 get this from options.
    private static final int BATCH_SIZE = 2048;

    /**
     * An array with Delta table's column names that should be read.
     */
    private final String[] columnNames;

    /**
     * An array of {@link LogicalType} for column names tha should raed from Delta table.
     */
    private final LogicalType[] columnTypes;

    /**
     * An instance of Hadoop configuration used to read Parquet files.
     */
    private final Configuration hadoopConfiguration;

    /**
     * An array with Delta table partition columns.
     */
    private List<String> partitionColumns;

    RowDataFormatBuilder(String[] columnNames,
        LogicalType[] columnTypes, Configuration hadoopConfiguration) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
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
        validateFormat();

        if (partitionColumns.isEmpty()) {
            return buildFormatWithoutPartitions(columnNames, columnTypes, hadoopConfiguration);
        } else {
            // TODO PR 8
            throw new UnsupportedOperationException("Partition support will be added later.");
            /* return
                buildPartitionedFormat(columnNames, columnTypes, configuration, partitions,
                    sourceConfiguration);*/
        }
    }

    private void validateFormat() {
        Validator validator = validateMandatoryOptions();
        if (validator.containsMessages()) {
            // RowDataFormatBuilder does not know Delta's table path,
            // hence null argument in DeltaSourceValidationException
            throw new DeltaSourceValidationException(null, validator.getValidationMessages());
        }
    }

    private RowDataFormat buildFormatWithoutPartitions(
        String[] columnNames,
        LogicalType[] columnTypes,
        Configuration configuration) {

        return new RowDataFormat(
            configuration,
            RowType.of(columnTypes, columnNames),
            BATCH_SIZE,
            PARQUET_UTC_TIMESTAMP,
            PARQUET_CASE_SENSITIVE);
    }

    /**
     * Validates a mandatory options for {@link RowDataFormatBuilder} such as
     * <ul>
     *     <li>null check on arguments</li>
     *     <li>null values in arrays</li>
     *     <li>size mismatch for column name and type arrays.</li>
     * </ul>
     *
     * @return {@link Validator} instance containing validation error messages if any.
     */
    private Validator validateMandatoryOptions() {

        Validator validator = new Validator()
            // validate against null references
            .checkNotNull(columnNames, EXCEPTION_PREFIX + "missing Delta table column names.")
            .checkNotNull(columnTypes, EXCEPTION_PREFIX + "missing Delta table column types.")
            .checkNotNull(hadoopConfiguration, EXCEPTION_PREFIX + "missing Hadoop configuration.");

        if (columnNames != null) {
            validator
                .checkArgument(columnNames.length > 0,
                    EXCEPTION_PREFIX + "empty array with column names.")
                // validate invalid array element
                .checkArgument(Stream.of(columnNames)
                        .noneMatch(StringUtils::isNullOrWhitespaceOnly),
                    EXCEPTION_PREFIX
                        + "Column names array contains at least one element that is null, "
                        + "empty, or contains only whitespace characters.");
        }

        if (columnTypes != null) {
            validator
                .checkArgument(columnTypes.length > 0,
                    EXCEPTION_PREFIX + "empty array with column names.")
                .checkArgument(Stream.of(columnTypes)
                    .noneMatch(Objects::isNull), EXCEPTION_PREFIX + "Column type array contains at "
                    + "least one null element.");
        }

        if (columnNames != null && columnTypes != null) {
            validator
                .checkArgument(columnNames.length == columnTypes.length,
                    EXCEPTION_PREFIX + "column names and column types size does not match.");
        }

        return validator;
    }
}
