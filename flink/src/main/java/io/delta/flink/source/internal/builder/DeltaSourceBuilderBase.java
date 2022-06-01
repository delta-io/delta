package io.delta.flink.source.internal.builder;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.enumerator.supplier.SnapshotSupplier;
import io.delta.flink.source.internal.enumerator.supplier.SnapshotSupplierFactory;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceSchema;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.StringUtils;
import org.apache.hadoop.conf.Configuration;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.types.StructType;

/**
 * The base class for {@link io.delta.flink.source.DeltaSource} builder.
 * <p>
 * This builder carries a <i>SELF</i> type to make it convenient to extend this for subclasses,
 * using the following pattern.
 *
 * <pre>{@code
 * public class SubBuilder<T> extends DeltaSourceBuilderBase<T, SubBuilder<T>> {
 *     ...
 * }
 * }</pre>
 *
 * <p>That way, all return values from builder method defined here are typed to the sub-class
 * type and support fluent chaining.
 *
 * <p>We don't make the publicly visible builder generic with a SELF type, because it leads to
 * generic signatures that can look complicated and confusing.
 *
 * @param <T> A type that this source produces.
 */
public abstract class DeltaSourceBuilderBase<T, SELF> {

    /**
     * The provider for {@link FileSplitAssigner}.
     */
    protected static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
        LocalityAwareSplitAssigner::new;

    /**
     * The provider for {@link AddFileEnumerator}.
     */
    protected static final AddFileEnumerator.Provider<DeltaSourceSplit>
        DEFAULT_SPLITTABLE_FILE_ENUMERATOR = DeltaFileEnumerator::new;

    /**
     * Default reference value for column names list.
     */
    protected static final List<String> DEFAULT_COLUMNS = new ArrayList<>(0);

    /**
     * Message prefix for validation exceptions.
     */
    protected static final String EXCEPTION_PREFIX = "DeltaSourceBuilder - ";

    /**
     * A placeholder object for Delta source configuration used for {@link DeltaSourceBuilderBase}
     * instance.
     */
    protected final DeltaSourceConfiguration sourceConfiguration = new DeltaSourceConfiguration();
    /**
     * A {@link Path} to Delta table that should be read by created {@link
     * io.delta.flink.source.DeltaSource}.
     */
    protected final Path tablePath;
    /**
     * The Hadoop's {@link Configuration} for this Source.
     */
    protected final Configuration hadoopConfiguration;

    protected final SnapshotSupplierFactory snapshotSupplierFactory;

    /**
     * An array with Delta table's column names that should be read.
     */
    protected List<String> userColumnNames;

    protected DeltaSourceBuilderBase(
            Path tablePath,
            Configuration hadoopConfiguration,
            SnapshotSupplierFactory snapshotSupplierFactory) {
        this.tablePath = tablePath;
        this.hadoopConfiguration = hadoopConfiguration;
        this.snapshotSupplierFactory = snapshotSupplierFactory;
        this.userColumnNames = DEFAULT_COLUMNS;
    }

    /**
     * Sets a {@link List} of column names that should be read from Delta table.
     */
    public SELF columnNames(List<String> columnNames) {
        this.userColumnNames = columnNames;
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, String optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, boolean optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, int optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    /**
     * Sets a configuration option.
     */
    public SELF option(String optionName, long optionValue) {
        ConfigOption<?> configOption = validateOptionName(optionName);
        sourceConfiguration.addOption(configOption.key(), optionValue);
        return self();
    }

    public abstract <V extends DeltaSource<T>> V build();

    /**
     * This method should implement any logic for validation of mutually exclusive options.
     *
     * @return {@link Validator} instance with validation error message.
     */
    protected abstract Validator validateOptionExclusions();

    /**
     * Validate definition of Delta source builder including mandatory and optional options.
     */
    protected void validate() {
        Validator mandatoryValidator = validateMandatoryOptions();
        Validator exclusionsValidator = validateOptionExclusions();
        Validator optionalValidator = validateOptionalParameters();

        List<String> validationMessages = new LinkedList<>();

        validationMessages.addAll(mandatoryValidator.getValidationMessages());
        validationMessages.addAll(exclusionsValidator.getValidationMessages());
        validationMessages.addAll(optionalValidator.getValidationMessages());

        if (!validationMessages.isEmpty()) {
            String tablePathString =
                (tablePath != null) ? SourceUtils.pathToString(tablePath) : "null";
            throw new DeltaSourceValidationException(tablePathString, validationMessages);
        }
    }

    protected Validator validateMandatoryOptions() {

        return new Validator()
            // validate against null references
            .checkNotNull(tablePath, EXCEPTION_PREFIX + "missing path to Delta table.")
            .checkNotNull(hadoopConfiguration, EXCEPTION_PREFIX + "missing Hadoop configuration.");
    }

    protected Validator validateOptionalParameters() {
        Validator validator = new Validator();

        if (userColumnNames != DEFAULT_COLUMNS) {
            validator.checkNotNull(userColumnNames,
                EXCEPTION_PREFIX + "used a null reference for user columns.");

            if (userColumnNames != null) {
                validator.checkArgument(!userColumnNames.isEmpty(),
                    EXCEPTION_PREFIX + "user column names list is empty.");
                if (!userColumnNames.isEmpty()) {
                    validator.checkArgument(
                        userColumnNames.stream().noneMatch(StringUtils::isNullOrWhitespaceOnly),
                        EXCEPTION_PREFIX
                            + "user column names list contains at least one element that is null, "
                            + "empty, or has only whitespace characters.");
                }
            }
        }

        return validator;
    }

    protected String prepareOptionExclusionMessage(String... mutualExclusiveOptions) {
        return String.format(
            "Used mutually exclusive options for Source definition. Invalid options [%s]",
            String.join(",", mutualExclusiveOptions));
    }

    // TODO Refactor Option name validation in PR 12
    protected ConfigOption<?> validateOptionName(String optionName) {
        ConfigOption<?> option = DeltaSourceOptions.USER_FACING_SOURCE_OPTIONS.get(optionName);
        if (option == null) {
            throw DeltaSourceExceptions.invalidOptionNameException(
                SourceUtils.pathToString(tablePath), optionName);
        }
        return option;
    }

    /**
     * Extracts Delta table schema from DeltaLog {@link io.delta.standalone.actions.Metadata}
     * including column names and column types converted to
     * {@link org.apache.flink.table.types.logical.LogicalType}.
     * <p>
     * If {@link #userColumnNames} were defined, only those columns will be included in extracted
     * schema.
     *
     * @return A {@link SourceSchema} including Delta table column names with their types that
     * should be read from Delta table.
     */
    protected SourceSchema getSourceSchema() {
        DeltaLog deltaLog =
            DeltaLog.forTable(hadoopConfiguration, SourceUtils.pathToString(tablePath));
        SnapshotSupplier snapshotSupplier = snapshotSupplierFactory.create(deltaLog);
        Snapshot snapshot = snapshotSupplier.getSnapshot(sourceConfiguration);

        StructType tableSchema = snapshot.getMetadata().getSchema();
        if (tableSchema == null) {
            throw DeltaSourceExceptions.tableSchemaMissingException(
                SourceUtils.pathToString(tablePath), snapshot.getVersion());
        }
        try {
            return SourceUtils.buildSourceSchema(userColumnNames, tableSchema,
                snapshot.getVersion());
        } catch (IllegalArgumentException e) {
            throw DeltaSourceExceptions.generalSourceException(
                SourceUtils.pathToString(tablePath),
                snapshot.getVersion(),
                e
            );
        }
    }

    @SuppressWarnings("unchecked")
    protected SELF self() {
        return (SELF) this;
    }
}
