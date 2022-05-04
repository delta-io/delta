package io.delta.flink.source.internal.builder;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import io.delta.flink.source.DeltaSource;
import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceExceptions;
import io.delta.flink.source.internal.exceptions.DeltaSourceValidationException;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.file.DeltaFileEnumerator;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;

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
     * An instance of {@link FormatBuilder} that will be used to build {@link DeltaBulkFormat}
     * instance.
     */
    protected final FormatBuilder<T> formatBuilder;

    /**
     * The Hadoop's {@link Configuration} for this Source.
     */
    protected final Configuration hadoopConfiguration;

    protected DeltaSourceBuilderBase(
            Path tablePath,
            FormatBuilder<T> formatBuilder,
            Configuration hadoopConfiguration) {
        this.tablePath = tablePath;
        this.formatBuilder = formatBuilder;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    public SELF partitionColumns(List<String> partitions) {
        formatBuilder.partitionColumns(partitions);
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
     * Validates {@link FormatBuilder} and returns new instance of {@link DeltaBulkFormat}.
     *
     * @return {@link DeltaBulkFormat} instance.
     * @throws DeltaSourceValidationException if {@link FormatBuilder} definition has anny
     *                                        validation issues.
     */
    protected DeltaBulkFormat<T> validateSourceAndFormat() {
        DeltaBulkFormat<T> format = null;
        Collection<String> formatValidationMessages = Collections.emptySet();
        try {
            format = formatBuilder.build();
        } catch (DeltaSourceValidationException e) {
            formatValidationMessages = e.getValidationMessages();
        }
        validateSource(formatValidationMessages);
        return format;
    }

    /**
     * Validate definition of Delta source builder including mandatory and optional options.
     *
     * @param extraValidationMessages other validation messages that should be included in this
     *                                validation check. If collection is not empty, the {@link
     *                                DeltaSourceValidationException} will be thrown.
     */
    protected void validateSource(Collection<String> extraValidationMessages) {
        Validator mandatoryValidator = validateMandatoryOptions();
        Validator exclusionsValidator = validateOptionExclusions();

        List<String> validationMessages = new LinkedList<>();
        if (mandatoryValidator.containsMessages() || exclusionsValidator.containsMessages()) {

            validationMessages.addAll(mandatoryValidator.getValidationMessages());
            validationMessages.addAll(exclusionsValidator.getValidationMessages());
        }

        if (extraValidationMessages != null) {
            validationMessages.addAll(extraValidationMessages);
        }

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

    protected String prepareOptionExclusionMessage(String... mutualExclusiveOptions) {
        return String.format(
            "Used mutually exclusive options for Source definition. Invalid options [%s]",
            String.join(",", mutualExclusiveOptions));
    }

    // TODO Refactor Option name validation in PR 9.1
    protected ConfigOption<?> validateOptionName(String optionName) {
        ConfigOption<?> option = DeltaSourceOptions.VALID_SOURCE_OPTIONS.get(optionName);
        if (option == null) {
            throw DeltaSourceExceptions.invalidOptionNameException(
                SourceUtils.pathToString(tablePath), optionName);
        }
        return option;
    }

    @SuppressWarnings("unchecked")
    protected SELF self() {
        return (SELF) this;
    }
}
