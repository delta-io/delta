package io.delta.flink.source.internal;

import java.util.HashMap;
import java.util.Map;

import io.delta.flink.source.internal.builder.BooleanOptionTypeConverter;
import io.delta.flink.source.internal.builder.DeltaConfigOption;
import io.delta.flink.source.internal.builder.NonNegativeNumberTypeConverter;
import io.delta.flink.source.internal.builder.StartingVersionOptionTypeConverter;
import io.delta.flink.source.internal.builder.TimestampOptionTypeConverter;
import org.apache.flink.configuration.ConfigOptions;

/**
 * This class contains all available options for {@link io.delta.flink.source.DeltaSource} with
 * their type and default values. It may be viewed as a kind of dictionary class. This class will be
 * used both by Streaming and Table source.
 *
 * @implNote This class is used as a dictionary to work with {@link DeltaSourceConfiguration} class
 * that contains an actual configuration options used for particular {@code DeltaSourceInternal}
 * instance.
 */
public class DeltaSourceOptions {

    /**
     * The constant that represents a value for {@link #STARTING_VERSION} option which indicates
     * that source connector should stream changes starting from the latest {@link
     * io.delta.standalone.Snapshot} version.
     */
    public static final String STARTING_VERSION_LATEST = "latest";

    /**
     * A map of all valid {@code DeltaSourceOptions}. This map can be used for example by {@code
     * BaseDeltaSourceStepBuilder} to do configuration sanity check.
     *
     * @implNote All {@code ConfigOption} defined in {@code DeltaSourceOptions} class must be added
     * to {@code VALID_SOURCE_OPTIONS} map.
     */
    public static final Map<String, DeltaConfigOption<?>> USER_FACING_SOURCE_OPTIONS =
        new HashMap<>();

    /**
     * A map of all {@code DeltaSourceOptions} that are internal only, meaning that they must not be
     * used by end user through public API. This map can be used for example by {@code
     * BaseDeltaSourceStepBuilder} to do configuration sanity check.
     *
     * @implNote All options categorized for "internal use only" defined in {@code
     * DeltaSourceOptions} class must be added to {@code INNER_SOURCE_OPTIONS} map.
     */
    public static final Map<String, DeltaConfigOption<?>> INNER_SOURCE_OPTIONS = new HashMap<>();

    // ----- PUBLIC AND NONE-PUBLIC OPTIONS ----- //
    // This options can be set/used by end user while configuring Flink Delta source.

    /**
     * An option that allow time travel to {@link io.delta.standalone.Snapshot} version to read
     * from. Applicable for {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode
     * only.
     * <p>
     * <p>
     * The String representation for this option is <b>versionAsOf</b>.
     */
    public static final DeltaConfigOption<Long> VERSION_AS_OF =
        DeltaConfigOption.of(
            ConfigOptions.key("versionAsOf").longType().noDefaultValue(),
            Long.class,
            new NonNegativeNumberTypeConverter<>());

    /**
     * An option that allow time travel to the latest {@link io.delta.standalone.Snapshot} that was
     * generated at or before given timestamp. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>timestampAsOf</b>.
     */
    public static final DeltaConfigOption<Long> TIMESTAMP_AS_OF =
        DeltaConfigOption.of(
            ConfigOptions.key("timestampAsOf").longType().noDefaultValue(),
            Long.class,
            new TimestampOptionTypeConverter());

    /**
     * An option to specify a {@link io.delta.standalone.Snapshot} version to only read changes
     * from. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>startingVersion</b>.
     */
    public static final DeltaConfigOption<String> STARTING_VERSION =
        DeltaConfigOption.of(
            ConfigOptions.key("startingVersion").stringType().noDefaultValue(),
            String.class,
            new StartingVersionOptionTypeConverter());

    /**
     * An option used to read only changes from {@link io.delta.standalone.Snapshot} that was
     * generated at or before given timestamp. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>startingTimestamp</b>.
     */
    public static final DeltaConfigOption<Long> STARTING_TIMESTAMP =
        DeltaConfigOption.of(
            ConfigOptions.key("startingTimestamp").longType().noDefaultValue(),
            Long.class,
            new TimestampOptionTypeConverter());

    /**
     * An option to specify check interval (in milliseconds) for monitoring Delta table changes.
     * Applicable for {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>updateCheckIntervalMillis</b> and its default
     * value is 5000.
     */
    public static final DeltaConfigOption<Long> UPDATE_CHECK_INTERVAL =
        DeltaConfigOption.of(
            ConfigOptions.key("updateCheckIntervalMillis").longType().defaultValue(5000L),
            Long.class,
            new NonNegativeNumberTypeConverter<>());

    /**
     * An option to specify initial delay (in milliseconds) for starting periodical Delta table
     * checks for updates. Applicable for
     * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
     * mode only.
     * <p>
     * <p>
     * The String representation for this option is <b>updateCheckDelayMillis</b> and its default
     * value is 1000.
     */
    public static final DeltaConfigOption<Long> UPDATE_CHECK_INITIAL_DELAY =
        DeltaConfigOption.of(
            ConfigOptions.key("updateCheckDelayMillis").longType().defaultValue(1000L),
            Long.class,
            new NonNegativeNumberTypeConverter<>());

    /**
     * An option used to allow processing Delta table versions containing only {@link
     * io.delta.standalone.actions.RemoveFile} actions.
     * <p>
     * If this option is set to true, Source connector will not throw an exception when processing
     * version containing only {@link io.delta.standalone.actions.RemoveFile} actions regardless of
     * {@link io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     * <p>
     * <p>
     * The String representation for this option is <b>ignoreDeletes</b> and its default value is
     * false.
     */
    public static final DeltaConfigOption<Boolean> IGNORE_DELETES =
        DeltaConfigOption.of(
            ConfigOptions.key("ignoreDeletes").booleanType().defaultValue(false),
            Boolean.class,
            new BooleanOptionTypeConverter());

    /**
     * An option used to allow processing Delta table versions containing both {@link
     * io.delta.standalone.actions.RemoveFile} {@link io.delta.standalone.actions.AddFile} actions.
     * <p>
     * This option subsumes {@link #IGNORE_DELETES} option.
     * <p>
     * If this option is set to true, Source connector will not throw an exception when processing
     * version containing combination of {@link io.delta.standalone.actions.RemoveFile} and {@link
     * io.delta.standalone.actions.AddFile} actions regardless of {@link
     * io.delta.standalone.actions.RemoveFile#isDataChange()} flag.
     * <p>
     * <p>
     * The String representation for this option is <b>ignoreChanges</b> and its default value is
     * false.
     */
    public static final DeltaConfigOption<Boolean> IGNORE_CHANGES =
        DeltaConfigOption.of(
            ConfigOptions.key("ignoreChanges").booleanType().defaultValue(false),
            Boolean.class,
            new BooleanOptionTypeConverter());

    /**
     * An option to set the number of rows read per Parquet Reader per batch from underlying Parquet
     * file. This can improve read performance reducing IO cals to Parquet file at cost of memory
     * consumption on Task Manager nodes.
     */
    public static final DeltaConfigOption<Integer> PARQUET_BATCH_SIZE =
        DeltaConfigOption.of(
            ConfigOptions.key("parquetBatchSize").intType().defaultValue(2048),
            Integer.class,
            new NonNegativeNumberTypeConverter<>());

    // ----- INNER ONLY OPTIONS ----- //
    // Inner options should not be set by user, and they are used internally by Flin connector.

    /**
     * An option to set Delta table {@link io.delta.standalone.Snapshot} version that should be
     * initialized during
     * {@link io.delta.flink.source.internal.enumerator.DeltaSourceSplitEnumerator}
     * first initialization.
     *
     * @implNote The {@link org.apache.flink.api.connector.source.SplitEnumerator} implementations
     * for Delta source has to use the same Delta Snapshot that was used for schema discovery by
     * source builder. This is needed to avoid anny issues caused by schema changes that might have
     * happened between source initialization and enumerator initialization. The version of the
     * snapshot used for schema discovery in Source builder is passed to the SplitEnumerator via
     * {@link DeltaSourceConfiguration} using LOADED_SCHEMA_SNAPSHOT_VERSION option.
     * <p>
     * When the job is submitted to the Flink cluster, the entire job graph including operators,
     * source and sink classes is serialized on a "client side" and deserialized back on a Job
     * Manager node. Because both {@link io.delta.standalone.Snapshot} and {@link
     * io.delta.standalone.DeltaLog} are not serializable, we cannot simply pass reference value to
     * Delta Source instance, since this will throw an exception during job initialization, failing
     * on the deserialization.
     */
    public static final DeltaConfigOption<Long> LOADED_SCHEMA_SNAPSHOT_VERSION =
        DeltaConfigOption.of(
            ConfigOptions.key("loadedSchemaSnapshotVersion").longType().noDefaultValue(),
            Long.class);

    // ----------------------------- //

    static {
        USER_FACING_SOURCE_OPTIONS.put(VERSION_AS_OF.key(), VERSION_AS_OF);
        USER_FACING_SOURCE_OPTIONS.put(TIMESTAMP_AS_OF.key(), TIMESTAMP_AS_OF);
        USER_FACING_SOURCE_OPTIONS.put(STARTING_VERSION.key(), STARTING_VERSION);
        USER_FACING_SOURCE_OPTIONS.put(STARTING_TIMESTAMP.key(), STARTING_TIMESTAMP);
        USER_FACING_SOURCE_OPTIONS.put(UPDATE_CHECK_INTERVAL.key(), UPDATE_CHECK_INTERVAL);
        USER_FACING_SOURCE_OPTIONS.put(
            UPDATE_CHECK_INITIAL_DELAY.key(),
            UPDATE_CHECK_INITIAL_DELAY);
        USER_FACING_SOURCE_OPTIONS.put(IGNORE_DELETES.key(), IGNORE_DELETES);
        USER_FACING_SOURCE_OPTIONS.put(IGNORE_CHANGES.key(), IGNORE_CHANGES);
        USER_FACING_SOURCE_OPTIONS.put(PARQUET_BATCH_SIZE.key(), PARQUET_BATCH_SIZE);
    }

    static {
        INNER_SOURCE_OPTIONS.put(LOADED_SCHEMA_SNAPSHOT_VERSION.key(),
            LOADED_SCHEMA_SNAPSHOT_VERSION);
    }

    public static boolean isUserFacingOption(String optionName) {
        return USER_FACING_SOURCE_OPTIONS.containsKey(optionName);
    }
}
