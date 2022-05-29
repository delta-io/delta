package io.delta.flink.source.internal.builder;

import io.delta.flink.source.internal.enumerator.ContinuousSplitEnumeratorProvider;
import io.delta.flink.source.internal.enumerator.supplier.ContinuousSnapshotSupplierFactory;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.IGNORE_CHANGES;
import static io.delta.flink.source.internal.DeltaSourceOptions.IGNORE_DELETES;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.UPDATE_CHECK_INTERVAL;

/**
 * A base class for Delta source builders that should create Delta source instance for {@link
 * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode. This implementation
 * contains methods from {@link DeltaSourceBuilderBase} base class and methods applicable only for
 * Continuous mode.
 *
 * @param <T> Type of element produced by created source.
 * @param <SELF> This builder carries a <i>SELF</i> type to make it convenient to extend this for
 *               subclasses. Please, see {@link DeltaSourceBuilderBase} for details.
 */
public abstract class ContinuousDeltaSourceBuilder<T, SELF>
    extends DeltaSourceBuilderBase<T, SELF> {

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED} mode.
     */
    protected static final ContinuousSplitEnumeratorProvider
        DEFAULT_CONTINUOUS_SPLIT_ENUMERATOR_PROVIDER =
        new ContinuousSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    public ContinuousDeltaSourceBuilder(
            Path tablePath,
            Configuration hadoopConfiguration,
            ContinuousSnapshotSupplierFactory snapshotSupplierFactory) {
        super(tablePath, hadoopConfiguration, snapshotSupplierFactory);
    }
    // TODO PR 12 add tests for options.
    public SELF startingVersion(String startingVersion) {
        sourceConfiguration.addOption(STARTING_VERSION.key(), startingVersion);
        return self();
    }

    public SELF startingVersion(long startingVersion) {
        startingVersion(String.valueOf(startingVersion));
        return self();
    }

    public SELF startingTimestamp(String startingTimestamp) {
        sourceConfiguration.addOption(STARTING_TIMESTAMP.key(), startingTimestamp);
        return self();
    }

    public SELF updateCheckIntervalMillis(long updateCheckInterval) {
        sourceConfiguration.addOption(UPDATE_CHECK_INTERVAL.key(), updateCheckInterval);
        return self();
    }

    public SELF ignoreDeletes(boolean ignoreDeletes) {
        sourceConfiguration.addOption(IGNORE_DELETES.key(), ignoreDeletes);
        return self();
    }

    public SELF ignoreChanges(boolean ignoreChanges) {
        sourceConfiguration.addOption(IGNORE_CHANGES.key(), ignoreChanges);
        return self();
    }

    @Override
    protected Validator validateOptionExclusions() {

        // mutually exclusive check for STARTING_VERSION and STARTING_TIMESTAMP in Streaming
        // mode.
        return new Validator()
            .checkArgument(
                !sourceConfiguration.hasOption(STARTING_TIMESTAMP)
                    || !sourceConfiguration.hasOption(STARTING_VERSION),
                prepareOptionExclusionMessage(STARTING_VERSION.key(), STARTING_TIMESTAMP.key()));
    }
}
