package io.delta.flink.source.internal.builder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import io.delta.flink.source.internal.enumerator.BoundedSplitEnumeratorProvider;
import io.delta.flink.source.internal.enumerator.supplier.BoundedSnapshotSupplierFactory;
import io.delta.flink.source.internal.enumerator.supplier.TimestampFormatConverter;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.conf.Configuration;
import static io.delta.flink.source.internal.DeltaSourceOptions.PARQUET_BATCH_SIZE;
import static io.delta.flink.source.internal.DeltaSourceOptions.TIMESTAMP_AS_OF;
import static io.delta.flink.source.internal.DeltaSourceOptions.VERSION_AS_OF;

/**
 * A base class for Delta source builders that should create Delta source instance for {@link
 * org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode. This implementation
 * contains methods from {@link DeltaSourceBuilderBase} base class and methods applicable only for
 * Bounded mode.
 *
 * @param <T> Type of element produced by created source.
 * @param <SELF> This builder carries a <i>SELF</i> type to make it convenient to extend this for
 *               subclasses. Please, see {@link DeltaSourceBuilderBase} for details.
 */
public abstract class BoundedDeltaSourceBuilder<T, SELF> extends DeltaSourceBuilderBase<T, SELF> {

    /**
     * The provider for {@link org.apache.flink.api.connector.source.SplitEnumerator} in {@link
     * org.apache.flink.api.connector.source.Boundedness#BOUNDED} mode.
     */
    protected static final BoundedSplitEnumeratorProvider
        DEFAULT_BOUNDED_SPLIT_ENUMERATOR_PROVIDER =
        new BoundedSplitEnumeratorProvider(DEFAULT_SPLIT_ASSIGNER,
            DEFAULT_SPLITTABLE_FILE_ENUMERATOR);

    protected static final List<String> APPLICABLE_OPTIONS = Collections.unmodifiableList(
        Arrays.asList(
            VERSION_AS_OF.key(),
            TIMESTAMP_AS_OF.key(),
            PARQUET_BATCH_SIZE.key()
        )
    );

    public BoundedDeltaSourceBuilder(
            Path tablePath,
            Configuration hadoopConfiguration,
            BoundedSnapshotSupplierFactory snapshotSupplierFactory) {
        super(tablePath, hadoopConfiguration, snapshotSupplierFactory);
    }

    public SELF versionAsOf(long snapshotVersion) {
        sourceConfiguration.addOption(VERSION_AS_OF, snapshotVersion);
        return self();
    }

    public SELF timestampAsOf(String snapshotTimestamp) {
        long toTimestamp = TimestampFormatConverter.convertToTimestamp(snapshotTimestamp);
        sourceConfiguration.addOption(TIMESTAMP_AS_OF, toTimestamp);
        return self();
    }

    @Override
    protected Validator validateOptionExclusions() {

        return new Validator()

            // mutually exclusive check for VERSION_AS_OF and TIMESTAMP_AS_OF in Bounded mode.
            .checkArgument(
                !sourceConfiguration.hasOption(VERSION_AS_OF)
                    || !sourceConfiguration.hasOption(TIMESTAMP_AS_OF),
                prepareOptionExclusionMessage(VERSION_AS_OF.key(), TIMESTAMP_AS_OF.key()));
    }

    @Override
    protected Collection<String> getApplicableOptions() {
        return APPLICABLE_OPTIONS;
    }
}
