package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.utils.TransitiveOptional;
import org.apache.flink.configuration.ConfigOption;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * This class abstract's logic needed to acquirer Delta table {@link Snapshot} based on {@link
 * DeltaSourceConfiguration} and any other implementation specific logic.
 */
public abstract class SnapshotSupplier {

    /**
     * The {@link DeltaLog} instance that will be used to get the desire {@link Snapshot} instance.
     */
    protected final DeltaLog deltaLog;

    /**
     * The {@link DeltaSourceConfiguration} with used
     * {@link io.delta.flink.source.internal.DeltaSourceOptions}.
     */
    protected final DeltaSourceConfiguration sourceConfiguration;

    protected SnapshotSupplier(
        DeltaLog deltaLog,
        DeltaSourceConfiguration sourceConfiguration) {
        this.deltaLog = deltaLog;
        this.sourceConfiguration = sourceConfiguration;
    }

    /**
     * @return A {@link Snapshot} instance acquired from {@link #deltaLog}. Every implementation of
     * {@link SnapshotSupplier} class can have its own rules about how snapshot should be acquired.
     */
    public abstract Snapshot getSnapshot();

    /**
     * A helper method that returns the latest {@link Snapshot} at moment when this method was
     * called.
     * <p>
     * If underlying Delta table, represented by {@link #deltaLog} field is changing, for example a
     * new data is being added to the table, every call to this method can return different {@link
     * Snapshot}.
     */
    protected TransitiveOptional<Snapshot> getHeadSnapshot() {
        return TransitiveOptional.ofNullable(deltaLog.snapshot());
    }

    /**
     * A helper method to get the value of {@link io.delta.flink.source.internal.DeltaSourceOptions}
     * from {@link #sourceConfiguration}.
     */
    protected <T> T getOptionValue(ConfigOption<T> option) {
        return this.sourceConfiguration.getValue(option);
    }
}
