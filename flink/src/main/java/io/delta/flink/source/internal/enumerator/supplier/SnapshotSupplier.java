package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.flink.source.internal.DeltaSourceConfiguration;
import io.delta.flink.source.internal.utils.TransitiveOptional;

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

    protected SnapshotSupplier(DeltaLog deltaLog) {
        this.deltaLog = deltaLog;
    }

    /**
     * @return A {@link Snapshot} instance acquired from {@link #deltaLog}. Every implementation of
     * {@link SnapshotSupplier} class can have its own rules about how snapshot should be acquired.
     */
    public abstract Snapshot getSnapshot(DeltaSourceConfiguration sourceConfiguration);

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
}
