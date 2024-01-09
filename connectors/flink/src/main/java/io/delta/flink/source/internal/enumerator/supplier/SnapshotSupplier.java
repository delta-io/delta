package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.utils.TransitiveOptional;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.client.TableClient;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
/**
 * This class abstract's logic needed to acquirer Delta table {@link Snapshot} based on {@link
 * DeltaConnectorConfiguration} and any other implementation specific logic.
 */
public abstract class SnapshotSupplier {

    /**
     * The {@link DeltaLog} instance that will be used to get the desire {@link Snapshot} instance.
     */
    protected final DeltaLog deltaLog;
    protected final TableClient tableClient;
    protected final Table table;

    protected SnapshotSupplier(DeltaLog deltaLog, TableClient tableClient, Table table) {
        this.deltaLog = deltaLog;
        this.tableClient = tableClient;
        this.table = table;
    }

    /**
     * @return A {@link Snapshot} instance acquired from {@link #deltaLog}. Every implementation of
     * {@link SnapshotSupplier} class can have its own rules about how snapshot should be acquired.
     */
    public abstract Snapshot getSnapshot(DeltaConnectorConfiguration sourceConfiguration);

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
     * A helper method that returns the latest {@link Snapshot} at moment when this method was
     * called.
     * <p>
     * This uses delta-kernel-java to return a snapshot.
     *
     * NB: The snapshot returned here currently ONLY supports the getMetadata and getVersion
     * functions. All other calls on the returned snaphot will throw an Exception
     */
    protected TransitiveOptional<Snapshot> getHeadSnapshotViaKernel() {
        try {
            io.delta.kernel.internal.SnapshotImpl kernelSnapshot =
                (io.delta.kernel.internal.SnapshotImpl)table.getLatestSnapshot(tableClient);
            return TransitiveOptional.ofNullable(new KernelSnapshotWrapper(kernelSnapshot));
        } catch (TableNotFoundException e) {
            return TransitiveOptional.ofNullable(null);
        }
    }
}
