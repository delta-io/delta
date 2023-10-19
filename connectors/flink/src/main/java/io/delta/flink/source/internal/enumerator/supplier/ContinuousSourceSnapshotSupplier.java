package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.utils.TransitiveOptional;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_TIMESTAMP;
import static io.delta.flink.source.internal.DeltaSourceOptions.STARTING_VERSION;
import static io.delta.flink.source.internal.DeltaSourceOptions.USE_KERNEL_FOR_SNAPSHOTS;

import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

/**
 * An implementation of {@link SnapshotSupplier} for {#link
 * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}}
 * mode.
 */
public class ContinuousSourceSnapshotSupplier extends SnapshotSupplier {

    public ContinuousSourceSnapshotSupplier(DeltaLog deltaLog, TableClient tableClient, Table table) {
	super(deltaLog, tableClient, table);
    }

    /**
     * This method returns a {@link Snapshot} instance acquired from {@link #deltaLog}. This
     * implementation tries to query the {@code Snapshot} in below order, stopping at first
     * non-empty result:
     * <ul>
     *     <li>If {@link DeltaSourceOptions#STARTING_VERSION} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForVersionAsOf(long)}.</li>
     *     <li>If {@link DeltaSourceOptions#STARTING_TIMESTAMP} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForTimestampAsOf(long)}.</li>
     *     <li>Get the head version using {@link DeltaLog#snapshot()}</li>
     * </ul>
     *
     * @return A {@link Snapshot} instance or throws {@link java.util.NoSuchElementException} if no
     * snapshot was found.
     */
    @Override
    public Snapshot getSnapshot(DeltaConnectorConfiguration sourceConfiguration) {
	TransitiveOptional<Snapshot> snapshot = getSnapshotFromStartingVersionOption(sourceConfiguration)
	    .or(() -> getSnapshotFromStartingTimestampOption(sourceConfiguration));
	boolean useKernel = sourceConfiguration.getValue(USE_KERNEL_FOR_SNAPSHOTS);
	if (useKernel) {
	    snapshot = snapshot.or(this::getHeadSnapshotViaKernel);
	} else {
	    snapshot = snapshot.or(this::getHeadSnapshot);
	}
        return snapshot.get();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromStartingVersionOption(
            DeltaConnectorConfiguration sourceConfiguration) {

        String startingVersion = sourceConfiguration.getValue(STARTING_VERSION);
        if (startingVersion != null) {
            if (DeltaSourceOptions.STARTING_VERSION_LATEST.equalsIgnoreCase(startingVersion)) {
                return TransitiveOptional.ofNullable(deltaLog.snapshot());
            } else {
                return TransitiveOptional.ofNullable(
                    deltaLog.getSnapshotForVersionAsOf(
                        Long.parseLong(startingVersion))
                );
            }
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromStartingTimestampOption(
            DeltaConnectorConfiguration sourceConfiguration) {
        Long startingTimestamp = sourceConfiguration.getValue(STARTING_TIMESTAMP);
        if (startingTimestamp != null) {
            // Delta Lake streaming semantics match timestamps to versions using
            // 'at or after' semantics. Here we do the same.
            long versionAtOrAfterTimestamp =
                deltaLog.getVersionAtOrAfterTimestamp(startingTimestamp);
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForVersionAsOf(versionAtOrAfterTimestamp));
        }
        return TransitiveOptional.empty();
    }
}
