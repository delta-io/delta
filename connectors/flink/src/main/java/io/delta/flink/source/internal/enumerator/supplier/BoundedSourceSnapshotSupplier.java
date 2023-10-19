package io.delta.flink.source.internal.enumerator.supplier;

import io.delta.flink.internal.options.DeltaConnectorConfiguration;
import io.delta.flink.source.internal.DeltaSourceOptions;
import io.delta.flink.source.internal.utils.TransitiveOptional;

import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;

import org.apache.flink.core.fs.Path;

/**
 * An implementation of {@link SnapshotSupplier} for {#link
 * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}}
 * mode.
 */
public class BoundedSourceSnapshotSupplier extends SnapshotSupplier {

    public BoundedSourceSnapshotSupplier(DeltaLog deltaLog, TableClient tableClient, Table table) {
        super(deltaLog, tableClient, table);
    }

    /**
     * This method returns a {@link Snapshot} instance acquired from {@link #deltaLog}. This
     * implementation tries to quire the {@code Snapshot} in below order, stopping at first
     * non-empty result:
     * <ul>
     *     <li>If {@link DeltaSourceOptions#VERSION_AS_OF} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForVersionAsOf(long)}.</li>
     *     <li>If {@link DeltaSourceOptions#TIMESTAMP_AS_OF} was specified, use it to call
     *     {@link DeltaLog#getSnapshotForTimestampAsOf(long)}.</li>
     *     <li>Get the head version using {@link DeltaLog#snapshot()}</li>
     * </ul>
     *
     * @return A {@link Snapshot} instance or throws {@link java.util.NoSuchElementException} if no
     * snapshot was found.
     */
    @Override
    public Snapshot getSnapshot(DeltaConnectorConfiguration sourceConfiguration) {
        return getSnapshotFromVersionAsOfOption(sourceConfiguration)
            .or(() -> getSnapshotFromTimestampAsOfOption(sourceConfiguration))
            .or(this::getHeadSnapshot)
            .get();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromVersionAsOfOption(
            DeltaConnectorConfiguration sourceConfiguration) {
        Long versionAsOf = sourceConfiguration.getValue(DeltaSourceOptions.VERSION_AS_OF);
        if (versionAsOf != null) {
            return TransitiveOptional.ofNullable(deltaLog.getSnapshotForVersionAsOf(versionAsOf));
        }
        return TransitiveOptional.empty();
    }

    private TransitiveOptional<Snapshot> getSnapshotFromTimestampAsOfOption(
        DeltaConnectorConfiguration sourceConfiguration) {
        Long timestampAsOf = sourceConfiguration.getValue(DeltaSourceOptions.TIMESTAMP_AS_OF);
        if (timestampAsOf != null) {
            return TransitiveOptional.ofNullable(
                deltaLog.getSnapshotForTimestampAsOf(timestampAsOf)
            );
        }
        return TransitiveOptional.empty();
    }
}
