package io.delta.flink.source.internal.enumerator.processor;

import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;

import io.delta.standalone.Snapshot;

/**
 * This implementation of {@link TableProcessor} process both, content of {@link
 * io.delta.standalone.Snapshot} and changes applied to monitored Delta table by converting them to
 * {@link DeltaSourceSplit} objects.
 *
 * <p>
 * This implementation uses both {@link SnapshotProcessor} to read {@code Snapshot} content and
 * {@link ChangesProcessor} to read all changes applied after snapshot processed by encapsulated
 * {@code SnapshotProcessor}.
 */
public class SnapshotAndChangesTableProcessor implements ContinuousTableProcessor {

    /**
     * The {@link SnapshotProcessor} used to read {@link io.delta.standalone.Snapshot} content.
     */
    private final SnapshotProcessor snapshotProcessor;

    /**
     * The {@link ChangesProcessor} used to read changes applied to Delta table after {@link
     * io.delta.standalone.Snapshot} read by {@link #snapshotProcessor}.
     */
    private final ChangesProcessor changesProcessor;

    /**
     * Flag to indicate whether this processor started processing Delta table changes.
     */
    private boolean monitoringForChanges;

    public SnapshotAndChangesTableProcessor(
        SnapshotProcessor snapshotProcessor, ChangesProcessor changesProcessor) {
        this.snapshotProcessor = snapshotProcessor;
        this.changesProcessor = changesProcessor;
        this.monitoringForChanges = false;
    }

    /**
     * Starts processing content of {@link io.delta.standalone.Snapshot} defined by {@link
     * #snapshotProcessor} and Delta table changes applied after that snapshot.
     *
     * @param processCallback A {@link Consumer} callback that will be called after processing
     *                        {@link Snapshot} content by {@link #snapshotProcessor} and all {@link
     *                        io.delta.standalone.actions.Action} after converting them to {@link
     *                        DeltaSourceSplit}. This callback will be executed for every new
     *                        discovered Delta table version.
     */
    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        snapshotProcessor.process(processCallback);
        monitoringForChanges = true;
        changesProcessor.process(processCallback);
    }

    /**
     * @return false if processor is sitll processing {@link Snapshot} via {@link
     * #snapshotProcessor} or true if processor started processing following changes from Delta
     * Table.
     */
    @Override
    public boolean isMonitoringForChanges() {
        return this.monitoringForChanges;
    }

    /**
     * @return {@link Snapshot} version that this processor currently process. The value returned by
     * this method can be different for every call, since this processor also process changes
     * applied to monitored Delta table.
     */
    public long getSnapshotVersion() {
        return (monitoringForChanges) ? changesProcessor.getSnapshotVersion()
            : snapshotProcessor.getSnapshotVersion();
    }

    @Override
    public DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> snapshotState(
        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder) {

        checkpointBuilder.withMonitoringForChanges(isMonitoringForChanges());
        if (isMonitoringForChanges()) {
            checkpointBuilder = changesProcessor.snapshotState(checkpointBuilder);
        } else {
            checkpointBuilder = snapshotProcessor.snapshotState(checkpointBuilder);
        }

        return checkpointBuilder;
    }
}
