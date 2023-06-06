package io.delta.flink.source.internal.state;

import java.util.Collection;

import io.delta.flink.source.internal.enumerator.processor.ContinuousTableProcessor;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;

/**
 * A checkpoint of the current state of {@link SplitEnumerator}.
 *
 * <p>It contains all necessary information need by SplitEnumerator to resume work after
 * checkpoint recovery including currently pending splits that are not yet assigned and resume
 * changes discovery task on Delta table in {@link Boundedness#CONTINUOUS_UNBOUNDED} mode</p>
 *
 * <p>During checkpoint, Flink will serialize this object and persist it in checkpoint location.
 * During the recovery, Flink will deserialize this object from Checkpoint/Savepoint and will use it
 * to recreate {@code SplitEnumerator}.
 *
 * @param <SplitT> The concrete type of {@link SourceSplit} that is kept in @param * splits
 *                 collection.
 */
public class DeltaEnumeratorStateCheckpoint<SplitT extends DeltaSourceSplit> {

    /**
     * {@link Path} to Delta table used for this snapshot.
     */
    private final Path deltaTablePath;

    /**
     * The Delta table snapshot version used to create this checkpoint.
     */
    private final long snapshotVersion;

    /**
     * Flag indicating that source start monitoring Delta Table for changes.
     * <p>
     * This field is mapped from {@link ContinuousTableProcessor #isMonitoringForChanges()} method.
     */
    private final boolean monitoringForChanges;

    /**
     * Decorated {@link PendingSplitsCheckpoint} that keeps details about checkpointed splits in
     * enumerator.
     */
    private final PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint;

    protected DeltaEnumeratorStateCheckpoint(Path deltaTablePath,
        long snapshotVersion, boolean monitoringForChanges,
        PendingSplitsCheckpoint<SplitT> pendingSplitsCheckpoint) {
        this.deltaTablePath = deltaTablePath;
        this.snapshotVersion = snapshotVersion;
        this.monitoringForChanges = monitoringForChanges;
        this.pendingSplitsCheckpoint = pendingSplitsCheckpoint;
    }

    /**
     * @return The initial version of Delta Table from witch we started reading the Delta Table.
     */
    public long getSnapshotVersion() {
        return snapshotVersion;
    }

    /**
     * @return The checkpointed {@link DeltaSourceSplit} that were not yet assigned to file readers.
     */
    public Collection<SplitT> getSplits() {
        return pendingSplitsCheckpoint.getSplits();
    }

    /**
     * @return The paths that are no longer in the enumerator checkpoint, but have been processed
     * before and should be ignored.
     */
    public Collection<Path> getAlreadyProcessedPaths() {
        return pendingSplitsCheckpoint.getAlreadyProcessedPaths();
    }

    /**
     * @return {@link Path} to Delta Table used for this snapshot.
     */
    public Path getDeltaTablePath() {
        return deltaTablePath;
    }

    /**
     * @return Boolean flag indicating that {@code DeltaSourceSplitEnumerator} started monitoring
     * for changes on Delta Table.
     */
    public boolean isMonitoringForChanges() {
        return monitoringForChanges;
    }

    // Package protected For (De)Serializer only
    PendingSplitsCheckpoint<SplitT> getPendingSplitsCheckpoint() {
        return pendingSplitsCheckpoint;
    }

}
