package io.delta.flink.source.internal.state;

import java.util.Collection;
import java.util.Collections;

import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkNotNull;

public class DeltaEnumeratorStateCheckpointBuilder<SplitT extends DeltaSourceSplit> {

    /**
     * {@link Path} to Delta Table used for this snapshot.
     */
    private final Path deltaTablePath;

    /**
     * The Delta table snapshot version used to create this checkpoint.
     */
    private final long snapshotVersion;

    /**
     * Created {@link DeltaSourceSplit} that were not yet assigned to source readers.
     */
    private final Collection<SplitT> splits;

    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in
     * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
     * mode.
     */
    private Collection<Path> processedPaths = Collections.emptySet();

    /**
     * Flag indicating that source started monitoring Delta table for changes.
     * <p>
     * The default value is false.
     */
    private boolean monitoringForChanges;

    public DeltaEnumeratorStateCheckpointBuilder(
        Path deltaTablePath, long snapshotVersion, Collection<SplitT> splits) {
        this.deltaTablePath = deltaTablePath;
        this.snapshotVersion = snapshotVersion;
        this.splits = splits;
        this.monitoringForChanges = false;
    }

    public static <T extends DeltaSourceSplit> DeltaEnumeratorStateCheckpointBuilder<T>
        builder(Path deltaTablePath, long snapshotVersion, Collection<T> splits) {

        checkNotNull(deltaTablePath);
        checkNotNull(snapshotVersion);

        return new DeltaEnumeratorStateCheckpointBuilder<>(deltaTablePath, snapshotVersion, splits);
    }

    public DeltaEnumeratorStateCheckpointBuilder<SplitT> withProcessedPaths(
        Collection<Path> processedPaths) {
        this.processedPaths = processedPaths;
        return this;
    }

    public DeltaEnumeratorStateCheckpointBuilder<SplitT> withMonitoringForChanges(
        boolean monitoringForChanges) {
        this.monitoringForChanges = monitoringForChanges;
        return this;
    }

    public DeltaEnumeratorStateCheckpoint<SplitT> build() {
        PendingSplitsCheckpoint<SplitT> splitsCheckpoint =
            PendingSplitsCheckpoint.fromCollectionSnapshot(splits, processedPaths);

        return new DeltaEnumeratorStateCheckpoint<>(deltaTablePath, snapshotVersion,
            monitoringForChanges, splitsCheckpoint);
    }
}
