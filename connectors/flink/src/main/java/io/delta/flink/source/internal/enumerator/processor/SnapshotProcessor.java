package io.delta.flink.source.internal.enumerator.processor;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.core.fs.Path;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

/**
 * This implementation of {@link TableProcessor} process data from Delta table {@link Snapshot}.
 */
public class SnapshotProcessor extends TableProcessorBase {

    /**
     * A {@link Snapshot} that is processed by this processor.
     */
    private final Snapshot snapshot;

    /**
     * Set with already processed paths for Parquet Files. Processor will skip (i.e. not process)
     * parquet files from this set.
     * <p>
     * The use case for this set is a recovery from checkpoint scenario, where we don't want to
     * reprocess already processed Parquet files.
     */
    private final HashSet<Path> alreadyProcessedPaths;

    public SnapshotProcessor(Path deltaTablePath, Snapshot snapshot,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        Collection<Path> alreadyProcessedPaths) {
        super(deltaTablePath, fileEnumerator);
        this.snapshot = snapshot;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);
    }

    /**
     * Process all {@link AddFile} from {@link Snapshot} passed to this {@code SnapshotProcessor}
     * constructor by converting them to {@link DeltaSourceSplit} objects.
     *
     * <p><strong>PERFORMANCE NOTE:</strong> Current implementation loads all files at once via
     * {@code snapshot.getAllFiles()}. For tables with millions of files, this may cause memory
     * pressure on the JobManager.
     *
     * <p><strong>FUTURE OPTIMIZATION:</strong> Implement chunked/paginated file loading:
     * <ul>
     *   <li>Process files in batches (e.g., 10K files per chunk)</li>
     *   <li>Maintain pagination state in {@link DeltaEnumeratorStateCheckpointBuilder}</li>
     *   <li>Signal to {@link DeltaSourceSplitEnumerator} when more chunks are available</li>
     *   <li>Implement backpressure mechanism if downstream can't keep up</li>
     * </ul>
     *
     * <p><strong>CURRENT STATUS:</strong> Works well for tables with &lt; 1M files. For larger
     * tables, increase JobManager heap size or enable RocksDB state backend.
     *
     * @param processCallback A {@link Consumer} callback that will be called after converting all
     *                        {@link AddFile} to {@link DeltaSourceSplit}.
     */
    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        // OPTIMIZATION: Future enhancement for very large tables (>1M files)
        // Current implementation loads all files at once, which works well for most use cases.
        // For extreme scale, implement chunked loading with pagination state.
        List<DeltaSourceSplit> splits =
            prepareSplits(new ChangesPerVersion<>(
                    SourceUtils.pathToString(deltaTablePath),
                    snapshot.getVersion(),
                    snapshot.getAllFiles()),
                alreadyProcessedPaths::add);
        processCallback.accept(splits);
    }

    @Override
    public DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> snapshotState(
        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder) {

        checkpointBuilder.withProcessedPaths(alreadyProcessedPaths);

        // false means that this processor does not check Delta table for changes.
        checkpointBuilder.withMonitoringForChanges(false);
        return checkpointBuilder;
    }

    /**
     * @return A {@link Snapshot} version that this processor reads.
     */
    @Override
    public long getSnapshotVersion() {
        return snapshot.getVersion();
    }
}
