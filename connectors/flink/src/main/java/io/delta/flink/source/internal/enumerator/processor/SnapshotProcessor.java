package io.delta.flink.source.internal.enumerator.processor;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.delta.flink.source.internal.enumerator.monitor.ChangesPerVersion;
import io.delta.flink.source.internal.file.AddFileEnumerator;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import io.delta.flink.source.internal.utils.SourceUtils;
import org.apache.flink.core.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;

/**
 * This implementation of {@link TableProcessor} process data from Delta table {@link Snapshot}.
 */
public class SnapshotProcessor extends TableProcessorBase {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotProcessor.class);

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

    /**
     * A map of partition column name to partition value to filter files by partition values.
     */
    private final Map<String, Set<String>> partitionsToFilter = new LinkedHashMap<>();

    public SnapshotProcessor(Path deltaTablePath, Snapshot snapshot,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        Collection<Path> alreadyProcessedPaths) {
        super(deltaTablePath, fileEnumerator);
        this.snapshot = snapshot;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);
    }

    public SnapshotProcessor(Path deltaTablePath, Snapshot snapshot,
                             AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
                             Collection<Path> alreadyProcessedPaths,
                             Map<String, Set<String>> partitionsToFilter) {
        this(deltaTablePath, snapshot, fileEnumerator, alreadyProcessedPaths);
        this.partitionsToFilter.putAll(partitionsToFilter);
        LOG.info("Created SnapshotProcessor with partition filters {}", this.partitionsToFilter);
    }

    /**
     * Process all {@link AddFile} from {@link Snapshot} passed to this {@code SnapshotProcessor}
     * constructor by converting them to {@link DeltaSourceSplit} objects.
     *
     * @param processCallback A {@link Consumer} callback that will be called after converting all
     *                        {@link AddFile} to {@link DeltaSourceSplit}.
     */
    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        // TODO Initial data read. This should be done in chunks since snapshot.getAllFiles()
        //  can have millions of files, and we would OOM the Job Manager
        //  if we would read all of them at once.
        List<AddFile> allFiles = snapshot.getAllFiles();
        LOG.info("SnapshotProcessor processing snapshot version {}, total AddFiles to process: {}",
                snapshot.getVersion(), allFiles.size());

        List<AddFile> filteredFiles = allFiles.stream().filter(addFile -> {
            if (partitionsToFilter.isEmpty()) {
                return true;
            }
            Map<String, String> filePartitionValues = addFile.getPartitionValues();
            for (String partitionCol : partitionsToFilter.keySet()) {
                Set<String> partitionValues = partitionsToFilter.get(partitionCol);
                if (!filePartitionValues.containsKey(partitionCol)
                        || !partitionValues.contains(filePartitionValues.get(partitionCol))) {
                    return false;
                }
            }
            return true;
        }).collect(Collectors.toList());
        LOG.info("After filtering with partitions {} for version {}, total AddFiles to process: {}",
                partitionsToFilter, snapshot.getVersion(), filteredFiles.size());

        List<DeltaSourceSplit> splits =
                prepareSplits(
                        new ChangesPerVersion<>(
                                SourceUtils.pathToString(deltaTablePath),
                                snapshot.getVersion(),
                                filteredFiles),
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
