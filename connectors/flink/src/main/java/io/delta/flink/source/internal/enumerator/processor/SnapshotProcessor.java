package io.delta.flink.source.internal.enumerator.processor;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nullable;

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
 * This implementation of {@link TableProcessor} process data from Delta table {@link Snapshot}
 * using chunked file loading to efficiently handle tables with millions of files.
 *
 * <p><strong>Chunked File Loading:</strong> Files are processed in configurable batches
 * (default: 10,000 files per chunk) to avoid memory pressure on the Flink JobManager. This
 * allows reading extremely large tables without OOM errors.
 *
 * <p><strong>Checkpoint Support:</strong> Progress is tracked via {@link #filesProcessedCount}
 * and persisted in checkpoints, allowing seamless recovery after failures.
 */
public class SnapshotProcessor extends TableProcessorBase {

    private static final Logger LOG = LoggerFactory.getLogger(SnapshotProcessor.class);

    /**
     * Default chunk size: 10,000 files per batch.
     * This provides a good balance between memory usage and processing efficiency.
     */
    public static final int DEFAULT_CHUNK_SIZE = 10_000;

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
     * The maximum number of files to process in each chunk.
     */
    private final int chunkSize;

    /**
     * The chunked file iterator for processing files in batches.
     * Null until the first call to {@link #process(Consumer)}.
     */
    @Nullable
    private ChunkedFileIterator chunkedIterator;

    /**
     * The number of files already processed from this snapshot.
     * Used for checkpoint recovery and progress tracking.
     */
    private long filesProcessedCount;

    /**
     * Creates a SnapshotProcessor with default chunk size.
     *
     * @param deltaTablePath the Delta table path
     * @param snapshot the snapshot to process
     * @param fileEnumerator the file enumerator for creating splits
     * @param alreadyProcessedPaths paths already processed (for checkpoint recovery)
     */
    public SnapshotProcessor(Path deltaTablePath, Snapshot snapshot,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        Collection<Path> alreadyProcessedPaths) {
        this(deltaTablePath, snapshot, fileEnumerator, alreadyProcessedPaths,
            DEFAULT_CHUNK_SIZE, 0L);
    }

    /**
     * Creates a SnapshotProcessor with custom chunk size and recovery state.
     *
     * @param deltaTablePath the Delta table path
     * @param snapshot the snapshot to process
     * @param fileEnumerator the file enumerator for creating splits
     * @param alreadyProcessedPaths paths already processed (for checkpoint recovery)
     * @param chunkSize the maximum number of files per chunk
     * @param filesProcessedCount the number of files already processed (for recovery)
     */
    public SnapshotProcessor(Path deltaTablePath, Snapshot snapshot,
        AddFileEnumerator<DeltaSourceSplit> fileEnumerator,
        Collection<Path> alreadyProcessedPaths,
        int chunkSize, long filesProcessedCount) {
        super(deltaTablePath, fileEnumerator);
        if (chunkSize <= 0) {
            throw new IllegalArgumentException(
                "Chunk size must be positive, got: " + chunkSize);
        }
        if (filesProcessedCount < 0) {
            throw new IllegalArgumentException(
                "Files processed count must be non-negative, got: " + filesProcessedCount);
        }

        this.snapshot = snapshot;
        this.alreadyProcessedPaths = new HashSet<>(alreadyProcessedPaths);
        this.chunkSize = chunkSize;
        this.filesProcessedCount = filesProcessedCount;
        this.chunkedIterator = null;

        LOG.debug("Created SnapshotProcessor with chunkSize={}, filesProcessedCount={}",
            chunkSize, filesProcessedCount);
    }

    /**
     * Processes the next chunk of {@link AddFile} from the {@link Snapshot} by converting them
     * to {@link DeltaSourceSplit} objects.
     *
     * <p><strong>Chunked Processing:</strong> This method processes files in configurable batches
     * (default: 10,000 files per chunk). On the first call, it initializes a
     * {@link ChunkedFileIterator} using {@code snapshot.scan().getFiles()}. Subsequent calls
     * process the next chunk until all files are enumerated.
     *
     * <p><strong>Checkpoint Recovery:</strong> If {@link #filesProcessedCount} is non-zero
     * (from checkpoint recovery), the iterator will skip already-processed files and resume
     * from the correct position.
     *
     * <p><strong>Memory Efficiency:</strong> By processing in chunks, this implementation
     * efficiently handles tables with millions of files without causing JobManager OOM errors.
     * Progress is tracked via {@link #filesProcessedCount} and persisted in checkpoints.
     *
     * <p><strong>Usage Pattern:</strong> The enumerator should call this method repeatedly
     * until {@link #hasMoreFiles()} returns {@code false}:
     * <pre>{@code
     * while (processor.hasMoreFiles()) {
     *     processor.process(this::addSplits);
     * }
     * }</pre>
     *
     * @param processCallback A {@link Consumer} callback that will be called with the
     *                        {@link DeltaSourceSplit} objects from the current chunk
     */
    @Override
    public void process(Consumer<List<DeltaSourceSplit>> processCallback) {
        // Lazy initialization of chunked iterator on first process() call
        if (chunkedIterator == null) {
            try {
                LOG.info("Initializing chunked file iterator for snapshot version {} " +
                        "with chunkSize={}, resuming from filesProcessedCount={}",
                    snapshot.getVersion(), chunkSize, filesProcessedCount);

                chunkedIterator = new ChunkedFileIterator(
                    snapshot.scan().getFiles(),
                    chunkSize,
                    filesProcessedCount);

            } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to initialize chunked file iterator for snapshot version "
                        + snapshot.getVersion(), e);
            }
        }

        // Process the next chunk if available
        if (!chunkedIterator.hasMoreChunks()) {
            LOG.debug("No more file chunks to process for snapshot version {}. " +
                "Total files processed: {}", snapshot.getVersion(), filesProcessedCount);
            return;
        }

        try {
            // Get the next chunk of files
            List<AddFile> fileChunk = chunkedIterator.nextChunk();
            filesProcessedCount = chunkedIterator.getFilesProcessedCount();

            LOG.debug("Processing chunk of {} files from snapshot version {}. " +
                    "Total files processed so far: {}",
                fileChunk.size(), snapshot.getVersion(), filesProcessedCount);

            // Convert AddFiles to DeltaSourceSplits
            List<DeltaSourceSplit> splits = prepareSplits(
                new ChangesPerVersion<>(
                    SourceUtils.pathToString(deltaTablePath),
                    snapshot.getVersion(),
                    fileChunk),
                alreadyProcessedPaths::add);

            // Invoke the callback with the splits from this chunk
            processCallback.accept(splits);

        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to process file chunk from snapshot version " + snapshot.getVersion()
                    + ". Files processed so far: " + filesProcessedCount, e);
        }
    }

    /**
     * Checks if there are more files to process from the snapshot.
     *
     * @return {@code true} if more files are available, {@code false} if all files have been
     * processed
     */
    public boolean hasMoreFiles() {
        if (chunkedIterator == null) {
            // First call - we haven't started processing yet, so there are files to process
            return true;
        }
        return chunkedIterator.hasMoreChunks();
    }

    @Override
    public DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> snapshotState(
        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder) {

        checkpointBuilder.withProcessedPaths(alreadyProcessedPaths);

        // false means that this processor does not check Delta table for changes.
        checkpointBuilder.withMonitoringForChanges(false);

        // Persist chunked file loading state for checkpoint recovery
        checkpointBuilder.withFilesProcessedCount(filesProcessedCount);
        checkpointBuilder.withHasMoreFiles(hasMoreFiles());

        LOG.debug("Snapshot state: filesProcessedCount={}, hasMoreFiles={}",
            filesProcessedCount, hasMoreFiles());

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
