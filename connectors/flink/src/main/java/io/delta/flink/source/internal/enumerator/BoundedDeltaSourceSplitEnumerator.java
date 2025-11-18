package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.enumerator.processor.SnapshotProcessor;
import io.delta.flink.source.internal.enumerator.processor.TableProcessor;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;

/**
 * A SplitEnumerator implementation for
 * {@link org.apache.flink.api.connector.source.Boundedness#BOUNDED}
 * mode.
 *
 * <p>This enumerator takes all files that are present in the configured Delta table directory,
 * converts them to {@link DeltaSourceSplit} and assigns them to the readers. Once all files are
 * processed, the source is finished.
 *
 * <p>The actual logic for creating the set of
 * {@link DeltaSourceSplit} to process, and the logic to decide which reader gets what split can be
 * found {@link DeltaSourceSplitEnumerator} and in {@link FileSplitAssigner}, respectively.
 */
public class BoundedDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    /**
     * The {@link TableProcessor} used to process Delta table data.
     */
    private final TableProcessor snapshotProcessor;

    public BoundedDeltaSourceSplitEnumerator(
        Path deltaTablePath, SnapshotProcessor snapshotProcessor,
        FileSplitAssigner splitAssigner, SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        super(deltaTablePath, splitAssigner, enumContext);
        this.snapshotProcessor = snapshotProcessor;
    }

    /**
     * Starts Delta table processing.
     *
     * <p><strong>Chunked Processing:</strong> With the chunked file loading implementation,
     * this method processes the first chunk of files from the snapshot. Subsequent chunks
     * are processed automatically after splits are assigned (see {@link #assignSplits()}).
     *
     * <p>This ensures that we don't load all files into memory at once for tables with
     * millions of files, while still providing continuous split assignment to readers.
     */
    @Override
    public void start() {
        processNextChunk();
    }

    /**
     * Processes the next chunk of files from the snapshot and adds them as splits.
     *
     * <p>This method delegates to {@link SnapshotProcessor#process(java.util.function.Consumer)}
     * which handles the chunked file loading internally. If more chunks are available after
     * this call, they will be processed when {@link #assignSplits()} is called.
     */
    private void processNextChunk() {
        if (snapshotProcessor.hasMoreFiles()) {
            snapshotProcessor.process(this::addSplits);
        }
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {

        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder =
            DeltaEnumeratorStateCheckpointBuilder
                .builder(
                    deltaTablePath, snapshotProcessor.getSnapshotVersion(), getRemainingSplits());

        checkpointBuilder = snapshotProcessor.snapshotState(checkpointBuilder);
        return checkpointBuilder.build();
    }

    /**
     * Overrides the default split assignment to support chunked file loading.
     *
     * <p>When no more splits are available in the current chunk but more files exist
     * in the snapshot, this method automatically processes the next chunk before signaling
     * "no more splits" to readers.
     *
     * <p>This ensures continuous split assignment for tables with millions of files without
     * loading all files into memory at once.
     *
     * @return the status of the split assignment operation
     */
    @Override
    protected AssignSplitStatus assignSplits() {
        AssignSplitStatus status = super.assignSplits();

        // If no more splits in current batch but more files available, process next chunk
        if (status == AssignSplitStatus.NO_MORE_SPLITS && snapshotProcessor.hasMoreFiles()) {
            processNextChunk();
            // Try assigning splits from the newly loaded chunk
            status = super.assignSplits();
        }

        return status;
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        enumContext.signalNoMoreSplits(subtaskId);
    }
}
