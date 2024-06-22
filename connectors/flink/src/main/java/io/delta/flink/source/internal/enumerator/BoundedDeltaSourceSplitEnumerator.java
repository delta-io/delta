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
     */
    @Override
    public void start() {
        snapshotProcessor.process(this::addSplits);
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

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        enumContext.signalNoMoreSplits(subtaskId);
    }
}
