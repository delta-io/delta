package io.delta.flink.source.internal.enumerator;

import io.delta.flink.source.internal.enumerator.processor.ContinuousTableProcessor;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpoint;
import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.core.fs.Path;

/**
 * A SplitEnumerator implementation for
 * {@link org.apache.flink.api.connector.source.Boundedness#CONTINUOUS_UNBOUNDED}
 * mode.
 *
 * <p>This enumerator takes all files that are present in the configured Delta table directory,
 * convert them to {@link DeltaSourceSplit} and assigns them to the readers. Once all files from
 * initial snapshot are processed, it starts monitoring Delta table for changes. Each appending data
 * change is converted to {@code DeltaSourceSplit} and assigned to readers.
 * <p>
 * <p>
 * <p>The actual logic for creating the set of {@link DeltaSourceSplit} to process, and the logic
 * to decide which reader gets what split can be found {@link DeltaSourceSplitEnumerator} and in
 * {@link FileSplitAssigner}, respectively.
 */
public class ContinuousDeltaSourceSplitEnumerator extends DeltaSourceSplitEnumerator {

    private final ContinuousTableProcessor continuousTableProcessor;

    public ContinuousDeltaSourceSplitEnumerator(
        Path deltaTablePath, ContinuousTableProcessor continuousTableProcessor,
        FileSplitAssigner splitAssigner, SplitEnumeratorContext<DeltaSourceSplit> enumContext) {

        super(deltaTablePath, splitAssigner, enumContext);

        this.continuousTableProcessor = continuousTableProcessor;
    }

    /**
     * Starts Delta table processing.
     */
    @Override
    public void start() {
        continuousTableProcessor.process(deltaSourceSplits -> {
            addSplits(deltaSourceSplits);
            assignSplits();
        });
    }

    @Override
    public DeltaEnumeratorStateCheckpoint<DeltaSourceSplit> snapshotState(long checkpointId)
        throws Exception {

        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder =
            DeltaEnumeratorStateCheckpointBuilder
                .builder(
                    deltaTablePath, continuousTableProcessor.getSnapshotVersion(),
                    getRemainingSplits());

        checkpointBuilder = continuousTableProcessor.snapshotState(checkpointBuilder);
        return checkpointBuilder.build();
    }

    @Override
    protected void handleNoMoreSplits(int subtaskId) {
        // We should do nothing, since we are continuously monitoring Delta table.
    }
}
