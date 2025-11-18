package io.delta.flink.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SourceReader for CheckpointCountingSource.
 * Implements the core logic of emitting records coordinated with checkpoints.
 */
public class CheckpointCountingSourceReader
    implements SourceReader<RowData, CheckpointCountingSplit>, CheckpointListener {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(CheckpointCountingSourceReader.class);

    private final SourceReaderContext context;
    private final int numberOfCheckpoints;
    private final int recordsPerCheckpoint;
    private final CheckpointCountingSource.RowProducer rowProducer;

    private CheckpointCountingSplit currentSplit;
    private int nextValue;
    private boolean isCanceled;
    private volatile boolean waitingForCheckpoint;
    private int recordsEmittedInCurrentCheckpoint;
    private int checkpointsCompleted;
    private boolean noMoreSplits;

    public CheckpointCountingSourceReader(
            SourceReaderContext context,
            int numberOfCheckpoints,
            int recordsPerCheckpoint,
            CheckpointCountingSource.RowProducer rowProducer) {
        this.context = context;
        this.numberOfCheckpoints = numberOfCheckpoints;
        this.recordsPerCheckpoint = recordsPerCheckpoint;
        this.rowProducer = rowProducer;
        this.nextValue = 0;
        this.isCanceled = false;
        this.waitingForCheckpoint = false;
        this.recordsEmittedInCurrentCheckpoint = 0;
        this.checkpointsCompleted = 0;
        this.noMoreSplits = false;

        LOGGER.info("Created CheckpointCountingSourceReader for subtask {}",
            context.getIndexOfSubtask());
    }

    @Override
    public void start() {
        LOGGER.info("Started CheckpointCountingSourceReader for subtask {}",
            context.getIndexOfSubtask());
    }

    @Override
    public InputStatus pollNext(ReaderOutput<RowData> output) throws Exception {
        if (isCanceled) {
            return InputStatus.END_OF_INPUT;
        }

        if (currentSplit == null) {
            return noMoreSplits ? InputStatus.END_OF_INPUT : InputStatus.NOTHING_AVAILABLE;
        }

        // Check if we've emitted all records
        if (checkpointsCompleted >= numberOfCheckpoints) {
            // Wait for one more checkpoint before finishing (as in original implementation)
            if (waitingForCheckpoint) {
                return InputStatus.NOTHING_AVAILABLE;
            } else {
                waitingForCheckpoint = true;
                return InputStatus.NOTHING_AVAILABLE;
            }
        }

        // If waiting for checkpoint, don't emit more records
        if (waitingForCheckpoint) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        // Emit records for this checkpoint
        if (recordsEmittedInCurrentCheckpoint < recordsPerCheckpoint) {
            RowData row = rowProducer.createRow(nextValue);
            output.collect(row);
            nextValue++;
            recordsEmittedInCurrentCheckpoint++;

            // If we've emitted all records for this checkpoint, wait for checkpoint
            if (recordsEmittedInCurrentCheckpoint >= recordsPerCheckpoint) {
                waitingForCheckpoint = true;
                LOGGER.info("Emitted {} records (total {}), waiting for checkpoint; subtask={}",
                    recordsPerCheckpoint, nextValue, context.getIndexOfSubtask());
            }

            return InputStatus.MORE_AVAILABLE;
        }

        return InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<CheckpointCountingSplit> snapshotState(long checkpointId) {
        LOGGER.info("Snapshot state; checkpointId={}; nextValue={}; subtask={}",
            checkpointId, nextValue, context.getIndexOfSubtask());

        if (currentSplit != null) {
            // Return the current split with updated nextValue
            return Collections.singletonList(currentSplit.withNextValue(nextValue));
        }
        return Collections.emptyList();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        if (isCanceled || (checkpointsCompleted >= numberOfCheckpoints && !waitingForCheckpoint)) {
            return CompletableFuture.completedFuture(null);
        }

        if (waitingForCheckpoint) {
            // Return a future that will be completed when checkpoint completes
            return CompletableFuture.supplyAsync(() -> {
                while (waitingForCheckpoint && !isCanceled) {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
                return null;
            });
        }

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<CheckpointCountingSplit> splits) {
        if (splits.size() != 1) {
            throw new IllegalArgumentException(
                "CheckpointCountingSourceReader expects exactly 1 split, got: " + splits.size());
        }

        CheckpointCountingSplit split = splits.get(0);
        this.currentSplit = split;
        this.nextValue = split.getNextValue();

        LOGGER.info("Assigned split {} with nextValue={} to subtask {}",
            split.splitId(), nextValue, context.getIndexOfSubtask());
    }

    @Override
    public void notifyNoMoreSplits() {
        this.noMoreSplits = true;
        LOGGER.info("No more splits for subtask {}", context.getIndexOfSubtask());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        waitingForCheckpoint = false;
        recordsEmittedInCurrentCheckpoint = 0;

        // Only count checkpoints while we're still emitting records
        if (checkpointsCompleted < numberOfCheckpoints) {
            checkpointsCompleted++;
        }

        LOGGER.info("Checkpoint {} complete (total: {}); subtask={}",
            checkpointId, checkpointsCompleted, context.getIndexOfSubtask());
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        LOGGER.info("Checkpoint {} aborted; subtask={}", checkpointId,
            context.getIndexOfSubtask());
    }

    @Override
    public void close() throws Exception {
        isCanceled = true;
        waitingForCheckpoint = false;
        LOGGER.info("Closed CheckpointCountingSourceReader for subtask {}",
            context.getIndexOfSubtask());
    }
}

