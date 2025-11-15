package io.delta.flink.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Split enumerator for CheckpointCountingSource.
 * Creates one split per subtask on startup.
 */
public class CheckpointCountingSplitEnumerator
    implements SplitEnumerator<CheckpointCountingSplit, CheckpointCountingSplitState> {

    private static final Logger LOGGER =
        LoggerFactory.getLogger(CheckpointCountingSplitEnumerator.class);

    private final SplitEnumeratorContext<CheckpointCountingSplit> context;
    private final List<CheckpointCountingSplit> remainingSplits;

    public CheckpointCountingSplitEnumerator(
            SplitEnumeratorContext<CheckpointCountingSplit> context,
            int numberOfCheckpoints,
            int recordsPerCheckpoint) {
        this.context = context;
        this.remainingSplits = new ArrayList<>();

        // Create one split per subtask
        int parallelism = context.currentParallelism();
        for (int i = 0; i < parallelism; i++) {
            remainingSplits.add(new CheckpointCountingSplit("split-" + i, i));
        }

        LOGGER.info("Created {} splits for {} subtasks", remainingSplits.size(), parallelism);
    }

    public CheckpointCountingSplitEnumerator(
            SplitEnumeratorContext<CheckpointCountingSplit> context,
            CheckpointCountingSplitState state) {
        this.context = context;
        this.remainingSplits = new ArrayList<>(state.getRemainingSplits());

        LOGGER.info("Restored {} splits from checkpoint", remainingSplits.size());
    }

    @Override
    public void start() {
        // Assign splits immediately
        assignSplits();
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        // This source assigns splits eagerly on start, so no splits should be requested
        LOGGER.warn("Received unexpected split request from subtask {}", subtaskId);
    }

    @Override
    public void addSplitsBack(List<CheckpointCountingSplit> splits, int subtaskId) {
        LOGGER.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        remainingSplits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // When a new reader is added (e.g., after recovery), assign its split
        LOGGER.info("Reader {} added/registered", subtaskId);
        assignSplits();
    }

    @Override
    public CheckpointCountingSplitState snapshotState(long checkpointId) {
        LOGGER.debug("Snapshotting state for checkpoint {}: {} remaining splits",
            checkpointId, remainingSplits.size());
        return new CheckpointCountingSplitState(new ArrayList<>(remainingSplits));
    }

    @Override
    public void close() throws IOException {
        // Nothing to close
    }

    private void assignSplits() {
        for (CheckpointCountingSplit split : new ArrayList<>(remainingSplits)) {
            int subtaskId = split.getSubtaskId();
            if (context.registeredReaders().containsKey(subtaskId)) {
                context.assignSplit(split, subtaskId);
                remainingSplits.remove(split);
                LOGGER.info("Assigned split {} to subtask {}", split.splitId(), subtaskId);
            }
        }

        // Signal no more splits for all registered readers
        for (int subtaskId : context.registeredReaders().keySet()) {
            context.signalNoMoreSplits(subtaskId);
        }
    }
}

