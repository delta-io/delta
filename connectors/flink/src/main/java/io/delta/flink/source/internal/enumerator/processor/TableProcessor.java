package io.delta.flink.source.internal.enumerator.processor;

import java.util.List;
import java.util.function.Consumer;

import io.delta.flink.source.internal.state.DeltaEnumeratorStateCheckpointBuilder;
import io.delta.flink.source.internal.state.DeltaSourceSplit;

/**
 * A processor for Delta table data.
 * <p>
 * The implementations of this interface should encapsulate logic for processing Delta table Changes
 * and Add Files.
 */
public interface TableProcessor {

    /**
     * Process Delta table data. Can call {@code processCallback} during this process.
     *
     * @param processCallback A {@link Consumer} callback that can be called during Delta table
     *                        processing. The exact condition when this callback will be called
     *                        depends on {@code TableProcessor} implementation.
     */
    void process(Consumer<List<DeltaSourceSplit>> processCallback);

    /**
     * @return A {@link io.delta.standalone.Snapshot} version on which this processor operates.
     */
    long getSnapshotVersion();

    /**
     * Add {@link TableProcessor} state information to {@link DeltaEnumeratorStateCheckpointBuilder}
     * to be stored in Flink's checkpoint.
     * <p>
     * The implementation of this method should add the latest state information to {@link
     * DeltaEnumeratorStateCheckpointBuilder} needed to recreate {@link TableProcessor} instance
     * during Flink recovery.
     *
     * @param checkpointBuilder the {@link DeltaEnumeratorStateCheckpointBuilder} instance that
     *                          should be updated with {@link TableProcessor} state information.
     * @return the {@link DeltaEnumeratorStateCheckpointBuilder} instance with {@link
     * TableProcessor} state information.
     */
    DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> snapshotState(
        DeltaEnumeratorStateCheckpointBuilder<DeltaSourceSplit> checkpointBuilder);
}
