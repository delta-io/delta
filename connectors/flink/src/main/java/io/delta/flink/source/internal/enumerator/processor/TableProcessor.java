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

    /**
     * Checks if there are more files to process from the Delta table.
     *
     * <p><strong>Chunked File Loading:</strong> For processors that implement chunked file
     * loading (e.g., {@link SnapshotProcessor}), this method returns {@code true} if more
     * file chunks are available to be loaded and processed.
     *
     * <p><strong>Continuous Processing:</strong> For processors that monitor Delta table
     * for changes (e.g., {@link ChangesProcessor}), this method always returns {@code false}
     * since they process changes as they occur rather than enumerating a fixed set of files.
     *
     * <p><strong>Default Behavior:</strong> The default implementation returns {@code false},
     * indicating all files have been processed. Processors that support chunked loading should
     * override this method to track their pagination state.
     *
     * @return {@code true} if more files are available to process, {@code false} if all files
     * have been enumerated or if the processor doesn't support chunked loading
     */
    default boolean hasMoreFiles() {
        return false;
    }
}
