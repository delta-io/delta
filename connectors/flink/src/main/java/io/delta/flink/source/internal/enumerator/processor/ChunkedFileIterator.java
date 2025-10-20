package io.delta.flink.source.internal.enumerator.processor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;

/**
 * A wrapper around {@link CloseableIterator} that processes Delta table files in configurable
 * chunks/batches.
 *
 * <p>This is critical for tables with millions of files where loading all files at once would
 * cause memory pressure on the Flink JobManager. By processing files in chunks (e.g., 10K files
 * at a time), we can enumerate large tables efficiently while maintaining low memory footprint.
 *
 * <p><strong>Usage Example:</strong>
 * <pre>{@code
 * CloseableIterator<AddFile> fileIterator = snapshot.scan().getFiles();
 * ChunkedFileIterator chunkedIterator = new ChunkedFileIterator(fileIterator, 10000);
 *
 * while (chunkedIterator.hasMoreChunks()) {
 *     List<AddFile> chunk = chunkedIterator.nextChunk();
 *     // Process chunk...
 * }
 * chunkedIterator.close();
 * }</pre>
 *
 * <p><strong>Thread Safety:</strong> This class is NOT thread-safe. It should be used by a
 * single thread only.
 */
public class ChunkedFileIterator implements AutoCloseable {

    /**
     * The underlying iterator from Delta Standalone.
     */
    private final CloseableIterator<AddFile> delegate;

    /**
     * The maximum number of files to include in each chunk.
     */
    private final int chunkSize;

    /**
     * The total number of files processed so far across all chunks.
     */
    private long filesProcessedCount;

    /**
     * Flag indicating if we've already checked whether more files exist.
     * Used to avoid unnecessary hasNext() calls.
     */
    private boolean hasCheckedForMore;

    /**
     * Cached result of hasNext() to avoid repeated calls.
     */
    private boolean cachedHasMore;

    /**
     * The number of files to skip before starting to read chunks.
     * Used for checkpoint recovery to resume from the correct position.
     */
    private final long skipCount;

    /**
     * Creates a new ChunkedFileIterator.
     *
     * @param delegate the underlying Delta file iterator
     * @param chunkSize the maximum number of files per chunk (must be > 0)
     */
    public ChunkedFileIterator(CloseableIterator<AddFile> delegate, int chunkSize) {
        this(delegate, chunkSize, 0L);
    }

    /**
     * Creates a new ChunkedFileIterator with a skip count for checkpoint recovery.
     *
     * @param delegate the underlying Delta file iterator
     * @param chunkSize the maximum number of files per chunk (must be > 0)
     * @param skipCount the number of files to skip before reading (for recovery)
     */
    public ChunkedFileIterator(CloseableIterator<AddFile> delegate, int chunkSize,
        long skipCount) {
        if (chunkSize <= 0) {
            throw new IllegalArgumentException(
                "Chunk size must be positive, got: " + chunkSize);
        }
        if (skipCount < 0) {
            throw new IllegalArgumentException(
                "Skip count must be non-negative, got: " + skipCount);
        }

        this.delegate = delegate;
        this.chunkSize = chunkSize;
        this.skipCount = skipCount;
        this.filesProcessedCount = 0L;
        this.hasCheckedForMore = false;
        this.cachedHasMore = false;

        // Skip files if recovering from checkpoint
        skipFilesForRecovery();
    }

    /**
     * Skips files that were already processed before checkpoint recovery.
     * This ensures we resume from the correct position.
     */
    private void skipFilesForRecovery() {
        if (skipCount == 0) {
            return;
        }

        long skipped = 0;
        while (skipped < skipCount) {
            if (!hasNext()) {
                break;
            }
            next();
            skipped++;
        }
        filesProcessedCount = skipped;
    }

    /**
     * Checks if there are more file chunks to process.
     *
     * @return {@code true} if more chunks are available, {@code false} otherwise
     */
    public boolean hasMoreChunks() {
        if (!hasCheckedForMore) {
            cachedHasMore = hasNext();
            hasCheckedForMore = true;
        }
        return cachedHasMore;
    }

    /**
     * Retrieves the next chunk of files.
     *
     * <p>The returned list will contain up to {@link #chunkSize} files, but may contain fewer
     * if we've reached the end of the file list.
     *
     * @return a list of AddFile objects (never null, but may be empty if no more files)
     * @throws IllegalStateException if called when {@link #hasMoreChunks()} returns false
     */
    public List<AddFile> nextChunk() {
        if (!hasMoreChunks()) {
            throw new IllegalStateException(
                "No more chunks available. Check hasMoreChunks() before calling nextChunk()");
        }

        List<AddFile> chunk = new ArrayList<>(chunkSize);
        int filesInChunk = 0;

        while (filesInChunk < chunkSize && hasNext()) {
            AddFile file = next();
            chunk.add(file);
            filesInChunk++;
            filesProcessedCount++;
        }

        // Reset the hasNext cache since we've consumed some elements
        hasCheckedForMore = false;

        return chunk;
    }

    /**
     * Wrapper for delegate.hasNext() for consistent naming.
     */
    private boolean hasNext() {
        return delegate.hasNext();
    }

    /**
     * Wrapper for delegate.next() for consistent naming.
     */
    private AddFile next() {
        return delegate.next();
    }

    /**
     * Gets the total number of files processed so far across all chunks.
     *
     * @return the count of files processed
     */
    public long getFilesProcessedCount() {
        return filesProcessedCount;
    }

    /**
     * Gets the configured chunk size.
     *
     * @return the maximum number of files per chunk
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * Closes the underlying iterator and releases any associated resources.
     *
     * @throws IOException if an error occurs while closing
     */
    @Override
    public void close() throws IOException {
        delegate.close();
    }
}

