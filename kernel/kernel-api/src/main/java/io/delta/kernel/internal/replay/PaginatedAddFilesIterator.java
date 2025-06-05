package io.delta.kernel.internal.replay;

import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * We don't return partial batch. If page size is returned, we terminate pagination early.
 * We use number of batches to skip as page token.
* */
public class PaginatedAddFilesIterator implements CloseableIterator<FilteredColumnarBatch> {

    private final Iterator<FilteredColumnarBatch> originalIterator;
    private final long numBatchesToSkip;  // how many files to skip
    private final long pageSize;   // max files to return in this page

    private long numBatchesRead = 0;
    private long numFilesReturned = 0;

    private FilteredColumnarBatch nextBatch = null;  // next batch ready to return

    // make a page token class: 1. how many addFiles we should skip
    // add a bunch of logging , when you start a page, end a page
    // add a test (simple test): see a test & log - verifies data is correct
    // craete a table with id: 0 -1000 (in first commit)
    // think of edge cases - sidecar, log segment issue
    // skip files: insufficient page token , skip 'entire checkpoint files' - edge case
    // which files are special: first json files, last json files, first checkpoint files
    
    public PaginatedAddFilesIterator(Iterator<FilteredColumnarBatch> originalIterator, long numBatchesToSkip, long pageSize) {
        this.originalIterator = originalIterator;
        this.numBatchesToSkip = numBatchesToSkip;
        this.pageSize = pageSize;
    }

    @Override
    public boolean hasNext() {
        if (nextBatch != null) {
            return true;
        }
        if (numFilesReturned >= pageSize) {
            return false; // page limit reached
        }
        while (originalIterator.hasNext()) {
            FilteredColumnarBatch batch = originalIterator.next();

            int batchSize = batch.getSelectionVector().get().getSize(); // batch size = num of AddFile + num of RemoveFile
            int numActiveAddFiles = batchSize; // we don't have good way to evaluate this right now, numActiveAddFiles = batchSize is there is no RemoveFile action

            if (numBatchesRead <= numBatchesToSkip) {
                // skip whole batch
                numBatchesRead++;
            } else if (numFilesReturned + numActiveAddFiles > pageSize ) {
                // terminate current pagination
                return false;
            } else {
                // no skipping needed, return full batch
                nextBatch = batch;
                numFilesReturned += numActiveAddFiles;
                numBatchesRead++;
                return true;
            }
        }
        return false;
    }

    @Override
    public FilteredColumnarBatch next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        FilteredColumnarBatch result = nextBatch;
        nextBatch = null;
        return result;
    }

    @Override
    public void close() throws IOException {
        // Close original iterator if it supports close (if applicable)
        if (originalIterator instanceof Closeable) {
            ((Closeable) originalIterator).close();
        }
    }

    /**
     * Get the next page token representing how many batches to skip in next page request
     * Caller can use this token for next page request.
     */
    public long getNextPageToken() {
        return numBatchesRead;
    }
}
