package io.delta.kernel.client;

import io.delta.kernel.data.Row;
import io.delta.kernel.expressions.Expression;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Provides file handling functionality to Delta Kernel. Delta Kernel can use this client to
 * give the connector an option to provide their best implementation of file splitting,
 * additional predicate pushdown and any other connector specific capability.
 */
public interface FileHandler
{
    /**
     * Associate a connector specific {@link FileReadContext} for each scan file given
     * as {@link Row}. The Delta Kernel uses the returned {@link FileReadContext} to request data
     * from the connector. Delta Kernel doesn't interpret the {@link FileReadContext}. It just
     * passes it back to the connector for interpretation. For example the connector can attach
     * split information in its own implementation of {@link FileReadContext} or attach any
     * predicates.
     *
     * @param fileIter Iterator of {@link FileStatus}
     * @param predicate Predicate to use prune data file. This is optional for the connector to use.
     *                  Delta Kernel doesn't require the connector filtering the data by this
     *                  predicate.
     * @return Iterator of {@link FileReadContext} tuples to read data from.
     */
    CloseableIterator<FileReadContext> contextualizeFileReads(
            CloseableIterator<Row> fileIter,
            Expression predicate);
}
