package io.delta.kernel.client;

import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.utils.CloseableIterator;

import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Provides file system related functionalities to Delta Kernel. Delta Kernel uses this client
 * whenever it needs to access the underlying file system where the Delta table is present.
 * Connector implementation of this interface can hide filesystem specific details from Delta
 * Kernel.
 */
public interface FileSystemClient
{
    /**
     * List the paths in the same directory that are lexicographically greater or equal to
     * (UTF-8 sorting) the given `path`. The result should also be sorted by the file name.
     *
     * @param filePath Fully qualified path to a file
     * @return Closeable iterator of files. It is the responsibility of the caller to close the
     *         iterator.
     * @throws FileNotFoundException if the file at the given path is not found
     */
    CloseableIterator<FileStatus> listFrom(String filePath)
            throws FileNotFoundException;
}
