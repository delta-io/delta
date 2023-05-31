package io.delta.kernel.fs;

import java.util.Objects;

/**
 * Class for encapsulating metadata about a file in Delta Lake table.
 */
public class FileStatus {

    private final String path;
    private final long size;
    private final long modificationTime;
    private final boolean hasDeletionVector;

    private FileStatus(
            String path,
            long size,
            long modificationTime,
            boolean hasDeletionVector) {
        this.path = Objects.requireNonNull(path, "path is null");
        this.size = size; // TODO: validation
        this.modificationTime = modificationTime; // TODO: validation
        this.hasDeletionVector = hasDeletionVector;
    }

    /**
     * Get the path to the file.
     * @return Fully qualified file path
     */
    public String getPath() {
        return path;
    }

    /**
     * Get the size of the file in bytes.
     * @return File size in bytes.
     */
    public long getSize()
    {
        return size;
    }

    /**
     * Get the modification time of the file in epoch millis.
     * @return Modification time in epoch millis
     */
    public long getModificationTime()
    {
        return modificationTime;
    }

    /**
     * Whether this file has any associated deletion vector?
     * @return True if the file has an associated deletion vector. Otherwise false.
     */
    public boolean isHasDeletionVector()
    {
        return hasDeletionVector;
    }

    /**
     * Create a {@link FileStatus} representing the given path and file size.
     * @param path Fully qualified file path.
     * @param size File size in bytes
     * @param modificationTime Modification time of the file in epoch millis
     * @return
     */
    public static FileStatus of(String path, long size, long modificationTime) {
        return new FileStatus(path, size, modificationTime, false);
    }

    /**
     * Create a {@link FileStatus}.
     *
     * @param path Fully qualified file path.
     * @param size File size in bytes
     * @param modificationTime Modification time of the file in epoch millis
     * @param hasDeletionVector Whether the file has an associated deletion vector file.
     * @return
     */
    public static FileStatus of(
            String path,
            long size,
            long modificationTime,
            boolean hasDeletionVector) {
        return new FileStatus(path, size, modificationTime, hasDeletionVector);
    }
}
