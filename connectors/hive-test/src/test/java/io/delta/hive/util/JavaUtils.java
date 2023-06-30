package io.delta.hive.util;

import java.io.*;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * General utilities available in the network package. Many of these are sourced from Spark's
 * own Utils, just accessible within this package.
 */
public class JavaUtils {
    private static final Logger logger = LoggerFactory.getLogger(JavaUtils.class);


    /**
     * Delete a file or directory and its contents recursively.
     * Don't follow directories if they are symlinks.
     *
     * @param file Input file / dir to be deleted
     * @throws IOException if deletion is unsuccessful
     */
    public static void deleteRecursively(File file) throws IOException {
        deleteRecursively(file, null);
    }

    /**
     * Delete a file or directory and its contents recursively.
     * Don't follow directories if they are symlinks.
     *
     * @param file Input file / dir to be deleted
     * @param filter A filename filter that make sure only files / dirs with the satisfied filenames
     *               are deleted.
     * @throws IOException if deletion is unsuccessful
     */
    public static void deleteRecursively(File file, FilenameFilter filter) throws IOException {
        if (file == null) { return; }

        // On Unix systems, use operating system command to run faster
        // If that does not work out, fallback to the Java IO way
        if (SystemUtils.IS_OS_UNIX && filter == null) {
            try {
                deleteRecursivelyUsingUnixNative(file);
                return;
            } catch (IOException e) {
                logger.warn("Attempt to delete using native Unix OS command failed for path = {}. "
                    + "Falling back to Java IO way", file.getAbsolutePath(), e);
            }
        }

        deleteRecursivelyUsingJavaIO(file, filter);
    }

    private static void deleteRecursivelyUsingJavaIO(
        File file,
        FilenameFilter filter) throws IOException {
        if (file.isDirectory() && !isSymlink(file)) {
            IOException savedIOException = null;
            for (File child : listFilesSafely(file, filter)) {
                try {
                    deleteRecursively(child, filter);
                } catch (IOException e) {
                    // In case of multiple exceptions, only last one will be thrown
                    savedIOException = e;
                }
            }
            if (savedIOException != null) {
                throw savedIOException;
            }
        }

        // Delete file only when it's a normal file or an empty directory.
        if (file.isFile() || (file.isDirectory() && listFilesSafely(file, null).length == 0)) {
            boolean deleted = file.delete();
            // Delete can also fail if the file simply did not exist.
            if (!deleted && file.exists()) {
                throw new IOException("Failed to delete: " + file.getAbsolutePath());
            }
        }
    }

    private static void deleteRecursivelyUsingUnixNative(File file) throws IOException {
        ProcessBuilder builder = new ProcessBuilder("rm", "-rf", file.getAbsolutePath());
        Process process = null;
        int exitCode = -1;

        try {
            // In order to avoid deadlocks, consume the stdout (and stderr) of the process
            builder.redirectErrorStream(true);
            builder.redirectOutput(new File("/dev/null"));

            process = builder.start();

            exitCode = process.waitFor();
        } catch (Exception e) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath(), e);
        } finally {
            if (process != null) {
                process.destroy();
            }
        }

        if (exitCode != 0 || file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath());
        }
    }

    private static File[] listFilesSafely(File file, FilenameFilter filter) throws IOException {
        if (file.exists()) {
            File[] files = file.listFiles(filter);
            if (files == null) {
                throw new IOException("Failed to list files for dir: " + file);
            }
            return files;
        } else {
            return new File[0];
        }
    }

    private static boolean isSymlink(File file) throws IOException {
        Preconditions.checkNotNull(file);
        File fileInCanonicalDir = null;
        if (file.getParent() == null) {
            fileInCanonicalDir = file;
        } else {
            fileInCanonicalDir = new File(file.getParentFile().getCanonicalFile(), file.getName());
        }
        return !fileInCanonicalDir.getCanonicalFile().equals(fileInCanonicalDir.getAbsoluteFile());
    }
}
