package io.delta.storage.internal;

import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;

/**
 * Helper for misc functions relating to file names for delta commits.
 */
public final class FileNameUtils {
    static Pattern DELTA_FILE_PATTERN = Pattern.compile("\\d+\\.json");
    static Pattern DELTA_FILE_PREFIX_PATTERN = Pattern.compile("\\d{20}\\.");

    /**
     * Returns the delta (json format) path for a given delta file.
     */
    public static Path deltaFile(Path path, long version) {
        return new Path(path, String.format("%020d.json", version));
    }

    /**
     * Returns the version for the given delta path.
     */
    public static long deltaVersion(Path path) {
        return Long.parseLong(path.getName().split("\\.")[0]);
    }

    /**
     * Returns true if the given path is a delta file, else false.
     */
    public static boolean isDeltaFile(Path path) {
        return DELTA_FILE_PATTERN.matcher(path.getName()).matches();
    }

    /**
     * Returns true if the given path is a delta file prefix, else false.
     */
    public static boolean isDeltaFilePrefix(Path path) {
        return DELTA_FILE_PREFIX_PATTERN.matcher(path.getName()).matches();
    }
}
