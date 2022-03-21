package io.delta.storage.internal;

import org.apache.hadoop.fs.Path;

import java.util.regex.Pattern;

public class FileNameUtils {
    static Pattern DELTA_FILE_PATTERN = Pattern.compile("\\d+\\.json");

    public static long deltaVersion(Path path) {
        return Long.parseLong(path.getName().split("\\.")[0]);
    }

    public static boolean isDeltaFile(Path path) {
        return DELTA_FILE_PATTERN.matcher(path.getName()).matches();
    }
}
