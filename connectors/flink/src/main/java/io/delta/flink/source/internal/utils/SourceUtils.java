package io.delta.flink.source.internal.utils;

import org.apache.flink.core.fs.Path;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A utility class for Source connector
 */
public final class SourceUtils {

    private SourceUtils() {

    }

    /**
     * Converts Flink's {@link Path} to String
     *
     * @param path Flink's {@link Path}
     * @return String representation of {@link Path}
     */
    public static String pathToString(Path path) {
        checkArgument(path != null, "Path argument cannot be be null.");
        return path.toUri().normalize().toString();
    }
}
