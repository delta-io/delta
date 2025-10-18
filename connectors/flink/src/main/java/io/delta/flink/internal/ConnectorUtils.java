package io.delta.flink.internal;

import java.util.HashSet;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ConnectorUtils {

    public static final String ENGINE_INFO =
        "flink-engine/" + io.delta.flink.internal.Meta.FLINK_VERSION +
            " flink-delta-connector/" + io.delta.flink.internal.Meta.CONNECTOR_VERSION;

    /**
     * Given a path `child`: 1. Returns `child` if the path is already relative 2. Tries
     * relativizing `child` with respect to `basePath` a) If the `child` doesn't live within the
     * same base path, returns `child` as is b) If `child` lives in a different FileSystem, throws
     * an exception Note that `child` may physically be pointing to a path within `basePath`, but
     * may logically belong to a different FileSystem, e.g. DBFS mount points and direct S3 paths.
     */
    public static String tryRelativizePath(FileSystem fs, Path basePath, Path child) {

        if (child.isAbsolute()) {
            try {
                // We can map multiple schemes to the same `FileSystem` class, but `FileSystem
                // .getScheme` is usually just a hard-coded string. Hence, we need to use the
                // scheme of the URI that we use to create the FileSystem here.
                return new Path(
                    fs.makeQualified(basePath).toUri()
                        .relativize(fs.makeQualified(child).toUri())).toString();
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    String.format("Failed to relativize the path (%s)", child), e);
            }
        }
        return child.toString();
    }

    public static <T> boolean listEqualsIgnoreOrder(List<T> list1, List<T> list2) {
        return new HashSet<>(list1).equals(new HashSet<>(list2));
    }

}
