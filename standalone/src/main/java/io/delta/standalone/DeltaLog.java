package io.delta.standalone;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import io.delta.standalone.internal.DeltaLogImpl;

/**
 * Used to query (read-only) the current state of the log.
 * <p>
 * Internally, this class implements an optimistic concurrency control
 * algorithm to handle multiple readers or writers. Any single read
 * is guaranteed to see a consistent snapshot of the table.
 */
public interface DeltaLog {

    /**
     * @return the current snapshot. Note this does not automatically call {@code update}.
     */
    Snapshot snapshot();

    /**
     * Update DeltaLog by applying the new delta files if any.
     *
     * @return the updated snapshot
     */
    Snapshot update();

    /**
     * Travel back in time to the snapshot with the provided {@code version} number.
     *
     * @param version  the snapshot version to generate
     * @return the snapshot at the provided {@code version}
     * @throws IllegalArgumentException if the {@code version} is outside the range of available versions
     */
    Snapshot getSnapshotForVersionAsOf(long version);

    /**
     * Travel back in time to the latest snapshot that was generated at or before {@code timestamp}.
     *
     * @param timestamp  the number of miliseconds since midnight, January 1, 1970 UTC
     * @return the snapshot nearest to, but not after, the provided {@code timestamp}
     * @throws RuntimeException if the snapshot is unable to be recreated
     * @throws IllegalArgumentException if the {@code timestamp} is before the earliest possible snapshot or after the latest possible snapshot
     */
    Snapshot getSnapshotForTimestampAsOf(long timestamp);

    /**
     * @return the path to the {@code _delta_log} files for this log
     */
    Path getLogPath();

    /**
     * @return the path to the data files for this log
     */
    Path getDataPath();

    /**
     * Helper for creating a log when it is stored at the root of the data.
     *
     * @param hadoopConf  Hadoop Configuration for this log
     * @param dataPath  the path to the data files for this log
     * @return the new instance of {@code DeltaLog} for the provided {@code dataPath}
     */
    static DeltaLog forTable(Configuration hadoopConf, String dataPath) {
        return DeltaLogImpl.forTable(hadoopConf, dataPath);
    }

    /**
     * Helper for creating a log when it is stored at the root of the data.
     *
     * @param hadoopConf  Hadoop Configuration for this log
     * @param dataPath  the path to the data files for this log
     * @return the new instance of {@code DeltaLog} for the provided {@code dataPath}
     */
    static DeltaLog forTable(Configuration hadoopConf, File dataPath) {
        return DeltaLogImpl.forTable(hadoopConf, dataPath);
    }

    /**
     * Helper for creating a log when it is stored at the root of the data.
     *
     * @param hadoopConf  Hadoop Configuration for this log
     * @param dataPath  the path to the data files for this log
     * @return the new instance of {@code DeltaLog} for the provided {@code dataPath}
     */
    static DeltaLog forTable(Configuration hadoopConf, Path dataPath) {
        return DeltaLogImpl.forTable(hadoopConf, dataPath);
    }
}
