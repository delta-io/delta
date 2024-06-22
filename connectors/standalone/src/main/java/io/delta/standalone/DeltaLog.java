/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.internal.DeltaLogImpl;

/**
 * Represents the transaction logs of a Delta table. It provides APIs to access the states of a
 * Delta table.
 * <p>
 * You can use the following code to create a {@link DeltaLog} instance.
 * <pre>{@code
 *   Configuration conf = ... // Create your own Hadoop Configuration instance
 *   DeltaLog deltaLog = DeltaLog.forTable(conf, "/the/delta/table/path");
 * }</pre>
 */
public interface DeltaLog {

    /**
     * @return the current {@link Snapshot} of the Delta table. You may need to call
     *         {@link #update()} to access the latest snapshot if the current snapshot is stale.
     */
    Snapshot snapshot();

    /**
     * Bring {@link DeltaLog}'s current {@link Snapshot} to the latest state if there are any new
     * transaction logs.
     *
     * @return the latest snapshot after applying the new transaction logs.
     */
    Snapshot update();

    /**
     * Travel back in time to the {@link Snapshot} with the provided {@code version} number.
     *
     * @param version  the snapshot version to generate
     * @return the snapshot at the provided {@code version}
     * @throws IllegalArgumentException if the {@code version} is outside the range of available
     *                                  versions
     */
    Snapshot getSnapshotForVersionAsOf(long version);

    /**
     * Travel back in time to the latest {@link Snapshot} that was generated at or before
     * {@code timestamp}.
     *
     * @param timestamp  the number of milliseconds since midnight, January 1, 1970 UTC
     * @return the snapshot nearest to, but not after, the provided {@code timestamp}
     * @throws RuntimeException if the snapshot is unable to be recreated
     * @throws IllegalArgumentException if the {@code timestamp} is before the earliest possible
     *                                  snapshot or after the latest possible snapshot
     */
    Snapshot getSnapshotForTimestampAsOf(long timestamp);

    /**
     * Returns a new {@link OptimisticTransaction} that can be used to read the current state of the
     * log and then commit updates. The reads and updates will be checked for logical conflicts
     * with any concurrent writes to the log.
     * <p>
     * Note that all reads in a transaction must go through the returned transaction object, and not
     * directly to the {@link DeltaLog} otherwise they will not be checked for conflicts.
     *
     * @return a new {@link OptimisticTransaction}.
     */
    OptimisticTransaction startTransaction();

    /**
     * @param version  the commit version to retrieve {@link CommitInfo}
     * @return the {@link CommitInfo} of the commit at the provided version.
     */
    CommitInfo getCommitInfoAt(long version);

    /** @return the path of the Delta table. */
    Path getPath();

    /**
     * Get all actions starting from {@code startVersion} (inclusive) in increasing order of
     * committed version.
     * <p>
     * If {@code startVersion} doesn't exist, return an empty {@code Iterator}.
     *
     * @param startVersion the table version to begin retrieving actions from (inclusive)
     * @param failOnDataLoss whether to throw when data loss detected
     * @return an {@code Iterator} of {@link VersionLog}s starting from {@code startVersion}
     * @throws IllegalArgumentException if {@code startVersion} is negative
     * @throws IllegalStateException if data loss detected and {@code failOnDataLoss} is true
     */
    Iterator<VersionLog> getChanges(long startVersion, boolean failOnDataLoss);

    /**
     * Returns the latest version that was committed before or at {@code timestamp}. If no version
     * exists, returns -1.
     *
     * Specifically:
     * <ul>
     *     <li>if a commit version exactly matches the provided timestamp, we return it</li>
     *     <li>else, we return the latest commit version with a timestamp less than the
     *         provided one</li>
     *     <li>If the provided timestamp is less than the timestamp of any committed version,
     *         we throw an error.</li>
     * </ul>.
     *
     * @param timestamp the number of milliseconds since midnight, January 1, 1970 UTC
     * @return latest commit that happened before or at {@code timestamp}.
     * @throws IllegalArgumentException if the timestamp is less than the timestamp of any committed
     *                                  version
     */
    long getVersionBeforeOrAtTimestamp(long timestamp);

    /**
     * Returns the latest version that was committed at or after {@code timestamp}. If no version
     * exists, returns -1.
     *
     * Specifically:
     * <ul>
     *     <li>if a commit version exactly matches the provided timestamp, we return it</li>
     *     <li>else, we return the earliest commit version with a timestamp greater than the
     *         provided one</li>
     *     <li>If the provided timestamp is larger than the timestamp of any committed version,
     *         we throw an error.</li>
     * </ul>.
     *
     * @param timestamp the number of milliseconds since midnight, January 1, 1970 UTC
     * @return latest commit that happened at or before {@code timestamp}.
     * @throws IllegalArgumentException if the timestamp is more than the timestamp of any committed
     *                                  version
     */
    long getVersionAtOrAfterTimestamp(long timestamp);

    /**
     * @return Whether a Delta table exists at this directory.
     */
    boolean tableExists();

    /**
     * Create a {@link DeltaLog} instance representing the table located at the provided
     * {@code path}.
     *
     * @param hadoopConf  Hadoop {@code Configuration} to use when accessing the Delta table
     * @param path  the path to the Delta table
     * @return the {@code DeltaLog} for the provided {@code path}
     */
    static DeltaLog forTable(Configuration hadoopConf, String path) {
        return DeltaLogImpl.forTable(hadoopConf, path);
    }

    /**
     * Create a {@link DeltaLog} instance representing the table located at the provided
     * {@code path}.
     *
     * @param hadoopConf  Hadoop {@code Configuration} to use when accessing the Delta table
     * @param path  the path to the Delta table
     * @return the {@code DeltaLog} for the provided {@code path}
     */
    static DeltaLog forTable(Configuration hadoopConf, Path path) {
        return DeltaLogImpl.forTable(hadoopConf, path);
    }
}
