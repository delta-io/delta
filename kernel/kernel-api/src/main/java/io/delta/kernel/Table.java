/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.client.TableClient;

import io.delta.kernel.internal.TableImpl;

/**
 * Represents the Delta Lake table for a given path.
 *
 * @since 3.0.0
 */
@Evolving
public interface Table {
    /**
     * Instantiate a table object for the Delta Lake table at the given path.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param path        location where the Delta table is present. Path is resolved to fully
     *                    qualified path using the given {@code tableClient}.
     * @return an instance of {@link Table} representing the Delta table at given path
     * @throws TableNotFoundException when there is no Delta table at the given path.
     */
    static Table forPath(TableClient tableClient, String path)
        throws TableNotFoundException {
        return TableImpl.forPath(tableClient, path);
    }

    /**
     * Get the latest snapshot of the table.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return an instance of {@link Snapshot}
     */
    Snapshot getLatestSnapshot(TableClient tableClient)
        throws TableNotFoundException;

    /**
     * The fully qualified path of this {@link Table} instance.
     *
     * @return the table path
     */
    String getPath();

    /**
     * Get the snapshot at the given {@code versionId}.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param versionId snapshot version to retrieve
     * @return an instance of {@link Snapshot}
     */
    Snapshot getSnapshotAtVersion(TableClient tableClient, long versionId)
        throws TableNotFoundException;

    /**
     * Get the snapshot of the table at the given {@code timestamp}. This is the latest version of
     * the table that was committed before or at {@code timestamp}.
     * <p>
     * Specifically:
     * <ul>
     *     <li>If a commit version exactly matches the provided timestamp, we return the table
     *     snapshot at that version.</li>
     *     <li>Else, we return the latest commit version with a timestamp less than the provided
     *     one.</li>
     *     <li>If the provided timestamp is less than the timestamp of any committed version,
     *         we throw an error.</li>
     *     <li>If the provided timestamp is after (strictly greater than) the timestamp of the
     *     latest version of the table, we throw an error</li>
     * </ul>.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param millisSinceEpochUTC timestamp to fetch the snapshot for in milliseconds since the
     *                            unix epoch
     * @return an instance of {@link Snapshot}
     */
    Snapshot getSnapshotAtTimestamp(TableClient tableClient, long millisSinceEpochUTC)
        throws TableNotFoundException;
}
