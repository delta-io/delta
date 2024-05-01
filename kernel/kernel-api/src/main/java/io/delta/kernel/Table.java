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

import java.io.IOException;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.exceptions.KernelException;

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
     * <ul>
     *     <li>
     *         Behavior when the table location doesn't exist:
     *         <ul>
     *         <li>reads will fail with a {@link TableNotFoundException}}</li>
     *         <li>writes will create the location</li>
     *         </ul>
     *     </li>
     *     <li>
     *         Behavior when the table location exists (with contents or not) but not a Delta table
     *         <ul>
     *          <li>reads will fail with a {@link TableNotFoundException}</li>
     *          <li>writes will create a Delta table to the given location. If there are any
     *          existing files in the location that are not already part of the Delta table, they
     *          will remain excluded as part of the Delta table.</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param path        location of the table. Path is resolved to fully
     *                    qualified path using the given {@code tableClient}.
     * @return an instance of {@link Table} representing the Delta table at given path
     */
    static Table forPath(TableClient tableClient, String path) {
        return TableImpl.forPath(tableClient, path);
    }

    /**
     * The fully qualified path of this {@link Table} instance.
     *
     * @param tableClient {@link TableClient} instance.
     * @return the table path.
     * @since 3.2.0
     */
    String getPath(TableClient tableClient);

    /**
     * Get the latest snapshot of the table.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @return an instance of {@link Snapshot}
     */
    Snapshot getLatestSnapshot(TableClient tableClient)
        throws TableNotFoundException;

    /**
     * Get the snapshot at the given {@code versionId}.
     *
     * @param tableClient {@link TableClient} instance to use in Delta Kernel.
     * @param versionId snapshot version to retrieve
     * @return an instance of {@link Snapshot}
     * @throws KernelException if the provided version is less than the first available version
     *                         or greater than the last available version
     * @since 3.2.0
     */
    Snapshot getSnapshotAsOfVersion(TableClient tableClient, long versionId)
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
     * @throws KernelException if the provided timestamp is before the earliest available version or
     *                         after the latest available version
     * @since 3.2.0
     */
    Snapshot getSnapshotAsOfTimestamp(TableClient tableClient, long millisSinceEpochUTC)
        throws TableNotFoundException;

    /**
     * Checkpoint the table at given version. It writes a single checkpoint file.
     *
     * @param tableClient {@link TableClient} instance to use.
     * @param version     Version to checkpoint.
     * @throws TableNotFoundException if the table is not found
     * @throws CheckpointAlreadyExistsException if a checkpoint already exists at the given version
     * @throws IOException for any I/O error.
     * @since 3.2.0
     */
    void checkpoint(TableClient tableClient, long version)
            throws TableNotFoundException, CheckpointAlreadyExistsException, IOException;
}
