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
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.CheckpointAlreadyExistsException;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
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
     *         <li>Reads will fail with a {@link TableNotFoundException}</li>
     *         <li>Writes will create the location</li>
     *         </ul>
     *     </li>
     *     <li>
     *         Behavior when the table location exists (with contents or not) but not a Delta table:
     *         <ul>
     *          <li>Reads will fail with a {@link TableNotFoundException}</li>
     *          <li>Writes will create a Delta table at the given location. If there are any
     *          existing files in the location that are not already part of the Delta table, they
     *          will remain excluded from the Delta table.</li>
     *         </ul>
     *     </li>
     * </ul>
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @param path        location of the table. Path is resolved to fully
     *                    qualified path using the given {@code engine}.
     * @return an instance of {@link Table} representing the Delta table at the given path
     */
    static Table forPath(Engine engine, String path) {
        return TableImpl.forPath(engine, path);
    }

    /**
     * The fully qualified path of this {@link Table} instance.
     *
     * @param engine {@link Engine} instance.
     * @return the table path.
     * @since 3.2.0
     */
    String getPath(Engine engine);

    /**
     * Get the latest snapshot of the table.
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @return an instance of {@link Snapshot}
     * @throws TableNotFoundException if the table is not found
     */
    Snapshot getLatestSnapshot(Engine engine)
        throws TableNotFoundException;

    /**
     * Get the snapshot at the given {@code versionId}.
     *
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @param versionId snapshot version to retrieve
     * @return an instance of {@link Snapshot}
     * @throws TableNotFoundException if the table is not found
     * @throws KernelException if the provided version is less than the first available version
     *                         or greater than the last available version
     * @since 3.2.0
     */
    Snapshot getSnapshotAsOfVersion(Engine engine, long versionId)
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
     * @param engine {@link Engine} instance to use in Delta Kernel.
     * @param millisSinceEpochUTC timestamp to fetch the snapshot for in milliseconds since the
     *                            unix epoch
     * @return an instance of {@link Snapshot}
     * @throws TableNotFoundException if the table is not found
     * @throws KernelException if the provided timestamp is before the earliest available version or
     *                         after the latest available version
     * @since 3.2.0
     */
    Snapshot getSnapshotAsOfTimestamp(Engine engine, long millisSinceEpochUTC)
        throws TableNotFoundException;

    /**
     * Create a {@link TransactionBuilder} which can create a {@link Transaction} object to mutate
     * the table.
     *
     * @param engine {@link Engine} instance to use.
     * @param engineInfo information about the engine that is making the updates.
     * @param operation metadata of operation that is being performed. E.g. "insert", "delete".
     * @return {@link TransactionBuilder} instance to build the transaction.
     * @since 3.2.0
     */
    TransactionBuilder createTransactionBuilder(
            Engine engine,
            String engineInfo,
            Operation operation);

    /**
     * Checkpoint the table at given version. It writes a single checkpoint file.
     *
     * @param engine {@link Engine} instance to use.
     * @param version     Version to checkpoint.
     * @throws TableNotFoundException if the table is not found
     * @throws CheckpointAlreadyExistsException if a checkpoint already exists at the given version
     * @throws IOException for any I/O error.
     * @since 3.2.0
     */
    void checkpoint(Engine engine, long version)
            throws TableNotFoundException, CheckpointAlreadyExistsException, IOException;
}
