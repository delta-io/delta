/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.engine;

import java.io.IOException;
import java.util.Map;

import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.coordinatedcommits.*;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.utils.CloseableIterator;

/**
 * An interface that encapsulates all the functions needed by Kernel to perform commits to a table
 * owned by a Commit Coordinator.
 * Commit coordinator is defined by the Delta Protocol.
 * @see <a href="https://github.com/delta-io/delta/blob/master/protocol_rfcs/managed-commits.md#sample-commit-owner-api">Coordinated commit protocol table feature</a>.
 *
 * @since 3.3.0
 */
@Evolving
public interface CommitCoordinatorClientHandler {

    /**
     * API to register the table represented by the given `logPath` at the provided
     * currentTableVersion with the commit coordinator this commit coordinator client represents.
     * <p>
     * This API is called when the table is being converted from a file system table to a
     * coordinated-commit table.
     * <p>
     * When a new coordinated-commit table is being created, the currentTableVersion will be -1 and
     * the upgrade commit needs to be a file system commit which will write the backfilled file
     * directly.
     *
     * @param logPath         The path to the delta log of the table that should be converted
     * @param currentVersion  The currentTableVersion is the version of the table just before
     *                        conversion. currentTableVersion + 1 represents the commit that
     *                        will do the conversion. This must be backfilled atomically.
     *                        currentTableVersion + 2 represents the first commit after conversion.
     *                        This will go through the CommitCoordinatorClient and the client is
     *                        free to choose when it wants to backfill this commit.
     * @param currentMetadata The metadata of the table at currentTableVersion
     * @param currentProtocol The protocol of the table at currentTableVersion
     * @return A map of key-value pairs which is issued by the commit coordinator to identify the
     * table. This should be stored in the table's metadata. This information needs to be
     * passed to the {@link #commit}, {@link #getCommits}, and {@link #backfillToVersion}
     * APIs to identify the table.
     */
    Map<String, String> registerTable(
            String logPath,
            long currentVersion,
            AbstractMetadata currentMetadata,
            AbstractProtocol currentProtocol);

    /**
     * Commits a set of actions to a specified table at a given version.
     *
     * <p>This method applies the provided actions to the table identified by {@code logPath}
     * as given table version {@code commitVersion}.</p>
     *
     * @param logPath                The path to the delta log of the target table
     * @param tableConf            Configuration details returned by the commit coordinator client
     *                             during table registration
     * @param commitVersion   The version of the commit that is being committed.
     * @param actions                The set of actions to be committed
     * @param updatedActions Additional information for the commit, including:
     *                       <ul>
     *                         <li>Commit info</li>
     *                         <li>Metadata changes</li>
     *                         <li>Protocol changes</li>
     *                       </ul>
     * @return CommitResponse containing the file status of the committed file.
     *         Note: If the commit is already backfilled, the file status may be omitted,
     *         and the client can retrieve this information independently.
     * @throws CommitFailedException if the commit operation fails
     */
    CommitResponse commit(
            String logPath,
            Map<String, String> tableConf,
            long commitVersion,
            CloseableIterator<Row> actions,
            UpdatedActions updatedActions) throws CommitFailedException;

    /**
     * Retrieves unbackfilled commits for a specified table within a given version range.
     *
     * <p>This method fetches commits that have not yet been backfilled for the table identified by
     * {@code logPath}. It returns commits between {@code startVersion} and {@code endVersion}
     * (inclusive), ignoring commits outside this range. The returned commits are guaranteed to be
     * contiguous and in ascending version order.</p>
     *
     * <p><strong>Note:</strong> The first returned version may not equal {@code startVersion} if
     * some versions have already been backfilled and are no longer tracked by the commit
     * coordinator.</p>
     *
     * <p>The {@code latestTableVersion} in the response represents the highest commit version
     * ratified by the commit coordinator. A value of -1 is only valid if the commit coordinator
     * has never ratified any version (i.e., never accepted any unbackfilled commit).</p>
     *
     * @param logPath The path to the delta log of the target table
     * @param tableConf The table configuration returned by the commit coordinator during
     *                  registration
     * @param startVersion The minimum commit version to retrieve (inclusive, can be null)
     * @param endVersion The maximum commit version to retrieve (inclusive, can be null)
     * @return GetCommitsResponse containing:
     *         <ul>
     *           <li>A list of {@link Commit} objects</li>
     *           <li>The {@code latestTableVersion} tracked by
     *           {@link CommitCoordinatorClientHandler}</li>
     *         </ul>
     */
    GetCommitsResponse getCommits(
            String logPath,
            Map<String, String> tableConf,
            Long startVersion,
            Long endVersion);

    /**
     * Requests the commit coordinator client to backfill commits up to a specified version.
     *
     * <p>This method instructs the commit coordinator client to backfill all commits up to the
     * given {@code version} and notify the commit coordinator of the completion. A successful
     * return from this method guarantees that the backfill has been completed, even if the commit
     * coordinator hasn't been notified yet.</p>
     *
     * @param logPath The path to the delta log of the table to be backfilled
     * @param tableConf The table configuration returned by the commit coordinator during
     *                  registration
     * @param version The target version up to which commits should be backfilled
     * @param lastKnownBackfilledVersion The most recent version known to be backfilled before this
     *                                   call. If null or invalid, the client will backfill from the
     *                                   table's beginning. Can be null.
     * @throws IOException if the backfill operation fails
     *
     * <p><strong>Note:</strong> A successful return indicates backfill completion, but the commit
     * coordinator may not be immediately aware of this update.</p>
     */
    void backfillToVersion(
            String logPath,
            Map<String, String> tableConf,
            long version,
            Long lastKnownBackfilledVersion) throws IOException;

    /**
     * Compares this {@link CommitCoordinatorClientHandler} for semantic equality with another
     * instance.
     *
     * <p>Semantic equality is defined by the specific implementation of
     * {@link CommitCoordinatorClientHandler}. Two instances are considered semantically equal if
     * they can be used interchangeably for any {@link CommitCoordinatorClientHandler} API,
     * including but not limited to:</p>
     *
     * <ul>
     *   <li>{@link #commit}</li>
     *   <li>{@link #getCommits}</li>
     * </ul>
     *
     * <p>For example, semantic equality might be based on both instances pointing to the same
     * underlying endpoint.</p>
     *
     * @param other The {@link CommitCoordinatorClientHandler} to compare with this instance
     * @return boolean True if the instances are semantically equal, false otherwise
     */
    boolean semanticEquals(CommitCoordinatorClientHandler other);
}
