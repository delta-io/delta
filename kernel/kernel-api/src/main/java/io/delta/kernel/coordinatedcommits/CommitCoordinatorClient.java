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

package io.delta.kernel.coordinatedcommits;

import io.delta.kernel.TableIdentifier;
import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.annotation.Nullable;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.engine.coordinatedcommits.CommitFailedException;
import io.delta.kernel.engine.coordinatedcommits.CommitResponse;
import io.delta.kernel.engine.coordinatedcommits.GetCommitsResponse;
import io.delta.kernel.engine.coordinatedcommits.UpdatedActions;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractMetadata;
import io.delta.kernel.engine.coordinatedcommits.actions.AbstractProtocol;
import io.delta.kernel.utils.CloseableIterator;
import java.io.IOException;
import java.util.Map;

/**
 * The CommitCoordinatorClient is responsible for communicating with the commit coordinator and
 * backfilling commits. It has four main APIs that need to be implemented:
 *
 * <ul>
 *   <li>{@link #registerTable}: Determine the table config during commit coordinator registration.
 *   <li>{@link #commit}: Commit a new version of the table.
 *   <li>{@link #getCommits}: Tracks and returns unbackfilled commits.
 *   <li>{@link #backfillToVersion}: Ensure that commits are backfilled if/when needed.
 * </ul>
 *
 * @since 3.3.0
 */
@Evolving
public interface CommitCoordinatorClient {

  /**
   * Register the table represented by the given {@code logPath} at the provided {@code
   * currentVersion} with the commit coordinator this commit coordinator client represents.
   *
   * <p>This API is called when the table is being converted from an existing file system table to a
   * coordinated-commit table.
   *
   * <p>When a new coordinated-commit table is being created, the {@code currentVersion} will be -1
   * and the upgrade commit needs to be a file system commit which will write the backfilled file
   * directly.
   *
   * @param engine The {@link Engine} instance to use.
   * @param logPath The path to the delta log of the table that should be converted.
   * @param tableIdentifier The table identifier for the table, or {@code null} if the table doesn't
   *     use any identifier (i.e. it is path-based).
   * @param currentVersion The version of the table just before conversion. currentVersion + 1
   *     represents the commit that will do the conversion. This must be backfilled atomically.
   *     currentVersion + 2 represents the first commit after conversion. This will go through the
   *     CommitCoordinatorClient and the client is free to choose when it wants to backfill this
   *     commit.
   * @param currentMetadata The metadata of the table at currentVersion
   * @param currentProtocol The protocol of the table at currentVersion
   * @return A map of key-value pairs which is issued by the commit coordinator to uniquely identify
   *     the table. This should be stored in the table's metadata for table property {@link
   *     io.delta.kernel.internal.TableConfig#COORDINATED_COMMITS_TABLE_CONF}. This information
   *     needs to be passed to the {@link #commit}, {@link #getCommits}, and {@link
   *     #backfillToVersion} APIs to identify the table.
   */
  Map<String, String> registerTable(
      Engine engine,
      String logPath,
      @Nullable TableIdentifier tableIdentifier,
      long currentVersion,
      AbstractMetadata currentMetadata,
      AbstractProtocol currentProtocol);

  /**
   * Commit the given set of actions to the table represented by {@code tableDescriptor}.
   *
   * @param engine The {@link Engine} instance to use.
   * @param tableDescriptor The descriptor for the table.
   * @param commitVersion The version of the commit that is being committed.
   * @param actions The set of actions to be committed
   * @param updatedActions Additional information for the commit, including:
   *     <ul>
   *       <li>Commit info
   *       <li>Metadata changes
   *       <li>Protocol changes
   *     </ul>
   *
   * @return {@link CommitResponse} containing the file status of the committed file. Note: If the
   *     commit is already backfilled, the file status may be omitted, and the client can retrieve
   *     this information independently.
   * @throws CommitFailedException if the commit operation fails
   */
  CommitResponse commit(
      Engine engine,
      TableDescriptor tableDescriptor,
      long commitVersion,
      CloseableIterator<Row> actions,
      UpdatedActions updatedActions)
      throws CommitFailedException;

  /**
   * Get the unbackfilled commits for the table represented by the given tableDescriptor. Commits
   * older than startVersion (if given) or newer than endVersion (if given) are ignored. The
   * returned commits are contiguous and in ascending version order.
   *
   * <p>Note that the first version returned by this API may not be equal to startVersion. This
   * happens when some versions starting from startVersion have already been backfilled and so the
   * commit coordinator may have stopped tracking them.
   *
   * <p>The returned latestTableVersion is the maximum commit version ratified by the commit
   * coordinator. Note that returning latestTableVersion as -1 is acceptable only if the commit
   * coordinator never ratified any version, i.e. it never accepted any unbackfilled commit.
   *
   * @param engine The {@link Engine} instance to use.
   * @param tableDescriptor The descriptor for the table.
   * @param startVersion The minimum version of the commit that should be returned, or {@code null}
   *     if there is no minimum.
   * @param endVersion The maximum version of the commit that should be returned, or {@code null} if
   *     there is no maximum.
   * @return {@link GetCommitsResponse} which has a list of {@link
   *     io.delta.kernel.engine.coordinatedcommits.Commit}s and the latestTableVersion which is
   *     tracked by the {@link CommitCoordinatorClient}.
   */
  GetCommitsResponse getCommits(
      Engine engine,
      TableDescriptor tableDescriptor,
      @Nullable Long startVersion,
      @Nullable Long endVersion);

  /**
   * Backfill all commits up to {@code version} and notify the commit coordinator.
   *
   * <p>If this API returns successfully, that means the backfill must have been completed, although
   * the commit coordinator may not be aware of it yet.
   *
   * @param engine The {@link Engine} instance to use.
   * @param tableDescriptor The descriptor for the table.
   * @param version The version until which the commit coordinator client should backfill.
   * @param lastKnownBackfilledVersion The last known version that was backfilled before this API
   *     was called. If it is {@code null}, then the commit coordinator client should backfill from
   *     the beginning of the table.
   * @throws IOException if there is an IO error while backfilling the commits.
   */
  void backfillToVersion(
      Engine engine,
      TableDescriptor tableDescriptor,
      long version,
      @Nullable Long lastKnownBackfilledVersion)
      throws IOException;

  /**
   * Checks if this CommitCoordinatorClient is semantically equal to another
   * CommitCoordinatorClient.
   */
  boolean semanticEquals(CommitCoordinatorClient other);
}
