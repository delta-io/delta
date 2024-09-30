/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage.commit;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import io.delta.storage.commit.actions.AbstractMetadata;
import io.delta.storage.commit.actions.AbstractProtocol;
import io.delta.storage.LogStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * The CommitCoordinatorClient is responsible for communicating with the commit coordinator
 * and backfilling commits. It has four main APIs that need to be implemented
 *
 * <ul>
 * <li>Commit a new version of the table. See {@link #commit}.</li>
 * <li>Ensure that commits are backfilled if/when needed. See {@link #backfillToVersion}</li>
 * <li>Tracks and returns unbackfilled commits. See {@link #getCommits}.</li>
 * <li>Determine the table config during commit coordinator registration.
 *     See {@link #registerTable}</li>
 * </ul>
 */
public interface CommitCoordinatorClient {

  /**
   * API to register the table represented by the given `logPath` at the provided
   * currentTableVersion with the commit coordinator this commit coordinator client represents.
   *
   * This API is called when the table is being converted from a file system table to a
   * coordinated-commit table.
   *
   * When a new coordinated-commit table is being created, the currentTableVersion will be -1 and
   * the upgrade commit needs to be a file system commit which will write the backfilled file
   * directly.
   *
   * @param logPath         The path to the delta log of the table that should be converted
   * @param tableIdentifier The optional tableIdentifier for the table. Some commit coordinators may
   *                         choose to make this compulsory and error out if this is not provided.
   * @param currentVersion  The currentTableVersion is the version of the table just before
   *                        conversion. currentTableVersion + 1 represents the commit that
   *                        will do the conversion. This must be backfilled atomically.
   *                        currentTableVersion + 2 represents the first commit after conversion.
   *                        This will go through the CommitCoordinatorClient and the client is
   *                        free to choose when it wants to backfill this commit.
   * @param currentMetadata The metadata of the table at currentTableVersion
   * @param currentProtocol The protocol of the table at currentTableVersion
   * @return A map of key-value pairs which is issued by the commit coordinator to identify the
   *         table. This should be stored in the table's metadata. This information needs to be
   *         passed to the {@link #commit}, {@link #getCommits}, and {@link #backfillToVersion}
   *         APIs to identify the table.
   */
  Map<String, String> registerTable(
    Path logPath,
    Optional<TableIdentifier> tableIdentifier,
    long currentVersion,
    AbstractMetadata currentMetadata,
    AbstractProtocol currentProtocol);

  /**
   * API to commit the given set of actions to the table represented by logPath at the
   * given commitVersion.
   *
   * @param logStore        The log store to use for writing the commit file.
   * @param hadoopConf      The Hadoop configuration required to access the file system.
   * @param tableDescriptor The descriptor for the table.
   * @param commitVersion   The version of the commit that is being committed.
   * @param actions         The actions that need to be committed.
   * @param updatedActions  The commit info and any metadata or protocol changes that are made
   *                        as part of this commit.
   * @return CommitResponse which contains the file status of the committed commit file. If the
   *         commit is already backfilled, then the file status could be omitted from the response
   *         and the client could retrieve the information by itself.
   * @throws CommitFailedException if the commit operation fails.
   */
  CommitResponse commit(
    LogStore logStore,
    Configuration hadoopConf,
    TableDescriptor tableDescriptor,
    long commitVersion,
    Iterator<String> actions,
    UpdatedActions updatedActions) throws CommitFailedException;

  /**
   * API to get the unbackfilled commits for the table represented by the given logPath.
   * Commits older than startVersion or newer than endVersion (if given) are ignored. The
   * returned commits are contiguous and in ascending version order.
   *
   * Note that the first version returned by this API may not be equal to startVersion. This
   * happens when some versions starting from startVersion have already been backfilled and so
   * the commit coordinator may have stopped tracking them.
   *
   * The returned latestTableVersion is the maximum commit version ratified by the commit
   * coordinator. Note that returning latestTableVersion as -1 is acceptable only if the commit
   * coordinator never ratified any version, i.e. it never accepted any unbackfilled commit.
   *
   * @param tableDescriptor The descriptor for the table.
   * @param startVersion    The minimum version of the commit that should be returned. Can be null.
   * @param endVersion      The maximum version of the commit that should be returned. Can be null.
   * @return GetCommitsResponse which has a list of {@link Commit}s and the latestTableVersion which
   *         is tracked by {@link CommitCoordinatorClient}.
   */
  GetCommitsResponse getCommits(
    TableDescriptor tableDescriptor,
    Long startVersion,
    Long endVersion);

  /**
   * API to ask the commit coordinator client to backfill all commits up to {@code version}
   * and notify the commit coordinator.
   *
   * If this API returns successfully, that means the backfill must have been completed, although
   * the commit coordinator may not be aware of it yet.
   *
   * @param logStore                   The log store to use for writing the backfilled commits.
   * @param hadoopConf                 The Hadoop configuration required to access the file system.
   * @param tableDescriptor            The descriptor for the table.
   * @param version                    The version till which the commit coordinator client should
   *                                   backfill.
   * @param lastKnownBackfilledVersion The last known version that was backfilled before this API
   *                                   was called. If it is None or invalid, then the commit
   *                                   coordinator client should backfill from the beginning of
   *                                   the table. Can be null.
   * @throws IOException if there is an IO error while backfilling the commits.
   */
  void backfillToVersion(
    LogStore logStore,
    Configuration hadoopConf,
    TableDescriptor tableDescriptor,
    long version,
    Long lastKnownBackfilledVersion) throws IOException;

  /**
   * Determines whether this CommitCoordinatorClient is semantically equal to another
   * CommitCoordinatorClient.
   *
   * Semantic equality is determined by each CommitCoordinatorClient implementation based on
   * whether the two instances can be used interchangeably when invoking any of the
   * CommitCoordinatorClient APIs, such as {@link #commit}, {@link #getCommits}, etc. For example,
   * both instances might be pointing to the same underlying endpoint.
   */
  boolean semanticEquals(CommitCoordinatorClient other);
}
