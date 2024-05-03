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

package org.apache.spark.sql.delta.managedcommit

import scala.collection.mutable

import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

/** Representation of a commit file */
case class Commit(
  version: Long,
  fileStatus: FileStatus,
  commitTimestamp: Long)

/**
 * Exception raised by [[CommitOwnerClient.commit]] method.
 *  | retryable | conflict  | meaning                                                         |
 *  |   no      |   no      | something bad happened (e.g. auth failure)                      |
 *  |   no      |   yes     | permanent transaction conflict (e.g. multi-table commit failed) |
 *  |   yes     |   no      | transient error (e.g. network hiccup)                           |
 *  |   yes     |   yes     | physical conflict (allowed to rebase and retry)                 |
 */
case class CommitFailedException(
    retryable: Boolean, conflict: Boolean, message: String) extends Exception(message)

/** Response container for [[CommitOwnerClient.commit]] API */
case class CommitResponse(commit: Commit)

/** Response container for [[CommitOwnerClient.getCommits]] API */
case class GetCommitsResponse(commits: Seq[Commit], latestTableVersion: Long)

/** A container class to inform the [[CommitOwnerClient]] about any changes in Protocol/Metadata */
case class UpdatedActions(
  commitInfo: AbstractCommitInfo,
  newMetadata: AbstractMetadata,
  newProtocol: AbstractProtocol,
  oldMetadata: AbstractMetadata,
  oldProtocol: AbstractProtocol)

/**
 * [[CommitOwnerClient]] is responsible for managing commits for a managed-commit delta table.
 * 1. It provides API to commit a new version of the table. See [[CommitOwnerClient.commit]] API.
 * 2. It makes sure that commits are backfilled if/when needed
 * 3. It also tracks and returns unbackfilled commits. See [[CommitOwnerClient.getCommits]] API.
 */
trait CommitOwnerClient {

  /**
   * API to register the table represented by the given `logPath` with the given
   * `currentTableVersion`.
   * This API is called when the table is being converted from filesystem table to managed-commit
   * table.
   * - The `currentTableVersion` is the version of the table just before conversion.
   * - The `currentTableVersion` + 1 represents the commit that will do the conversion. This must be
   *   backfilled atomically.
   * - The `currentTableVersion` + 2 represents the first commit after conversion. This will go via
   *   the [[CommitOwnerClient]] and it could choose whether it wants to write unbackfilled commits
   *   and backfill them later.
   * When a new managed-commit table is being created, the `currentTableVersion` will be -1 and the
   * upgrade commit needs to be a file-system commit which will write the backfilled file directly.
   *
   * @return A map of key-value pairs which is issued by the commit-owner to identify the table.
   *         This should be stored in the table's metadata. This information needs to be passed in
   *         other table specific APIs like commit / getCommits / backfillToVersion to identify the
   *         table.
   */
  def registerTable(
      logPath: Path,
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): Map[String, String] = Map.empty

  /**
   * API to commit the given set of `actions` to the table represented by given `logPath` at the
   * given `commitVersion`.
   * @return CommitResponse which contains the file status of the commit file. If the commit is
   *         already backfilled, then the fileStatus could be omitted from response and the client
   *         could get the info by themselves.
   */
  def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse

  /**
   * API to get the un-backfilled commits for the table represented by the given `logPath`.
   * Commits older than `startVersion`, or newer than `endVersion` (if given), are ignored. The
   * returned commits are contiguous and in ascending version order.
   * Note that the first version returned by this API may not be equal to the `startVersion`. This
   * happens when few versions starting from `startVersion` are already backfilled and so
   * commit-owner may have stopped tracking them.
   * The returned latestTableVersion is the maximum commit version ratified by the Commit-Owner.
   * Note that returning latestTableVersion as -1 is acceptable only if the commit-owner never
   * ratified any version i.e. it never accepted any un-backfilled commit.
   *
   * @return GetCommitsResponse which has a list of [[Commit]]s and the latestTableVersion which is
   *         tracked by [[CommitOwnerClient]].
   */
  def getCommits(
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      startVersion: Option[Long] = None,
      endVersion: Option[Long] = None): GetCommitsResponse

  /**
   * API to ask the Commit-Owner to backfill all commits > `lastKnownBackfilledVersion` and
   * <= `endVersion`.
   *
   * If this API returns successfully, that means the backfill must have been completed, although
   * the Commit-Owner may not be aware of it yet.
   *
   * @param version The version till which the Commit-Owner should backfill.
   * @param lastKnownBackfilledVersion The last known version that was backfilled by Commit-Owner
   *                                   before this API was called. If it's None or invalid, then the
   *                                   Commit-Owner should backfill from the beginning of the table.
   */
  def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      version: Long,
      lastKnownBackfilledVersion: Option[Long]): Unit

  /**
   * Determines whether this [[CommitOwnerClient]] is semantically equal to another
   * [[CommitOwnerClient]].
   *
   * Semantic equality is determined by each [[CommitOwnerClient]] implementation based on whether
   * the two instances can be used interchangeably when invoking any of the CommitOwnerClient APIs,
   * such as `commit`, `getCommits`, etc. For e.g., both the instances might be pointing to the same
   * underlying endpoint.
   */
  def semanticEquals(other: CommitOwnerClient): Boolean
}

object CommitOwnerClient {
  def semanticEquals(
      commitOwnerClientOpt1: Option[CommitOwnerClient],
      commitOwnerClientOpt2: Option[CommitOwnerClient]): Boolean = {
    (commitOwnerClientOpt1, commitOwnerClientOpt2) match {
      case (Some(commitOwnerClient1), Some(commitOwnerClient2)) =>
        commitOwnerClient1.semanticEquals(commitOwnerClient2)
      case (None, None) =>
        true
      case _ =>
        false
    }
  }
}

/** A builder interface for [[CommitOwnerClient]] */
trait CommitOwnerBuilder {

  /** Name of the commit-owner */
  def name: String

  /** Returns a commit-owner client based on the given conf */
  def build(conf: Map[String, String]): CommitOwnerClient
}

/** Factory to get the correct [[CommitOwnerClient]] for a table */
object CommitOwnerProvider {
  // mapping from different commit-owner names to the corresponding [[CommitOwnerBuilder]]s.
  private val nameToBuilderMapping = mutable.Map.empty[String, CommitOwnerBuilder]

  /** Registers a new [[CommitOwnerBuilder]] with the [[CommitOwnerProvider]] */
  def registerBuilder(commitOwnerBuilder: CommitOwnerBuilder): Unit = synchronized {
    nameToBuilderMapping.get(commitOwnerBuilder.name) match {
      case Some(commitOwnerBuilder: CommitOwnerBuilder) =>
        throw new IllegalArgumentException(s"commit-owner: ${commitOwnerBuilder.name} already" +
          s" registered with builder ${commitOwnerBuilder.getClass.getName}")
      case None =>
        nameToBuilderMapping.put(commitOwnerBuilder.name, commitOwnerBuilder)
    }
  }

  /** Returns a [[CommitOwnerClient]] for the given `name` and `conf` */
  def getCommitOwnerClient(
      name: String, conf: Map[String, String]): CommitOwnerClient = synchronized {
    nameToBuilderMapping.get(name).map(_.build(conf)).getOrElse {
      throw new IllegalArgumentException(s"Unknown commit-owner: $name")
    }
  }

  // Visible only for UTs
  private[delta] def clearNonDefaultBuilders(): Unit = synchronized {
    val initialCommitOwnerNames = initialCommitOwnerBuilders.map(_.name).toSet
    nameToBuilderMapping.retain((k, _) => initialCommitOwnerNames.contains(k))
  }

  private val initialCommitOwnerBuilders = Seq[CommitOwnerBuilder](
    // Any new commit-owner builder will be registered here.
  )
  initialCommitOwnerBuilders.foreach(registerBuilder)
}
