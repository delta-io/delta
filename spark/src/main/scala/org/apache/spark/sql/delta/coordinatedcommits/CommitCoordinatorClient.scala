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

package org.apache.spark.sql.delta.coordinatedcommits

import scala.collection.mutable

import org.apache.spark.sql.delta.storage.LogStore
import io.delta.dynamodbcommitcoordinator.DynamoDBCommitCoordinatorClientBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession

/** Representation of a commit file */
case class Commit(
    private val version: Long,
    private val fileStatus: FileStatus,
    private val commitTimestamp: Long) {
  def getVersion: Long = version
  def getFileStatus: FileStatus = fileStatus
  def getCommitTimestamp: Long = commitTimestamp
}

/**
 * Exception raised by [[CommitCoordinatorClient.commit]] method.
 *  | retryable | conflict  | meaning                                                         |
 *  |   no      |   no      | something bad happened (e.g. auth failure)                      |
 *  |   no      |   yes     | permanent transaction conflict (e.g. multi-table commit failed) |
 *  |   yes     |   no      | transient error (e.g. network hiccup)                           |
 *  |   yes     |   yes     | physical conflict (allowed to rebase and retry)                 |
 */
case class CommitFailedException(
    private val retryable: Boolean,
    private val conflict: Boolean,
    private val message: String) extends RuntimeException(message) {
  def getRetryable: Boolean = retryable
  def getConflict: Boolean = conflict
}

/** Response container for [[CommitCoordinatorClient.commit]] API */
case class CommitResponse(private val commit: Commit) {
  def getCommit: Commit = commit
}

/** Response container for [[CommitCoordinatorClient.getCommits]] API */
case class GetCommitsResponse(
    private val commits: Seq[Commit],
    private val latestTableVersion: Long) {
  def getCommits: Seq[Commit] = commits
  def getLatestTableVersion: Long = latestTableVersion
}

/**
 * A container class to inform the [[CommitCoordinatorClient]] about any changes in
 * Protocol/Metadata
 */
case class UpdatedActions(
    private val commitInfo: AbstractCommitInfo,
    private val newMetadata: AbstractMetadata,
    private val newProtocol: AbstractProtocol,
    private val oldMetadata: AbstractMetadata,
    private val oldProtocol: AbstractProtocol) {
  def getCommitInfo: AbstractCommitInfo = commitInfo
  def getNewMetadata: AbstractMetadata = newMetadata
  def getNewProtocol: AbstractProtocol = newProtocol
  def getOldMetadata: AbstractMetadata = oldMetadata
  def getOldProtocol: AbstractProtocol = oldProtocol

}

/**
 * [[CommitCoordinatorClient]] is responsible for managing commits for a coordinated-commits delta
 * table.
 * 1. It provides API to commit a new version of the table. See [[CommitCoordinatorClient.commit]]
 * API.
 * 2. It makes sure that commits are backfilled if/when needed
 * 3. It also tracks and returns unbackfilled commits. See [[CommitCoordinatorClient.getCommits]]
 *  API.
 */
trait CommitCoordinatorClient {

  /**
   * API to register the table represented by the given `logPath` with the given
   * `currentTableVersion`.
   * This API is called when the table is being converted from filesystem table to
   * coordinated-commits table.
   * - The `currentTableVersion` is the version of the table just before conversion.
   * - The `currentTableVersion` + 1 represents the commit that will do the conversion. This must be
   *   backfilled atomically.
   * - The `currentTableVersion` + 2 represents the first commit after conversion. This will go via
   *   the [[CommitCoordinatorClient]] and it could choose whether it wants to write unbackfilled
   *   commits and backfill them later.
   * When a new coordinated-commits table is being created, the `currentTableVersion` will be -1 and
   * the upgrade commit needs to be a file-system commit which will write the backfilled file
   * directly.
   *
   * @return A map of key-value pairs which is issued by the commit-coordinator to identify the
   *         table. This should be stored in the table's metadata. This information needs to be
   *         passed in other table specific APIs like commit / getCommits / backfillToVersion to
   *         identify the table.
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
      coordinatedCommitsTableConf: Map[String, String],
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse

  /**
   * API to get the un-backfilled commits for the table represented by the given `logPath`.
   * Commits older than `startVersion`, or newer than `endVersion` (if given), are ignored. The
   * returned commits are contiguous and in ascending version order.
   * Note that the first version returned by this API may not be equal to the `startVersion`. This
   * happens when few versions starting from `startVersion` are already backfilled and so
   * commit-coordinator may have stopped tracking them.
   * The returned latestTableVersion is the maximum commit version ratified by the
   * Commit-Coordinator.
   * Note that returning latestTableVersion as -1 is acceptable only if the commit-coordinator never
   * ratified any version i.e. it never accepted any un-backfilled commit.
   *
   * @return GetCommitsResponse which has a list of [[Commit]]s and the latestTableVersion which is
   *         tracked by [[CommitCoordinatorClient]].
   */
  def getCommits(
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      startVersion: Option[Long] = None,
      endVersion: Option[Long] = None): GetCommitsResponse

  /**
   * API to ask the Commit-Coordinator to backfill all commits > `lastKnownBackfilledVersion` and
   * <= `endVersion`.
   *
   * If this API returns successfully, that means the backfill must have been completed, although
   * the Commit-Coordinator may not be aware of it yet.
   *
   * @param version The version till which the Commit-Coordinator should backfill.
   * @param lastKnownBackfilledVersion The last known version that was backfilled by
   *                                   Commit-Coordinator before this API was called. If it's None
   *                                   or invalid, then the Commit-Coordinator should backfill from
   *                                   the beginning of the table.
   */
  def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      coordinatedCommitsTableConf: Map[String, String],
      version: Long,
      lastKnownBackfilledVersion: Option[Long]): Unit

  /**
   * Determines whether this [[CommitCoordinatorClient]] is semantically equal to another
   * [[CommitCoordinatorClient]].
   *
   * Semantic equality is determined by each [[CommitCoordinatorClient]] implementation based on
   * whether the two instances can be used interchangeably when invoking any of the
   * CommitCoordinatorClient APIs, such as `commit`, `getCommits`, etc. For e.g., both the instances
   * might be pointing to the same underlying endpoint.
   */
  def semanticEquals(other: CommitCoordinatorClient): Boolean
}

object CommitCoordinatorClient {
  def semanticEquals(
      commitCoordinatorClientOpt1: Option[CommitCoordinatorClient],
      commitCoordinatorClientOpt2: Option[CommitCoordinatorClient]): Boolean = {
    (commitCoordinatorClientOpt1, commitCoordinatorClientOpt2) match {
      case (Some(commitCoordinatorClient1), Some(commitCoordinatorClient2)) =>
        commitCoordinatorClient1.semanticEquals(commitCoordinatorClient2)
      case (None, None) =>
        true
      case _ =>
        false
    }
  }
}

/** A builder interface for [[CommitCoordinatorClient]] */
trait CommitCoordinatorBuilder {

  /** Name of the commit-coordinator */
  def getName: String

  /** Returns a commit-coordinator client based on the given conf */
  def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient
}

/** Factory to get the correct [[CommitCoordinatorClient]] for a table */
object CommitCoordinatorProvider {
  // mapping from different commit-coordinator names to the corresponding
  // [[CommitCoordinatorBuilder]]s.
  private val nameToBuilderMapping = mutable.Map.empty[String, CommitCoordinatorBuilder]

  /** Registers a new [[CommitCoordinatorBuilder]] with the [[CommitCoordinatorProvider]] */
  def registerBuilder(commitCoordinatorBuilder: CommitCoordinatorBuilder): Unit = synchronized {
    nameToBuilderMapping.get(commitCoordinatorBuilder.getName) match {
      case Some(commitCoordinatorBuilder: CommitCoordinatorBuilder) =>
        throw new IllegalArgumentException(
          s"commit-coordinator: ${commitCoordinatorBuilder.getName} already" +
          s" registered with builder ${commitCoordinatorBuilder.getClass.getName}")
      case None =>
        nameToBuilderMapping.put(commitCoordinatorBuilder.getName, commitCoordinatorBuilder)
    }
  }

  /** Returns a [[CommitCoordinatorClient]] for the given `name`, `conf`, and `spark` */
  def getCommitCoordinatorClient(
      name: String,
      conf: Map[String, String],
      spark: SparkSession): CommitCoordinatorClient = synchronized {
    nameToBuilderMapping.get(name).map(_.build(spark, conf)).getOrElse {
      throw new IllegalArgumentException(s"Unknown commit-coordinator: $name")
    }
  }

  // Visible only for UTs
  private[delta] def clearNonDefaultBuilders(): Unit = synchronized {
    val initialCommitCoordinatorNames = initialCommitCoordinatorBuilders.map(_.getName).toSet
    nameToBuilderMapping.retain((k, _) => initialCommitCoordinatorNames.contains(k))
  }

  private val initialCommitCoordinatorBuilders = Seq[CommitCoordinatorBuilder](
    new DynamoDBCommitCoordinatorClientBuilder()
  )
  initialCommitCoordinatorBuilders.foreach(registerBuilder)
}
