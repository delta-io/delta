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

import org.apache.spark.sql.delta.{DeltaConfigs, InitialSnapshot, ManagedCommitTableFeature, SerializableFileStatus, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

import org.apache.spark.internal.Logging

/** Representation of a commit file */
case class Commit(
  version: Long,
  fileStatus: FileStatus,
  commitTimestamp: Long)

/**
 * Exception raised by [[CommitStore.commit]] method.
 *  | retryable | conflict  | meaning                                                         |
 *  |   no      |   no      | something bad happened (e.g. auth failure)                      |
 *  |   no      |   yes     | permanent transaction conflict (e.g. multi-table commit failed) |
 *  |   yes     |   no      | transient error (e.g. network hiccup)                           |
 *  |   yes     |   yes     | physical conflict (allowed to rebase and retry)                 |
 */
case class CommitFailedException(
    retryable: Boolean, conflict: Boolean, message: String) extends Exception(message)

/** Response container for [[CommitStore.commit]] API */
case class CommitResponse(commit: Commit)

/** Response container for [[CommitStore.getCommits]] API */
case class GetCommitsResponse(commits: Seq[Commit], latestTableVersion: Long)

/** A container class to inform the CommitStore about any changes in Protocol/Metadata */
case class UpdatedActions(
  commitInfo: CommitInfo,
  newMetadata: Option[Metadata],
  newProtocol: Option[Protocol])

/**
 * CommitStore is responsible for managing commits for a managed-commit delta table.
 * 1. It provides API to commit a new version of the table. See [[CommitStore.commit]] API.
 * 2. It makes sure that commits are backfilled if/when needed
 * 3. It also tracks and returns unbackfilled commits. See [[CommitStore.getCommits]] API.
 */
trait CommitStore {
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
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse

  /**
   * API to get the un-backfilled commits for the table represented by the given `logPath`.
   * Commits older than `startVersion`, or newer than `endVersion` (if given), are ignored. The
   * returned commits are contiguous and in ascending version order.
   * Note that the first version returned by this API may not be equal to the `startVersion`. This
   * happens when few versions starting from `startVersion` are already backfilled and so
   * CommitStore may have stopped tracking them.
   * The returned latestTableVersion is the maximum commit version ratified by the Commit-Owner.
   * Note that returning latestTableVersion as -1 is acceptable only if the commit-owner never
   * ratified any version i.e. it never accepted any un-backfilled commit.
   *
   * @return GetCommitsResponse which has a list of [[Commit]]s and the latestTableVersion which is
   *         tracked by [[CommitStore]].
   */
  def getCommits(
    logPath: Path,
    startVersion: Long,
    endVersion: Option[Long] = None): GetCommitsResponse
}

/** A builder interface for CommitStore */
trait CommitStoreBuilder {

  /** Name of the commit-store */
  def name: String

  /** Returns a commit store based on the given conf */
  def build(conf: Map[String, String]): CommitStore
}

/** Factory to get the correct CommitStore for a table */
object CommitStoreProvider {
  // mapping from different commit-owner names to the corresponding [[CommitStoreBuilder]]s.
  private val nameToBuilderMapping = mutable.Map.empty[String, CommitStoreBuilder]

  /** Returns a [[CommitStore]] for the given `name` and `conf` */
  def getCommitStore(name: String, conf: Map[String, String]): CommitStore = synchronized {
    nameToBuilderMapping.get(name).map(_.build(conf)).getOrElse {
      throw new IllegalArgumentException(s"Unknown commit store: $name")
    }
  }

  /** Registers a new [[CommitStoreBuilder]] with the [[CommitStoreProvider]] */
  def registerBuilder(commitStoreBuilder: CommitStoreBuilder): Unit = synchronized {
    nameToBuilderMapping.get(commitStoreBuilder.name) match {
      case Some(commitStoreBuilder: CommitStoreBuilder) =>
        throw new IllegalArgumentException(s"commit store: ${commitStoreBuilder.name} already" +
          s" registered with builder ${commitStoreBuilder.getClass.getName}")
      case None =>
        nameToBuilderMapping.put(commitStoreBuilder.name, commitStoreBuilder)
    }
  }

  def getCommitStore(snapshotDescriptor: SnapshotDescriptor): Option[CommitStore] = {
    DeltaConfigs.MANAGED_COMMIT_OWNER_NAME
      .fromMetaData(snapshotDescriptor.metadata)
      .map { commitOwnerStr =>
        assert(snapshotDescriptor.protocol.isFeatureSupported(ManagedCommitTableFeature))
        val conf = DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.fromMetaData(snapshotDescriptor.metadata)
        CommitStoreProvider.getCommitStore(commitOwnerStr, conf)
      }
  }

  // Visible only for UTs
  private[delta] def clearNonDefaultBuilders(): Unit = synchronized {
    val initialCommitStoreBuilderNames = initialCommitStoreBuilders.map(_.name).toSet
    nameToBuilderMapping.retain((k, _) => initialCommitStoreBuilderNames.contains(k))
  }

  val initialCommitStoreBuilders = Seq[CommitStoreBuilder](
    // Any new commit-store builder will be registered here.
  )
  initialCommitStoreBuilders.foreach(registerBuilder)
}
