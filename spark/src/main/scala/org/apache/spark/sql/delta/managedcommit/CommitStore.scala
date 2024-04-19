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

import org.apache.spark.sql.delta.{DeltaConfigs, ManagedCommitTableFeature, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

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
  newMetadata: Metadata,
  newProtocol: Protocol,
  oldMetadata: Metadata,
  oldProtocol: Protocol)

/**
 * CommitStore is responsible for managing commits for a managed-commit delta table.
 * 1. It provides API to commit a new version of the table. See [[CommitStore.commit]] API.
 * 2. It makes sure that commits are backfilled if/when needed
 * 3. It also tracks and returns unbackfilled commits. See [[CommitStore.getCommits]] API.
 */
trait CommitStore {

  /**
   * API to register the table represented by the given `logPath` with the given
   * `currentTableVersion`.
   * This API is called when the table is being converted from filesystem table to managed-commit
   * table.
   * - The `currentTableVersion` is the version of the table just before conversion.
   * - The `currentTableVersion` + 1 represents the commit that will do the conversion. This must be
   *   backfilled atomically.
   * - The `currentTableVersion` + 2 represents the first commit after conversion. This will go via
   *   the [[CommitStore]] and it could choose whether it wants to write unbackfilled commits and
   *   backfill them later.
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
      currentMetadata: Metadata,
      currentProtocol: Protocol): Map[String, String] = Map.empty

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
      managedCommitTableConf: Map[String, String],
      startVersion: Long,
      endVersion: Option[Long] = None): GetCommitsResponse

  /**
   * API to ask the Commit-Owner to backfill all commits >= 'startVersion' and <= `endVersion`.
   *
   * If this API returns successfully, that means the backfill must have been completed, although
   * the Commit-Owner may not be aware of it yet.
   */
  def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      startVersion: Long,
      endVersion: Option[Long]): Unit

  /**
   * Determines whether this CommitStore is semantically equal to another CommitStore.
   *
   * Semantic equality is determined by each CommitStore implementation based on whether the two
   * instances can be used interchangeably when invoking any of the CommitStore APIs, such as
   * `commit`, `getCommits`, etc. For e.g., both the instances might be pointing to the same
   * underlying endpoint.
   */
  def semanticEquals(other: CommitStore): Boolean
}

object CommitStore {
  def semanticEquals(
      commitStore1Opt: Option[CommitStore],
      commitStore2Opt: Option[CommitStore]): Boolean = {
    (commitStore1Opt, commitStore2Opt) match {
      case (Some(commitStore1), Some(commitStore2)) => commitStore1.semanticEquals(commitStore2)
      case (None, None) => true
      case _ => false
    }
  }
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

  /** Returns a [[CommitStore]] for the given `name` and `conf` */
  def getCommitStore(name: String, conf: Map[String, String]): CommitStore = synchronized {
    nameToBuilderMapping.get(name).map(_.build(conf)).getOrElse {
      throw new IllegalArgumentException(s"Unknown commit store: $name")
    }
  }

  def getCommitStore(metadata: Metadata, protocol: Protocol): Option[CommitStore] = {
    metadata.managedCommitOwnerName.map { commitOwnerStr =>
      assert(protocol.isFeatureSupported(ManagedCommitTableFeature))
      CommitStoreProvider.getCommitStore(commitOwnerStr, metadata.managedCommitOwnerConf)
    }
  }

  def getCommitStore(snapshotDescriptor: SnapshotDescriptor): Option[CommitStore] = {
    getCommitStore(snapshotDescriptor.metadata, snapshotDescriptor.protocol)
  }


  def getTableCommitStore(snapshotDescriptor: SnapshotDescriptor): Option[TableCommitStore] = {
    getCommitStore(snapshotDescriptor.metadata, snapshotDescriptor.protocol).map { store =>
      TableCommitStore(
        store,
        snapshotDescriptor.deltaLog.logPath,
        snapshotDescriptor.metadata.managedCommitTableConf,
        snapshotDescriptor.deltaLog.newDeltaHadoopConf(),
        snapshotDescriptor.deltaLog.store)
    }
  }

  // Visible only for UTs
  private[delta] def clearNonDefaultBuilders(): Unit = synchronized {
    val initialCommitStoreBuilderNames = initialCommitStoreBuilders.map(_.name).toSet
    nameToBuilderMapping.retain((k, _) => initialCommitStoreBuilderNames.contains(k))
  }

  private val initialCommitStoreBuilders = Seq[CommitStoreBuilder](
    // Any new commit-store builder will be registered here.
  )
  initialCommitStoreBuilders.foreach(registerBuilder)
}

object CommitOwner {
  def getManagedCommitConfs(metadata: Metadata): (Option[String], Map[String, String]) = {
    metadata.managedCommitOwnerName match {
      case Some(name) => (Some(name), metadata.managedCommitOwnerConf)
      case None => (None, Map.empty)
    }
  }
}
