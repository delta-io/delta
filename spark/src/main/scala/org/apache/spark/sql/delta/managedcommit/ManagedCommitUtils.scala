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

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, ManagedCommitTableFeature, Snapshot, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.FileNames.{DeltaFile, UnbackfilledDeltaFile}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.SparkSession

object ManagedCommitUtils extends DeltaLogging {

  /**
   * Returns an iterator of commit files starting from startVersion.
   * If the iterator is consumed beyond what the file system listing shows, this method do a
   * deltaLog.update() to find the latest version and returns listing results upto that version.
   *
   * @return an iterator of (file status, version) pair corresponding to commit files
   */
  def commitFilesIterator(
      deltaLog: DeltaLog,
      startVersion: Long): Iterator[(FileStatus, Long)] = {

    def listDeltas(startVersion: Long, endVersion: Option[Long]): Iterator[(FileStatus, Long)] = {
      deltaLog
        .listFrom(startVersion)
        .collect { case DeltaFile(fileStatus, version) => (fileStatus, version) }
        .takeWhile { case (_, version) => endVersion.forall(version <= _) }
    }

    var maxVersionSeen = startVersion - 1
    val listedDeltas = listDeltas(startVersion, endVersion = None).filter { case (_, version) =>
      maxVersionSeen = math.max(maxVersionSeen, version)
      true
    }

    def tailFromSnapshot(): Iterator[(FileStatus, Long)] = {
      val currentSnapshotInDeltaLog = deltaLog.unsafeVolatileSnapshot
      if (currentSnapshotInDeltaLog.version == maxVersionSeen &&
          currentSnapshotInDeltaLog.tableCommitOwnerClientOpt.isEmpty) {
        // If the last version in listing is same as the `unsafeVolatileSnapshot` in deltaLog and
        // if that snapshot doesn't have a commit-owner => this table was not a managed-commit table
        // at the time of listing. This is because the commit which converts the file-system table
        // to a managed-commit table must be a file-system commit as per the spec.
        return Iterator.empty
      }

      val endSnapshot = deltaLog.update()
      // No need to worry if we already reached the end
      if (maxVersionSeen >= endSnapshot.version) {
        return Iterator.empty
      }
      val unbackfilledDeltas = endSnapshot.logSegment.deltas.collect {
        case UnbackfilledDeltaFile(fileStatus, version, _) if version > maxVersionSeen =>
          (fileStatus, version)
      }
      // Check for a gap between listing and commit files in the logsegment
      val gapListing = unbackfilledDeltas.headOption match {
        case Some((_, version)) if maxVersionSeen + 1 < version =>
          listDeltas(maxVersionSeen + 1, Some(version))
        // no gap before
        case _ => Iterator.empty
      }
      gapListing ++ unbackfilledDeltas
    }

    // We want to avoid invoking `tailFromSnapshot()` as it internally calls deltaLog.update()
    // So we append the two iterators and the second iterator will be created only if the first one
    // is exhausted.
    Iterator(1, 2).flatMap {
      case 1 => listedDeltas
      case 2 => tailFromSnapshot()
    }
  }

  /**
   * Write a UUID-based commit file for the specified version to the
   * table at [[logPath]].
   */
  def writeCommitFile(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      commitVersion: Long,
      actions: Iterator[String],
      uuid: String): FileStatus = {
    val commitPath = FileNames.unbackfilledDeltaFile(logPath, commitVersion, Some(uuid))
    logStore.write(commitPath, actions, overwrite = false, hadoopConf)
    commitPath.getFileSystem(hadoopConf).getFileStatus(commitPath)
  }

  /**
   * Get the table path from the provided log path.
   */
  def getTablePath(logPath: Path): Path = logPath.getParent

  def getCommitOwnerClient(
      spark: SparkSession, metadata: Metadata, protocol: Protocol): Option[CommitOwnerClient] = {
    metadata.managedCommitOwnerName.map { commitOwnerStr =>
      assert(protocol.isFeatureSupported(ManagedCommitTableFeature))
      CommitOwnerProvider.getCommitOwnerClient(
        commitOwnerStr, metadata.managedCommitOwnerConf, spark)
    }
  }

  def getTableCommitOwner(
      spark: SparkSession,
      snapshotDescriptor: SnapshotDescriptor): Option[TableCommitOwnerClient] = {
    getCommitOwnerClient(spark, snapshotDescriptor.metadata, snapshotDescriptor.protocol).map {
      commitOwner =>
        TableCommitOwnerClient(
          commitOwner,
          snapshotDescriptor.deltaLog.logPath,
          snapshotDescriptor.metadata.managedCommitTableConf,
          snapshotDescriptor.deltaLog.newDeltaHadoopConf(),
          snapshotDescriptor.deltaLog.store
        )
    }
  }

  def getManagedCommitConfs(metadata: Metadata): (Option[String], Map[String, String]) = {
    metadata.managedCommitOwnerName match {
      case Some(name) => (Some(name), metadata.managedCommitOwnerConf)
      case None => (None, Map.empty)
    }
  }

  /**
   * The main table properties used to instantiate a TableCommitOwnerClient.
   */
  val TABLE_PROPERTY_KEYS: Seq[String] = Seq(
    DeltaConfigs.MANAGED_COMMIT_OWNER_NAME.key,
    DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.key,
    DeltaConfigs.MANAGED_COMMIT_TABLE_CONF.key)

  /**
   * Returns true if any ManagedCommit-related table properties is present in the metadata.
   */
  def tablePropertiesPresent(metadata: Metadata): Boolean = {
    val managedCommitProperties = Seq(
      DeltaConfigs.MANAGED_COMMIT_OWNER_NAME.key,
      DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.key,
      DeltaConfigs.MANAGED_COMMIT_TABLE_CONF.key)
    managedCommitProperties.exists(metadata.configuration.contains)
  }

  /**
   * Returns true if the snapshot is backed by unbackfilled commits.
   */
  def unbackfilledCommitsPresent(snapshot: Snapshot): Boolean = {
    snapshot.logSegment.deltas.exists {
      case FileNames.UnbackfilledDeltaFile(_, _, _) => true
      case _ => false
    }
  }

  /**
   * This method takes care of backfilling any unbackfilled delta files when managed commit is
   * not enabled on the table (i.e. commit-owner is not present) but there are still unbackfilled
   * delta files in the table. This can happen if an error occurred during the MC -> FS commit
   * where the commit-owner was able to register the downgrade commit but it failed to backfill
   * it. This method must be invoked before doing the next commit as otherwise there will be a
   * gap in the backfilled commit sequence.
   */
  def backfillWhenManagedCommitDisabled(snapshot: Snapshot): Unit = {
    if (snapshot.tableCommitOwnerClientOpt.nonEmpty) {
      // Managed commits is enabled on the table. Don't backfill as backfills are managed by
      // commit-owners.
      return
    }
    val unbackfilledFilesAndVersions = snapshot.logSegment.deltas.collect {
      case UnbackfilledDeltaFile(unbackfilledDeltaFile, version, _) =>
        (unbackfilledDeltaFile, version)
    }
    if (unbackfilledFilesAndVersions.isEmpty) return
    // Managed commits are disabled on the table but the table still has un-backfilled files.
    val deltaLog = snapshot.deltaLog
    val hadoopConf = deltaLog.newDeltaHadoopConf()
    val fs = deltaLog.logPath.getFileSystem(hadoopConf)
    val overwrite = !deltaLog.store.isPartialWriteVisible(deltaLog.logPath, hadoopConf)
    var numAlreadyBackfilledFiles = 0L
    unbackfilledFilesAndVersions.foreach { case (unbackfilledDeltaFile, version) =>
      val backfilledFilePath = FileNames.unsafeDeltaFile(deltaLog.logPath, version)
      if (!fs.exists(backfilledFilePath)) {
        val actionsIter = deltaLog.store.readAsIterator(unbackfilledDeltaFile.getPath, hadoopConf)
        deltaLog.store.write(
          backfilledFilePath,
          actionsIter,
          overwrite,
          hadoopConf)
        logInfo(s"Delta file ${unbackfilledDeltaFile.getPath.toString} backfilled to path" +
          s" ${backfilledFilePath.toString}.")
      } else {
        numAlreadyBackfilledFiles += 1
        logInfo(s"Delta file ${unbackfilledDeltaFile.getPath.toString} already backfilled.")
      }
    }
    recordDeltaEvent(
      deltaLog,
      opType = "delta.managedCommit.backfillWhenManagedCommitSupportedAndDisabled",
      data = Map(
        "numUnbackfilledFiles" -> unbackfilledFilesAndVersions.size,
        "unbackfilledFiles" -> unbackfilledFilesAndVersions.map(_._1.getPath.toString),
        "numAlreadyBackfilledFiles" -> numAlreadyBackfilledFiles
      )
    )
  }
}
