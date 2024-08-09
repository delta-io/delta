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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CoordinatedCommitsTableFeature, DeltaConfig, DeltaConfigs, DeltaLog, Snapshot, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.util.FileNames.{DeltaFile, UnbackfilledDeltaFile}
import io.delta.storage.LogStore
import io.delta.storage.commit.{CommitCoordinatorClient, GetCommitsResponse => JGetCommitsResponse}
import io.delta.storage.commit.actions.AbstractMetadata
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

object CoordinatedCommitsUtils extends DeltaLogging {

  /**
   * Returns the [[CommitCoordinatorClient.getCommits]] response for the given startVersion and
   * versionToLoad.
   */
  def getCommitsFromCommitCoordinatorWithUsageLogs(
      deltaLog: DeltaLog,
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      startVersion: Long,
      versionToLoad: Option[Long],
      isAsyncRequest: Boolean): JGetCommitsResponse = {
    recordFrameProfile("DeltaLog", s"CommitCoordinatorClient.getCommits.async=$isAsyncRequest") {
      val startTimeMs = System.currentTimeMillis()
      def recordEvent(additionalData: Map[String, Any]): Unit = {
        recordDeltaEvent(
          deltaLog,
          opType = CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_CLIENT_GET_COMMITS,
          data = Map(
            "startVersion" -> startVersion,
            "versionToLoad" -> versionToLoad.getOrElse(-1L),
            "async" -> isAsyncRequest.toString,
            "durationMs" -> (System.currentTimeMillis() - startTimeMs).toString
          ) ++ additionalData
        )
      }

      try {
        val response =
          tableCommitCoordinatorClient.getCommits(Some(startVersion), endVersion = versionToLoad)
        val additionalEventData = Map(
          "responseCommitsSize" -> response.getCommits.size,
          "responseLatestTableVersion" -> response.getLatestTableVersion)
        recordEvent(additionalEventData)
        response
      } catch {
        case NonFatal(e) =>
          recordEvent(Map("exception" -> Utils.exceptionString(e)))
          throw e
      }
    }
  }

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
          currentSnapshotInDeltaLog.tableCommitCoordinatorClientOpt.isEmpty) {
        // If the last version in listing is same as the `unsafeVolatileSnapshot` in deltaLog and
        // if that snapshot doesn't have a commit-coordinator => this table was not a
        // coordinated-commits table at the time of listing. This is because the commit which
        // converts the file-system table to a coordinated-commits table must be a file-system
        // commit as per the spec.
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
    logStore.write(commitPath, actions.asJava, true, hadoopConf)
    commitPath.getFileSystem(hadoopConf).getFileStatus(commitPath)
  }

  /**
   * Get the table path from the provided log path.
   */
  def getTablePath(logPath: Path): Path = logPath.getParent

  def getCommitCoordinatorClient(
      spark: SparkSession,
      metadata: Metadata,
      protocol: Protocol): Option[CommitCoordinatorClient] = {
    metadata.coordinatedCommitsCoordinatorName.map { commitCoordinatorStr =>
      assert(protocol.isFeatureSupported(CoordinatedCommitsTableFeature))
      CommitCoordinatorProvider.getCommitCoordinatorClient(
        commitCoordinatorStr, metadata.coordinatedCommitsCoordinatorConf, spark)
    }
  }

  def getTableCommitCoordinator(
      spark: SparkSession,
      snapshotDescriptor: SnapshotDescriptor): Option[TableCommitCoordinatorClient] = {
    getCommitCoordinatorClient(
      spark, snapshotDescriptor.metadata, snapshotDescriptor.protocol).map {
      commitCoordinator =>
        TableCommitCoordinatorClient(
          commitCoordinator,
          snapshotDescriptor.deltaLog.logPath,
          snapshotDescriptor.metadata.coordinatedCommitsTableConf,
          snapshotDescriptor.deltaLog.newDeltaHadoopConf(),
          snapshotDescriptor.deltaLog.store
        )
    }
  }

  def getCoordinatedCommitsConfs(metadata: Metadata): (Option[String], Map[String, String]) = {
    metadata.coordinatedCommitsCoordinatorName match {
      case Some(name) => (Some(name), metadata.coordinatedCommitsCoordinatorConf)
      case None => (None, Map.empty)
    }
  }

  /**
   * Helper method to recover the saved value of `deltaConfig` from `abstractMetadata`.
   * If undefined, fall back to alternate keys, returning defaultValue if none match.
   */
  private[delta] def fromAbstractMetadataAndDeltaConfig[T](
      abstractMetadata: AbstractMetadata,
      deltaConfig: DeltaConfig[T]): T = {
    val conf = abstractMetadata.getConfiguration
    for (key <- deltaConfig.key +: deltaConfig.alternateKeys) {
      Option(conf.get(key)).map { value => return deltaConfig.fromString(value) }
    }
    deltaConfig.fromString(deltaConfig.defaultValue)
  }

  /**
   * Get the commit coordinator name from the provided abstract metadata.
   */
  def getCommitCoordinatorName(abstractMetadata: AbstractMetadata): Option[String] = {
    fromAbstractMetadataAndDeltaConfig(
      abstractMetadata, DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME)
  }

  /**
   * Get the commit coordinator configuration from the provided abstract metadata.
   */
  def getCommitCoordinatorConf(abstractMetadata: AbstractMetadata): Map[String, String] = {
    fromAbstractMetadataAndDeltaConfig(
      abstractMetadata, DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF)
  }

  /**
   * Get the coordinated commits table configuration from the provided abstract metadata.
   */
  def getCoordinatedCommitsTableConf(abstractMetadata: AbstractMetadata): Map[String, String] = {
    fromAbstractMetadataAndDeltaConfig(
      abstractMetadata, DeltaConfigs.COORDINATED_COMMITS_TABLE_CONF)
  }

  val TABLE_PROPERTY_CONFS = Seq(
    DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME,
    DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF,
    DeltaConfigs.COORDINATED_COMMITS_TABLE_CONF)

  /**
   * The main table properties used to instantiate a TableCommitCoordinatorClient.
   */
  val TABLE_PROPERTY_KEYS: Seq[String] = TABLE_PROPERTY_CONFS.map(_.key)

  /**
   * Returns true if any CoordinatedCommits-related table properties is present in the metadata.
   */
  def tablePropertiesPresent(metadata: Metadata): Boolean = {
    TABLE_PROPERTY_KEYS.exists(metadata.configuration.contains)
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
   * This method takes care of backfilling any unbackfilled delta files when coordinated commits is
   * not enabled on the table (i.e. commit-coordinator is not present) but there are still
   * unbackfilled delta files in the table. This can happen if an error occurred during the CC -> FS
   * commit where the commit-coordinator was able to register the downgrade commit but it failed to
   * backfill it. This method must be invoked before doing the next commit as otherwise there will
   * be a gap in the backfilled commit sequence.
   */
  def backfillWhenCoordinatedCommitsDisabled(snapshot: Snapshot): Unit = {
    if (snapshot.tableCommitCoordinatorClientOpt.nonEmpty) {
      // Coordinated commits is enabled on the table. Don't backfill as backfills are managed by
      // commit-coordinators.
      return
    }
    val unbackfilledFilesAndVersions = snapshot.logSegment.deltas.collect {
      case UnbackfilledDeltaFile(unbackfilledDeltaFile, version, _) =>
        (unbackfilledDeltaFile, version)
    }
    if (unbackfilledFilesAndVersions.isEmpty) return
    // Coordinated commits are disabled on the table but the table still has un-backfilled files.
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
        logInfo(log"Delta file ${MDC(DeltaLogKeys.PATH, unbackfilledDeltaFile.getPath.toString)} " +
          log"backfilled to path ${MDC(DeltaLogKeys.PATH2, backfilledFilePath.toString)}.")
      } else {
        numAlreadyBackfilledFiles += 1
        logInfo(log"Delta file ${MDC(DeltaLogKeys.PATH, unbackfilledDeltaFile.getPath.toString)} " +
          log"already backfilled.")
      }
    }
    recordDeltaEvent(
      deltaLog,
      opType = "delta.coordinatedCommits.backfillWhenCoordinatedCommitsSupportedAndDisabled",
      data = Map(
        "numUnbackfilledFiles" -> unbackfilledFilesAndVersions.size,
        "unbackfilledFiles" -> unbackfilledFilesAndVersions.map(_._1.getPath.toString),
        "numAlreadyBackfilledFiles" -> numAlreadyBackfilledFiles
      )
    )
  }
}
