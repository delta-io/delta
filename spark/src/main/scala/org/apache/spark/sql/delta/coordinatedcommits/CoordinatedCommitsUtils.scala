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

import java.util.Optional

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CoordinatedCommitsTableFeature, DeltaConfig, DeltaConfigs, DeltaIllegalArgumentException, DeltaLog, Snapshot, SnapshotDescriptor}
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.commands.CloneTableCommand
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.spark.sql.delta.util.FileNames.{BackfilledDeltaFile, CompactedDeltaFile, DeltaFile, UnbackfilledDeltaFile}
import io.delta.storage.LogStore
import io.delta.storage.commit.{CommitCoordinatorClient, GetCommitsResponse => JGetCommitsResponse, TableIdentifier}
import io.delta.storage.commit.actions.AbstractMetadata
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{TableIdentifier => CatalystTableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.Utils

object CoordinatedCommitsUtils extends DeltaLogging {

  /**
   * Returns the [[CommitCoordinatorClient.getCommits]] response for the given startVersion and
   * versionToLoad.
   */
  def getCommitsFromCommitCoordinatorWithUsageLogs(
      deltaLog: DeltaLog,
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      tableIdentifierOpt: Option[CatalystTableIdentifier],
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
          tableCommitCoordinatorClient.getCommits(
            tableIdentifierOpt, Some(startVersion), endVersion = versionToLoad)
        val additionalEventData = Map(
          "responseCommitsSize" -> response.getCommits.size,
          "responseLatestTableVersion" -> response.getLatestTableVersion)
        recordEvent(additionalEventData)
        response
      } catch {
        case NonFatal(e) =>
          recordEvent(
            Map(
              "exceptionClass" -> e.getClass.getName,
              "exceptionString" -> Utils.exceptionString(e)
            )
          )
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

  def getCommitCoordinatorClient(
      spark: SparkSession,
      deltaLog: DeltaLog, // Used for logging
      metadata: Metadata,
      protocol: Protocol,
      failIfImplUnavailable: Boolean): Option[CommitCoordinatorClient] = {
    metadata.coordinatedCommitsCoordinatorName.flatMap { commitCoordinatorStr =>
      assert(protocol.isFeatureSupported(CoordinatedCommitsTableFeature))
      val coordinatorConf = metadata.coordinatedCommitsCoordinatorConf
      val coordinatorOpt = CommitCoordinatorProvider.getCommitCoordinatorClientOpt(
        commitCoordinatorStr, coordinatorConf, spark)
      if (coordinatorOpt.isEmpty) {
        recordDeltaEvent(
          deltaLog,
          CoordinatedCommitsUsageLogs.COMMIT_COORDINATOR_MISSING_IMPLEMENTATION,
          data = Map(
            "commitCoordinatorName" -> commitCoordinatorStr,
            "registeredCommitCoordinators" ->
              CommitCoordinatorProvider.getRegisteredCoordinatorNames.mkString(", "),
            "commitCoordinatorConf" -> coordinatorConf,
            "failIfImplUnavailable" -> failIfImplUnavailable.toString
          )
        )
        if (failIfImplUnavailable) {
          throw new IllegalArgumentException(
            s"Unknown commit-coordinator: $commitCoordinatorStr")
        }
      }
      coordinatorOpt
    }
  }

  /**
   * Get the table commit coordinator client from the provided snapshot descriptor.
   * Returns None if either this is not a coordinated-commits table. Also returns None when
   * `failIfImplUnavailable` is false and the commit-coordinator implementation is not available.
   */
  def getTableCommitCoordinator(
      spark: SparkSession,
      deltaLog: DeltaLog, // Used for logging
      snapshotDescriptor: SnapshotDescriptor,
      failIfImplUnavailable: Boolean): Option[TableCommitCoordinatorClient] = {
    getCommitCoordinatorClient(
      spark,
      deltaLog,
      snapshotDescriptor.metadata,
      snapshotDescriptor.protocol,
      failIfImplUnavailable).map {
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

  val TABLE_PROPERTY_CONFS = Seq(
    DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME,
    DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF,
    DeltaConfigs.COORDINATED_COMMITS_TABLE_CONF)

  val ICT_TABLE_PROPERTY_CONFS = Seq(
    DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED,
    DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
    DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP)

  /**
   * The main table properties used to instantiate a TableCommitCoordinatorClient.
   */
  val TABLE_PROPERTY_KEYS: Seq[String] = TABLE_PROPERTY_CONFS.map(_.key)

  /**
   * The main ICT table properties used as dependencies for Coordinated Commits.
   */
  val ICT_TABLE_PROPERTY_KEYS: Seq[String] = ICT_TABLE_PROPERTY_CONFS.map(_.key)

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
    } && !snapshot.allCommitsBackfilled
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
    if (snapshot.getTableCommitCoordinatorForWrites.nonEmpty) {
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

  /**
   * Returns the last backfilled file in the given list of `deltas` if it exists. This could be
   * 1. A backfilled delta
   * 2. A minor compaction
   */
  def getLastBackfilledFile(deltas: Seq[FileStatus]): Option[FileStatus] = {
    var maxFile: Option[FileStatus] = None
    deltas.foreach {
      case BackfilledDeltaFile(f, _) => maxFile = Some(f)
      case CompactedDeltaFile(f, _, _) => maxFile = Some(f)
      case _ => // do nothing
    }
    maxFile
  }

  /**
   * Extracts the Coordinated Commits configurations from the provided properties.
   */
  def getExplicitCCConfigurations(
      properties: Map[String, String]): Map[String, String] = {
    properties.filter { case (k, _) => TABLE_PROPERTY_KEYS.contains(k) }
  }

  /**
   * Extracts the ICT configurations from the provided properties.
   */
  def getExplicitICTConfigurations(properties: Map[String, String]): Map[String, String] = {
    properties.filter { case (k, _) => ICT_TABLE_PROPERTY_KEYS.contains(k) }
  }

  /**
   * Fetches the SparkSession default configurations for Coordinated Commits. The `withDefaultKey`
   * flag controls whether the keys in the returned map should have the default prefix or not.
   * For example, if property 'coordinatedCommits.commitCoordinator-preview' is set to 'dynamodb'
   * in SparkSession default, then
   *
   *   - fetchDefaultCoordinatedCommitsConfigurations(spark) =>
   *       Map("delta.coordinatedCommits.commitCoordinator-preview" -> "dynamodb")
   *
   *   - fetchDefaultCoordinatedCommitsConfigurations(spark, withDefaultKey = true) =>
   *       Map("spark.databricks.delta.properties.defaults
   *            .coordinatedCommits.commitCoordinator-preview" -> "dynamodb")
   */
  def getDefaultCCConfigurations(
      spark: SparkSession, withDefaultKey: Boolean = false): Map[String, String] = {
    TABLE_PROPERTY_CONFS.flatMap { conf =>
      spark.conf.getOption(conf.defaultTablePropertyKey).map { value =>
        val finalKey = if (withDefaultKey) conf.defaultTablePropertyKey else conf.key
        finalKey -> value
      }
    }.toMap
  }

  /**
   * Verifies that the properties contain exactly the Coordinator Name and Coordinator Conf.
   * If `fromDefault` is true, then the properties have keys with the default prefix.
   */
  private def verifyContainsOnlyCoordinatorNameAndConf(
      properties: Map[String, String],
      command: String,
      fromDefault: Boolean): Unit = {
    Seq(DeltaConfigs.COORDINATED_COMMITS_TABLE_CONF).foreach { conf =>
      if (fromDefault) {
        if (properties.contains(conf.defaultTablePropertyKey)) {
          throw new DeltaIllegalArgumentException(
            errorClass = "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_SESSION",
            messageParameters = Array(
              command, conf.defaultTablePropertyKey, conf.defaultTablePropertyKey))
        }
      } else {
        if (properties.contains(conf.key)) {
          throw new DeltaIllegalArgumentException(
            errorClass = "DELTA_CONF_OVERRIDE_NOT_SUPPORTED_IN_COMMAND",
            messageParameters = Array(command, conf.key))
        }
      }
    }
    Seq(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME,
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF).foreach { conf =>
      if (fromDefault) {
        if (!properties.contains(conf.defaultTablePropertyKey)) {
          throw new DeltaIllegalArgumentException(
            errorClass = "DELTA_MUST_SET_ALL_COORDINATED_COMMITS_CONFS_IN_SESSION",
            messageParameters = Array(command, conf.defaultTablePropertyKey))
        }
      } else {
        if (!properties.contains(conf.key)) {
          throw new DeltaIllegalArgumentException(
            errorClass = "DELTA_MUST_SET_ALL_COORDINATED_COMMITS_CONFS_IN_COMMAND",
            messageParameters = Array(command, conf.key))
        }
      }
    }
  }

  /**
   * Verifies that the property keys do not contain any ICT dependencies for Coordinated Commits.
   */
  private def verifyNotContainsICTConfigurations(
      propKeys: Seq[String], command: String, errorClass: String): Unit = {
    ICT_TABLE_PROPERTY_KEYS.foreach { key =>
      if (propKeys.contains(key)) {
        throw new DeltaIllegalArgumentException(
          errorClass,
          messageParameters = Array(command))
      }
    }
  }

  /**
   * Validates the Coordinated Commits configurations in explicit command overrides for
   * `AlterTableSetPropertiesDeltaCommand`.
   *
   * If the table already has Coordinated Commits configurations present, then we do not allow
   * users to override them via `ALTER TABLE t SET TBLPROPERTIES ...`. Users must downgrade the
   * table and then upgrade it with the new Coordinated Commits configurations.
   * If the table is a Coordinated Commits table or will be one via this ALTER command, then we
   * do not allow users to disable any ICT properties that Coordinated Commits depends on.
   */
  def validateConfigurationsForAlterTableSetPropertiesDeltaCommand(
      existingConfs: Map[String, String],
      propertyOverrides: Map[String, String]): Unit = {
    val existingCoordinatedCommitsConfs = getExplicitCCConfigurations(existingConfs)
    val coordinatedCommitsOverrides = getExplicitCCConfigurations(propertyOverrides)
    if (coordinatedCommitsOverrides.nonEmpty) {
      if (existingCoordinatedCommitsConfs.nonEmpty) {
        throw new DeltaIllegalArgumentException(
          "DELTA_CANNOT_OVERRIDE_COORDINATED_COMMITS_CONFS",
          Array("ALTER"))
      }
      verifyNotContainsICTConfigurations(propertyOverrides.keys.toSeq, command = "ALTER",
        errorClass = "DELTA_CANNOT_SET_COORDINATED_COMMITS_DEPENDENCIES")
      verifyContainsOnlyCoordinatorNameAndConf(
        coordinatedCommitsOverrides, command = "ALTER", fromDefault = false)
    }
    if (existingCoordinatedCommitsConfs.nonEmpty) {
      verifyNotContainsICTConfigurations(propertyOverrides.keys.toSeq, command = "ALTER",
        errorClass = "DELTA_CANNOT_MODIFY_COORDINATED_COMMITS_DEPENDENCIES")
    }
  }

  /**
   * Validates the configurations to unset for `AlterTableUnsetPropertiesDeltaCommand`.
   *
   * If the table already has Coordinated Commits configurations present, then we do not allow users
   * to unset them via `ALTER TABLE t UNSET TBLPROPERTIES ...`. Users could only downgrade the table
   * via `ALTER TABLE t DROP FEATURE ...`. We also do not allow users to unset any ICT properties
   * that Coordinated Commits depends on.
   */
  def validateConfigurationsForAlterTableUnsetPropertiesDeltaCommand(
      existingConfs: Map[String, String],
      propKeysToUnset: Seq[String]): Unit = {
    // If the table does not have any Coordinated Commits configurations, then we do not check the
    // properties to unset. This is because unsetting non-existent entries would either be caught
    // earlier (without `IF EXISTS`) or simply be a no-op (with `IF EXISTS`). Thus, we ignore them
    // instead of throwing an exception.
    if (getExplicitCCConfigurations(existingConfs).nonEmpty) {
      if (propKeysToUnset.exists(TABLE_PROPERTY_KEYS.contains)) {
        throw new DeltaIllegalArgumentException(
          "DELTA_CANNOT_UNSET_COORDINATED_COMMITS_CONFS",
          Array.empty)
      }
      verifyNotContainsICTConfigurations(propKeysToUnset, command = "ALTER",
        errorClass = "DELTA_CANNOT_MODIFY_COORDINATED_COMMITS_DEPENDENCIES")
    }
  }

  /**
   * Validates the Coordinated Commits configurations in explicit command overrides and default
   * SparkSession properties for `CreateDeltaTableCommand`.
   * See `validateConfigurationsForCreateDeltaTableCommandImpl` for details.
   */
  def validateConfigurationsForCreateDeltaTableCommand(
      spark: SparkSession,
      tableExists: Boolean,
      query: Option[LogicalPlan],
      catalogTableProperties: Map[String, String]): Unit = {
    val (command, propertyOverrides) = query match {
      // For CLONE, we cannot use the properties from the catalog table, because they are already
      // the result of merging the source table properties with the overrides, but we do not
      // consider the source table properties for Coordinated Commits.
      case Some(cmd: CloneTableCommand) =>
        (if (tableExists) "REPLACE with CLONE" else "CREATE with CLONE",
          cmd.tablePropertyOverrides)
      case _ => (if (tableExists) "REPLACE" else "CREATE", catalogTableProperties)
    }
    validateConfigurationsForCreateDeltaTableCommandImpl(
      spark, propertyOverrides, tableExists, command)
  }

  /**
   * Validates the Coordinated Commits configurations for the table.
   *   - If the table already exists, the explicit command property overrides must not contain any
   *     Coordinated Commits configurations.
   *   - If the table does not exist, the explicit command property overrides must contain exactly
   *     the Coordinator Name and Coordinator Conf, and no Table Conf. Default configurations are
   *     checked similarly if non of the three properties is present in explicit overrides.
   */
  private[delta] def validateConfigurationsForCreateDeltaTableCommandImpl(
      spark: SparkSession,
      propertyOverrides: Map[String, String],
      tableExists: Boolean,
      command: String): Unit = {
    val coordinatedCommitsConfs = getExplicitCCConfigurations(propertyOverrides)
    if (tableExists) {
      if (coordinatedCommitsConfs.nonEmpty) {
        throw new DeltaIllegalArgumentException(
          "DELTA_CANNOT_OVERRIDE_COORDINATED_COMMITS_CONFS",
          Array(command))
      }
    } else {
      if (coordinatedCommitsConfs.nonEmpty) {
        verifyContainsOnlyCoordinatorNameAndConf(
          coordinatedCommitsConfs, command, fromDefault = false)
      } else {
        val defaultCoordinatedCommitsConfs = getDefaultCCConfigurations(
          spark, withDefaultKey = true)
        if (defaultCoordinatedCommitsConfs.nonEmpty) {
          verifyContainsOnlyCoordinatorNameAndConf(
            defaultCoordinatedCommitsConfs, command, fromDefault = true)
        }
      }
    }
  }

  /**
   * Converts a given Spark [[CatalystTableIdentifier]] to Coordinated Commits [[TableIdentifier]]
   */
  def toCCTableIdentifier(
      catalystTableIdentifierOpt: Option[CatalystTableIdentifier]): Optional[TableIdentifier] = {
    catalystTableIdentifierOpt.map { catalystTableIdentifier =>
      val namespace =
        catalystTableIdentifier.catalog.toSeq ++
          catalystTableIdentifier.database.toSeq
      new TableIdentifier(namespace.toArray, catalystTableIdentifier.table)
    }.map(Optional.of[TableIdentifier]).getOrElse(Optional.empty[TableIdentifier])
  }
}
