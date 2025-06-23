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

package org.apache.spark.sql.delta.util

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, CommittedTransaction, CoordinatedCommitsStats, CoordinatedCommitType, DeltaConfigs, DeltaLog, IsolationLevel, Snapshot}
import org.apache.spark.sql.delta.DeltaOperations.Operation
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.coordinatedcommits.{CatalogOwnedTableUtils, TableCommitCoordinatorClient}
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable

/**
 * Contains helper methods for Delta transactions.
 */
trait TransactionHelper extends DeltaLogging {
  def deltaLog: DeltaLog
  def catalogTable: Option[CatalogTable]
  def snapshot: Snapshot

  /** Unique identifier for the transaction */
  def txnId: String

  /**
   * Returns the metadata for this transaction. The metadata refers to the metadata of the snapshot
   * at the transaction's read version unless updated during the transaction.
   */
  def metadata: Metadata

  /** The protocol of the snapshot that this transaction is reading at. */
  def protocol: Protocol

  /**
   * Default [[IsolationLevel]] as set in table metadata.
   */
  private[delta] def getDefaultIsolationLevel(): IsolationLevel = {
    DeltaConfigs.ISOLATION_LEVEL.fromMetaData(metadata)
  }

  /**
   * Return the user-defined metadata for the operation.
   */
  def getUserMetadata(op: Operation): Option[String] = {
    // option wins over config if both are set
    op.userMetadata match {
      case data @ Some(_) => data
      case None => spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_USER_METADATA)
    }
  }

  /** The current spark session */
  protected def spark: SparkSession = SparkSession.active

  private[delta] lazy val readSnapshotTableCommitCoordinatorClientOpt:
      Option[TableCommitCoordinatorClient] = {
    if (snapshot.isCatalogOwned) {
      // Catalog owned table's commit coordinator is always determined by the catalog.
      CatalogOwnedTableUtils.populateTableCommitCoordinatorFromCatalog(
        spark, catalogTable, snapshot)
    } else {
      // The commit-coordinator of a table shouldn't change. If it is changed by a concurrent
      // commit, then it will be detected as a conflict and the transaction will anyway fail.
      snapshot.getTableCommitCoordinatorForWrites
    }
  }

  def createCoordinatedCommitsStats(newProtocol: Option[Protocol]) : CoordinatedCommitsStats = {
    val (coordinatedCommitsType, metadataToUse) =
      readSnapshotTableCommitCoordinatorClientOpt match {
        // TODO: Capture the CO -> FS downgrade case when we start
        //       supporting downgrade for CO.
        case Some(_) if snapshot.isCatalogOwned =>                             // CO commit
          (CoordinatedCommitType.CO_COMMIT, snapshot.metadata)
        case Some(_) if metadata.coordinatedCommitsCoordinatorName.isEmpty =>  // CC -> FS
          (CoordinatedCommitType.CC_TO_FS_DOWNGRADE_COMMIT, snapshot.metadata)
        // Only the 0th commit to a table can be a FS -> CO upgrade for now.
        // Upgrading an existing FS table to CO through ALTER TABLE is not supported yet.
        case None if newProtocol.exists(_.readerAndWriterFeatureNames
            .contains(CatalogOwnedTableFeature.name)) =>                       // FS -> CO
          (CoordinatedCommitType.FS_TO_CO_UPGRADE_COMMIT, metadata)
        case None if metadata.coordinatedCommitsCoordinatorName.isDefined =>   // FS -> CC
          (CoordinatedCommitType.FS_TO_CC_UPGRADE_COMMIT, metadata)
        case Some(_) =>                                                        // CC commit
          (CoordinatedCommitType.CC_COMMIT, snapshot.metadata)
        case None =>                                                           // FS commit
          (CoordinatedCommitType.FS_COMMIT, snapshot.metadata)
        // Errors out in rest of the cases.
        case _ =>
          throw new IllegalStateException(
            "Unexpected state found when trying " +
            s"to generate CoordinatedCommitsStats for table ${deltaLog.logPath}. " +
            s"$readSnapshotTableCommitCoordinatorClientOpt, " +
            s"$metadata, $snapshot, $catalogTable")
      }
    CoordinatedCommitsStats(
      coordinatedCommitsType = coordinatedCommitsType.toString,
      commitCoordinatorName = if (Set(CoordinatedCommitType.CO_COMMIT,
        CoordinatedCommitType.FS_TO_CO_UPGRADE_COMMIT).contains(coordinatedCommitsType)) {
        // The catalog for FS -> CO upgrade commit would be
        // "CATALOG_EMPTY" because `catalogTable` is not available
        // for the 0th FS commit.
        catalogTable.flatMap { ct =>
          CatalogOwnedTableUtils.getCatalogName(
            spark,
            identifier = ct.identifier)
        }.getOrElse("CATALOG_MISSING")
      } else {
        metadataToUse.coordinatedCommitsCoordinatorName.getOrElse("NONE")
      },
      // For Catalog-Owned table, the coordinator conf for UC-CC is [[Map.empty]]
      // so we don't distinguish between CO/CC here.
      commitCoordinatorConf = metadataToUse.coordinatedCommitsCoordinatorConf)
  }

  /**
   * Determines if we should checkpoint the version that has just been committed.
   */
  protected def isCheckpointNeeded(
      committedVersion: Long, postCommitSnapshot: Snapshot): Boolean = {
    def checkpointInterval = deltaLog.checkpointInterval(postCommitSnapshot.metadata)
    committedVersion != 0 && committedVersion % checkpointInterval == 0
  }

  /** Runs a post-commit hook, handling any exceptions that occur. */
  protected def runPostCommitHook(
      hook: PostCommitHook,
      committedTransaction: CommittedTransaction): Unit = {
    val version = committedTransaction.committedVersion
    try {
      hook.run(spark, committedTransaction)
    } catch {
      case NonFatal(e) =>
        logWarning(log"Error when executing post-commit hook " +
          log"${MDC(DeltaLogKeys.HOOK_NAME, hook.name)} " +
          log"for commit ${MDC(DeltaLogKeys.VERSION, version)}", e)
        recordDeltaEvent(deltaLog, "delta.commit.hook.failure", data = Map(
          "hook" -> hook.name,
          "version" -> version,
          "exception" -> e.toString
        ))
        hook.handleError(spark, e, version)
    }
  }

  /**
   * Generates a timestamp which is greater than the commit timestamp
   * of the last snapshot. Note that this is only needed when the
   * feature `inCommitTimestamps` is enabled.
   */
  protected[delta] def generateInCommitTimestampForFirstCommitAttempt(
      currentTimestamp: Long): Option[Long] =
    Option.when(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetaData(metadata)) {
      val lastCommitTimestamp = snapshot.timestamp
      math.max(currentTimestamp, lastCommitTimestamp + 1)
    }
}
