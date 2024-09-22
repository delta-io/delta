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

package org.apache.spark.sql.delta.test

import java.io.File

import org.apache.spark.sql.delta.{DeltaLog, OptimisticTransaction, Snapshot}
import org.apache.spark.sql.delta.DeltaOperations.{ManualUpdate, Operation, Write}
import org.apache.spark.sql.delta.actions.{Action, AddFile, Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.optimize.OptimizeMetrics
import org.apache.spark.sql.delta.coordinatedcommits.TableCommitCoordinatorClient
import org.apache.spark.sql.delta.hooks.AutoCompact
import org.apache.spark.sql.delta.stats.StatisticsCollection
import io.delta.storage.commit.{CommitResponse, GetCommitsResponse, UpdatedActions}
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.util.Clock

/**
 * Additional method definitions for Delta classes that are intended for use only in testing.
 */
object DeltaTestImplicits {
  implicit class OptimisticTxnTestHelper(txn: OptimisticTransaction) {

    /** Ensure that the initial commit of a Delta table always contains a Metadata action */
    def commitActions(op: Operation, actions: Action*): Long = {
      if (txn.readVersion == -1) {
        val metadataOpt = actions.collectFirst { case m: Metadata => m }
        val protocolOpt = actions.collectFirst { case p: Protocol => p }
        val otherActions =
          actions.filterNot(a => a.isInstanceOf[Metadata] || a.isInstanceOf[Protocol])
        (metadataOpt, protocolOpt) match {
          case (Some(metadata), Some(protocol)) =>
            // When both metadata and protocol are explicitly passed, use them.
            txn.updateProtocol(protocol)
            // This will auto upgrade any required table features in the passed protocol as per
            // given metadata.
            txn.updateMetadataForNewTable(metadata)
          case (Some(metadata), None) =>
            // When just metadata is passed, use it.
            // This will auto generate protocol as per metadata.
            txn.updateMetadataForNewTable(metadata)
          case (None, Some(protocol)) =>
            txn.updateProtocol(protocol)
            txn.updateMetadataForNewTable(Metadata())
          case (None, None) =>
            // If neither metadata nor protocol is explicitly passed, then use default Metadata and
            // with the maximum protocol.
            txn.updateMetadataForNewTable(Metadata())
            txn.updateProtocol(Action.supportedProtocolVersion())
        }
        txn.commit(otherActions, op)
      } else {
        txn.commit(actions, op)
      }
    }

    def commitManually(actions: Action*): Long = {
      commitActions(ManualUpdate, actions: _*)
    }

    def commitWriteAppend(actions: Action*): Long = {
      commitActions(Write(SaveMode.Append), actions: _*)
    }
  }

  /** Add test-only File overloads for DeltaTable.forPath */
  implicit class DeltaLogObjectTestHelper(deltaLog: DeltaLog.type) {
    def forTable(spark: SparkSession, dataPath: File): DeltaLog = {
      DeltaLog.forTable(spark, new Path(dataPath.getCanonicalPath))
    }

    def forTable(spark: SparkSession, dataPath: File, clock: Clock): DeltaLog = {
      DeltaLog.forTable(spark, new Path(dataPath.getCanonicalPath), clock)
    }
  }

  /** Helper class for working with [[TableCommitCoordinatorClient]] */
  implicit class TableCommitCoordinatorClientTestHelper(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient) {

    def commit(
        commitVersion: Long,
        actions: Iterator[String],
        updatedActions: UpdatedActions): CommitResponse = {
      tableCommitCoordinatorClient.commit(
        commitVersion, actions, updatedActions, tableIdentifierOpt = None)
    }

    def getCommits(
        startVersion: Option[Long] = None,
        endVersion: Option[Long] = None): GetCommitsResponse = {
      tableCommitCoordinatorClient.getCommits(tableIdentifierOpt = None, startVersion, endVersion)
    }

    def backfillToVersion(
        version: Long,
        lastKnownBackfilledVersion: Option[Long] = None): Unit = {
      tableCommitCoordinatorClient.backfillToVersion(
        tableIdentifierOpt = None, version, lastKnownBackfilledVersion)
    }
  }


  /** Helper class for working with [[Snapshot]] */
  implicit class SnapshotTestHelper(snapshot: Snapshot) {
    def ensureCommitFilesBackfilled(): Unit = {
      snapshot.ensureCommitFilesBackfilled(tableIdentifierOpt = None)
    }
  }

  /**
   * Helper class for working with the most recent snapshot in the deltaLog
   */
  implicit class DeltaLogTestHelper(deltaLog: DeltaLog) {
    def snapshot: Snapshot = {
      deltaLog.unsafeVolatileSnapshot
    }

    def checkpoint(): Unit = {
      deltaLog.checkpoint(snapshot)
    }

    def checkpointInterval(): Int = {
      deltaLog.checkpointInterval(snapshot.metadata)
    }

    def deltaRetentionMillis(): Long = {
      deltaLog.deltaRetentionMillis(snapshot.metadata)
    }

    def enableExpiredLogCleanup(): Boolean = {
      deltaLog.enableExpiredLogCleanup(snapshot.metadata)
    }

    def upgradeProtocol(newVersion: Protocol): Unit = {
      upgradeProtocol(deltaLog.unsafeVolatileSnapshot, newVersion)
    }

    def upgradeProtocol(snapshot: Snapshot, newVersion: Protocol): Unit = {
      deltaLog.upgradeProtocol(None, snapshot, newVersion)
    }
  }

  implicit class DeltaTableV2ObjectTestHelper(dt: DeltaTableV2.type) {
    /** Convenience overload that omits the cmd arg (which is not helpful in tests). */
    def apply(spark: SparkSession, id: TableIdentifier): DeltaTableV2 =
      dt.apply(spark, id, "test")
  }

  implicit class DeltaTableV2TestHelper(deltaTable: DeltaTableV2) {
    /** For backward compatibility with existing unit tests */
    def snapshot: Snapshot = deltaTable.initialSnapshot
  }

  implicit class AutoCompactObjectTestHelper(ac: AutoCompact.type) {
    private[delta] def compact(
      spark: SparkSession,
      deltaLog: DeltaLog,
      partitionPredicates: Seq[Expression] = Nil,
      opType: String = AutoCompact.OP_TYPE): Seq[OptimizeMetrics] = {
      AutoCompact.compact(
        spark, deltaLog, catalogTable = None,
        partitionPredicates, opType)
    }
  }

  implicit class StatisticsCollectionObjectTestHelper(sc: StatisticsCollection.type) {

    /**
     * This is an implicit helper required for backward compatibility with existing
     * unit tests. It allows to call [[StatisticsCollection.recompute]] without a
     * catalog table and in the actual call, sets it to [[None]].
     */
    def recompute(
      spark: SparkSession,
      deltaLog: DeltaLog,
      predicates: Seq[Expression] = Seq(Literal(true)),
      fileFilter: AddFile => Boolean = af => true): Unit = {
      StatisticsCollection.recompute(
        spark, deltaLog, catalogTable = None, predicates, fileFilter)
    }
  }
}
