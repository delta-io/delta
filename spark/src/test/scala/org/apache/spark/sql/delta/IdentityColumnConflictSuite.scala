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

package org.apache.spark.sql.delta

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.util.ThreadUtils

/**
 * Helper class used in this test suite for describing different transaction conflict scenarios.
 */
sealed trait TransactionConflictTestCase {
  /** label for this transaction scenario. */
  def name: String
  /** SQL command to be executed. */
  def sqlCommand: String
  /** Boolean indicating whether this transaction does a metadata update. */
  def hasMetadataUpdate: Boolean
  /** Boolean indicating whether the SQL command appends data (add files) to the table. */
  def isAppend: Boolean
}

case class NoMetadataUpdateTestCase(
    name: String,
    sqlCommand: String,
    isAppend: Boolean) extends TransactionConflictTestCase {
  val hasMetadataUpdate = false
}

/**
 * A transaction that will do a metadata update but will not be tagged as identity column only nor
 * row tracking enablement only.
 */
case class GenericMetadataUpdateTestCase(
    name: String,
    sqlCommand: String,
    isAppend: Boolean) extends TransactionConflictTestCase {
  val hasMetadataUpdate = true
}

/** A transaction that will be tagged as a metadata update only for identity column. */
case class IdentityOnlyMetadataUpdateTestCase(
    name: String,
    sqlCommand: String,
    isAppend: Boolean) extends TransactionConflictTestCase {
  val hasMetadataUpdate = true
}

/** A transaction that will be tagged as a metadata update only for row tracking enablement. */
case class RowTrackingEnablementOnlyTestCase(
    name: String,
    sqlCommand: String,
    isAppend: Boolean) extends TransactionConflictTestCase {
  val hasMetadataUpdate = true
}

trait IdentityColumnConflictSuiteBase
    extends IdentityColumnTestUtils
    with TransactionExecutionTestMixin
    with PhaseLockingTestMixin {
  override def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key, "true")

  val colName = "id"

  private def setupEmptyTableWithRowTrackingTableFeature(
      tblIsoLevel: Option[IsolationLevel], tblName: String): Unit = {
    val tblPropertiesMap: Map[String, String] = Map(
      TableFeatureProtocolUtils.propertyKey(RowTrackingFeature) -> "supported",
      DeltaConfigs.ROW_TRACKING_ENABLED.key -> "false",
      DeltaConfigs.ISOLATION_LEVEL.key ->
        tblIsoLevel.map(_.toString).getOrElse(DeltaConfigs.ISOLATION_LEVEL.defaultValue)
    )

    createTableWithIdColAndIntValueCol(
      tableName = tblName,
      generatedAsIdentityType = GeneratedAsIdentityType.GeneratedByDefault,
      startsWith = Some(1),
      incrementBy = Some(1),
      tblProperties = tblPropertiesMap
    )
  }

  /**
   * Returns the expected exception class for the test case.
   * Returns None if no exception is expected.
   */
  private def expectedExceptionClass(
      currentTxn: TransactionConflictTestCase,
      winningTxn: TransactionConflictTestCase): Option[Class[_ <: RuntimeException]] = {
    val currentTxnShouldAbortDueToMetadataUpdate = winningTxn match {
      case _: NoMetadataUpdateTestCase => false
      case _: IdentityOnlyMetadataUpdateTestCase if !currentTxn.hasMetadataUpdate => false
      case _: RowTrackingEnablementOnlyTestCase if !currentTxn.hasMetadataUpdate => false
      case _ => true
    }

    // Metadata update is checked before concurrent append in ConflictChecker.
    if (currentTxnShouldAbortDueToMetadataUpdate) {
      return Some(classOf[io.delta.exceptions.MetadataChangedException])
    }

    val currentTxnShouldAbortDueToConcurrentAppend = winningTxn.isAppend &&
      currentTxn.isInstanceOf[IdentityOnlyMetadataUpdateTestCase] && !currentTxn.isAppend

    if (currentTxnShouldAbortDueToConcurrentAppend) {
        return Some(classOf[io.delta.exceptions.ConcurrentAppendException])
    }

    None
  }

  /**
   * Helper function to test two concurrently running commands. Winning transaction commits before
   * current transaction commits.
   */
  private def transactionIdentityConflictHelper(
      currentTxn: TransactionConflictTestCase,
      winningTxn: TransactionConflictTestCase,
      tblIsoLevel: Option[IsolationLevel]): Unit = {
    val tblName = getRandomTableName
    withTable(tblName) {
      // We start with an empty table that has row tracking table feature support and row tracking
      // table property disabled. This way, when we set the table property to true, it will not
      // also do a protocol upgrade and we don't need any backfill commit.
      setupEmptyTableWithRowTrackingTableFeature(tblIsoLevel, tblName)

      val threadPool =
        ThreadUtils.newDaemonSingleThreadExecutor(threadName = "identity-column-thread-pool")
      var (txnObserver, future) = runQueryWithObserver(
        name = "current", threadPool, currentTxn.sqlCommand.replace("{tblName}", tblName))

      // If the current txn is enabling row tracking on an existing table, the first txn is
      // a NOOP since there are no files in the table initially. No commit will be made.
      // Let's "skip" this txn obj. Replace the observer after the first commit.
      // We want to observe the metadata update in this test, which happens after backfill.
      val metadataUpdateObserver = new PhaseLockingTransactionExecutionObserver(
        OptimisticTransactionPhases.forName("late-txn"))
      if (currentTxn.isInstanceOf[RowTrackingEnablementOnlyTestCase]) {
        txnObserver.setNextObserver(metadataUpdateObserver, autoAdvance = true)
        unblockAllPhases(txnObserver)
        txnObserver.phases.backfillPhase.exitBarrier.unblock()
        txnObserver = metadataUpdateObserver
      }

      unblockUntilPreCommit(txnObserver)
      busyWaitFor(txnObserver.phases.preparePhase.hasEntered, timeout)

      sql(winningTxn.sqlCommand.replace("{tblName}", tblName))

      val expectedException = expectedExceptionClass(currentTxn, winningTxn)
      val events = Log4jUsageLogger.track {
        try {
          unblockCommit(txnObserver)
          ThreadUtils.awaitResult(future, Duration.Inf)
          assert(expectedException.isEmpty, "Expected txn to fail, but no exception was thrown")
        } catch {
          case NonFatal(e) =>
            assert(expectedException.isDefined, "Unexpected failure")
            assert(e.getCause.getClass === expectedException.get)
        }
      }

      // We should log if the txn is aborted due to identity column only metadata update.
      if (currentTxn.hasMetadataUpdate
          && winningTxn.isInstanceOf[IdentityOnlyMetadataUpdateTestCase]) {
        val identityColumnAbortEvents = events
            .filter(_.tags.get("opType").contains(IdentityColumn.opTypeAbort))
        assert(identityColumnAbortEvents.size === 1)
      }
    }
  }

  // scalastyle:off line.size.limit
  /**
   * We are testing the following combinations (see [[ConflictChecker.checkNoMetadataUpdates]]
   * for details).
   *
   * |                                               | Winning Metadata (id) | Winning Metadata Row Tracking Enablement Only | Winning Metadata (other) | Winning No Metadata |
   * | --------------------------------------------- | --------------------- | --------------------------------------------- | ------------------------ | ------------------- |
   * | Current Metadata (id)                         | Conflict              | Conflict                                      | Conflict                 | No conflict         |
   * | Current Metadata Row Tracking Enablement Only | Conflict              | Conflict                                      | Conflict                 | No conflict         |
   * | Current Metadata (other)                      | Conflict              | Conflict                                      | Conflict                 | No conflict         |
   * | Current No Metadata                           | No conflict           | No conflict                                   | Conflict                 | No conflict         |
   */
  // scalastyle:on line.size.limit

  // System generated IDENTITY value will have a metadata update for IDENTITY high water marks.
  private val generatedIdTestCase = IdentityOnlyMetadataUpdateTestCase(
    name = "generatedId",
    sqlCommand = s"INSERT INTO {tblName}(value) VALUES (1)",
    isAppend = true
  )

  // SYNC IDENTITY updates the high water mark based on the values in the IDENTITY column.
  private val syncIdentityTestCase = IdentityOnlyMetadataUpdateTestCase(
    name = "syncIdentity",
    sqlCommand = s"ALTER TABLE {tblName} ALTER COLUMN $colName SYNC IDENTITY",
    isAppend = false
  )

  // Explicitly provided IDENTITY value will not generate a metadata update.
  private val noMetadataUpdateTestCase =
    NoMetadataUpdateTestCase(
      name = "noMetadataUpdate",
      sqlCommand = s"INSERT INTO {tblName} VALUES (1, 1)",
      isAppend = true
    )

  private val rowTrackingEnablementTestCase = RowTrackingEnablementOnlyTestCase(
      name = "rowTrackingEnablement",
      sqlCommand =
        s"""ALTER TABLE {tblName}
           |SET TBLPROPERTIES(
           |'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true'
           |)""".stripMargin,
      isAppend = false
    )

  private val otherMetadataUpdateTestCase = GenericMetadataUpdateTestCase(
      name = "otherMetadataUpdate",
      sqlCommand = s"ALTER TABLE {tblName} ADD COLUMN value2 STRING",
      isAppend = false
    )

  private val conflictTestCases: Seq[TransactionConflictTestCase] = Seq(
    generatedIdTestCase,
    syncIdentityTestCase,
    noMetadataUpdateTestCase,
    rowTrackingEnablementTestCase,
    otherMetadataUpdateTestCase
  )

  for {
    currentTxn <- conflictTestCases
    winningTxn <- conflictTestCases
  } {
    val testName =
      s"identity conflict test: [currentTxn: ${currentTxn.name}, winningTxn: ${winningTxn.name}]"
    test(testName) {
      transactionIdentityConflictHelper(
        currentTxn,
        winningTxn,
        tblIsoLevel = None
      )
    }
  }

  test("ALTER TABLE SYNC IDENTITY conflict on serializable table") {
    transactionIdentityConflictHelper(
      syncIdentityTestCase,
      noMetadataUpdateTestCase,
      tblIsoLevel = Some(Serializable)
    )
  }

  test("high watermark changes after analysis but before execution of merge") {
    val tblName = getRandomTableName
    withIdentityColumnTable(GeneratedAsIdentityType.GeneratedAlways, tblName) {
      // Create a QueryExecution object for a MERGE statement, and it forces the command to be
      // analyzed, but does not execute the command yet.
      val parsedMerge = spark.sessionState.sqlParser.parsePlan(
        s"""MERGE INTO $tblName t
           |USING (SELECT * FROM range(1000)) s
           |ON t.id = s.id
           |WHEN NOT MATCHED THEN INSERT (value) VALUES (s.id)""".stripMargin)
      val qeMerge = new QueryExecution(spark, parsedMerge)
      qeMerge.analyzed

      // Insert a row, forcing the high watermark to be updated.
      sql(s"INSERT INTO $tblName (value) VALUES (0)")

      // Force merge to be executed. This should fail, as MERGE is still using the old high
      // watermark in its insert action.
      intercept[MetadataChangedException] {
        qeMerge.commandExecuted
      }
    }
  }
}

class IdentityColumnConflictScalaSuite
  extends IdentityColumnConflictSuiteBase
  with ScalaDDLTestUtils
