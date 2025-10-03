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

package org.apache.spark.sql.delta.rowid

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta.{ConflictResolutionTestUtils, DeltaConfigs, DeltaErrors, DeltaIllegalStateException, DeltaLog, DeltaOperations, DeltaTableFeatureException, ProtocolChangedException, RemovableFeature, RowTrackingFeature, TableFeature}
import org.apache.spark.sql.delta.DeltaTestUtils.BOOLEAN_DOMAIN
import org.apache.spark.sql.delta.actions.{AddFile, DropTableFeatureUtils}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableSetPropertiesDeltaCommand
import org.apache.spark.sql.delta.commands.backfill.{BackfillCommandStats, BackfillExecutionObserver, BackfillExecutor, RowTrackingBackfillCommand, RowTrackingBackfillExecutor, RowTrackingUnBackfillCommand, RowTrackingUnBackfillExecutor}
import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}
import org.apache.spark.sql.delta.fuzzer.{AtomicBarrier, OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver => TransactionObserver}
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkThrowable}
import org.apache.spark.sql.Row
import org.apache.spark.util.ThreadUtils

class RowTrackingRemovalConcurrencySuite
    extends RowTrackingRemovalSuiteBase
    with ConflictResolutionTestUtils
    with TransactionExecutionTestMixin
    with PhaseLockingTestMixin {

  protected val areDVsEnabled = true
  private val ignoreSuspensionConf =
    DeltaSQLConf.DELTA_ROW_TRACKING_IGNORE_SUSPENSION.key ->  "true"

  protected override def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey,
      areDVsEnabled.toString)

  protected def dropRowTrackingTransaction(tableName: String): Array[Row] = {
    sql(s"""ALTER TABLE $tableName DROP FEATURE ${RowTrackingFeature.name}""").collect()
  }

  protected def enableRowTrackingTransaction(tableName: String): Array[Row] = {
    sql(s"""ALTER TABLE $tableName
           |SET TBLPROPERTIES('${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true')
           |""".stripMargin)
      .collect()
  }

  protected def backfillTransaction(deltaLog: DeltaLog): Seq[Row] = {
    RowTrackingBackfillCommand(
      deltaLog,
      nameOfTriggeringOperation = "TEST",
      catalogTableOpt = None).run(spark)
  }

  protected def unBackfillTransaction(deltaLog: DeltaLog): Seq[Row] = {
    RowTrackingUnBackfillCommand(
      deltaLog,
      nameOfTriggeringOperation = "TEST",
      catalogTableOpt = None).run(spark)
  }

  /**
   * Represents a delete transaction by a third party writer that does not respect
   * property `delta.rowTrackingSuspended`.
   */
  case class ThirdPartyDelete(
      condition: String) extends TestTransaction(Map(ignoreSuspensionConf)) {

    override val name: String = s"THIRD PARTY DELETE($condition)($sqlConfStr)"
    override def dataChange: Boolean = true
    override def toSQL(tableName: String): String = s"DELETE FROM $tableName WHERE $condition"
  }

  /**
   * Represents an update transaction by a third party writer that does not respect
   * property `delta.rowTrackingSuspended`.
   */
  case class ThirdPartyUpdate(
      set: String,
      condition: String) extends TestTransaction(Map(ignoreSuspensionConf)) {

    override val name: String = s"THIRD PARTY UPDATE($set)($condition)($sqlConfStr)"
    override def dataChange: Boolean = true
    override def toSQL(tableName: String): String = s"UPDATE $tableName SET $set WHERE $condition"
  }

  /**
   * Represents an insert transaction by a third party writer that does not respect
   * property `delta.rowTrackingSuspended`.
   */
  case class ThirdPartyInsert(
      start: Long,
      end: Long,
      numPartitions: Int = 2,
      sqlConf: Map[String, String] = Map(ignoreSuspensionConf)) extends TestTransaction(sqlConf) {

    override val name: String = s"THIRD PARTY INSERT($start-$end)($sqlConfStr)"
    override def dataChange: Boolean = true
    override def toSQL(tableName: String): String = {
      throw new UnsupportedOperationException("No SQL implementation for ThirdPartyInsert")
    }

    override def executeImpl(ctx: TestContext): Unit = {
      withSQLConf(sqlConf.toSeq: _*) {
        // This should generate baseRowIds.
        spark.range(start, end, step = 1, numPartitions)
          .write
          .format("delta")
          .mode("append")
          .insertInto(s"delta.`${ctx.deltaLog.dataPath}`")
      }
    }
  }

  /**
   * Test transaction that performs a protocol downgrade for a given feature.
   * Note, it does not add the checkpointProtection feature.
   */
  case class DowngradeProtocol(
      feature: TableFeature with RemovableFeature) extends TestTransaction(Map.empty) {

    override val name: String = s"Downgrade(${feature.name})($sqlConfStr)"
    override def dataChange: Boolean = false

    override def toSQL(tableName: String): String = {
      throw new UnsupportedOperationException("No SQL implementation for DowngradeProtocol")
    }

    override def executeImpl(ctx: TestContext): Unit = {
      val deltaLog = ctx.deltaLog
      val table = DeltaTableV2(spark, deltaLog.dataPath)
      val txn = deltaLog.startTransaction(catalogTableOpt = None)

      if (!feature.validateDropInvariants(table, txn.snapshot)) {
        throw DeltaErrors.dropTableFeatureConflictRevalidationFailed()
      }

      txn.updateProtocol(txn.protocol.removeFeature(feature))
      val metadataWithNewConfiguration = DropTableFeatureUtils
        .getDowngradedProtocolMetadata(feature, txn.metadata)
      txn.updateMetadata(metadataWithNewConfiguration)
      val commitActions = feature.actionsToIncludeAtDowngradeCommit(txn.snapshot)
      txn.commit(commitActions, DeltaOperations.DropTableFeature(feature.name, false))
    }
  }

  /** Test implementation of backfill batch. */
  private def backfillBatchImplementation(
      deltaLog: DeltaLog,
      executor: BackfillExecutor): Unit = {
    BackfillExecutionObserver.getObserver.executeBatch {
      val txn = deltaLog.startTransaction(catalogTableOpt = None)
      val filesInBatch = executor
        .filesToBackfill(txn.snapshot)
        .collect()
      if (filesInBatch.isEmpty) {
        return
      }

      val batch = executor.constructBatch(filesInBatch)
      txn.trackFilesRead(filesInBatch)
      batch.execute(
        spark,
        backfillTxnId = executor.backfillTxnId,
        batchId = 0,
        txn = txn,
        numSuccessfulBatch = new AtomicInteger(0),
        numFailedBatch = new AtomicInteger(0))
    }
  }

  /**
   * Test transaction that unbackfill baseRowIDs. It assumes all files can be
   * unbackfilled in a single commit.
   */
  case class UnbackfillBatch() extends TestTransaction(Map.empty) {
    override val name: String = "UNBACKFILL"
    override def dataChange: Boolean = false

    override def toSQL(tableName: String): String = {
      throw new UnsupportedOperationException("No SQL implementation for Unbackfill")
    }

    override def executeImpl(ctx: TestContext): Unit = {
      val deltaLog = ctx.deltaLog
      val propertyKey = DeltaSQLConf.DELTA_ROW_TRACKING_IGNORE_SUSPENSION.key
      withSQLConf(propertyKey -> "false") {
        val backfillStats = BackfillCommandStats(
          transactionId = "test-backfill-batch",
          "TEST"
        )
        backfillBatchImplementation(
          deltaLog,
          new RowTrackingUnBackfillExecutor(
            spark,
            deltaLog,
            catalogTableOpt = None,
            backfillTxnId = backfillStats.transactionId,
            backfillStats = backfillStats
          ))
      }
    }
  }

  /**
   * Test transaction that backfills baseRowIDs. It ignores
   * `rowTrackingSuspended` property. However,it assumes the third party
   * client uses ROW_TRACKING_BACKFILL_OPERATION_NAME. Finally, it assumes all files can be
   * backfilled in a single commit.
   */
  case class ThirdPartyBackfillBatch() extends TestTransaction(Map.empty) {
    override val name: String = "BACKFILL"
    override def dataChange: Boolean = false

    override def toSQL(tableName: String): String = {
      throw new UnsupportedOperationException("No SQL implementation for Backfill")
    }

    override def executeImpl(ctx: TestContext): Unit = {
      val deltaLog = ctx.deltaLog
      withSQLConf(DeltaSQLConf.DELTA_ROW_TRACKING_IGNORE_SUSPENSION.key -> "true") {
        val backfillStats = BackfillCommandStats(
          transactionId = "test-backfill-batch",
          "TEST"
        )
        backfillBatchImplementation(
          deltaLog,
          new RowTrackingBackfillExecutor(
            spark,
            deltaLog,
            catalogTableOpt = None,
            backfillTxnId = backfillStats.transactionId,
            backfillStats = backfillStats
          ))
      }
    }
  }

  private def createTestTable(
      dir: File,
      numPartitions: Int = 2,
      rowTrackingEnabled: Boolean = true): DeltaLog = {
    sql(
      s"""CREATE TABLE delta.`${dir.getCanonicalPath}` (id bigint)
         |USING delta
         |TBLPROPERTIES(
         |'${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = '${rowTrackingEnabled.toString}'
         |)""".stripMargin)

    spark.range(start = 0, end = 100, step = 1, numPartitions)
      .write
      .format("delta")
      .mode("append")
      .save(dir.getAbsolutePath)

    val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)
    if (rowTrackingEnabled) {
      validateRowTrackingState(deltaLog, isPresent = true)
    }
    val expectedFileCountWithRowIDs = if (rowTrackingEnabled) numPartitions else 0
    validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs)
    deltaLog
  }

  private def validateRowTrackingMetadataInAddFiles(
      deltaLog: DeltaLog,
      expectedFileCountWithRowIDs: Int): Unit = {
    val snapshot = deltaLog.update()
    val filesWithRowIDsCount = snapshot
      .allFiles
      .filter("baseRowId IS NOT NULL or defaultRowCommitVersion IS NOT NULL")
      .count()
    assert(filesWithRowIDsCount === expectedFileCountWithRowIDs)
  }

  private def disableRowTracking(tablePath: String): Unit = {
    // Fist stage of row tracking removal: disable the feature.
    val propertiesToSet = Map(
      DeltaConfigs.ROW_TRACKING_ENABLED.key -> "false",
      DeltaConfigs.ROW_TRACKING_SUSPENDED.key -> "true")
    val table = DeltaTableV2(spark, new Path(tablePath))
    AlterTableSetPropertiesDeltaCommand(table, propertiesToSet).run(spark)
  }

  private def waitForTransactionStart(
      observer: TransactionObserver): Unit = {
    def transactionStart: Boolean = {
      observer.phases.initialPhase.entryBarrier.load() ==
        AtomicBarrier.State.Requested
    }
    busyWaitFor(transactionStart, timeout)
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Drop Feature
   * Command        Metadata upgrade -------- Unbackfill batches ---------------- Downgrade Commit
   *                prepare + commit          prepare + commit                    prepare + commit
   *
   *
   * Business Txn                                                prepare + commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  for (op <- Seq("insert", "update", "delete"))
  test(s"$op interleaves between the last unbackfill and the protocol downgrade") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir)
      val ctx = new TestContext(deltaLog)
      val table = s"delta.`${dir.getAbsolutePath}`"
      val dropRowTrackingFn = () => dropRowTrackingTransaction(table)

      val Seq(dropFuture) = runFunctionsWithOrderingFromObserver(Seq(dropRowTrackingFn)) {
        case (updateMetadataDropObserver :: Nil) =>
          val unbackfillDropObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("unbackfill-drop-txn"))
          val downgradeDropObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("downgrade-drop-txn"))

          updateMetadataDropObserver.setNextObserver(
            unbackfillDropObserver, autoAdvance = true)
          unbackfillDropObserver.setNextObserver(
            downgradeDropObserver, autoAdvance = true)

          prepareAndCommitWithNextObserverSet(updateMetadataDropObserver)
          prepareAndCommitWithNextObserverSet(unbackfillDropObserver)

          waitForTransactionStart(downgradeDropObserver)

          val businessTxn = op match {
            case "insert" => ThirdPartyInsert(start = 100, end = 200)
            case "update" => ThirdPartyUpdate(set = "id = 200", condition = "id = 90")
            case "delete" => ThirdPartyDelete("id = 90")
          }

          businessTxn.execute(ctx)

          val expectedFileCountWithRowIDs = op match {
            case "insert" => 2
            case "update" => if (areDVsEnabled) 2 else 1
            case "delete" => 1
          }
          validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs)

          prepareAndCommit(downgradeDropObserver)
      }
      ThreadUtils.awaitResult(dropFuture, timeout)
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)
      validateRowTrackingState(deltaLog, isPresent = false)

      val targetTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
      val expectedValues = op match {
        case "insert" => (0 to 199)
        case "update" => (0 to 99).filterNot(_ == 90) :+ 200
        case "delete" => (0 to 99).filterNot(_ == 90)
      }
      checkAnswer(targetTable.toDF, expectedValues.map(Row(_)))
    }
  }


  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Drop Feature
   * Command        Metadata upgrade -------- Unbackfill -------------- Unbackfill --- Downgrade
   *                prepare + commit          Batch 1                   Batch 2        Commit
   *                                          prep+commit               prep+commit    prep+commit
   *
   *
   * Business Txn                                          prep+commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  test("Business txn interleaves between two unbackfill batches") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir, numPartitions = 3)
      val ctx = new TestContext(deltaLog)
      val table = s"delta.`${dir.getAbsolutePath}`"
      val dropRowTrackingFn = () => dropRowTrackingTransaction(table)

      withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key -> "2") {
        val Seq(dropFuture) = runFunctionsWithOrderingFromObserver(Seq(dropRowTrackingFn)) {
          case (updateMetadataDropObserver :: Nil) =>
            val unbackfillBatch1DropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("unbackfill-batch-1-drop-txn"))
            val unbackfillBatch2DropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("unbackfill-batch-2-drop-txn"))
            val downgradeDropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("downgrade-drop-txn"))

            updateMetadataDropObserver.setNextObserver(
              unbackfillBatch1DropObserver, autoAdvance = true)
            unbackfillBatch1DropObserver.setNextObserver(
              unbackfillBatch2DropObserver, autoAdvance = true)
            unbackfillBatch2DropObserver.setNextObserver(
              downgradeDropObserver, autoAdvance = true)

            prepareAndCommitWithNextObserverSet(updateMetadataDropObserver)

            // Block unbackfill batch 1 right after commit.
            unblockUntilPreCommit(unbackfillBatch1DropObserver)
            waitForPrecommit(unbackfillBatch1DropObserver)
            unblockCommit(unbackfillBatch1DropObserver)
            busyWaitFor(unbackfillBatch1DropObserver.phases.commitPhase.hasLeft, timeout)
            busyWaitFor(unbackfillBatch1DropObserver.phases.backfillPhase.hasLeft, timeout)

            // We unbackfilled 2 of the 3 files.
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 1)

            ThirdPartyInsert(start = 100, end = 110, numPartitions = 1).execute(ctx)

            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)

            // Allow to proceed to batch 2.
            unbackfillBatch1DropObserver.phases.postCommitPhase.leave()

            // Batch 2 picked the single backfilled file by the interleaved txn.
            prepareAndCommitWithNextObserverSet(unbackfillBatch2DropObserver)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)

            prepareAndCommit(downgradeDropObserver)
        }
        ThreadUtils.awaitResult(dropFuture, timeout)
        validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)
        validateRowTrackingState(deltaLog, isPresent = false)

        val targetTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
        val expectedValues = (0 to 109)
        checkAnswer(targetTable.toDF, expectedValues.map(Row(_)))
      }
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Drop Feature
   * Command        Metadata upgrade -------- Unbackfill ---- Unbackfill --------------- Downgrade
   *                prepare + commit          Batch 1         Batch 2                    Commit
   *                                          prep+commit     prepare             commit prep+commit
   *
   *
   * Enable Row Tracking
   * Scenario 1:                                                        Metadata
   *                                                                    upgrade
   * Scenario 2:                                                        Backfill
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  for (isScenario1 <- BOOLEAN_DOMAIN)
  test(s"Enable row tracking during unbackfill - isScenario1: $isScenario1") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir, numPartitions = 3)
      val table = s"delta.`${dir.getAbsolutePath}`"
      val dropRowTrackingFn = () => dropRowTrackingTransaction(table)

      withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key -> "2") {
        val Seq(dropFuture) = runFunctionsWithOrderingFromObserver(Seq(dropRowTrackingFn)) {
          case (updateMetadataDropObserver :: Nil) =>
            val unbackfillBatch1DropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("unbackfill-batch-1-drop-txn"))
            val unbackfillBatch2DropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("unbackfill-batch-2-drop-txn"))
            val downgradeDropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("downgrade-drop-txn"))

            updateMetadataDropObserver.setNextObserver(
              unbackfillBatch1DropObserver, autoAdvance = true)
            unbackfillBatch1DropObserver.setNextObserver(
              unbackfillBatch2DropObserver, autoAdvance = true)
            unbackfillBatch2DropObserver.setNextObserver(
              downgradeDropObserver, autoAdvance = true)

            prepareAndCommitWithNextObserverSet(updateMetadataDropObserver)
            prepareAndCommitWithNextObserverSet(unbackfillBatch1DropObserver)

            unblockUntilPreCommit(unbackfillBatch2DropObserver)
            waitForPrecommit(unbackfillBatch2DropObserver)

            if (isScenario1) {
              // Trying to re-enable row tracking during removal causes the alter table command
              // to fail.
              val e = intercept[DeltaIllegalStateException] {
                enableRowTrackingTransaction(table)
              }
              assert(e.getErrorClass === "DELTA_ROW_TRACKING_ILLEGAL_PROPERTY_COMBINATION")
            } else {
              // Backfill fails if run together backfill.
              assertThrows[IllegalStateException] {
                backfillTransaction(deltaLog)
              }
            }

            // Commit batch 2.
            unblockCommit(unbackfillBatch2DropObserver)
            unbackfillBatch2DropObserver.phases.postCommitPhase.leave()
            waitForCommit(unbackfillBatch2DropObserver)

            prepareAndCommit(downgradeDropObserver)
        }
        ThreadUtils.awaitResult(dropFuture, timeout)
        validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)
        validateRowTrackingState(deltaLog, isPresent = false)
      }
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Drop Feature
   * Command        Metadata upgrade -------- Unbackfill ---------- Downgrade --------------------
   *                                          Batch 1               Protocol
   *                prepare + commit          prep+commit           prep+commit
   *
   *
   * Business Txn                                         prepare                  commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  for (op <- Seq("insert", "update", "delete"))
  test(s"Interleaved $op right after protocol downgrade should abort due to protocol change") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir)
      val table = s"delta.`${dir.getAbsolutePath}`"
      val ctx = new TestContext(deltaLog)
      val dropRowTrackingFn = () => dropRowTrackingTransaction(table)

      val Seq(dropFuture) = runFunctionsWithOrderingFromObserver(Seq(dropRowTrackingFn)) {
        case (updateMetadataDropObserver :: Nil) =>
          val unbackfillDropObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("unbackfill-drop-txn"))
          val downgradeDropObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("downgrade-drop-txn"))

          updateMetadataDropObserver.setNextObserver(
            unbackfillDropObserver, autoAdvance = true)
          unbackfillDropObserver.setNextObserver(
            downgradeDropObserver, autoAdvance = true)

          prepareAndCommitWithNextObserverSet(updateMetadataDropObserver)
          prepareAndCommitWithNextObserverSet(unbackfillDropObserver)

          val businessTxn = op match {
            case "insert" => ThirdPartyInsert(start = 100, end = 200)
            case "update" => ThirdPartyUpdate(set = "id = 200", condition = "id = 10")
            case "delete" => ThirdPartyDelete("id = 10")
          }
          val businessTxnFn = () => {
            businessTxn.execute(ctx)
            Array.empty[Row]
          }

          val Seq(businessTxnFuture) = runFunctionsWithOrderingFromObserver(Seq(businessTxnFn)) {
            case (businessTxnObserver :: Nil) =>
              unblockUntilPreCommit(businessTxnObserver)
              waitForPrecommit(businessTxnObserver)

              prepareAndCommit(downgradeDropObserver)

              unblockCommit(businessTxnObserver)
          }
          val e = intercept[org.apache.spark.SparkException] {
            ThreadUtils.awaitResult(businessTxnFuture, timeout)
          }
          assert(e.getCause.isInstanceOf[ProtocolChangedException])
          validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)
      }
      ThreadUtils.awaitResult(dropFuture, timeout)
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)
      validateRowTrackingState(deltaLog, isPresent = false)
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Unbackfill Batch:      prepare                         commit
   * Business Txn:                      prepare + commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  for (op <- Seq("insert", "update", "delete"))
  test(s"Third party $op that interleaves with unbackfill is resolved") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir)
      val ctx = new TestContext(deltaLog)

      // Fist stage of row tracking removal: disable the feature.
      disableRowTracking(dir.getAbsolutePath)

      val txnA = UnbackfillBatch()
      val txnB = op match {
        case "insert" => ThirdPartyInsert(start = 100, end = 200)
        case "update" => ThirdPartyUpdate("id = 200", "id = 90")
        case "delete" => ThirdPartyDelete("id = 90")
      }

      txnA.start(ctx)
      txnA.observer.foreach(o => busyWaitFor(o.phases.commitPhase.hasReached, timeout))

      txnB.execute(ctx)

      val expectedFileCountWithRowIDs = op match {
        case "insert" => 4
        case "update" => if (areDVsEnabled) 3 else 2
        case "delete" => 2
      }
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs)
      txnA.commit(ctx)

      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)

      // Unbackfill conflict should be resolved and the downgrade should proceed.
      DowngradeProtocol(RowTrackingFeature).execute(ctx)
      validateRowTrackingState(deltaLog, isPresent = false)
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Unbackfill Batch                       prepare + commit
   * Business Txn               prepare                           commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  for (op <- Seq("insert", "update", "delete"))
  test(s"Single Unbackfill batch interleaves $op") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir, numPartitions = 3)
      val ctx = new TestContext(deltaLog)
      disableRowTracking(dir.getAbsolutePath)

      val businessTxn = op match {
        case "insert" => ThirdPartyInsert(start = 100, end = 200)
        case "update" => ThirdPartyUpdate(set = "id = 200", condition = "id = 10")
        case "delete" => ThirdPartyDelete("id = 10")
      }

      businessTxn.start(ctx)
      UnbackfillBatch().execute(ctx)
      businessTxn.commit(ctx)

      val expectedFileCountWithRowIDs = op match {
        case "insert" => 2
        case "update" => if (areDVsEnabled) 2 else 1
        case "delete" => 1
      }
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs)

      val targetTable = io.delta.tables.DeltaTable.forPath(dir.getAbsolutePath)
      val expectedValues = op match {
        case "insert" => (0 to 199)
        case "update" => (0 to 99).filterNot(_ == 10) :+ 200
        case "delete" => (0 to 99).filterNot(_ == 10)
      }
      checkAnswer(targetTable.toDF, expectedValues.map(Row(_)))
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Drop Feature
   * Command        Metadata upgrade -------- Unbackfill ----- Downgrade -------------------------
   *                                          Batch 1          Protocol
   *                prepare + commit          prep+commit      Prepare                 Commit
   *
   *
   * Business Txn                                                         prep+commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  for (op <- Seq("insert", "update", "delete"))
  test(s"Drop feature conflict resolves unbackfills addFiles of interleaved commits ($op)") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir)
      val table = s"delta.`${dir.getAbsolutePath}`"
      val ctx = new TestContext(deltaLog)
      val dropRowTrackingFn = () => dropRowTrackingTransaction(table)

      val Seq(dropFuture) = runFunctionsWithOrderingFromObserver(Seq(dropRowTrackingFn)) {
        case (updateMetadataDropObserver :: Nil) =>
          val unbackfillDropObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("unbackfill-drop-txn"))
          val downgradeDropObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("downgrade-drop-txn"))

          updateMetadataDropObserver.setNextObserver(
            unbackfillDropObserver, autoAdvance = true)
          unbackfillDropObserver.setNextObserver(
            downgradeDropObserver, autoAdvance = true)

          prepareAndCommitWithNextObserverSet(updateMetadataDropObserver)
          prepareAndCommitWithNextObserverSet(unbackfillDropObserver)
          validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)

          unblockUntilPreCommit(downgradeDropObserver)
          waitForPrecommit(downgradeDropObserver)

          val businessTxn = op match {
            case "insert" => ThirdPartyInsert(start = 100, end = 200)
            case "update" => ThirdPartyUpdate(set = "id = 200", condition = "id = 10")
            case "delete" => ThirdPartyDelete("id = 10")
          }
          businessTxn.execute(ctx)

          val expectedFileCountWithRowIDs = op match {
            case "insert" => 2
            case "update" => if (areDVsEnabled) 2 else 1
            case "delete" => 1
          }
          validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs)

          unblockCommit(downgradeDropObserver)
          waitForCommit(downgradeDropObserver)
      }
      ThreadUtils.awaitResult(dropFuture, timeout)
      validateRowTrackingState(deltaLog, isPresent = false)
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Unbackfill Batch:      prepare                         commit
   * Backfill Batch:                    prepare + commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  test("Backfill interleaves unbackfill") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir)
      val ctx = new TestContext(deltaLog)

      // Fist stage of row tracking removal: disable the feature.
      disableRowTracking(dir.getAbsolutePath)

      // Add some more data without row IDs.
      addData(dir.getAbsolutePath, start = 100, end = 200)

      val txnA = UnbackfillBatch()
      val txnB = ThirdPartyBackfillBatch()

      txnA.start(ctx)
      txnB.execute(ctx)
      val e = intercept[org.apache.spark.SparkException] {
        txnA.commit(ctx)
      }
      checkError(
        e.getCause.asInstanceOf[SparkThrowable],
        "DELTA_ROW_TRACKING_BACKFILL_RUNNING_CONCURRENTLY_WITH_UNBACKFILL",
        parameters = Map.empty)
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Backfill Batch:      prepare                         commit
   * Unackfill Batch:                prepare + commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  test("Unbackfill interleaves backfill") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir)
      val ctx = new TestContext(deltaLog)

      // Fist stage of row tracking removal: disable the feature.
      disableRowTracking(dir.getAbsolutePath)
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)

      // Add some more data without row IDs.
      addData(dir.getAbsolutePath, start = 100, end = 200)
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)

      val txnA = ThirdPartyBackfillBatch()
      val txnB = UnbackfillBatch()

      txnA.start(ctx)
      txnB.execute(ctx)
      txnA.commit(ctx)
      // Backfill backfilled the initial 2 files. The unbackfill unbackfilled the following 2 files.
      // The backfill at conflict resolution did not find any common files with the conflicting
      // unbackfill and proceeded.
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)

      // Downgrade commit detects the issue and aborts.
      val e = intercept[DeltaTableFeatureException] {
        DowngradeProtocol(RowTrackingFeature).execute(ctx)
      }
      assert(e.getErrorClass === "DELTA_FEATURE_DROP_CONFLICT_REVALIDATION_FAIL")
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Unbackfill Command:
   *                 Unbackfill ------------------------------ Unbackfill ------------------------
   *                 Batch 1                                   Batch 2
   *                 prepare + commit                          prepare + commit
   *
   *
   * Business Txn A                      prepare + commit
   * Business Txn B                                                               prepare + commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  test(s"Unbackfill terminates when small competing txns run concurrently ") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir, numPartitions = 4)
      val ctx = new TestContext(deltaLog)
      disableRowTracking(dir.getAbsolutePath)
      val unbackillFn = () => {
        unBackfillTransaction(deltaLog)
        Array.empty[Row]
      }

      withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key -> "3") {
        val Seq(unbackfillFuture) = runFunctionsWithOrderingFromObserver(Seq(unbackillFn)) {
          case (unbackfillBatch1DropObserver :: Nil) =>
            val unbackfillBatch2DropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("unbackfill-batch-2-drop-txn"))

            unbackfillBatch1DropObserver.setNextObserver(
              unbackfillBatch2DropObserver, autoAdvance = true)

            unblockUntilPreCommit(unbackfillBatch1DropObserver)
            waitForPrecommit(unbackfillBatch1DropObserver)
            unblockCommit(unbackfillBatch1DropObserver)
            busyWaitFor(unbackfillBatch1DropObserver.phases.commitPhase.hasLeft, timeout)
            busyWaitFor(unbackfillBatch1DropObserver.phases.backfillPhase.hasLeft, timeout)
            // We started with 4 files and unbackfilled 3.
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 1)

            // Adds 1 backfilled file.
            ThirdPartyInsert(start = 100, end = 110, numPartitions = 1).execute(ctx)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)

            unbackfillBatch1DropObserver.phases.postCommitPhase.leave()
            // Finds both files and unbackfills them. It terminates since the number of found
            // files is less than the max batch size.
            prepareAndCommit(unbackfillBatch2DropObserver)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 0)

            // Adds 1 backfilled file.
            ThirdPartyInsert(start = 110, end = 120, numPartitions = 1).execute(ctx)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 1)
        }
        ThreadUtils.awaitResult(unbackfillFuture, timeout)
        validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 1)
      }
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Unbackfill Command:
   *                 Unbackfill ------------- Unbackfill ------------ Unbackfill -----------------
   *                 Batch 1                  Batch 2                 Batch 3
   *                 prep+commit              prep+commit             prep+commit
   *
   *
   * Business Txn A              prep+commit
   * Business Txn B                                       prep+commit
   * Business Txn C                                                                prep+commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  test(s"Unbackfill terminates when large competing txns run concurrently") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir, numPartitions = 4)
      val ctx = new TestContext(deltaLog)
      disableRowTracking(dir.getAbsolutePath)
      val unbackillFn = () => {
        unBackfillTransaction(deltaLog)
        Array.empty[Row]
      }

      withSQLConf(
          DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key -> "3",
          DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_FACTOR.key -> "2") {
        val Seq(unbackfillFuture) = runFunctionsWithOrderingFromObserver(Seq(unbackillFn)) {
          case (unbackfillBatch1DropObserver :: Nil) =>
            val unbackfillBatch2DropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("unbackfill-batch-2-drop-txn"))
            val unbackfillBatch3DropObserver = new TransactionObserver(
              OptimisticTransactionPhases.forName("unbackfill-batch-3-drop-txn"))

            unbackfillBatch1DropObserver.setNextObserver(
              unbackfillBatch2DropObserver, autoAdvance = true)
            unbackfillBatch2DropObserver.setNextObserver(
              unbackfillBatch3DropObserver, autoAdvance = true)

            unblockUntilPreCommit(unbackfillBatch1DropObserver)
            waitForPrecommit(unbackfillBatch1DropObserver)
            unblockCommit(unbackfillBatch1DropObserver)
            busyWaitFor(unbackfillBatch1DropObserver.phases.commitPhase.hasLeft, timeout)
            busyWaitFor(unbackfillBatch1DropObserver.phases.backfillPhase.hasLeft, timeout)
            // We started with 4 files and unbackfilled 3.
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 1)

            // Adds 4 backfilled files (5 in total left).
            ThirdPartyInsert(start = 100, end = 150, numPartitions = 4).execute(ctx)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 5)
            unbackfillBatch1DropObserver.phases.postCommitPhase.leave()

            // Start batch 2.
            unblockUntilPreCommit(unbackfillBatch2DropObserver)
            waitForPrecommit(unbackfillBatch2DropObserver)
            unblockCommit(unbackfillBatch2DropObserver)
            busyWaitFor(unbackfillBatch2DropObserver.phases.commitPhase.hasLeft, timeout)
            busyWaitFor(unbackfillBatch2DropObserver.phases.backfillPhase.hasLeft, timeout)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)

            // Adds 4 backfilled files (6 in total left).
            ThirdPartyInsert(start = 150, end = 200, numPartitions = 4).execute(ctx)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 6)
            unbackfillBatch2DropObserver.phases.postCommitPhase.leave()

            // Start batch 3. Although there more than 3 files left to unbackfill the
            // job terminates. This is because we reached the max number of files to unbackfill
            // (Number of initial files in the table * 2).
            prepareAndCommit(unbackfillBatch3DropObserver)
            validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 3)

            // Adds 2 backfilled files (5 in total left).
            ThirdPartyInsert(start = 200, end = 220, numPartitions = 2).execute(ctx)
        }
        ThreadUtils.awaitResult(unbackfillFuture, timeout)
        validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 5)
      }
    }
  }

  /*
   * -------------------------------------------> TIME ------------------------------------------->
   *
   * Row Tracking Enablement:
   *                 Protocol  ------------- Backfill ---------------------------- Alter Table ---
   *                 Upgrade
   *                 prepare + commit        prepare + commit                      prepare + commit
   *
   *
   * Business Txn A                                             prepare + commit
   *
   * -------------------------------------------> TIME ------------------------------------------->
   */
  test(s"Row tracking enablement fails when not all files are backfilled") {
    withTempDir { dir =>
      val deltaLog = createTestTable(dir, rowTrackingEnabled = false)
      val enablementFn = () => {
        enableRowTrackingTransaction(s"delta.`${dir.getAbsolutePath}`")
        Array.empty[Row]
      }

      val Seq(enablementFuture) = runFunctionsWithOrderingFromObserver(Seq(enablementFn)) {
        case (protocolUpgradeObserver :: Nil) =>
          val backfillObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("backfill-txn"))
          val alterTableObserver = new TransactionObserver(
            OptimisticTransactionPhases.forName("alter-table-txn"))

          protocolUpgradeObserver.setNextObserver(backfillObserver, autoAdvance = true)
          backfillObserver.setNextObserver(alterTableObserver, autoAdvance = true)

          prepareAndCommitWithNextObserverSet(protocolUpgradeObserver)

          unblockUntilPreCommit(backfillObserver)
          waitForPrecommit(backfillObserver)
          unblockCommit(backfillObserver)
          busyWaitFor(backfillObserver.phases.commitPhase.hasLeft, timeout)
          busyWaitFor(backfillObserver.phases.backfillPhase.hasLeft, timeout)
          validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)

          // Add 2 non-backfilled files.
          val table = DeltaTableV2(spark, new Path(dir.getAbsolutePath))
          val propertiesToSet = Map(DeltaConfigs.ROW_TRACKING_SUSPENDED.key -> "true")
          AlterTableSetPropertiesDeltaCommand(table, propertiesToSet).run(spark)
          addData(dir.getAbsolutePath, start = 100, end = 200)
          val propertiesToUnSet = Map(DeltaConfigs.ROW_TRACKING_SUSPENDED.key -> "false")
          AlterTableSetPropertiesDeltaCommand(table, propertiesToUnSet).run(spark)

          // No row IDs were generated.
          validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)
          backfillObserver.phases.postCommitPhase.leave()

          // Alter table should throw an exception since not all files were backfilled.
          unblockUntilPreCommit(alterTableObserver)
      }
      val e = intercept[org.apache.spark.SparkException] {
        ThreadUtils.awaitResult(enablementFuture, timeout)
      }
      assert(e.getCause.isInstanceOf[ProtocolChangedException])
      validateRowTrackingMetadataInAddFiles(deltaLog, expectedFileCountWithRowIDs = 2)
    }
  }
}

class RowTrackingRemovalConcurrencyWithoutDVsSuite extends RowTrackingRemovalConcurrencySuite {
  override protected val areDVsEnabled = false
}
