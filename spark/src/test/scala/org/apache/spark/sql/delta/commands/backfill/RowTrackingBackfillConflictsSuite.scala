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

package org.apache.spark.sql.delta.commands.backfill

import java.util.concurrent.{ConcurrentLinkedDeque, ExecutionException, Future, ThreadFactory, TimeUnit}

import scala.annotation.tailrec

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.commands.DeletionVectorUtils
import org.apache.spark.sql.delta.concurrency.{PhaseLockingTestMixin, TransactionExecutionTestMixin}
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver}
import org.apache.spark.sql.delta.fuzzer.AtomicBarrier.State
import org.apache.spark.sql.delta.rowid.RowIdTestUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.exceptions.MetadataChangedException

import org.apache.spark.{SparkConf, SparkException, SparkThrowable}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/*
 * Tests for conflict detection with Backfill. Test suites have to override 'concurrentTransaction'
 * before testing the following scenarios. All scenarios include the execution one transaction
 * concurrently to a backfill command. Note that each backfill has 3 stages
 * - Upgrade the protocol to support the Row Tracking Table Feature
 * - Add baseRowId (done in multiple batches in parallel)
 * - Mark Row Tracking active by updating the table metadata
 *
 * -------------------------------------------> TIME -------------------------------------------->
 *                  RT           RT                                           RT
 * Backfill         protocol     protocol                                     metadata
 * Command          upgrade ---- upgrade -+--------------------------------+- update
 * Thread           prepare      commit    \                              /   prepare + commit
 *                                          \                            /
 * Backfill                                  \  BaseRowId     BaseRowId /
 * Threads                                    - Backfill ---- Backfill-/
 *                                              prepare       commit
 *

 *
 * Concurrent transaction
 *
 * Scenario 1  prepare --- commit
 * Scenario 2  prepare ----------------- commit
 * Scenario 3  prepare ------------------------------------------------ commit
 * Scenario 4  prepare --------------------------------------------------------------------- commit
 * Scenario 5                            prepare ------- commit
 * Scenario 6                            prepare ---------------------- commit
 * Scenario 7                            prepare ------------------------------------------- commit
 *
 * -------------------------------------------> TIME -------------------------------------------->
 */
trait RowTrackingBackfillConflictsTestBase extends RowIdTestUtils
  with TransactionExecutionTestMixin
  with PhaseLockingTestMixin
  with SharedSparkSession {

  override def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_ROW_TRACKING_BACKFILL_ENABLED.key, "true")

  protected val usePersistentDeletionVectors = false

  protected val testTableName = "target"
  protected val colName = "id"
  protected val partitionColumnName = "partition"

  protected def tableCreationDF: DataFrame =
    withPartitionColumn(spark.range(end = numRows).toDF(colName))


  protected def insertedRowDF: DataFrame = {
    val insertedRow = Seq((1337, 1))
    spark.createDataFrame(insertedRow)
  }

  protected def tableCreationAfterInsert(): Dataset[Row] = {
    tableCreationDF.union(insertedRowDF)
  }

  protected def withPartitionColumn(dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(partitionColumnName, (col(colName) % numPartitions).cast("int"))
  }

  protected val numPartitions: Int = 4
  private val numFilesPerPartition: Int = 2
  private val numRowsPerFile: Int = 10
  protected val numFiles: Int = numPartitions * numFilesPerPartition
  protected val numRows: Int = numFiles * numRowsPerFile

  protected def deltaLog: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))

  protected def latestSnapshot: Snapshot = deltaLog.update()

  protected def backfillTransaction(): Array[Row] = {
    sql(s"""ALTER TABLE $testTableName
           |SET TBLPROPERTIES('${DeltaConfigs.ROW_TRACKING_ENABLED.key}' = 'true')""".stripMargin)
      .collect()
  }

  // All observers for backfill threads will be added to the Deque.
  protected val backfillObservers =
    new ConcurrentLinkedDeque[PhaseLockingTransactionExecutionObserver]

  // A Thread Factory that adds transaction observers to `backfillObservers` for all new threads.
  private class BackfillObservingThreadFactory extends ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val observer = new PhaseLockingTransactionExecutionObserver(
        OptimisticTransactionPhases.forName("backfill-observer"))
      backfillObservers.addLast(observer)

      val runnable: Runnable = () => {
        TransactionExecutionObserver.withObserver(observer) { r.run() }
      }

      new Thread(runnable)
    }
  }

  // Wait for one backfill thread to be ready and unblock it. The thread is chosen at random.
  protected def commitSingleBackfillBatch(): Unit = {
    prepareSingleBackfillBatchCommit()
    commitPreparedBackfillBatchCommit()
  }

  // Wait for one backfill thread to be ready and unblock it up until pre commit. The thread will
  // be kept at the front of 'backfillObservers'.
  protected def prepareSingleBackfillBatchCommit(): Unit = {
    var nextBlockedObserver: Option[PhaseLockingTransactionExecutionObserver] = None
    def foundNextBlockedObserver(): Boolean = {
      backfillObservers.forEach { observer =>
        val prepareEntryState = observer.phases.preparePhase.entryBarrier.load()
        if (nextBlockedObserver.isEmpty &&
            Seq(State.Blocked, State.Requested).contains(prepareEntryState)) {
          nextBlockedObserver = Some(observer)
        }
      }
      nextBlockedObserver.isDefined
    }
    busyWaitFor(foundNextBlockedObserver, timeout)
    unblockUntilPreCommit(nextBlockedObserver.get)
    busyWaitFor(nextBlockedObserver.get.phases.preparePhase.hasEntered, timeout)
  }

  // Commit the backfill batch that has been prepared by 'prepareSingleBackfillBatchCommit'.
  protected def commitPreparedBackfillBatchCommit(): Unit = {
    require(!backfillObservers.isEmpty)
    val observer = backfillObservers.removeFirst()
    require(observer.phases.preparePhase.hasEntered)
    unblockCommit(observer)
    waitForCommit(observer)
  }

  protected def commitBatchesSimultaneously(numBatches: Int): Unit = {
    // Wait for all backfill commit threads to be launched.
    busyWaitFor(backfillObservers.size() == numBatches, timeout)

    // Unblock all commits.
    backfillObservers.forEach(unblockAllPhases(_))

    // Wait for all commits to be finished.
    while (!backfillObservers.isEmpty) {
      val observer = backfillObservers.removeFirst()
      waitForCommit(observer)
    }
  }

  // Launch the backfill command and wait until the table feature has been committed.
  protected def launchBackFillAndBlockAfterFeatureIsCommitted(): Future[_] = {
    val backfillFuture = launchBackfillInBackgroundThread()
    busyWaitFor(RowTracking.isSupported(latestSnapshot.protocol), timeout)
    backfillFuture
  }

  // Launch the backfill in a separate thread to run in parallel to `concurrentTransaction`.
  private def launchBackfillInBackgroundThread(): Future[_] = {
    val threadPool = ThreadUtils.newDaemonSingleThreadExecutor("backfill-thread-pool")
    val backfillRunnable: Runnable = () => backfillTransaction()
    threadPool.submit(backfillRunnable)
  }

  protected def withTrackedBackfillCommits(testBlock: => Unit): Unit = {
    // Use `BackfillObservingThreadFactory` on the backfill thread pool.
    val oldThreadFactory = BackfillExecutor.getOrCreateThreadPool().getThreadFactory
    assert(!oldThreadFactory.isInstanceOf[BackfillObservingThreadFactory])
    BackfillExecutor.getOrCreateThreadPool()
      .setThreadFactory(new BackfillObservingThreadFactory())

    try {
      withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key ->
          numFiles.toString) {
        testBlock
      }
    } finally {
      BackfillExecutor.getOrCreateThreadPool().setThreadFactory(oldThreadFactory)
    }
  }

  protected def withEmptyTestTable(testBlock: => Unit): Unit = {
    withTable(testTableName) {
      // Row tracking will be enabled by the backfill.
      withRowTrackingEnabled(enabled = false) {
        spark.range(0)
          .write.format("delta").saveAsTable(testTableName)

        testBlock
      }
    }
  }

  protected def withTestTable(testBlock: => Unit): Unit = {
    withTable(testTableName) {
      // Row tracking will be enabled by the backfill.
      withRowTrackingEnabled(enabled = false) {
        withSQLConf(DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey ->
           usePersistentDeletionVectors.toString) {
          tableCreationDF
            .repartitionByRange(numFilesPerPartition, col(colName))
            .write.format("delta").partitionBy(partitionColumnName).saveAsTable(testTableName)
        }

        val tableDF = spark.table(testTableName)
        val tableFiles: Array[AddFile] = latestSnapshot.allFiles.collect()
        assert(numPartitions === tableDF.select(partitionColumnName).distinct().count())
        tableFiles.groupBy(_.partitionValues.get(partitionColumnName)).foreach {
          case (_, filesInPartition) => assert(filesInPartition.length === numFilesPerPartition)
        }
        assert(tableFiles.forall(_.numLogicalRecords.get == numRowsPerFile))
        assert(numFiles === tableFiles.length)
        assert(numRows === tableDF.count())

        testBlock
      }
    }
  }

  protected def validateResult(expectedResult: () => DataFrame): Unit = {
    assert(RowId.isEnabled(latestSnapshot.protocol, latestSnapshot.metadata))
    assertRowIdsAreValid(deltaLog)
    checkAnswer(spark.table(testTableName), expectedResult())
  }

  private def causedByMetadataUpdate(exception: Throwable): Boolean = {
    exception match {
      case _: MetadataChangedException => true
      case null => false
      case same if same.getCause == same => false
      case other => causedByMetadataUpdate(other.getCause)
    }
  }

  protected def assertAbortedBecauseOfMetadataChange(exception: ExecutionException): Unit =
    assert(causedByMetadataUpdate(exception), s"Unexpected abort: ${exception.getMessage}")

  protected def assertAbortedBecauseOfMetadataChange(exception: SparkException): Unit =
    assert(causedByMetadataUpdate(exception.getCause), s"Unexpected abort: ${exception.getMessage}")

  protected def assertAbortedBecauseOfConcurrentWrite(
      exception: SparkException): Unit = {
    @tailrec
    def causedByConcurrentModification(exception: Throwable): Boolean = {
      exception match {
        case _: ConcurrentWriteException => true
        case null => false
        case same if same.getCause == same => false
        case other => causedByConcurrentModification(other.getCause)
      }
    }
    assert(causedByConcurrentModification(exception.getCause),
      s"Unexpected abort: ${exception.getMessage}")
  }

  /**
   * This is a modification of scenario 5 in [[RowTrackingBackfillConflictsSuite]] with an
   * extra insert and failure expectations for commitLarge test.
   */
  protected def testScenario5WithCommitLarge(
      concurrentTransaction: () => Array[Row],
      expectedResult: DataFrame): Unit = {
    withTestTable {
      withTrackedBackfillCommits {
        // Commit Row Tracking feature.
        val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()
        // Add some data to bump the table version. A RESTORE to the current table version is a NOOP
        // and will not even create the RestoreTableCommand object. We need a dummy commit in order
        // for a RESTORE to take place.
        insertedRowDF.write.insertInto(testTableName)
        assert(latestSnapshot.version > 1)

        val Seq(concurrentTransactionFuture) =
          runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction)) {
            case (concurrentTransactionObserver :: Nil) =>
              // Prepare concurrent transaction.
              unblockUntilPreCommit(concurrentTransactionObserver)
              busyWaitFor(concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

              // Prepare the commit of one backfill batch.
              prepareSingleBackfillBatchCommit()

              // Commit concurrent transaction.
              unblockCommit(concurrentTransactionObserver)
              waitForCommit(concurrentTransactionObserver)

              // Try to finish the backfill, which only has one batch. This will fail because the
              // concurrent txn does a metadata update.
              commitPreparedBackfillBatchCommit()
              val e = intercept[ExecutionException] {
                backfillFuture.get(timeout.toSeconds, TimeUnit.SECONDS)
              }
              assertAbortedBecauseOfMetadataChange(e)
          }

        ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
        checkAnswer(spark.table(testTableName), expectedResult)
      }
    }
  }

  /**
   * This is a modification of scenario 6 in [[RowTrackingBackfillConflictsSuite]] with an extra
   * insert and failure expectations for commitLarge test.
   */
  protected def testScenario6WithCommitLarge(concurrentTransaction: () => Array[Row]): Unit = {
    withTrackedBackfillCommits {
      withTestTable {
        // We enforce the backfill to use two commits such that we can stop the process before
        // the metadata update commit.
        withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key ->
          math.ceil(numFiles / 2.0).toInt.toString) {
          validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 2) {
            val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()
            // Add some data to bump the table version. A RESTORE to the current table version is a
            // NOOP and will not even create the RestoreTableCommand object. We need a dummy commit
            // in order for a RESTORE to take place.
            insertedRowDF.write.insertInto(testTableName)
            assert(latestSnapshot.version > 1)

            val Seq(concurrentTransactionFuture) =
              runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction)) {
                case (concurrentTransactionObserver :: Nil) =>
                  // Prepare concurrent commit.
                  unblockUntilPreCommit(concurrentTransactionObserver)
                  busyWaitFor(
                    concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

                  // Commit one backfill.
                  commitSingleBackfillBatch()

                  // Try to commit concurrent transaction.
                  unblockCommit(concurrentTransactionObserver)
                  waitForCommit(concurrentTransactionObserver)

                  // Finish the backfill command, which has 2 batches.
                  commitSingleBackfillBatch()
                  backfillFuture.get(timeout.toSeconds, TimeUnit.SECONDS)
              }
            val e = intercept[SparkException] {
              ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
            }
            assertAbortedBecauseOfConcurrentWrite(e)
            validateResult(tableCreationAfterInsert)
          }
        }
      }
    }
  }
}

class RowTrackingBackfillConflictsSuite extends RowTrackingBackfillConflictsTestBase {
  private def testAllScenarios(
      concurrentTransactionName: String)(
      concurrentTransaction: () => Array[Row])(
      expectedResult: () => DataFrame): Unit = {
    /**
     * Scenario 1: Commit concurrent transaction in parallel to the protocol upgrade.
     */
    test(s"$concurrentTransactionName - Scenario 1") {
      withTestTable {
        validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
          val Seq(concurrentTransactionFuture, backfillFuture) =
            runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction, backfillTransaction)) {
              case (concurrentTransactionObserver :: backfillObserver :: Nil) =>
                // Prepare concurrent transaction commit.
                unblockUntilPreCommit(concurrentTransactionObserver)
                busyWaitFor(concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

                // Prepare table feature commit.
                unblockUntilPreCommit(backfillObserver)
                busyWaitFor(backfillObserver.phases.preparePhase.hasEntered, timeout)

                // Commit concurrent transaction.
                unblockCommit(concurrentTransactionObserver)
                waitForCommit(concurrentTransactionObserver)

                // Commit table feature and unblock further backfill commits. We replace the
                // txnObserver on the main thread with a NoOp txnObserver, which means the remaining
                // transactions on the main thread (the parent txn object of backfill and the
                // metadata update) will not be observed.
                backfillObserver.setNextObserver(
                  NoOpTransactionExecutionObserver, autoAdvance = true)
                unblockCommit(backfillObserver)
                backfillObserver.phases.backfillPhase.exitBarrier.unblock()
                waitForCommit(backfillObserver)
            }

          ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
          ThreadUtils.awaitResult(backfillFuture, timeout)
          validateResult(expectedResult)
        }
      }
    }

    /**
     * Scenario 2: Commit concurrent transaction after enabling Table feature requiring
     * conflict resolution.
     */
    test(s"$concurrentTransactionName - Scenario 2") {
      withTrackedBackfillCommits {
        withTestTable {
          validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
            val Seq(concurrentTransactionFuture) =
              runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction)) {
                case (concurrentTransactionObserver :: Nil) =>
                  // Prepare concurrent commit.
                  unblockUntilPreCommit(concurrentTransactionObserver)
                  busyWaitFor(concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

                  val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()
                  busyWaitFor(backfillObservers.size() > 0, timeout)

                  // Commit concurrent transaction.
                  unblockCommit(concurrentTransactionObserver)
                  waitForCommit(concurrentTransactionObserver)

                  // Finish the backfill, which only has one batch.
                  commitSingleBackfillBatch()
                  backfillFuture.get(timeout.toSeconds, TimeUnit.SECONDS)
              }
            ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
            validateResult(expectedResult)
          }
        }
      }
    }

    /**
     * Scenario 3: Prepare the concurrent commit before the table feature is enabled
     * and commit after at least one backfill committed and before the metadata update commit.
     */
    test(s"$concurrentTransactionName - Scenario 3") {
      withTrackedBackfillCommits {
        withTestTable {
          // Enforce the backfill to use two commits to stop the process after one commit.
          withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key ->
              math.ceil(numFiles / 2.0).toInt.toString) {
            validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 2) {
              val Seq(concurrentTransactionFuture) =
                runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction)) {
                  case (concurrentTransactionObserver :: Nil) =>
                    // Prepare concurrent commit.
                    unblockUntilPreCommit(concurrentTransactionObserver)
                    busyWaitFor(
                      concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

                    val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()

                    // Commit one backfill batch.
                    commitSingleBackfillBatch()

                    // Commit concurrent transaction.
                    unblockCommit(concurrentTransactionObserver)
                    waitForCommit(concurrentTransactionObserver)

                    // Finish the backfill command, which has 2 batches.
                    commitSingleBackfillBatch()
                    backfillFuture.get(timeout.toSeconds, TimeUnit.SECONDS)
                }
              ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
              validateResult(expectedResult)
            }
          }
        }
      }
    }

    /**
     * Scenario 4: The concurrent operations starts before the backfill and ends after it.
     */
    test(s"$concurrentTransactionName - Scenario 4") {
      withTestTable {
        validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
          val concurrentTransactionFuture =
            runTxnsWithOrder__A_Start__B__A_end_without_observer_on_B(
              concurrentTransaction, backfillTransaction)

          ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
          validateResult(expectedResult)
        }
      }
    }

    /**
     * Scenario 5: The concurrent transaction commits after the table feature has been enabled
     * and concurrently to one backfill batch, requiring conflict resolution on the backfill.
     */
    test(s"$concurrentTransactionName - Scenario 5") {
      withTestTable {
        withTrackedBackfillCommits {
          validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
            val Seq(concurrentTransactionFuture) =
              runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction)) {
                case (concurrentTransactionObserver :: Nil) =>
                  // Commit Row Tracking feature.
                  val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()

                  // Prepare concurrent transaction.
                  unblockUntilPreCommit(concurrentTransactionObserver)
                  busyWaitFor(concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

                  // Prepare the commit of one backfill batch.
                  prepareSingleBackfillBatchCommit()

                  // Commit concurrent transaction.
                  unblockCommit(concurrentTransactionObserver)
                  waitForCommit(concurrentTransactionObserver)

                  // Finish the backfill, which only has one batch.
                  commitPreparedBackfillBatchCommit()
                  ThreadUtils.awaitResult(backfillFuture, timeout)
              }
            ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
            validateResult(expectedResult)
          }
        }
      }
    }

    /**
     * Scenario 6: The concurrent transaction starts after the table feature is enabled and commits
     * after one backfill has been committed concurrently, requiring conflict resolution on the
     * concurrent transaction.
     */
    test(s"$concurrentTransactionName - Scenario 6") {
      withTrackedBackfillCommits {
        withTestTable {
          // We enforce the backfill to use two commits such that we can stop the process before
          // the metadata update commit.
          withSQLConf(DeltaSQLConf.DELTA_BACKFILL_MAX_NUM_FILES_PER_COMMIT.key ->
              math.ceil(numFiles / 2.0).toInt.toString) {
            validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 2) {
              val Seq(concurrentTransactionFuture) =
                runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction)) {
                  case (concurrentTransactionObserver :: Nil) =>
                    val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()

                    // Prepare concurrent commit.
                    unblockUntilPreCommit(concurrentTransactionObserver)
                    busyWaitFor(
                      concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

                    // Commit one backfill.
                    commitSingleBackfillBatch()

                    // Commit concurrent transaction.
                    unblockCommit(concurrentTransactionObserver)
                    waitForCommit(concurrentTransactionObserver)

                    // Finish the backfill command, which has 2 batches.
                    commitSingleBackfillBatch()
                    ThreadUtils.awaitResult(backfillFuture, timeout)
                }
              ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
              validateResult(expectedResult)
            }
          }
        }
      }
    }

    /**
     * Scenario 7: The concurrent transaction starts after the table feature is enabled and
     * commits after the metadata update.
     */
    test(s"$concurrentTransactionName - Scenario 7") {
      withTrackedBackfillCommits {
        withTestTable {
          validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 1) {
            val Seq(concurrentTransactionFuture) =
              runFunctionsWithOrderingFromObserver(Seq(concurrentTransaction)) {
                case (concurrentTransactionObserver :: Nil) =>
                  val backfillFuture = launchBackFillAndBlockAfterFeatureIsCommitted()

                  // Prepare concurrent commit.
                  unblockUntilPreCommit(concurrentTransactionObserver)
                  busyWaitFor(concurrentTransactionObserver.phases.preparePhase.hasEntered, timeout)

                  // Finish the backfill, which only has one batch.
                  commitSingleBackfillBatch()
                  backfillFuture.get(timeout.toSeconds, TimeUnit.SECONDS)

                  // Unlock commit of concurrent transaction.
                  unblockCommit(concurrentTransactionObserver)
                  waitForCommit(concurrentTransactionObserver)
              }

            ThreadUtils.awaitResult(concurrentTransactionFuture, timeout)
            validateResult(expectedResult)
          }
        }
      }
    }
  }

  testAllScenarios("INSERT") { () =>
    sql(s"INSERT INTO $testTableName($colName, $partitionColumnName) VALUES(1337, 1)").collect()
  } { () =>
    val insertedRow = Seq((1337, 1))
    tableCreationDF.union(spark.createDataFrame(insertedRow))
  }

  testAllScenarios("DELETE") { () =>
    sql(s"DELETE FROM $testTableName WHERE $colName = 3").collect()
  } { () =>
    assert(!usePersistentDeletionVectors ||
      !DeletionVectorUtils.isTableDVFree(latestSnapshot))
    tableCreationDF.where(s"$colName != 3")
  }

  testAllScenarios("UPDATE") { () =>
    sql(s"UPDATE $testTableName SET $colName = 1337 WHERE $colName = 3").collect()
  } { () =>
    assert(
      !usePersistentDeletionVectors || !DeletionVectorUtils.isTableDVFree(latestSnapshot)
    )
    val updatedRow = Seq((1337, 3 % numPartitions))
    tableCreationDF.where("id != 3").union(spark.createDataFrame(updatedRow))
  }

  // DF to create the view used as a source for MERGEs. When joining on 'colName', the lower half
  // of the rows in 'testTableName' is unmatched, the upper half of 'testTableName' is matched
  // by the lower half of 'mergeSourceDF', and the upper half of 'mergeSourceDF' is unmatched.
  private lazy val mergeSourceDF: DataFrame =
    withPartitionColumn(tableCreationDF.select(col(colName) + (numRows / 2) as colName))

  // Create a temporary view used as the source for MERGEs based on 'mergeSourceDF'.
  private def withMergeSource(testBlock: String => Array[Row]): Array[Row] = {
    val sourceViewName = "source"
    mergeSourceDF.createTempView(sourceViewName)
    try {
      testBlock(sourceViewName)
    } finally {
      sql(s"DROP VIEW $sourceViewName")
    }
  }

  testAllScenarios("MERGE with not matched and not matched by source") { () =>
    withMergeSource { sourceViewName =>
      val mergeStatement =
        s"""MERGE INTO $testTableName t
           |USING $sourceViewName s
           |ON s.$colName = t.$colName
           |WHEN NOT MATCHED THEN INSERT *
           |WHEN NOT MATCHED BY SOURCE THEN DELETE
           |""".stripMargin
      sql(mergeStatement).collect()
    }
  } { () =>
    mergeSourceDF
  }

  testAllScenarios("MERGE with matched and not matched") { () =>
    withMergeSource { sourceViewName =>
      val mergeStatement =
        s"""MERGE INTO $testTableName t
           |USING $sourceViewName s
           |ON s.$colName = t.$colName
           |WHEN MATCHED THEN UPDATE SET *
           |WHEN NOT MATCHED THEN INSERT *
           |""".stripMargin
      sql(mergeStatement).collect()
    }
  } { () =>
    tableCreationDF.union(mergeSourceDF).distinct()
  }

  testAllScenarios("OPTIMIZE") { () =>
    sql(s"OPTIMIZE $testTableName WHERE $partitionColumnName < ($numFiles / 2)").collect()
  } { () =>
    tableCreationDF
  }

  /**
   * RESTORE uses commitLarge which does not do conflict checking nor retry. So when there is
   * a concurrent conflict, either RESTORE will fail from concurrent modification or
   * Backfill will fail from the metadata change.
   */
  test("Backfill fails from conflict with RESTORE") {
    // This tries to undo the insert.
    val concurrentTransaction = () => {
      sql(s"RESTORE TABLE $testTableName TO VERSION AS OF 1").collect()
    }

    testScenario5WithCommitLarge(concurrentTransaction, tableCreationDF)
  }

  test("RESTORE fails from conflict with Backfill") {
    // This tries to undo the insert.
    val concurrentTransaction = () => {
      sql(s"RESTORE TABLE $testTableName TO VERSION AS OF 1").collect()
    }

    testScenario6WithCommitLarge(concurrentTransaction)
  }
}

class RowTrackingBackfillConflictsDVSuite extends RowTrackingBackfillConflictsSuite {
 override val usePersistentDeletionVectors = true
}
