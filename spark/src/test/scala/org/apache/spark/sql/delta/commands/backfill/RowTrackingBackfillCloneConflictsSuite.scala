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

import org.apache.spark.sql.delta.{MetadataChangedException, RowTracking}
import org.apache.spark.sql.delta.fuzzer.{OptimisticTransactionPhases, PhaseLockingTransactionExecutionObserver => TransactionObserver}

import org.apache.spark.SparkException
import org.apache.spark.util.ThreadUtils

/**
 * Clone uses commitLarge which does not do conflict checking nor retry. So when there is
 * a concurrent conflict, either Clone will fail from concurrent modification or
 * Backfill will fail from the metadata change.
 */

/*
 * Tests for conflict detection with Backfill and Clone. Note that each backfill in this case has
 * 2 stages:
 * - Upgrade the protocol to support the Row Tracking Table Feature
 * - Mark Row Tracking active by updating the table metadata
 *
 * -------------------------------------------> TIME -------------------------------------------->
 *
 * Backfill         Row Tracking              Row Tracking               Row Tracking
 * Command          Protocol upgrade -------- metadata update ---------- metadata update
 * Thread           prepare + commit          prepare                    commit
 *
 *
 * Clone
 *
 * Scenario 1                        prepare ----------------- commit
 * Scenario 2                        prepare -------------------------------------------- commit
 *
 * -------------------------------------------> TIME -------------------------------------------->
 */
class RowTrackingBackfillCloneConflictsSuite extends RowTrackingBackfillConflictsTestBase {
  override val testTableName = "BackfillCloneTarget"

  val sourceTableName = "CloneSource"

  private def withSourceTable(testBlock: => Unit): Unit = {
    withTable(sourceTableName) {
      withRowTrackingEnabled(enabled = false) {
        tableCreationAfterInsert().write.format("delta").saveAsTable(sourceTableName)
        testBlock
      }
    }
  }

  private def createAndChainBackfillObservers(): (TransactionObserver, TransactionObserver) = {
    // This observes the transaction in [[BackfillCommand]] that does manual state
    // transitions to make the concurrency testing framework happy in other Suites.
    val manualTransitionsBackfillObserver = new TransactionObserver(
      OptimisticTransactionPhases.forName("manual-transitions-backfill-observer"))

    // This observes the transaction in [[alterDeltaTableCommands]] after the backfill
    // that tries to update the table's metadata.
    val updateMetadataBackfillObserver = new TransactionObserver(
      OptimisticTransactionPhases.forName("update-metadata-backfill-observer"))

    // Chain observers
    manualTransitionsBackfillObserver.setNextObserver(
      updateMetadataBackfillObserver, autoAdvance = true)

    (manualTransitionsBackfillObserver, updateMetadataBackfillObserver)
  }

  test("Backfill fails from conflict with CLONE") {
    val cloneTransaction =
      () => sql(s"CREATE OR REPLACE TABLE " +
        s"$testTableName SHALLOW CLONE $sourceTableName").collect()

    withTrackedBackfillCommits {
      withSourceTable {
        withEmptyTestTable {
          // Row tracking will be enabled by the backfill.
          validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 0) {
            val Seq(backfillFuture) =
              runFunctionsWithOrderingFromObserver(Seq(backfillTransaction)) {
                // The upgradeProtocolBackfillObserver observes the transaction in
                // [[RowTrackingBackfillCommands]] that adds the Row tracking table feature support.
                case (upgradeProtocolBackfillObserver :: Nil) =>
                  val (manualTransitionsBackfillObserver, updateMetadataBackfillObserver) =
                    createAndChainBackfillObservers()
                  upgradeProtocolBackfillObserver.setNextObserver(
                    manualTransitionsBackfillObserver, autoAdvance = true)

                  // Prepare and commit Row Tracking table feature support transaction.
                  prepareAndCommitWithNextObserverSet(upgradeProtocolBackfillObserver)

                  // Prepare and commit Manual State Transitions transaction.
                  prepareAndCommitWithNextObserverSet(manualTransitionsBackfillObserver)

                  val Seq(cloneFuture) =
                    runFunctionsWithOrderingFromObserver(Seq(cloneTransaction)) {
                      case (cloneTransactionObserver :: Nil) =>
                        // Prepare Clone commit.
                        unblockUntilPreCommit(cloneTransactionObserver)
                        waitForPrecommit(cloneTransactionObserver)

                        // Prepare Row Tracking metadata update commit.
                        unblockUntilPreCommit(updateMetadataBackfillObserver)
                        waitForPrecommit(updateMetadataBackfillObserver)

                        // Commit Clone.
                        unblockCommit(cloneTransactionObserver)
                        waitForCommit(cloneTransactionObserver)

                        // Commit Row Tracking metadata update.
                        unblockCommit(updateMetadataBackfillObserver)
                        waitForCommit(updateMetadataBackfillObserver)
                    }

                  ThreadUtils.awaitResult(cloneFuture, timeout)
                  checkAnswer(spark.table(testTableName), spark.table(sourceTableName))
              }

            val ex = intercept[SparkException] {
              ThreadUtils.awaitResult(backfillFuture, timeout)
            }
            assertAbortedBecauseOfMetadataChange(ex)
            assert(!RowTracking.isEnabled(latestSnapshot.protocol, latestSnapshot.metadata))
          }
        }
      }
    }
  }

  test("Clone fails from conflict with Backfill") {
    val cloneTransaction =
      () => sql(s"CREATE OR REPLACE TABLE " +
        s"$testTableName SHALLOW CLONE $sourceTableName").collect()

    withTrackedBackfillCommits {
      withSourceTable {
        withEmptyTestTable {
          // Row tracking will be enabled by the backfill.
          validateSuccessfulBackfillMetrics(expectedNumSuccessfulBatches = 0) {
            val Seq(backfillFuture) =
              runFunctionsWithOrderingFromObserver(Seq(backfillTransaction)) {
                // The upgradeProtocolBackfillObserver observes the transaction in
                // [[RowTrackingBackfillCommands]] that adds the Row tracking table feature support.
                case (upgradeProtocolBackfillObserver :: Nil) =>
                  val (manualTransitionsBackfillObserver, updateMetadataBackfillObserver) =
                    createAndChainBackfillObservers()
                  upgradeProtocolBackfillObserver.setNextObserver(
                    manualTransitionsBackfillObserver, autoAdvance = true)

                  // Prepare and commit Row Tracking table feature support transaction.
                  prepareAndCommitWithNextObserverSet(upgradeProtocolBackfillObserver)

                  // Prepare and commit Manual State Transitions transaction.
                  prepareAndCommitWithNextObserverSet(manualTransitionsBackfillObserver)

                  val Seq(cloneFuture) =
                    runFunctionsWithOrderingFromObserver(Seq(cloneTransaction)) {
                      case (cloneTransactionObserver :: Nil) =>
                        // Prepare Clone commit.
                        unblockUntilPreCommit(cloneTransactionObserver)
                        waitForPrecommit(cloneTransactionObserver)

                        // Prepare and commit Row Tracking metadata update commit.
                        prepareAndCommit(updateMetadataBackfillObserver)

                        // Commit Clone.
                        unblockCommit(cloneTransactionObserver)
                        waitForCommit(cloneTransactionObserver)
                  }

                  val ex = intercept[SparkException] {
                    ThreadUtils.awaitResult(cloneFuture, timeout)
                  }
                  assertAbortedBecauseOfConcurrentWrite(ex)
              }

            ThreadUtils.awaitResult(backfillFuture, timeout)
            assert(RowTracking.isEnabled(latestSnapshot.protocol, latestSnapshot.metadata))
            checkAnswer(spark.table(testTableName), Seq.empty)
          }
        }
      }
    }
  }
}
