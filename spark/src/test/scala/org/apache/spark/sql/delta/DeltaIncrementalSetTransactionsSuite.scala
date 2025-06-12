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

import java.io.File
import java.util.UUID

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.UsageRecord
import org.apache.spark.sql.delta.DeltaTestUtils.{collectUsageLogs, createTestAddFile, BOOLEAN_DOMAIN}
import org.apache.spark.sql.delta.actions.{AddFile, SetTransaction, SingleAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class DeltaIncrementalSetTransactionsSuite
  extends QueryTest
    with DeltaSQLCommandTest
    with SharedSparkSession {

  protected override def sparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")
    .set(DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key, "true")
    // needed for DELTA_WRITE_SET_TRANSACTIONS_IN_CRC
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key, "true")
    // This test suite is sensitive to stateReconstruction we do at different places. So we disable
    // [[INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS]] to simulate prod behaviour.
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key, "false")

  /**
   * Validates the result of [[Snapshot.setTransactions]] API for the latest snapshot of this
   * [[DeltaLog]].
   */
  private def assertSetTransactions(
      deltaLog: DeltaLog,
      expectedTxns: Map[String, Long],
      viaCRC: Boolean = false
  ): Unit = {
    val snapshot = deltaLog.update()
    if (viaCRC) {
      assert(snapshot.checksumOpt.flatMap(_.setTransactions).isDefined)
      snapshot.checksumOpt.flatMap(_.setTransactions).foreach { setTxns =>
        assert(setTxns.map(txn => (txn.appId, txn.version)).toMap === expectedTxns)
      }
    }
    assert(snapshot.setTransactions.map(txn => (txn.appId, txn.version)).toMap === expectedTxns)
    assert(snapshot.numOfSetTransactions === expectedTxns.size)
    assert(expectedTxns === snapshot.transactions)
  }


  /** Commit given [[SetTransaction]] to `deltaLog`` */
  private def commitSetTxn(
      deltaLog: DeltaLog, appId: String, version: Long, lastUpdated: Long): Unit = {
    commitSetTxn(deltaLog, Seq(SetTransaction(appId, version, Some(lastUpdated))))
  }

  /** Commit given [[SetTransaction]]s to `deltaLog`` */
  private def commitSetTxn(
      deltaLog: DeltaLog,
      setTransactions: Seq[SetTransaction]): Unit = {
    deltaLog.startTransaction().commit(
      setTransactions :+
        AddFile(
          s"file-${UUID.randomUUID().toString}",
          partitionValues = Map.empty,
          size = 1L,
          modificationTime = 1L,
          dataChange = true),
      DeltaOperations.Write(SaveMode.Append)
    )
  }

  test(
    "set-transaction tracking starts from 0th commit in CRC"
  ) {
    withSQLConf(
        DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true"
    ) {
      val tbl = "test_table"
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl USING delta as SELECT 1 as value") // 0th commit
        val log = DeltaLog.forTable(spark, TableIdentifier(tbl))
        log.update()
        // CRC for 0th commit has SetTransactions defined and are empty Seq.
        assert(log.unsafeVolatileSnapshot.checksumOpt.flatMap(_.setTransactions).isDefined)
        assert(log.unsafeVolatileSnapshot.checksumOpt.flatMap(_.setTransactions).get.isEmpty)
        assertSetTransactions(log, expectedTxns = Map())

        commitSetTxn(log, "app-1", version = 1, lastUpdated = 1) // 1st commit
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 1))
        commitSetTxn(log, "app-1", version = 3, lastUpdated = 2) // 2nd commit
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 3))
        commitSetTxn(log, "app-2", version = 100, lastUpdated = 3) // 3rd commit
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 3, "app-2" -> 100))
        commitSetTxn(log, "app-1", version = 4, lastUpdated = 4) // 4th commit
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 4, "app-2" -> 100))

        // 5th commit - Commit multiple [[SetTransaction]] in single commit
        commitSetTxn(
          log,
          setTransactions = Seq(
            SetTransaction("app-1", version = 100, lastUpdated = Some(4)),
            SetTransaction("app-3", version = 300, lastUpdated = Some(4))
          ))
        assertSetTransactions(
          log,
          expectedTxns = Map("app-1" -> 100, "app-2" -> 100, "app-3" -> 300))
      }
    }
  }

  test("set-transaction tracking starts for old tables after new commits") {
    withSQLConf(DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "false") {
      val tbl = "test_table"
      withTable(tbl) {
        // Create a table with feature disabled. So 0th/1st commit won't do SetTransaction
        // tracking in CRC.
        sql(s"CREATE TABLE $tbl USING delta as SELECT 1 as value") // 0th commit

        def deltaLog: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))

        assert(deltaLog.update().checksumOpt.get.setTransactions.isEmpty)
        commitSetTxn(deltaLog, "app-1", version = 1, lastUpdated = 1) // 1st commit
        assert(deltaLog.update().checksumOpt.get.setTransactions.isEmpty)

        // Enable the SetTransaction tracking config and do more commits in the table.
        withSQLConf(DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true") {
          DeltaLog.clearCache()
          commitSetTxn(deltaLog, "app-1", version = 2, lastUpdated = 2) // 2nd commit
          // By default, commit doesn't trigger stateReconstruction and so the
          // incremental CRC won't have setTransactions present until `setTransactions` API is
          // explicitly invoked before the commit.
          assert(deltaLog.update().checksumOpt.get.setTransactions.isEmpty) // crc has no set-txn
          assertSetTransactions(deltaLog, expectedTxns = Map("app-1" -> 2), viaCRC = false)
          DeltaLog.clearCache()

          // Do commit after forcing computeState. Now SetTransaction tracking will start.
          deltaLog.snapshot.setTransactions // This triggers computeState.
          commitSetTxn(deltaLog, "app-2", version = 100, lastUpdated = 3) // 3rd commit
          assert(deltaLog.update().checksumOpt.get.setTransactions.nonEmpty) // crc has set-txn
        }
      }
    }
  }

  test("validate that crc doesn't contain SetTransaction when tracking is disabled") {
    withSQLConf(DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "false") {
      val tbl = "test_table"
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl (value Int) USING delta")
        val log = DeltaLog.forTable(spark, TableIdentifier(tbl))
        // CRC for 0th commit should not have SetTransactions defined if conf is disabled.
        assert(log.unsafeVolatileSnapshot.checksumOpt.flatMap(_.setTransactions).isEmpty)
        assertSetTransactions(log, expectedTxns = Map(), viaCRC = false)

        commitSetTxn(log, "app-1", version = 1, lastUpdated = 1)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 1), viaCRC = false)

        commitSetTxn(log, "app-1", version = 3, lastUpdated = 2)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 3), viaCRC = false)

        commitSetTxn(log, "app-2", version = 100, lastUpdated = 3)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 3, "app-2" -> 100), viaCRC = false)

        commitSetTxn(log, "app-1", version = 4, lastUpdated = 4)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 4, "app-2" -> 100), viaCRC = false)
      }
    }
  }

  for(computeStatePreloaded <- BOOLEAN_DOMAIN) {
    test("set-transaction tracking should start if computeState is pre-loaded before" +
        s" commit [computeState preloaded: $computeStatePreloaded]") {

      // Enable INCREMENTAL COMMITS and disable verification - to make sure that we
      // don't trigger state reconstruction after a commit.
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "false",
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
        DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key -> "false"
      ) {
        val tbl = "test_table"

        def log: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))

        withTable(tbl) {
          sql(s"CREATE TABLE $tbl (value Int) USING delta")
          // After 0th commit - CRC shouldn't have SetTransactions as feature is disabled.
          assertSetTransactions(log, expectedTxns = Map(), viaCRC = false)

          DeltaLog.clearCache()
          withSQLConf(DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true") {
            commitSetTxn(log, "app-1", version = 1, lastUpdated = 1)
            // During 1st commit, the feature is enabled. But still the new commit crc shouldn't
            // contain the [[SetTransaction]] actions as we don't have an estimate of how many
            // [[SetTransaction]] actions might be already part of this table till now.
            // So incremental computation of [[SetTransaction]] won't trigger.
            assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)

            if (computeStatePreloaded) {
              // Calling `validateChecksum` will pre-load the computeState
              log.update().validateChecksum()
            }
            // During 2nd commit, we have following 2 cases:
            // 1. If `computeStatePreloaded` is set, then the Snapshot has already calculated
            //    computeState and so we have estimate of number of SetTransactions till this point.
            //    So next commit will trigger incremental computation of [[SetTransaction]].
            // 2. If `computeStatePreloaded` is not set, then Snapshot doesn't have computeState
            //    pre-computed. So next commit will not trigger incremental computation of
            //    [[SetTransaction]].
            commitSetTxn(log, "app-1", version = 100, lastUpdated = 1)
            assert(log.update().checksumOpt.flatMap(_.setTransactions).nonEmpty ===
              computeStatePreloaded)
          }
        }
      }
    }
  }

  test("set-transaction tracking in CRC should stop once threshold is crossed") {
    withSQLConf(
        DeltaSQLConf.DELTA_MAX_SET_TRANSACTIONS_IN_CRC.key -> "2",
        DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true") {
      val tbl = "test_table"
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl (value Int) USING delta")
        def log: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        assertSetTransactions(log, expectedTxns = Map())

        commitSetTxn(log, "app-1", version = 1, lastUpdated = 1)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isDefined)
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 1))

        commitSetTxn(log, "app-1", version = 3, lastUpdated = 2)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isDefined)
        assertSetTransactions(log, expectedTxns = Map("app-1" -> 3))

        commitSetTxn(log, "app-2", version = 100, lastUpdated = 3)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isDefined)
        assertSetTransactions(
          log, expectedTxns = Map("app-1" -> 3, "app-2" -> 100))

        commitSetTxn(log, "app-1", version = 4, lastUpdated = 4)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isDefined)
        assertSetTransactions(
          log, expectedTxns = Map("app-1" -> 4, "app-2" -> 100))

        commitSetTxn(log, "app-3", version = 1000, lastUpdated = 5)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)
        assertSetTransactions(
          log, expectedTxns = Map("app-1" -> 4, "app-2" -> 100, "app-3" -> 1000), viaCRC = false)
      }
    }
  }

  test("set-transaction tracking in CRC should stop once setTxn retention conf is set") {
    withSQLConf(
      DeltaSQLConf.DELTA_MAX_SET_TRANSACTIONS_IN_CRC.key -> "2",
      DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true") {
      val tbl = "test_table"
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl (value Int) USING delta")
        def log: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isDefined)

        // Do 1 commit to table - set-transaction tracking continue to happen.
        commitSetTxn(log, "app-1", version = 1, lastUpdated = 1)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isDefined)

        // Set any random table property - set-transaction tracking continue to happen.
        sql(s"ALTER TABLE $tbl SET TBLPROPERTIES ('randomProp1' = 'value1')")
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isDefined)

        // Set the `setTransactionRetentionDuration` table property - set-transaction tracking will
        // stop.
        sql(s"ALTER TABLE $tbl SET TBLPROPERTIES " +
          s"('delta.setTransactionRetentionDuration' = 'interval 1 days')")
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)
        commitSetTxn(log, "app-1", version = 1, lastUpdated = 1)
        log.update().setTransactions
        commitSetTxn(log, "app-1", version = 1, lastUpdated = 1)
        assert(log.update().checksumOpt.flatMap(_.setTransactions).isEmpty)

      }
    }
  }

  for(checksumVerificationFailureIsFatal <- BOOLEAN_DOMAIN) {
    // In this test we check that verification failed usage-logs are triggered when
    // there is an issue in incremental computation and verification is explicitly enabled.
    test("incremental set-transaction verification failures" +
        s" [checksumVerificationFailureIsFatal: $checksumVerificationFailureIsFatal]") {
      withSQLConf(
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_WRITE_SET_TRANSACTIONS_IN_CRC.key -> "true",
        // Enable verification explicitly as it is disabled by default.
        DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key -> true.toString,
        DeltaSQLConf.DELTA_CHECKSUM_MISMATCH_IS_FATAL.key -> s"$checksumVerificationFailureIsFatal"
      ) {
        withTempDir { tempDir =>
          // Procedure:
          // 1. Populate the table with 2 [[SetTransaction]]s and create a checkpoint, validate that
          //    CRC has setTransactions present.
          // 2. Intentionally corrupt the checkpoint - Remove one SetTransaction from it.
          // 3. Clear the delta log cache so we pick up the checkpoint
          // 4. Start a new transaction and attempt to commit the transaction
          //    a. Incremental SetTransaction verification should fail
          //    b. Post-commit snapshot should have checksumOpt with no [[SetTransaction]]s

          // Step-1
          val txn0 = SetTransaction("app-0", version = 1, lastUpdated = Some(1))
          val txn1 = SetTransaction("app-1", version = 888, lastUpdated = Some(2))

          def log: DeltaLog = DeltaLog.forTable(spark, tempDir)

          // commit-0
          val actions0 =
            (1 to 10).map(i => createTestAddFile(encodedPath = i.toString)) :+ txn0
          log.startTransaction().commitWriteAppend(actions0: _*)
          // commit-1
          val actions1 =
            (11 to 20).map(i => createTestAddFile(encodedPath = i.toString)) :+ txn1
          log.startTransaction().commitWriteAppend(actions1: _*)
          assert(log.readChecksum(version = 1).get.setTransactions.nonEmpty)
          log.checkpoint()

          // Step-2
          dropOneSetTransactionFromCheckpoint(log)

          // Step-3
          DeltaLog.clearCache()
          assert(!log.update().logSegment.checkpointProvider.isEmpty)

          // Step-4
          // Create the txn with [[DELTA_CHECKSUM_MISMATCH_IS_FATAL]] as false so that pre-commit
          // CRC validation doesn't fail. Our goal is to capture that post-commit verification
          // catches any issues.
          var txn: OptimisticTransactionImpl = null
          withSQLConf(DeltaSQLConf.DELTA_CHECKSUM_MISMATCH_IS_FATAL.key -> "false") {
            txn = log.startTransaction()
          }
          val Seq(corruptionReport) = collectSetTransactionCorruptionReport {
            if (checksumVerificationFailureIsFatal) {
              val e = intercept[DeltaIllegalStateException] {
                withSQLConf(DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key -> "true") {
                  txn.commit(Seq(), DeltaOperations.Write(SaveMode.Append))
                }
              }
              assert(e.getMessage.contains("SetTransaction mismatch"))
            } else {
              txn.commit(Seq(), DeltaOperations.Write(SaveMode.Append))
            }
          }
          val eventData = JsonUtils.fromJson[Map[String, Any]](corruptionReport.blob)

          val expectedErrorEventData = Map(
            "unmatchedSetTransactionsCRC" -> Seq(txn1),
            "unmatchedSetTransactionsComputedState" -> Seq.empty,
            "version" -> 2,
            "minSetTransactionRetentionTimestamp" -> None,
            "repeatedEntriesForSameAppId" -> Seq.empty,
            "exactMatchFailed" -> true)

          val observedMismatchingFields = eventData("mismatchingFields").asInstanceOf[Seq[String]]
          val observedErrorMessage = eventData("error").asInstanceOf[String]
          val observedDetailedErrorMap =
            eventData("detailedErrorMap").asInstanceOf[Map[String, String]]
          assert(observedMismatchingFields === Seq("setTransactions"))
          assert(observedErrorMessage.contains("SetTransaction mismatch"))
          assert(observedDetailedErrorMap("setTransactions") ===
            JsonUtils.toJson(expectedErrorEventData))

          if (checksumVerificationFailureIsFatal) {
            // Due to failure, post-commit snapshot couldn't be updated
            assert(log.snapshot.version === 1)
            assert(log.readChecksum(version = 2).isEmpty)
          } else {
            assert(log.snapshot.version === 2)
            assert(log.readChecksum(version = 2).get.setTransactions.isEmpty)
          }
        }
      }
    }
  }

  /** Drops one [[SetTransaction]] operation from checkpoint - the one with max appId */
  private def dropOneSetTransactionFromCheckpoint(log: DeltaLog): Unit = {
    import testImplicits._
    val checkpointPath = FileNames.checkpointFileSingular(log.logPath, log.snapshot.version)
    withTempDir { tmpCheckpoint =>
      // count total rows in checkpoint
      val checkpointDf = spark.read
        .schema(SingleAction.encoder.schema)
        .parquet(checkpointPath.toString)
      val initialActionCount = checkpointDf.count().toInt
      val corruptedCheckpointData = checkpointDf
        .orderBy(col("txn.appId").asc_nulls_first) // force non setTransaction actions to front
        .as[SingleAction].take(initialActionCount - 1) // Drop 1 action

      corruptedCheckpointData.toSeq.toDS().coalesce(1).write
        .mode("overwrite").parquet(tmpCheckpoint.toString)
      assert(spark.read.parquet(tmpCheckpoint.toString).count() === initialActionCount - 1)
      val writtenCheckpoint =
        tmpCheckpoint.listFiles().toSeq.filter(_.getName.startsWith("part")).head
      val checkpointFile = new File(checkpointPath.toUri)
      new File(log.logPath.toUri).listFiles().toSeq.foreach { file =>
        if (file.getName.startsWith(".0")) {
          // we need to delete checksum files, otherwise trying to replace our incomplete
          // checkpoint file fails due to the LocalFileSystem's checksum checks.
          assert(file.delete(), "Failed to delete checksum file")
        }
      }
      assert(checkpointFile.delete(), "Failed to delete old checkpoint")
      assert(writtenCheckpoint.renameTo(checkpointFile),
        "Failed to rename corrupt checkpoint")
      val newCheckpoint = spark.read.parquet(checkpointFile.toString)
      assert(newCheckpoint.count() === initialActionCount - 1,
        "Checkpoint file incorrect:\n" + newCheckpoint.collect().mkString("\n"))
    }
  }

  private def collectSetTransactionCorruptionReport(f: => Unit): Seq[UsageRecord] = {
    collectUsageLogs("delta.checksum.invalid")(f).toSeq
  }
}
