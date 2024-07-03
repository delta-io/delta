/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.Table
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.{AddFile, Metadata, RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.{DeltaLog, VersionNotFoundException}
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.scalatest.funsuite.AnyFunSuite
import java.io.File

import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{CheckpointAlreadyExistsException, TableNotFoundException}

/**
 * Test suite for `io.delta.kernel.Table.checkpoint(engine, version)`
 */
class CreateCheckpointSuite extends DeltaTableWriteSuiteBase {

  ///////////
  // Tests //
  ///////////

  Seq(true, false).foreach { includeRemoves =>
    val testMsgUpdate = if (includeRemoves) " and removes" else ""
    test(s"commits containing adds$testMsgUpdate, and no previous checkpoint") {
      withTempDirAndEngine { (tablePath, tc) =>
        addData(tablePath, alternateBetweenAddsAndRemoves = includeRemoves, numberIter = 10)

        // before creating checkpoint, read and save the expected results using Spark
        val expResults = readUsingSpark(tablePath)
        assert(expResults.size === (if (includeRemoves) 45 else 100))

        val checkpointVersion = 9
        kernelCheckpoint(tc, tablePath, checkpointVersion)

        verifyResults(tablePath, expResults, checkpointVersion)
        verifyLastCheckpointMetadata(
          tablePath,
          checkpointVersion,
          expSize = if (includeRemoves) 5 else 10)

        // add few more commits and verify the read still works
        appendCommit(tablePath)
        val newExpResults = expResults ++ Seq.range(0, 10).map(_.longValue()).map(TestRow(_))
        verifyResults(tablePath, newExpResults, checkpointVersion)
      }
    }
  }


  Seq(true, false).foreach { includeRemoves =>
    Seq(
      // Create a checkpoint using Spark (either classic or multi-part checkpoint)
      1000000, // use large number of actions per file to make Spark create a classic checkpoint
      3 // use small number of actions per file to make Spark create a multi-part checkpoint
    ).foreach { sparkCheckpointActionPerFile =>
      val testMsgUpdate = if (includeRemoves) " and removes" else ""

      test(s"commits containing adds$testMsgUpdate, and a previous checkpoint " +
        s"created using Spark (actions/perfile): $sparkCheckpointActionPerFile") {
        withTempDirAndEngine { (tablePath, tc) =>
          addData(tablePath, includeRemoves, numberIter = 6)

          // checkpoint using Spark
          sparkCheckpoint(tablePath, actionsPerFile = sparkCheckpointActionPerFile)

          addData(tablePath, includeRemoves, numberIter = 6) // add some more data

          // before creating checkpoint, read and save the expected results using Spark
          val expResults = readUsingSpark(tablePath)
          assert(expResults.size === (if (includeRemoves) 54 else 120))

          val checkpointVersion = 11

          kernelCheckpoint(tc, tablePath, checkpointVersion)
          verifyResults(tablePath, expResults, checkpointVersion)
          verifyLastCheckpointMetadata(
            tablePath,
            checkpointVersion,
            expSize = if (includeRemoves) 6 else 12)

          // add few more commits and verify the read still works
          appendCommit(tablePath)
          val newExpResults = expResults ++ Seq.range(0, 10).map(_.longValue()).map(TestRow(_))
          verifyResults(tablePath, newExpResults, checkpointVersion)
        }
      }
    }
  }

  test("commits with metadata updates") {
    withTempDirAndEngine { (tablePath, tc) =>
      addData(path = tablePath, alternateBetweenAddsAndRemoves = true, numberIter = 16)

      // makes the latest table version 16
      spark.sql(
        s"""ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES ('delta.appendOnly' = 'true')""")

      // before creating checkpoint, read and save the expected results using Spark
      val expResults = readUsingSpark(tablePath)
      assert(expResults.size === 72)

      val checkpointVersion = 16
      kernelCheckpoint(tc, tablePath, checkpointVersion)
      verifyResults(tablePath, expResults, checkpointVersion)
      verifyLastCheckpointMetadata(tablePath, checkpointVersion, expSize = 8)

      // verify there is only one metadata entry in the checkpoint and it has the
      // configuration with `delta.appendOnly` = `true`
      val result = spark.read.format("parquet")
        .load(checkpointFilePath(tablePath, checkpointVersion))
        .filter("metaData is not null")
        .select("metaData.configuration")
        .collect().toSeq.map(TestRow(_))

      val expected = Seq(TestRow(Map("delta.appendOnly" -> "true")))

      checkAnswer(result, expected)
    }
  }

  test("commits with protocol updates") {
    withTempDirAndEngine { (tablePath, tc) =>
      addData(path = tablePath, alternateBetweenAddsAndRemoves = true, numberIter = 16)

      spark.sql(
        s"""
           |ALTER TABLE delta.`$tablePath` SET TBLPROPERTIES (
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5'
           |)
           |""".stripMargin) // makes the latest table version 16

      // before creating checkpoint, read and save the expected results using Spark
      val expResults = readUsingSpark(tablePath)
      assert(expResults.size === 72)

      val checkpointVersion = 16
      kernelCheckpoint(tc, tablePath, checkpointVersion)
      verifyResults(tablePath, expResults, checkpointVersion)

      // verify there is only one protocol entry in the checkpoint and it has the
      // expected minReaderVersion and minWriterVersion
      val result = spark.read.format("parquet")
        .load(checkpointFilePath(tablePath, checkpointVersion))
        .filter("protocol is not null")
        .select("protocol.minReaderVersion", "protocol.minWriterVersion")
        .collect().toSeq.map(TestRow(_))

      val expected = Seq(TestRow(2, 5))

      checkAnswer(result, expected)
    }
  }

  test("commits with set transactions") {
    withTempDirAndEngine { (tablePath, tc) =>
      def idempotentAppend(appId: String, version: Int): Unit = {
        spark.range(end = 10).repartition(2).write.format("delta")
          .option("txnAppId", appId)
          .option("txnVersion", version)
          .mode("append").save(tablePath)
      }

      idempotentAppend("appId1", 0) // version 0
      idempotentAppend("appId1", 2) // version 1
      idempotentAppend("appId1", 3) // version 2
      deleteCommit(tablePath) // version 3
      idempotentAppend("appId2", 7) // version 4
      idempotentAppend("appId2", 25) // version 5
      idempotentAppend("appId3", 7908) // version 6
      appendCommit(tablePath) // version 7, no txn identifiers
      idempotentAppend("appId4", 12312312) // version 8

      // before creating checkpoint, read and save the expected results using Spark
      val expResults = readUsingSpark(tablePath)
      assert(expResults.size === 77)

      val checkpointVersion = 8
      kernelCheckpoint(tc, tablePath, checkpointVersion);
      verifyResults(tablePath, expResults, checkpointVersion)

      // Load the checkpoint and verify that only the last txn identifier for each appId is stored
      def verifyTxnIdInCheckpoint(appId: String, expVersion: Long): Unit = {
        val result = spark.read.format("parquet")
          .load(checkpointFilePath(tablePath, checkpointVersion))
          .filter(s"txn is not null and txn.appId='$appId'")
          .select("txn.appId", "txn.version")
          .collect().toSeq.map(TestRow(_))
        checkAnswer(result, Seq(TestRow(appId, expVersion)))
      }

      verifyTxnIdInCheckpoint("appId1", 3)
      verifyTxnIdInCheckpoint("appId2", 25)
      verifyTxnIdInCheckpoint("appId3", 7908)
      verifyTxnIdInCheckpoint("appId4", 12312312)
    }
  }

  Seq(None, Some("2 days"), Some("0 days")).foreach { retentionInterval =>
    test(s"checkpoint contains all not expired tombstones: $retentionInterval") {
      withTempDirAndEngine { (tablePath, tc) =>
        def addFile(path: String): AddFile = AddFile(
          path = path,
          partitionValues = Map.empty,
          size = 0,
          modificationTime = 0L,
          dataChange = true)

        def removeFile(path: String, deletionTimestamp: Long): Unit = {
          val remove = RemoveFile(path = path, deletionTimestamp = Some(deletionTimestamp))
          val deltaLog = DeltaLog.forTable(spark, tablePath)
          val txn = deltaLog.startTransaction()
          txn.commit(Seq(remove), ManualUpdate)
        }

        def addFiles(addFiles: String*): Unit = {
          val deltaLog = DeltaLog.forTable(spark, tablePath)
          val txn = deltaLog.startTransaction()
          val configuration = retentionInterval.map(interval =>
            Map("delta.deletedFileRetentionDuration" -> interval)).getOrElse(Map.empty)
          txn.updateMetadata(Metadata(
            schemaString = new StructType().add("c1", IntegerType).json,
            configuration = configuration))
          txn.commit(addFiles.map(addFile(_)), ManualUpdate)
        }

        def millisPerDays(days: Int): Long = days * 24 * 60 * 60 * 1000

        // version 0
        addFiles(
          "file1", "file2", "file3", "file4", "file5", "file6", "file7", "file8", "file9")

        val now = System.currentTimeMillis()
        removeFile("file8", deletionTimestamp = 1) // set delete time very old
        removeFile("file7", deletionTimestamp = now - millisPerDays(8))
        removeFile("file6", deletionTimestamp = now - millisPerDays(3))
        removeFile("file5", deletionTimestamp = now - 1000) // set delete time 1 second ago
        // end version 4

        // add few more files - version 5
        addFiles(
          "file10", "file11", "file12", "file13", "file14", "file15", "file16", "file17", "file18")

        // delete some files again
        removeFile("file3", deletionTimestamp = now - millisPerDays(9))
        removeFile("file2", deletionTimestamp = now - millisPerDays(1))
        // end version 7

        val expected = if (retentionInterval.isEmpty) {
          // Given the default retention interval is 1 week, the tombstones file8, file 7 and file 3
          // should be expired and not included in the checkpoint
          Seq("file6", "file5", "file2").map(TestRow(_))
        } else if (retentionInterval.get.equals("2 days")) {
          // Given the retention interval is 2 days, the tombstones file8, file 7, file 6, file 3
          // should be expired and not included in the checkpoint
          Seq("file5", "file2").map(TestRow(_))
        } else {
          // All tombstones should be excluded in the checkpoint
          Seq.empty
        }

        val checkpointVersion = 7
        kernelCheckpoint(tc, tablePath, checkpointVersion)

        val result = spark.read.format("parquet")
          .load(checkpointFilePath(tablePath, checkpointVersion))
          .filter("remove is not null")
          .select("remove.path")
          .collect().toSeq.map(TestRow(_))

        checkAnswer(result, expected)
      }
    }
  }

  test("try creating checkpoint on a non-existent table") {
    withTempDirAndEngine { (path, tc) =>
      Seq(0, 1, 2).foreach { checkpointVersion =>
        val ex = intercept[TableNotFoundException] {
          kernelCheckpoint(tc, path, checkpointVersion)
        }
        assert(ex.getMessage.contains("not found"))
      }
    }
  }

  test("try creating checkpoint at version that already has a " +
    "checkpoint or a version that doesn't exist") {
    withTempDirAndEngine { (path, tc) =>
      for (_ <- 0 to 3) {
        appendCommit(path)
      }

      val table = Table.forPath(tc, path)
      table.checkpoint(tc, 3)
      val ex = intercept[CheckpointAlreadyExistsException] {
        kernelCheckpoint(tc, path, 3)
      }
      assert(ex.getMessage.contains("Checkpoint for given version 3 already exists in the table"))

      val ex2 = intercept[Exception] {
        kernelCheckpoint(tc, path, checkpointVersion = 5)
      }
      assert(ex2.getMessage.contains("Cannot load table version 5 as it does not exist"))
    }
  }

  test("create a checkpoint on a existing table") {
    withTempDirAndEngine { (tablePath, tc) =>
      copyTable("time-travel-start-start20-start40", tablePath)

      // before creating checkpoint, read and save the expected results using Spark
      val expResults = readUsingSpark(tablePath)
      assert(expResults.size === 30)

      val checkpointVersion = 2
      kernelCheckpoint(tc, tablePath, checkpointVersion)
      verifyResults(tablePath, expResults, checkpointVersion)
    }
  }

  test("try create a checkpoint on a unsupported table feature table") {
    withTempDirAndEngine { (tablePath, tc) =>
      copyTable("dv-with-columnmapping", tablePath)

      val ex2 = intercept[Exception] {
        kernelCheckpoint(tc, tablePath, checkpointVersion = 5)
      }
      assert(ex2.getMessage.contains("Unsupported Delta writer feature") &&
        ex2.getMessage.contains("writer table feature \"deletionVectors\""))
    }
  }

  ////////////////////
  // Helper methods //
  ///////////////////
  def addData(path: String, alternateBetweenAddsAndRemoves: Boolean, numberIter: Int): Unit = {
    Seq.range(0, numberIter).foreach { version =>
      if (version % 2 == 1 && alternateBetweenAddsAndRemoves) {
        deleteCommit(path) // removes one file and adds a new one
      } else {
        appendCommit(path) // add one new file
      }
    }
  }

  def appendCommit(path: String): Unit =
    spark.range(end = 10).write.format("delta").mode("append").save(path)

  def deleteCommit(path: String): Unit = {
    spark.sql(s"DELETE FROM delta.`${path}` WHERE id = 5")
  }

  def sparkCheckpoint(path: String, actionsPerFile: Int = 10000000): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_CHECKPOINT_PART_SIZE.key -> actionsPerFile.toString) {
      DeltaLog.forTable(spark, path).checkpoint()
    }
  }

  def kernelCheckpoint(tc: Engine, tablePath: String, checkpointVersion: Long): Unit = {
    Table.forPath(tc, tablePath).checkpoint(tc, checkpointVersion)
  }

  def readUsingSpark(tablePath: String): Seq[TestRow] = {
    spark.read.format("delta").load(tablePath).collect().map(TestRow(_))
  }

  def verifyResults(
      tablePath: String,
      expResults: Seq[TestRow],
      checkpointVersion: Long): Unit = {
    // before verifying delete the delta commits before the checkpoint to make sure
    // the state is constructed using the table path
    deleteDeltaFilesBefore(tablePath, checkpointVersion)

    // verify using Spark reader
    checkAnswer(readUsingSpark(tablePath), expResults)

    // verify using Kernel reader
    checkTable(tablePath, expResults)
  }
}
