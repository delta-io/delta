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

// scalastyle:off import.ordering.noEmptyLine
import java.util.TimeZone
import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.DeltaTestUtils.{collectUsageLogs, BOOLEAN_DOMAIN}
import org.apache.spark.sql.delta.concurrency._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class DeltaAllFilesInCrcSuite
    extends QueryTest
    with SharedSparkSession
    with TransactionExecutionTestMixin
    with DeltaSQLCommandTest
    with PhaseLockingTestMixin {
  protected override def sparkConf = super.sparkConf
    .set(DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key, "true")
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key, "true")
    // Set the threshold to a very high number so that this test suite continues to use all files
    // from CRC.
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_THRESHOLD_FILES.key, "10000")
    // needed for DELTA_ALL_FILES_IN_CRC_ENABLED
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key, "true")
    .set(DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED.key, "true")
    // Turn on verification by default in the tests
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key, "true")
    // Turn off force verification for non-UTC timezones by default in the tests
    .set(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key,
      "false")

  private def setTimeZone(timeZone: String): Unit = {
    spark.sql(s"SET spark.sql.session.timeZone = $timeZone")
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
  }

  /** Filter usage records for specific `opType` */
  protected def filterUsageRecords(
      usageRecords: Seq[UsageRecord],
      opType: String): Seq[UsageRecord] = {
    usageRecords.filter { r =>
      r.tags.get("opType").contains(opType) || r.opType.map(_.typeName).contains(opType)
    }
  }

  /** Deletes all delta/crc/checkpoint files later that given `version` for the delta table */
  private def deleteDeltaFilesLaterThanVersion(deltaLog: DeltaLog, version: Long): Unit = {
    val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
    deltaLog.listFrom(version + 1).filter { f =>
      FileNames.isDeltaFile(f) || FileNames.isChecksumFile(f) || FileNames.isCheckpointFile(f)
    }.foreach(f => fs.delete(f.getPath, true))
    DeltaLog.clearCache()
    assert(DeltaLog.forTable(spark, deltaLog.dataPath).update().version === version)
  }

  test("allFiles are written to CRC and different threshold configs are respected") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath

      // Helper method to perform a commit with 10 AddFile actions to the table.
      def writeToTable(
          version: Long, newFilesToWrite: Int, expectedFilesInCRCOption: Option[Long]): Unit = {
        spark.range(start = 1, end = 100, step = 1, numPartitions = newFilesToWrite)
          .toDF("c1")
          .withColumn("c2", col("c1")).withColumn("c3", col("c1"))
          .write.format("delta").mode("append").save(path)
        assert(deltaLog.update().version === version)
        assert(deltaLog.snapshot.checksumOpt.get.allFiles.map(_.size) === expectedFilesInCRCOption)
        assert(deltaLog.readChecksum(version).get.allFiles.map(_.size) === expectedFilesInCRCOption)
      }

      def deltaLog: DeltaLog = DeltaLog.forTable(spark, path)

      withSQLConf(
          DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_THRESHOLD_FILES.key -> "55",
          DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key -> "false") {
        // Commit-0: Add 10 new files to table. Total files (10) is less than threshold.
        writeToTable(version = 0, newFilesToWrite = 10, expectedFilesInCRCOption = Some(10))

        withSQLConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "false") {
          // Commit-1: Add 10 more files to table. Total files (20) is less than threshold.
          // Still these won't be written to CRC as the conf is explicitly disabled.
          writeToTable(version = 1, newFilesToWrite = 10, expectedFilesInCRCOption = None)
        }
        // Commit-2: Add 20 more files to table. Total files (40) is less than threshold.
        writeToTable(version = 2, newFilesToWrite = 20, expectedFilesInCRCOption = Some(40))
        // Commit-3: Add 13 more files to table. Total files (53) is less than threshold.
        writeToTable(version = 3, newFilesToWrite = 13, expectedFilesInCRCOption = Some(53))
        // Commit-4: Add 7 more files to table. Total files (60) is greater than the threshold (55).
        // So files won't be persisted to CRC.
        writeToTable(version = 4, newFilesToWrite = 7, expectedFilesInCRCOption = None)

        // Commit-5: Delete all rows except with value=1. After this step, very few files will
        // remain in table, still they won't be persisted to CRC as previous version had more than
        // 55 files. We write files to CRC if both previous commit and this commit has files <= 55.
        sql(s"DELETE FROM delta.`$path` WHERE c1 != 1").collect()
        assert(deltaLog.update().version === 5)
        assert(deltaLog.snapshot.checksumOpt.get.allFiles === None)
        val fileCountAfterDeleteCommand = deltaLog.snapshot.checksumOpt.get.numFiles
        assert(fileCountAfterDeleteCommand < 55)

        // Commit-6: Commit 1 new file again. Now previous-version also had < 55 files. This version
        // also has < 55 files.
        writeToTable(version = 6, newFilesToWrite = 1,
          expectedFilesInCRCOption = Some(fileCountAfterDeleteCommand + 1))

        withSQLConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_THRESHOLD_INDEXED_COLS.key -> "2") {
          // Table collects stats on 3 cols (col1/col2/col3) which is more than threshold.
          // So optimization should be disabled by default.
          writeToTable(version = 7, newFilesToWrite = 1, expectedFilesInCRCOption = None)
        }

        writeToTable(version = 8, newFilesToWrite = 1,
          expectedFilesInCRCOption = Some(fileCountAfterDeleteCommand + 3))

        // Commit-7: Delete all rows from table
        sql(s"DELETE FROM delta.`$path` WHERE c1 >= 0").collect()
        assert(deltaLog.update().version === 9)
        assert(deltaLog.snapshot.checksumOpt.get.allFiles === Some(Seq()))
      }
    }
  }


  test("test all-files-in-crc verification failure also triggers and logs" +
    " incremental-commit verification result") {
    withTempDir { tempDir =>
      withSQLConf(
          DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_THRESHOLD_FILES.key -> "100",
          // Disable incremental commit force verifications in UTs - to mimic prod behavior
          DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key -> "false",
          // Enable all-files-in-crc verification mode
          DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key -> "true",
          DeltaSQLConf.DELTA_COLLECT_STATS.key -> "false") {

        val df = spark.range(2).coalesce(1).toDF()
        df.write.format("delta").save(tempDir.toString())
        val deltaLog = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        assert(deltaLog.update().allFiles.collect().map(_.numPhysicalRecords).forall(_.isEmpty))

        val records = Log4jUsageLogger.track {
          val executor = ThreadUtils.newDaemonSingleThreadExecutor(threadName = "executor-txn-A")
          try {
            val query = s"DELETE from delta.`${tempDir.getAbsolutePath}` WHERE id >= 0"
            val (observer, future) = runQueryWithObserver(name = "A", executor, query)
            observer.phases.initialPhase.entryBarrier.unblock()
            observer.phases.preparePhase.entryBarrier.unblock()
            // Make sure that delete query has run the actual computation and has reached
            // the 'prepare commit' phase. i.e. it just wants to commit.
            busyWaitFor(observer.phases.preparePhase.hasLeft, timeout)
            // Now delete and recreate the complete table.
            deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())
              .delete(deltaLog.dataPath, true)
            spark.range(3, 4).coalesce(1).toDF().write.format("delta").save(tempDir.toString())

            // Allow the delete query to commit.
            unblockCommit(observer)
            waitForCommit(observer)
            // Query will fail due to incremental-state-reconstruction validation failure.
            // Note that this failure happens only in test. In prod, this would have just logged
            // the incremental-state-reconstruction failure and query would have passed.
            val ex = intercept[SparkException] { ThreadUtils.awaitResult(future, Duration.Inf) }
            val message = ex.getMessage + "\n" + ex.getCause.getMessage
            assert(message.contains("Incremental state reconstruction validation failed"))
          } finally {
            executor.shutdownNow()
            executor.awaitTermination(timeout.toMillis, TimeUnit.MILLISECONDS)
          }
        }

        // We will see all files in CRC verification failure.
        // This will trigger the incremental commit verification which will fail.
        assert(filterUsageRecords(records, "delta.assertions.mismatchedAction").size === 1)
        val allFilesInCrcValidationFailureRecords =
          filterUsageRecords(records, "delta.allFilesInCrc.checksumMismatch.differentAllFiles")
        assert(allFilesInCrcValidationFailureRecords.size === 1)
        val eventData =
          JsonUtils.fromJson[Map[String, String]](allFilesInCrcValidationFailureRecords.head.blob)
        assert(eventData("version").toLong === 1L)
        assert(eventData("mismatchWithStatsOnly").toBoolean === false)
        val expectedFilesCountFromCrc = 1L
        assert(eventData("filesCountFromCrc").toLong === expectedFilesCountFromCrc)
        assert(eventData("filesCountFromStateReconstruction").toLong ===
          expectedFilesCountFromCrc + 1)
        assert(eventData("incrementalCommitCrcValidationPassed").toBoolean === false)
        assert(eventData("errorForIncrementalCommitCrcValidation").contains(
          "The metadata of your Delta table could not be recovered"))
      }
    }
  }

  test("schema changing metadata operations should disable putting AddFile" +
      " actions in crc but other metadata operations should not") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1, 5).toDF("c1").withColumn("c2", col("c1"))
        .write.format("delta").mode("append").save(path)

      def deltaLog: DeltaLog = DeltaLog.forTable(spark, path)
      assert(deltaLog.update().checksumOpt.get.allFiles.nonEmpty)
      sql(s"ALTER TABLE delta.`$dir` CHANGE COLUMN c2 FIRST")
      assert(deltaLog.update().checksumOpt.get.allFiles.isEmpty)
      sql(s"ALTER TABLE delta.`$dir` SET TBLPROPERTIES ('a' = 'b')")
      assert(deltaLog.update().checksumOpt.get.allFiles.nonEmpty)
    }
  }

  test("schema changing metadata operations on empty tables should not disable putting " +
    "AddFile actions in crc") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      def deltaLog: DeltaLog = DeltaLog.forTable(spark, path)

      def assertNoStateReconstructionTriggeredWhenPerfPackEnabled(f: => Unit): Unit = {
        val oldSnapshot = deltaLog.update()
        f
        val newSnapshot = deltaLog.update()
      }

      withSQLConf(
        // Disable test flags to make the behaviors verified in this test close to prod
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key -> "false",
        DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key -> "false"
      ) {
        assertNoStateReconstructionTriggeredWhenPerfPackEnabled {
          // Create a table with an empty schema so that the next write will change the schema
          sql(s"CREATE TABLE delta.`$path` USING delta LOCATION '$path'")
        }
        assert(deltaLog.update().checksumOpt.get.allFiles == Option(Nil))

        assertNoStateReconstructionTriggeredWhenPerfPackEnabled {
          // Write zero files but update the table schema
          spark.range(1, 5).filter("false").write.format("delta")
            .option("mergeSchema", "true").mode("append").save(path)
        }
        // Make sure writing zero files still make a Delta commit so that this test is valid
        assert(deltaLog.update().version == 1)
        assert(deltaLog.update().checksumOpt.get.allFiles == Option(Nil))

        assertNoStateReconstructionTriggeredWhenPerfPackEnabled {
          // Write some files to the table
          spark.range(1, 5).write.format("delta").mode("append").save(path)
        }
        assert(deltaLog.update().checksumOpt.get.allFiles.nonEmpty)
        assert(deltaLog.update().checksumOpt.get.allFiles.get.size > 0)
      }
    }
  }
  private def withCrcVerificationEnabled(testCode: => Unit): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key -> "true") {
      testCode
    }
  }

  private def withCrcVerificationDisabled(testCode: => Unit): Unit = {
    withSQLConf(DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_VERIFICATION_MODE_ENABLED.key -> "false") {
      testCode
    }
  }

  private def write(deltaLog: DeltaLog, numFiles: Int, expectedFilesInCrc: Option[Int]): Unit = {
    spark
      .range(1, 100, 1, numPartitions = numFiles)
      .write
      .format("delta")
      .mode("append")
      .save(deltaLog.dataPath.toString)
    assert(deltaLog.snapshot.checksumOpt.get.allFiles.map(_.size) === expectedFilesInCrc)
  }

  private def corruptCRCNumFiles(deltaLog: DeltaLog, version: Int): Unit = {
    val crc = deltaLog.readChecksum(version).get
    assert(crc.allFiles.nonEmpty)
    val filesInCrc = crc.allFiles.get

    // Corrupt the CRC
    val corruptedCrc = crc.copy(allFiles =
      Some(filesInCrc.dropRight(1)), numFiles = crc.numFiles - 1)
    val checksumFilePath = FileNames.checksumFile(deltaLog.logPath, version)
    deltaLog.store.write(
      checksumFilePath,
      actions = Seq(JsonUtils.toJson(corruptedCrc)).toIterator,
      overwrite = true,
      hadoopConf = deltaLog.newDeltaHadoopConf())
  }

  private def corruptCRCAddFilesModificationTime(deltaLog: DeltaLog, version: Int): Unit = {
    val crc = deltaLog.readChecksum(version).get
    assert(crc.allFiles.nonEmpty)
    val filesInCrc = crc.allFiles.get

    // Corrupt the CRC
    val corruptedCrc = crc.copy(allFiles = Some(filesInCrc.map(_.copy(modificationTime = 23))))
    val checksumFilePath = FileNames.checksumFile(deltaLog.logPath, version)
    deltaLog.store.write(
      checksumFilePath,
      actions = Seq(JsonUtils.toJson(corruptedCrc)).toIterator,
      overwrite = true,
      hadoopConf = deltaLog.newDeltaHadoopConf())
  }

  private def checkIfCrcModificationTimeCorrupted(
     deltaLog: DeltaLog,
     expectCorrupted: Boolean): Unit = {
    val crc = deltaLog.readChecksum(deltaLog.update().version).get
    assert(crc.allFiles.nonEmpty)
    assert(crc.allFiles.get.count(_.modificationTime == 23L) > 0 === expectCorrupted)
  }

  test("allFilesInCRC verification with flag manipulation for UTC timezone") {
    withSQLConf(
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key ->
          "true") {
      setTimeZone("UTC")
      withTempDir { dir =>
        var deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

        // Commit 0: Initial write with verification enabled
        withCrcVerificationEnabled {
          write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(10))
        }

        // Corrupt the CRC at Version 0
        corruptCRCAddFilesModificationTime(deltaLog, version = 0)
        DeltaLog.clearCache()
        deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

        // Commit 1: Write with verification flag off and Verify Incremental CRC at version 1 is
        // also corrupted
        withCrcVerificationDisabled {
          write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(20))
          checkIfCrcModificationTimeCorrupted(deltaLog, expectCorrupted = true)
        }

        // Commit 2: Write with verification flag on and it should fail because the AddFiles from
        // base CRC at Version 1 are incorrect.
        withCrcVerificationEnabled {
          val usageRecords =
            collectUsageLogs("delta.allFilesInCrc.checksumMismatch.differentAllFiles") {
              intercept[IllegalStateException] {
                write(deltaLog, numFiles = 10, expectedFilesInCrc = None)
              }
            }
          assert(usageRecords.size === 1)
        }

        // Commit 3: Write with verification flag on and it should pass since the base CRC is not
        // corrupted anymore.
        withCrcVerificationEnabled {
          write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(40))
          checkIfCrcModificationTimeCorrupted(deltaLog, expectCorrupted = false)
        }
      }
    }
  }

  test("allFilesInCRC verification with flag manipulation for non-UTC timezone") {
    withSQLConf(
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key ->
          "true") {
      setTimeZone("America/Los_Angeles")
      withTempDir { dir =>
        var deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

        // Commit 0: Initial write with verification enabled
        withCrcVerificationEnabled {
          write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(10))
        }

        // Corrupt the CRC at Version 0
        corruptCRCAddFilesModificationTime(deltaLog, version = 0)
        DeltaLog.clearCache()
        deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

        // Commit 1: Write with verification flag off and Verify Incremental CRC is still validated
        // because timezone is non-UTC.
        withCrcVerificationDisabled {
          val usageRecords =
            collectUsageLogs("delta.allFilesInCrc.checksumMismatch.differentAllFiles") {
              intercept[IllegalStateException] {
                write(deltaLog, numFiles = 10, expectedFilesInCrc = None)
              }
            }
          assert(usageRecords.size === 1)
        }

        // Commit 2: Write with verification flag on and it should pass since the base CRC is not
        // corrupted anymore.
        withCrcVerificationEnabled {
          write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(30))
          checkIfCrcModificationTimeCorrupted(deltaLog, expectCorrupted = false)
        }
      }
    }
  }

  test("Verify aggregate stats are matched even when allFilesInCrc " +
      "verification is disabled") {
    setTimeZone("UTC")
    withSQLConf(DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS.key -> "false") {
      withCrcVerificationDisabled {
        withTempDir { dir =>
          var deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

          // Commit 0
          write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(10))

          // Corrupt the CRC at Version 0
          corruptCRCNumFiles(deltaLog, version = 0)
          DeltaLog.clearCache()
          deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)

          // Commit 1: Verify aggregate stats are matched even when verification is off
          val usageRecords = collectUsageLogs("delta.allFilesInCrc.checksumMismatch.aggregated") {
            intercept[IllegalStateException] {
              write(deltaLog, numFiles = 10, expectedFilesInCrc = None)
            }
          }
          assert(usageRecords.size === 1)
        }
      }
    }
  }

  test("allFilesInCRC validation during checkpoint must be opposite of per-commit " +
      "validation") {
    withTempDir { dir =>
      var deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
      withCrcVerificationDisabled {
        write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(10))
        write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(20))
        corruptCRCAddFilesModificationTime(deltaLog, version = 1)

        DeltaLog.clearCache()
        deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        deltaLog.update()

        // Checkpoint should validate Checksum even when per-commit verification is disabled.
        val usageRecords =
          collectUsageLogs("delta.allFilesInCrc.checksumMismatch.differentAllFiles") {
            intercept[IllegalStateException] {
              deltaLog.checkpoint()
            }
          }
        assert(usageRecords.size === 1)
        assert(usageRecords.head.blob.contains("\"context\":" + "\"triggeredFromCheckpoint\""),
          usageRecords.head)

        write(deltaLog, numFiles = 10, expectedFilesInCrc = Some(30))
      }

      // Checkpoint should not validate Checksum when per-commit verification is enabled.
      withCrcVerificationEnabled {
        val usageRecords =
          collectUsageLogs("delta.allFilesInCrc.checksumMismatch.differentAllFiles") {
            deltaLog.checkpoint()
          }
        assert(usageRecords.isEmpty)
      }
    }
  }

  test("allFilesInCrcVerificationForceEnabled works as expected") {
    // Test with the non-UTC force verification conf enabled.
    withSQLConf(
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key ->
          "true") {
      setTimeZone("UTC")
      assert(!Snapshot.allFilesInCrcVerificationForceEnabled(spark))
      setTimeZone("America/Los_Angeles")
      assert(Snapshot.allFilesInCrcVerificationForceEnabled(spark))
    }
    // Test with the non-UTC force verification conf disabled.
    withSQLConf(
        DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_FORCE_VERIFICATION_MODE_FOR_NON_UTC_ENABLED.key ->
          "false") {
      assert(!Snapshot.allFilesInCrcVerificationForceEnabled(spark))
    }
  }
}
