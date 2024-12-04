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
import java.util.TimeZone

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta.DeltaTestUtils._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class ChecksumSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaTestUtilsBase
  with DeltaSQLCommandTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.INCREMENTAL_COMMIT_FORCE_VERIFY_IN_TESTS, false)

  test(s"A Checksum should be written after every commit when " +
    s"${DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key} is true") {
    def testChecksumFile(writeChecksumEnabled: Boolean): Unit = {
      withTempDir { tempDir =>
        withSQLConf(
          DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> writeChecksumEnabled.toString) {
          def checksumExists(deltaLog: DeltaLog, version: Long): Boolean = {
            val checksumFile = new File(FileNames.checksumFile(deltaLog.logPath, version).toUri)
            checksumFile.exists()
          }

          // Setup the log
          val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
          val initialTxn = log.startTransaction()
          initialTxn.commitManually(createTestAddFile())

          // Commit the txn
          val txn = log.startTransaction()
          val txnCommitVersion = txn.commit(Seq.empty, DeltaOperations.Truncate())
          assert(checksumExists(log, txnCommitVersion) == writeChecksumEnabled)
        }
      }
    }

    testChecksumFile(writeChecksumEnabled = true)
    testChecksumFile(writeChecksumEnabled = false)
  }

  private def setTimeZone(timeZone: String): Unit = {
    spark.sql(s"SET spark.sql.session.timeZone = $timeZone")
    TimeZone.setDefault(TimeZone.getTimeZone(timeZone))
  }

  test("Incremental checksums: post commit snapshot should have a checksum " +
      "without triggering state reconstruction") {
    for (incrementalCommitEnabled <- BOOLEAN_DOMAIN) {
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false",
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> incrementalCommitEnabled.toString
      ) {
        withTempDir { tempDir =>
          // Set the timezone to UTC to avoid triggering force verification of all files in CRC
          // for non utc environments.
          setTimeZone("UTC")
          val df = spark.range(1)
          df.write.format("delta").mode("append").save(tempDir.getCanonicalPath)
          val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
          log
            .startTransaction()
            .commit(Seq(createTestAddFile()), DeltaOperations.Write(SaveMode.Append))
          val postCommitSnapshot = log.snapshot
          assert(postCommitSnapshot.version == 1)
          assert(!postCommitSnapshot.stateReconstructionTriggered)
          assert(postCommitSnapshot.checksumOpt.isDefined == incrementalCommitEnabled)

          postCommitSnapshot.checksumOpt.foreach { incrementalChecksum =>
            val checksumFromStateReconstruction = postCommitSnapshot.computeChecksum
            assert(incrementalChecksum.copy(txnId = None) == checksumFromStateReconstruction)
          }
        }
      }
    }
  }

  def testIncrementalChecksumWrites(tableMutationOperation: String => Unit): Unit = {
    withTempDir { tempDir =>
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key ->"true") {
        val df = spark.range(10).withColumn("id2", col("id") % 2)
        df.write
          .format("delta")
          .partitionBy("id")
          .mode("append")
          .save(tempDir.getCanonicalPath)

        tableMutationOperation(tempDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        val checksumOpt = log.snapshot.checksumOpt
        assert(checksumOpt.isDefined)
        val checksum = checksumOpt.get
        val computedChecksum = log.snapshot.computeChecksum
        assert(checksum.copy(txnId = None) === computedChecksum)
      }
    }
  }

  test("Incremental checksums: INSERT") {
    testIncrementalChecksumWrites { tablePath =>
      sql(s"INSERT INTO delta.`$tablePath` SELECT *, 1 FROM range(10, 20)")
    }
  }

  test("Incremental checksums: UPDATE") {
    testIncrementalChecksumWrites { tablePath =>
      sql(s"UPDATE delta.`$tablePath` SET id2 = id + 1 WHERE id % 2 = 0")
    }
  }

  test("Incremental checksums: DELETE") {
    testIncrementalChecksumWrites { tablePath =>
      sql(s"DELETE FROM delta.`$tablePath` WHERE id % 2 = 0")
    }
  }

  test("Checksum validation should happen on checkpoint") {
    withSQLConf(
      DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "true",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      // Disabled for this test because with it enabled, a corrupted Protocol
      // or Metadata will trigger a failure earlier than the full validation.
      DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED.key -> "false"
    ) {
      withTempDir { tempDir =>
        spark.range(10).write.format("delta").save(tempDir.getCanonicalPath)
        spark.range(1)
          .write
          .format("delta")
          .mode("append")
          .save(tempDir.getCanonicalPath)
        val log = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        val checksumOpt = log.readChecksum(1)
        assert(checksumOpt.isDefined)
        val checksum = checksumOpt.get
        // Corrupt the checksum file.
        val corruptedChecksum = checksum.copy(
          protocol =
            checksum.protocol.copy(minReaderVersion = checksum.protocol.minReaderVersion + 1),
          metadata = checksum.metadata.copy(description = "corrupted"),
          numProtocol = 2,
          numMetadata = 2,
          tableSizeBytes = checksum.tableSizeBytes + 1,
          numFiles = checksum.numFiles + 1)
        val corruptedChecksumJson = JsonUtils.toJson(corruptedChecksum)
        log.store.write(
          FileNames.checksumFile(log.logPath, 1),
          Seq(corruptedChecksumJson).toIterator,
          overwrite = true)
        DeltaLog.clearCache()
        val log2 = DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        val usageLogs = Log4jUsageLogger.track {
          intercept[DeltaIllegalStateException] {
            log2.checkpoint()
          }
        }
        val validationFailureLogs = filterUsageRecords(usageLogs, "delta.checksum.invalid")
        assert(validationFailureLogs.size == 1)
        validationFailureLogs.foreach { log =>
          val usageLogBlob = JsonUtils.fromJson[Map[String, Any]](log.blob)
          val mismatchingFieldsOpt = usageLogBlob.get("mismatchingFields")
          assert(mismatchingFieldsOpt.isDefined)
          val mismatchingFieldsSet = mismatchingFieldsOpt.get.asInstanceOf[Seq[String]].toSet
          val expectedMismatchingFields = Set(
            "protocol",
            "metadata",
            "numOfProtocol",
            "numOfMetadata",
            "tableSizeBytes",
            "numFiles"
          )
          assert(mismatchingFieldsSet === expectedMismatchingFields)
        }
      }
    }
  }

  test("incremental commit verify mode should always detect invalid .crc") {
    withSQLConf(
      DeltaSQLConf.INCREMENTAL_COMMIT_VERIFY.key -> "true",
      DeltaSQLConf.DELTA_CHECKSUM_MISMATCH_IS_FATAL.key -> "false",
      DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> "true",
      DeltaSQLConf.DELTA_ALL_FILES_IN_CRC_ENABLED.key -> "false",
      DeltaSQLConf.USE_PROTOCOL_AND_METADATA_FROM_CHECKSUM_ENABLED.key -> "true"
    ) {
      withTempDir { tempDir =>
        import testImplicits._
        val numAddFiles = 10

        // Procedure:
        // 1. Populate the table with several files
        // 2. Start a new transaction
        // 3. Intentionally try to commit the same files again
        //    a. Silently duplicated AddFile breaks incremental commit invariants
        //    b. Incrementally computed .crc is thus invalid
        //    c. Incremental commit verification should detect the "invalid" .crc
        //    d. Post-commit snapshot should have empty checksumOpt
        // 4. Clear the delta log cache so we pick up the correct (fallback) .crc
        // 5. Create a new snapshot and manually validate the .crc

        val files = (1 to numAddFiles).map(i => createTestAddFile(encodedPath = i.toString))
        DeltaLog.forTable(spark, tempDir).startTransaction().commitWriteAppend(files: _*)

        val log = DeltaLog.forTable(spark, tempDir)
        val txn = log.startTransaction()
        val expected =
          s"""Table size (bytes) - Expected: ${2*numAddFiles} Computed: $numAddFiles
             |Number of files - Expected: ${2*numAddFiles} Computed: $numAddFiles
          """.stripMargin.trim

        val Seq(corruptionReport) = collectUsageLogs("delta.checksum.invalid") {
          // Intentionally re-add the same files, without identifying them as duplicates
          txn.commitWriteAppend(files: _*)
        }
        val error = JsonUtils.fromJson[Map[String, Any]](corruptionReport.blob).get("error")
        assert(error.exists(_.asInstanceOf[String].contains(expected)))
        assert(log.snapshot.checksumOpt.isEmpty)
      }
    }
  }
}
