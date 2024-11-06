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

import org.apache.spark.sql.delta.DeltaTestUtils._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.FileNames
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

class ChecksumSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

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

  test("Incremental checksums: post commit snapshot should have a checksum " +
      "without triggering state reconstruction") {
    for (incrementalCommitEnabled <- BOOLEAN_DOMAIN) {
      withSQLConf(
        DeltaSQLConf.DELTA_WRITE_CHECKSUM_ENABLED.key -> "false",
        DeltaSQLConf.INCREMENTAL_COMMIT_ENABLED.key -> incrementalCommitEnabled.toString) {
        withTempDir { tempDir =>
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
}
