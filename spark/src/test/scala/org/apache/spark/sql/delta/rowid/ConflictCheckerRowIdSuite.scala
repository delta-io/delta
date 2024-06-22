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

import scala.concurrent.duration.Duration

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.DeltaTestUtils.createTestAddFile
import org.apache.spark.sql.delta.concurrency.PhaseLockingTestMixin
import org.apache.spark.sql.delta.concurrency.TransactionExecutionTestMixin
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class ConflictCheckerRowIdSuite extends QueryTest
  with SharedSparkSession
  with PhaseLockingTestMixin
  with TransactionExecutionTestMixin
  with RowIdTestUtils {

  protected def appendRows(dir: File, numRows: Int, numFiles: Int): Unit = {
    withRowTrackingEnabled(enabled = true) {
      spark.range(start = 0, end = numRows, step = 1, numPartitions = numFiles)
        .write.format("delta").mode("append").save(dir.getAbsolutePath)
    }
  }

  protected def setIsolationLevel(tableDir: File): Unit = {
    spark.sql(
      s"""ALTER TABLE delta.`${tableDir.getAbsolutePath}`
         |SET TBLPROPERTIES ('${DeltaConfigs.ISOLATION_LEVEL.key}' = '$Serializable')
         |""".stripMargin
    )
  }

  test("concurrent transactions do not assign overlapping row IDs") {
    withTempDir { tempDir =>
      appendRows(tempDir, numRows = 100, numFiles = 1)

      setIsolationLevel(tempDir)

      def txnA(): Array[Row] = {
        appendRows(tempDir, numRows = 1000, numFiles = 2)
        Array.empty
      }

      def txnB(): Array[Row] = {
        appendRows(tempDir, numRows = 1500, numFiles = 3)
        Array.empty
      }

      val (_, futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      val log = DeltaLog.forTable(spark, tempDir)
      assertRowIdsAreValid(log)
    }
  }

  test("re-added files keep their row ids") {
    withTempDir { tempDir =>
      appendRows(tempDir, numRows = 100, numFiles = 3)

      setIsolationLevel(tempDir)

      val log = DeltaLog.forTable(spark, tempDir)
      val filesBefore = log.update().allFiles.collect()
      val baseRowIdsBefore = filesBefore.map(f => f.path -> f.baseRowId.get).toMap

      def txnA(): Array[Row] = {
        log.startTransaction().commit(filesBefore, ManualUpdate)
        Array.empty
      }

      def txnB(): Array[Row] = {
        appendRows(tempDir, numRows = 113, numFiles = 2)
        Array.empty
      }

      val (_, futureA, futureB) = runTxnsWithOrder__A_Start__B__A_End(txnA, txnB)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)

      assertRowIdsAreValid(log)
      val filesAfter = log.update().allFiles.collect()
      val baseRowIdsAfter = filesAfter.map(f => f.path -> f.baseRowId.get).toMap
      filesBefore.foreach { file =>
        assert(baseRowIdsBefore(file.path) === baseRowIdsAfter(file.path))
      }
    }
  }

  test("Re-added files keep their row IDs after conflict with txn not " +
    "updating high watermark") {
    withTempDir { dir =>
      appendRows(dir, numRows = 10, numFiles = 2)
      setIsolationLevel(dir)

      val log = DeltaLog.forTable(spark, dir)
      val filesBefore = log.update().allFiles.collect()
      assert(filesBefore.length === 2)

      // Adds one file that will change the high water mark, and one file that was
      // in the table before.
      def txnA(): Array[Row] = {
        val file1 = createTestAddFile()
        val oldFile = filesBefore.last
        log.startTransaction().commit(Seq(oldFile, file1), ManualUpdate)
        Array.empty
      }

      // Adds another file that was in the table before, so we have a conflict
      // with a txn that does not change the high water mark.
      def txnB(): Array[Row] = {
        log.startTransaction().commit(Seq(filesBefore.head), ManualUpdate)
        Array.empty
      }

      // One more transaction to change the high water mark, which will lead to txnA reassigning
      // its row IDs.
      def txnC(): Array[Row] = {
        appendRows(dir, numRows = 30, numFiles = 3)
        Array.empty
      }

      val (_, futureA, futureB, futureC) = runTxnsWithOrder__A_Start__B__C__A_End(txnA, txnB, txnC)
      ThreadUtils.awaitResult(futureA, Duration.Inf)
      ThreadUtils.awaitResult(futureB, Duration.Inf)
      ThreadUtils.awaitResult(futureC, Duration.Inf)
      assertRowIdsAreValid(log)

      val baseRowIdsAfter = log.update().allFiles.collect().map(f => f.path -> f.baseRowId).toMap
      filesBefore.foreach { file =>
        assert(file.baseRowId === baseRowIdsAfter(file.path))
      }
    }
  }
}

