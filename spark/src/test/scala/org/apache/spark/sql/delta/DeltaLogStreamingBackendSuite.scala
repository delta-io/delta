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
import java.sql.Timestamp

import org.apache.spark.sql.delta.sources.{DeltaLogStreamingBackend, DeltaSQLConf}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaLogStreamingBackendSuite
  extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaTableProvider {

  test("each read gets the catalog table again") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      spark.range(1).write.format(writeFormat).save(path)
      spark.range(1).write.format(writeFormat).mode("append").save(path)
      var catalogTableReads = 0
      val backend = new DeltaLogStreamingBackend(
        DeltaLog.forTable(spark, path),
        () => {
          catalogTableReads += 1
          None
        })

      def assertCatalogTableReads(expected: Int)(read: => Any): Unit = {
        val before = catalogTableReads
        read
        assert(catalogTableReads - before == expected)
      }

      // A source can refresh its catalog table while it runs, so a read must not reuse the value
      // that an earlier read got.
      assertCatalogTableReads(1)(backend.update())
      assertCatalogTableReads(1)(backend.update())
      assertCatalogTableReads(1)(backend.getSnapshotAt(0))
      assertCatalogTableReads(1)(backend.getSnapshotToValidateProtocol(0))
      assertCatalogTableReads(1)(backend.getActiveCommitAtTime(
        new Timestamp(System.currentTimeMillis()),
        canReturnLastCommit = true,
        mustBeRecreatable = true,
        canReturnEarliestCommit = false))
      assertCatalogTableReads(1)(backend.getSingleCommitIterator(0, failOnDataLoss = true).toList)
      // These reads have never used the catalog table.
      assertCatalogTableReads(0)(backend.unsafeVolatileSnapshot)
      assertCatalogTableReads(0)(backend.checkVersionExists(
        version = 1, mustBeRecreatable = false, allowOutOfRange = false))
    }
  }

  test("getSingleCommitIterator returns the commits from the start version") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      // Versions 0, 1 and 2.
      (0 until 3).foreach { _ =>
        spark.range(1).write.format(writeFormat).mode("append").save(path)
      }
      val backend = new DeltaLogStreamingBackend(DeltaLog.forTable(spark, path), () => None)

      def versions(startVersion: Long): Seq[Long] =
        backend.getSingleCommitIterator(startVersion, failOnDataLoss = true).map(_.version).toList

      assert(versions(0) === Seq(0, 1, 2))
      assert(versions(1) === Seq(1, 2))
      // A start after the latest version returns no commits.
      assert(versions(3).isEmpty)
    }
  }

  test("getSingleCommitIterator with a missing commit file") {
    withSQLConf(DeltaSQLConf.DELTA_GET_CHANGE_LOG_FILES_FAIL_ON_GAPS_IN_TESTS.key -> "false") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        (0 until 5).foreach { _ =>
          spark.range(1).write.format(writeFormat).mode("append").save(path)
        }
        val deltaLog = DeltaLog.forTable(spark, path)
        deltaLog.checkpoint()
        assert(new File(FileNames.unsafeDeltaFile(deltaLog.logPath, 2).toUri).delete())
        val backend = new DeltaLogStreamingBackend(deltaLog, () => None)

        def versions(failOnDataLoss: Boolean): Seq[Long] =
          backend.getSingleCommitIterator(startVersion = 0, failOnDataLoss).map(_.version).toList

        val e = intercept[DeltaIllegalStateException] {
          versions(failOnDataLoss = true)
        }
        checkError(e, "DELTA_MISSING_FILES_UNEXPECTED_VERSION", "42K03",
          Map("startVersion" -> "2", "earliestVersion" -> "3", "option" -> "failOnDataLoss"))
        assert(versions(failOnDataLoss = false) === Seq(0, 1, 3, 4))
      }
    }
  }
}
