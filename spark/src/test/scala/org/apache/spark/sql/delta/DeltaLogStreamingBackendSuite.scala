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

import java.sql.Timestamp

import org.apache.spark.sql.delta.sources.DeltaLogStreamingBackend
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

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
      // These reads have never used the catalog table.
      assertCatalogTableReads(0)(backend.unsafeVolatileSnapshot)
      assertCatalogTableReads(0)(backend.checkVersionExists(
        version = 1, mustBeRecreatable = false, allowOutOfRange = false))
    }
  }
}
