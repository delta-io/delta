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
}
