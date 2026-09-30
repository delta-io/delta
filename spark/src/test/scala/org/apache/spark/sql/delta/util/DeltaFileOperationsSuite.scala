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

package org.apache.spark.sql.delta.util

import java.io.File

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class DeltaFileOperationsSuite extends QueryTest with SharedSparkSession {

  test("REORG footer scan is stable when batching and parallelism are adjusted") {
    // ReorgTableHelper scans a partition's files as `iter.grouped(batchSize).flatMap { batch =>
    // readParquetFootersInParallel(batch, parallelism) }`. Neither the batch size nor the read
    // parallelism may change which footers are read: reading each batch returns exactly that
    // batch's footers, with no drops, duplicates, or cross-batch bleed.
    withTempDir { tempDir =>
      val path = new File(tempDir, "data").getCanonicalPath
      spark.range(0, 100, 1, numPartitions = 7).write.parquet(path)
      // scalastyle:off deltahadoopconfiguration
      val conf = spark.sessionState.newHadoopConf()
      // scalastyle:on deltahadoopconfiguration
      val statuses = new Path(path).getFileSystem(conf).listStatus(new Path(path))
        .filter(_.getPath.getName.endsWith(".parquet")).toList
      assert(statuses.size === 7)
      Seq(1, 2, statuses.size, statuses.size + 5).foreach { batchSize =>
        Seq(1, 4, statuses.size + 2).foreach { parallelism =>
          statuses.grouped(batchSize).foreach { batch =>
            val footers = DeltaFileOperations.readParquetFootersInParallel(
              conf, batch, ignoreCorruptFiles = false, parallelism = parallelism)
            assert(footers.map(_.getFile.getName).sorted === batch.map(_.getPath.getName).sorted,
              s"footers mismatch at batchSize=$batchSize, parallelism=$parallelism")
          }
        }
      }
    }
  }
}
