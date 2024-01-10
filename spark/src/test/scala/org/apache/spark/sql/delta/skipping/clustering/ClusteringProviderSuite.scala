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

package org.apache.spark.sql.delta.skipping.clustering

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.actions.SingleAction._
import org.apache.spark.sql.delta.stats.DataSkippingReader
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

class ClusteringProviderSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private def testAddFileWithSnapshotReconstructionHelper(
      prefix: String)(collectFiles: DeltaLog => Seq[AddFile]): Unit = {
    for (checkpointPolicy <- Seq("none", "classic", "v2")) {
      test(s"$prefix - Validate clusteringProvider in snapshot reconstruction, " +
        s"checkpointPolicy = $checkpointPolicy") {
        val file = AddFile(
          path = "path",
          partitionValues = Map.empty,
          size = 1,
          modificationTime = 1,
          dataChange = true,
          clusteringProvider = Some("liquid"))

        withTempDir { dir =>
          val log = DeltaLog.forTable(spark, new Path(dir.getCanonicalPath))
          log.startTransaction(None).commit(Metadata() :: Nil, DeltaOperations.ManualUpdate)
          log.startTransaction(None).commit(file :: Nil, DeltaOperations.ManualUpdate)

          if (checkpointPolicy != "none") {
            spark.sql(s"ALTER TABLE delta.`${dir.getAbsolutePath}` SET TBLPROPERTIES " +
              s"('${DeltaConfigs.CHECKPOINT_POLICY.key}' = '$checkpointPolicy')")
            log.checkpoint(log.update())
            // clear cache to force the snapshot reconstruction.
            DeltaLog.clearCache()
          }
          val files = collectFiles(log)
          assert(files.size === 1)
          assert(files.head.clusteringProvider === Some("liquid"))
        }
      }
    }
  }

  testAddFileWithSnapshotReconstructionHelper("Default snapshot reconstruction") { log =>
    log.update().allFiles.collect()
  }

  testAddFileWithSnapshotReconstructionHelper("AddFile with stats") { log =>
    val statsDF = log.update().withStats.withColumn("stats", DataSkippingReader.nullStringLiteral)
    statsDF.as[AddFile].collect()
  }

}
