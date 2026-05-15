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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.catalog.TruncateMetric
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class TruncateTableMetricsSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private final val numPartitionsToTest: Seq[Int] = Seq(1, 2, 4, 8, 16, 32, 64)
  private final val testTable: String = "t"

  private def forEachTruncateTarget(numPartitions: Int)(action: String => Unit): Unit =
    withTable(testTable) {
      spark.range(0L).write.format("delta").saveAsTable(testTable)
      val pathRef = s"delta.`${DeltaLog.forTable(spark, TableIdentifier(testTable)).dataPath}`"
      Seq(testTable, pathRef).foreach { truncateTarget =>
        spark.range(start = 0, end = numPartitions * 10L, step = 1, numPartitions = numPartitions)
          .write.mode("append").format("delta").saveAsTable(testTable)
        action(truncateTarget)
      }
    }

  private def truncateAndCheck(truncateTarget: String, expectedNumRemovedFiles: Long): Unit = {
    spark.sql(s"TRUNCATE TABLE $truncateTarget")
    val metrics = DeltaMetricsUtils.getLastOperationMetrics(testTable)
    assert(metrics.keySet === Set("numRemovedFiles", "executionTimeMs"))
    assert(metrics("numRemovedFiles") === expectedNumRemovedFiles)
    assert(metrics("executionTimeMs") >= 0) // default value is -1 in SQLMetric
  }

  numPartitionsToTest.foreach { numPartitions =>
    test(s"truncate metrics: truncate non-empty table then empty table" +
      s" (numPartitions=$numPartitions)") {
      withSQLConf(
        DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED.key -> "true",
        DeltaSQLConf.DELTA_SKIP_RECORDING_EMPTY_COMMITS.key -> "false"
      ) {
        forEachTruncateTarget(numPartitions) { truncateTarget =>
          truncateAndCheck(truncateTarget, numPartitions)
          truncateAndCheck(truncateTarget, 0)
        }
      }
    }

    test(s"truncate emits delta.dml.truncate.stats usage log event" +
      s" (numPartitions=$numPartitions)") {
      forEachTruncateTarget(numPartitions) { truncateTarget =>
        val events = DeltaTestUtils.collectUsageLogs("delta.dml.truncate.stats") {
          spark.sql(s"TRUNCATE TABLE $truncateTarget")
        }
        assert(events.size == 1)
        val truncateMetric = JsonUtils.fromJson[TruncateMetric](events.head.blob)
        assert(truncateMetric.numRemovedFiles === numPartitions)
        assert(truncateMetric.commitVersion.isDefined)
      }
    }
  }
}
