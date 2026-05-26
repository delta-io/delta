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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils, TestsStatistics}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DeltaTaskStatisticsTrackerSuite
    extends QueryTest
    with SharedSparkSession
    with TestsStatistics
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils {

  import testImplicits._

  test("DeltaTaskStatisticsTracker computes stats outside V1 write tracker") {
    withTempDir { dir =>
      val deltaLog = DeltaLog.forTable(spark, dir.getAbsolutePath)

      val data = Seq[java.lang.Integer](1, null, 3)
        .toDF("value")
        .coalesce(1)
      data.write.format("delta").save(dir.getAbsolutePath)
      val snapshot = deltaLog.update()

      val dataRenamed = data.toDF(data.columns: _*)

      val statsCollection = new StatisticsCollection {
        override val spark = DeltaTaskStatisticsTrackerSuite.this.spark
        override def tableSchema: StructType = dataRenamed.schema
        override def outputTableStatsSchema: StructType = dataRenamed.schema
        override def outputAttributeSchema: StructType = dataRenamed.schema
        override val statsColumnSpec = DeltaStatsColumnSpec(
          None,
          Some(
            DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromString(
              DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.defaultValue)
          )
        )
        override def columnMappingMode: DeltaColumnMappingMode = snapshot.columnMappingMode
        override val protocol: Protocol = snapshot.protocol
        override def getDataSkippingStringPrefixLength: Int =
          StatsCollectionUtils.getDataSkippingStringPrefixLength(spark, snapshot.metadata)
      }

      val (statsColExpr, resolvedStatsDataSchema) =
        DeltaWriteStatsUtils.getStatsColExpr(
          spark, dataRenamed.queryExecution.analyzed.output, statsCollection)
      val filePath = new Path(dir.getAbsolutePath, "part-00000-test.parquet").toString
      val tracker = new DeltaTaskStatisticsTracker(
        resolvedStatsDataSchema,
        statsColExpr,
        new Path(dir.getAbsolutePath),
        deltaLog.newDeltaHadoopConf())

      tracker.newFile(filePath)
      dataRenamed.queryExecution.toRdd.collect().foreach { row =>
        tracker.newRow(filePath, row.copy())
      }
      val statsJson = tracker
        .getFinalStats(taskCommitTime = 0L)
        .asInstanceOf[DeltaFileStatistics]
        .stats(new Path(filePath).getName)

      val expectedStatsJson = dataRenamed
        .select(statsCollection.statsCollector)
        .select(to_json($"stats").as[String])
        .collect()
        .head
      assert(statsJson === expectedStatsJson)

      val stats = JsonUtils.fromJson[Map[String, Any]](statsJson)
      assert(stats("numRecords") === 3)
      assert(stats.contains("minValues"))
      assert(stats.contains("maxValues"))
      assert(stats.contains("nullCount"))
    }
  }
}
