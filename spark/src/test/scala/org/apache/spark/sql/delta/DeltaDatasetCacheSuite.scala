/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import java.util.UUID

import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{DataFrame, functions, QueryTest, Row}
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.storage.StorageLevel

class DeltaDatasetCacheSuite extends QueryTest with DeltaSQLCommandTest {

  test("V1 relation identifies whether its Delta snapshot is version-pinned") {
    val tableName = "delta_time_travel_file_index"
    withConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
      withTable(tableName) {
        spark.range(10).write.format("delta").saveAsTable(tableName)

        assert(!getDeltaLogFileIndex(spark.table(tableName)).isTimeTravel)
        val pinned = spark.read.option("versionAsOf", 0).table(tableName)
        assert(getDeltaLogFileIndex(pinned).isTimeTravel)
      }
    }
  }

  test("append does not recompute a cached version-pinned DataFrame") {
    assume(
      classOf[FileIndex].getMethods.exists(_.getName == "isTimeTravel"),
      "requires Spark with FileIndex.isTimeTravel")

    val tableName = "delta_cached_time_travel"
    withConf(DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
      withTable(tableName) {
        spark.range(10).write.format("delta").saveAsTable(tableName)

        val udfCalls = spark.sparkContext.longAccumulator("cached-time-travel-udf-calls")
        val expensiveUdf = functions.udf { id: Long =>
          udfCalls.add(1L)
          s"$id-${UUID.randomUUID()}"
        }.asNondeterministic()
        val pinned = spark.read
          .option("versionAsOf", 0)
          .table(tableName)
          .withColumn("token", expensiveUdf(functions.col("id")))
          .persist()

        try {
          val tokensBeforeAppend = collectTokens(pinned.orderBy("id").select("token").collect())
          val callsAfterMaterialization = udfCalls.value
          assert(callsAfterMaterialization === 10L)

          spark.range(10, 20).write.format("delta").mode("append").saveAsTable(tableName)

          assert(pinned.storageLevel !== StorageLevel.NONE)
          val tokensAfterAppend = collectTokens(pinned.orderBy("id").select("token").collect())
          assert(udfCalls.value === callsAfterMaterialization)
          assert(tokensAfterAppend === tokensBeforeAppend)
          assert(pinned.count() === 10L)
          assert(spark.table(tableName).count() === 20L)
        } finally {
          pinned.unpersist(blocking = true)
        }
      }
    }
  }

  private def getDeltaLogFileIndex(dataFrame: DataFrame): TahoeLogFileIndex = {
    dataFrame.queryExecution.analyzed.collectFirst {
      case DeltaTable(index: TahoeLogFileIndex) => index
    }.getOrElse(fail("Delta relation does not contain a TahoeLogFileIndex"))
  }

  private def collectTokens(rows: Array[Row]): Seq[String] = rows.map(_.getString(0)).toSeq
}
