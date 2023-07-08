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

package org.apache.spark.sql.delta.cdc

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.commands.cdc.CDCReader._
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions.lit

class DeleteCDCSuite extends DeleteSQLSuite {
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")

  protected def testCDCDelete(name: String)(
      initialData: => Dataset[_],
      partitionColumns: Seq[String] = Seq.empty,
      deleteCondition: String,
      expectedData: => Dataset[_],
      expectedChangeDataWithoutVersion: => Dataset[_]
    ): Unit = {
    test(s"CDC - $name") {
      withSQLConf(
          (DeltaConfigs.CHANGE_DATA_FEED.defaultTablePropertyKey, "true")) {
        withTempDir { dir =>
          val path = dir.getAbsolutePath
          initialData.write.format("delta").partitionBy(partitionColumns: _*)
            .save(path)

          executeDelete(s"delta.`$path`", deleteCondition)

          val log = DeltaLog.forTable(spark, dir)
          val version = log.snapshot.version

          checkAnswer(
            spark.read.format("delta").load(path),
            expectedData.toDF())

          val expectedChangeData = expectedChangeDataWithoutVersion
            .withColumn(CDCReader.CDC_COMMIT_VERSION, lit(version))
          // The timestamp is nondeterministic so we drop it when comparing results.
          checkAnswer(
            CDCReader.changesToBatchDF(log, version, version, spark).drop(CDC_COMMIT_TIMESTAMP),
            expectedChangeData)
        }
      }
    }
  }

  testCDCDelete("unconditional")(
    initialData = spark.range(0, 10, step = 1, numPartitions = 3),
    deleteCondition = "",
    expectedData = spark.range(0),
    expectedChangeDataWithoutVersion = spark.range(10)
      .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
  )

  testCDCDelete("conditional covering all rows")(
    initialData = spark.range(0, 10, step = 1, numPartitions = 3),
    deleteCondition = "id < 100",
    expectedData = spark.range(0),
    expectedChangeDataWithoutVersion = spark.range(10)
      .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
  )

  testCDCDelete("two random rows")(
    initialData = spark.range(0, 10, step = 1, numPartitions = 3),
    deleteCondition = "id = 2 OR id = 8",
    expectedData = Seq(0, 1, 3, 4, 5, 6, 7, 9).toDF(),
    expectedChangeDataWithoutVersion = Seq(2, 8).toDF()
      .withColumn(CDC_TYPE_COLUMN_NAME, lit("delete"))
  )

  testCDCDelete("delete unconditionally - partitioned table")(
    initialData = spark.range(0, 100, step = 1, numPartitions = 10)
      .selectExpr("id % 10 as part", "id"),
    partitionColumns = Seq("part"),
    deleteCondition = "",
    expectedData = Seq.empty[(Long, Long)].toDF("part", "id"),
    expectedChangeDataWithoutVersion =
      spark.range(100)
        .selectExpr("id % 10 as part", "id", "'delete' as _change_type")
  )

  testCDCDelete("delete all rows by condition - partitioned table")(
    initialData = spark.range(0, 100, step = 1, numPartitions = 10)
      .selectExpr("id % 10 as part", "id"),
    partitionColumns = Seq("part"),
    deleteCondition = "id < 1000",
    expectedData = Seq.empty[(Long, Long)].toDF("part", "id"),
    expectedChangeDataWithoutVersion =
      spark.range(100)
        .selectExpr("id % 10 as part", "id", "'delete' as _change_type")
  )


  testCDCDelete("partition-optimized delete")(
    initialData = spark.range(0, 100, step = 1, numPartitions = 10)
      .selectExpr("id % 10 as part", "id"),
    partitionColumns = Seq("part"),
    deleteCondition = "part = 3",
    expectedData =
      spark.range(100).selectExpr("id % 10 as part", "id").where("part != 3"),
    expectedChangeDataWithoutVersion =
      Range(0, 10).map(x => x * 10 + 3).toDF("id")
        .selectExpr("3 as part", "id", "'delete' as _change_type"))

}

