/*
 * Copyright 2019 Databricks, Inc.
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

import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class DeltaSuiteOSS extends QueryTest
  with SharedSQLContext {
  import testImplicits._

  test("append then read") {
    val tempDir = Utils.createTempDir()
    Seq(1).toDF().write.format("delta").save(tempDir.toString)
    Seq(2, 3).toDF().write.format("delta").mode("append").save(tempDir.toString)

    def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
    checkAnswer(data, Row(1) :: Row(2) :: Row(3) :: Nil)

    // append more
    Seq(4, 5, 6).toDF().write.format("delta").mode("append").save(tempDir.toString)
    checkAnswer(data.toDF(), Row(1) :: Row(2) :: Row(3) :: Row(4) :: Row(5) :: Row(6) :: Nil)
  }

  test("partitioned append - nulls") {
    val tempDir = Utils.createTempDir()
    Seq(Some(1), None).toDF()
      .withColumn("is_odd", $"value" % 2 === 1)
      .write
      .format("delta")
      .partitionBy("is_odd")
      .save(tempDir.toString)

    val df = spark.read.format("delta").load(tempDir.toString)

    // Verify the correct partitioning schema is picked up
    val hadoopdFsRelations = df.queryExecution.analyzed.collect {
      case LogicalRelation(baseRelation, _, _, _) if
      baseRelation.isInstanceOf[HadoopFsRelation] =>
        baseRelation.asInstanceOf[HadoopFsRelation]
    }
    assert(hadoopdFsRelations.size === 1)
    assert(hadoopdFsRelations.head.partitionSchema.exists(_.name == "is_odd"))
    assert(hadoopdFsRelations.head.dataSchema.exists(_.name == "value"))

    checkAnswer(df.where("is_odd = true"), Row(1, true) :: Nil)
    checkAnswer(df.where("is_odd IS NULL"), Row(null, null) :: Nil)
  }

  test("handle partition filters and data filters") {
    withTempDir { inputDir =>
      val testPath = inputDir.getCanonicalPath
      spark.range(10)
        .map(_.toInt)
        .withColumn("part", $"value" % 2)
        .write
        .format("delta")
        .partitionBy("part")
        .mode("append")
        .save(testPath)

      val ds = spark.read.format("delta").load(testPath).as[(Int, Int)]
      // partition filter
      checkDatasetUnorderly(
        ds.where("part = 1"),
        (1 -> 1), (3 -> 1), (5 -> 1), (7 -> 1), (9 -> 1))
      checkDatasetUnorderly(
        ds.where("part = 0"),
        (0 -> 0), (2 -> 0), (4 -> 0), (6 -> 0), (8 -> 0))
      // data filter
      checkDatasetUnorderly(
        ds.where("value >= 5"),
        (5 -> 1), (6 -> 0), (7 -> 1), (8 -> 0), (9 -> 1))
      checkDatasetUnorderly(
        ds.where("value < 5"),
        (0 -> 0), (1 -> 1), (2 -> 0), (3 -> 1), (4 -> 0))
      // partition filter + data filter
      checkDatasetUnorderly(
        ds.where("part = 1 and value >= 5"),
        (5 -> 1), (7 -> 1), (9 -> 1))
      checkDatasetUnorderly(
        ds.where("part = 1 and value < 5"),
        (1 -> 1), (3 -> 1))
    }
  }

  test("partition column location should not impact table schema") {
    val tableColumns = Seq("c1", "c2")
    for (partitionColumn <- tableColumns) {
      withTempDir { inputDir =>
        val testPath = inputDir.getCanonicalPath
        Seq(1 -> "a", 2 -> "b").toDF(tableColumns: _*)
          .write
          .format("delta")
          .partitionBy(partitionColumn)
          .save(testPath)
        val ds = spark.read.format("delta").load(testPath).as[(Int, String)]
        checkDatasetUnorderly(ds, (1 -> "a"), (2 -> "b"))
      }
    }
  }
}
