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

import org.apache.spark.sql.delta.sources.{DeltaDataSource, DeltaSourceUtils}

import org.apache.spark.sql._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.{ThreadUtils, Utils}

class DeltaSuiteOSS extends QueryTest with SharedSQLContext {
  import testImplicits._

  implicit class BetterWriter[A](dfw: DataFrameWriter[A]) {
    def partitionByHack(columns: String*): DataFrameWriter[A] = {
      dfw.option(
        DeltaSourceUtils.PARTITIONING_COLUMNS_KEY,
        DeltaDataSource.encodePartitioningColumns(columns))
    }
  }

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
      .partitionByHack("is_odd")
      .save(tempDir.toString)

    def data: DataFrame = spark.read.format("delta").load(tempDir.toString)
    checkAnswer(data.toDF().where("is_odd = true"), Row(1, true) :: Nil)
    checkAnswer(data.toDF().where("is_odd IS NULL"), Row(null, null) :: Nil)
  }
}
