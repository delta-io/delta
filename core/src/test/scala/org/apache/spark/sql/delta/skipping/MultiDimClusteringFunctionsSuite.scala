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

package org.apache.spark.sql.delta.skipping

import java.nio.ByteBuffer

import scala.util.Random

import org.apache.spark.sql.delta.skipping.MultiDimClusteringFunctions._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/** Tests for [[MultiDimClusterFunctions]] */
class MultiDimClusteringFunctionsSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest {
  import testImplicits._

  test("range_partition_id(): simple") {
    val numTuples = 20
    val data = 0.to(numTuples - 1)

    for { div <- Seq(1, 2, 4, 5, 10, 20) } {
      checkAnswer(
        Random.shuffle(data).toDF("col")
          .withColumn("rpi", range_partition_id($"col", data.size / div)),
        data.map(i => Row(i, i / div))
      )
    }
  }

  test("range_partition_id(): two columns") {
    val data = Seq("a" -> 10, "b" -> 20, "c" -> 30, "d" -> 40)

    checkAnswer(
      Random.shuffle(data).toDF("c1", "c2")
        .withColumn("r1", range_partition_id($"c1", 2))
        .withColumn("r2", range_partition_id($"c2", 4)),
      Seq(
        Row("a", 10, 0, 0),
        Row("b", 20, 0, 1),
        Row("c", 30, 1, 2),
        Row("d", 40, 1, 3)))

    checkAnswer(
      Random.shuffle(data).toDF("c1", "c2")
        .withColumn("r1", range_partition_id($"c1", 2))
        .distinct
        .withColumn("r2", range_partition_id($"c2", 4)),
      Seq(
        Row("a", 10, 0, 0),
        Row("b", 20, 0, 1),
        Row("c", 30, 1, 2),
        Row("d", 40, 1, 3)))

    checkAnswer(
      Random.shuffle(data).toDF("c1", "c2")
        .where(range_partition_id($"c1", 2) === 0)
        .sort(range_partition_id($"c2", 4)),
      Seq(
        Row("a", 10),
        Row("b", 20)))
  }

  testQuietly("range_partition_id(): corner cases") {
    intercept[IllegalArgumentException] {
      spark.range(10).select(range_partition_id($"id", 0)).show
    }

    intercept[IllegalArgumentException] {
      withSQLConf(SQLConf.RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION.key -> "0") {
        spark.range(10).withColumn("rpi", range_partition_id($"id", 10)).show
      }
    }

    checkAnswer(
      spark.range(1).withColumn("rpi", range_partition_id($"id", 1000)),
      Row(0, 0))

    checkAnswer(
      spark.range(0).withColumn("rpi", range_partition_id($"id", 1000)),
      Seq.empty[Row])

    checkAnswer(
      Seq("a", null, "b", null).toDF("id").withColumn("rpi", range_partition_id($"id", 10)),
      Seq(
        Row("a", 0),
        Row("b", 1),
        Row(null, 0),
        Row(null, 0)))

    checkAnswer(
      spark.range(1).withColumn("id", lit(null)).withColumn("rpi", range_partition_id($"id", 10)),
      Row(null, 0))

    checkAnswer(
      spark.range(1).withColumn("id", lit(Array(1, 2)))
        .withColumn("rpi", range_partition_id($"id", 10)),
      Row(Array(1, 2), 0))
  }

  def intToBinary(x: Int): Array[Byte] = {
    ByteBuffer.allocate(4).putInt(x).array()
  }
}
