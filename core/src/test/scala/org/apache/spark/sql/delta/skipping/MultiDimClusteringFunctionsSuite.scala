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
      // randomize the order and expect the partition ids assigned correctly in sorted order
      Random.shuffle(data).toDF("c1", "c2")
        .withColumn("r1", range_partition_id($"c1", 2))
        .withColumn("r2", range_partition_id($"c2", 4)),
      Seq(
        // Column c1 has values (a, b, c, d). Splitting this value range into two partitions
        // gets ranges [a, b] and [c, d]. Values in each range map to partition 0 and 1.
        // Similarly column c2 has values (10, 20, 30, 40). Splitting this into four partitions
        // gets ranges [10], [20], [30] and [40] which map to partition ids 0 to 3.
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
    // invalid number of partitions
    val ex1 = intercept[IllegalArgumentException] {
      spark.range(10).select(range_partition_id($"id", 0)).show
    }
    assert(ex1.getMessage contains "expected the number partitions to be greater than zero")

    val ex2 = intercept[IllegalArgumentException] {
      withSQLConf(SQLConf.RANGE_EXCHANGE_SAMPLE_SIZE_PER_PARTITION.key -> "0") {
        spark.range(10).withColumn("rpi", range_partition_id($"id", 10)).show
      }
    }
    assert(ex2.getMessage contains "Sample points per partition must be greater than 0 but found 0")

    // Number of partitions is way more than the cardinality of input column values
    checkAnswer(
      spark.range(1).withColumn("rpi", range_partition_id($"id", 1000)),
      Row(0, 0))

    // compute range_partition_id on a dataframe with zero rows
    checkAnswer(
      spark.range(0).withColumn("rpi", range_partition_id($"id", 1000)),
      Seq.empty[Row])

    // compute range_partition_id on column with null values
    checkAnswer(
      Seq("a", null, "b", null).toDF("id").withColumn("rpi", range_partition_id($"id", 10)),
      Seq(
        Row("a", 0),
        Row("b", 1),
        Row(null, 0),
        Row(null, 0)))

    // compute range_partition_id on column with one value which is null
    checkAnswer(
      spark.range(1).withColumn("id", lit(null)).withColumn("rpi", range_partition_id($"id", 10)),
      Row(null, 0))

    // compute range_partition_id on array type column
    checkAnswer(
      spark.range(1).withColumn("id", lit(Array(1, 2)))
        .withColumn("rpi", range_partition_id($"id", 10)),
      Row(Array(1, 2), 0))
  }

}
