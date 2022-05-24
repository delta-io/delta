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

import java.io.File

import scala.util.Random

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.test.SharedSparkSession

class MultiDimClusteringSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest {

  private lazy val sparkSession = spark
  import sparkSession.implicits._

  test("Negative case - ZOrder clustering expression with zero columns") {
    val ex = intercept[AssertionError] {
      ZOrderClustering.getClusteringExpression(Seq.empty, 20)
    }
    assert(ex.getMessage contains "Cannot do Z-Order clustering by zero columns!")
  }

  test("ZOrder clustering expression with one column") {
    val cluster = ZOrderClustering.getClusteringExpression(Seq(expr("col1")), 20)
    assert(cluster.expr.toString ===
      "cast(interleavebits(rangepartitionid('col1, 20)) as string)")
  }

  test("ZOrder clustering expression with two column") {
    val cluster = ZOrderClustering.getClusteringExpression(Seq(expr("col1"), expr("col2")), 20)
    assert(cluster.expr.toString ===
      "cast(interleavebits(rangepartitionid('col1, 20), rangepartitionid('col2, 20)) as string)")
  }

  test("ensure records with close Z-order values are close in the output") {
    withTempDir { tempDir =>
      withSQLConf(
        MDC_NUM_RANGE_IDS.key -> "4",
        MDC_ADD_NOISE.key -> "false") {
        val data = Seq(
          "a" -> 20, "a" -> 20, // Same Z-order value // Z-Order 0
          "b" -> 20,                                  // Z-Order 1
          "c" -> 30,                                  // Z-Order 2
          "d" -> 70,                                  // Z-Order 3
          "e" -> 90, "e" -> 90, "e" -> 90, // Same Z-order value // Z-Order 5
          "f" -> 200,                                 // Z-Order 8
          "g" -> 10,                                  // Z-Order 6
          "h" -> 20)                                  // Z-Order 7

        // Randomize the data. Use seed for deterministic input.
        val inputDf = new Random(seed = 101).shuffle(data)
            .toDF("c1", "c2")

        // Cluster the data and range partition into four partitions
        val outputDf = MultiDimClustering.cluster(
          inputDf,
          approxNumPartitions = 4,
          colNames = Seq("c1", "c2"))
        outputDf.write.parquet(new File(tempDir, "source").getCanonicalPath)

        // Load the partition 0 and verify that it contains (a, 20), (a, 20), (b, 20)
        checkAnswer(
          Seq("a" -> 20, "a" -> 20, "b" -> 20).toDF("c1", "c2"),
          sparkSession.read.parquet(new File(tempDir, "source/part-00000*").getCanonicalPath))

        // partition 1
        checkAnswer(
          Seq("c" -> 30, "d" -> 70, "e" -> 90, "e" -> 90, "e" -> 90).toDF("c1", "c2"),
          sparkSession.read.parquet(new File(tempDir, "source/part-00001*").getCanonicalPath))

        // partition 2
        checkAnswer(
          Seq("h" -> 20, "g" -> 10).toDF("c1", "c2"),
          sparkSession.read.parquet(new File(tempDir, "source/part-00002*").getCanonicalPath))

        // partition 3
        checkAnswer(
          Seq("f" -> 200).toDF("c1", "c2"),
          sparkSession.read.parquet(new File(tempDir, "source/part-00003*").getCanonicalPath))
      }
    }
  }

  test(s"try clustering with different ranges and noise flag on/off") {
    Seq("true", "false").foreach { addNoise =>
      Seq("20", "100", "200", "1000").foreach { numRanges =>
        withSQLConf(
          MDC_NUM_RANGE_IDS.key -> numRanges,
          MDC_ADD_NOISE.key -> addNoise) {
          val data = Seq.range(0, 100)
          val inputDf = Random.shuffle(data).map(x => (x, x * 113 % 101)).toDF("col1", "col2")
          val outputDf = MultiDimClustering.cluster(
            inputDf,
            approxNumPartitions = 10,
            colNames = Seq("col1", "col2"))
          // Underlying data shouldn't change
          checkAnswer(outputDf, inputDf)
        }
      }
    }
  }
}
