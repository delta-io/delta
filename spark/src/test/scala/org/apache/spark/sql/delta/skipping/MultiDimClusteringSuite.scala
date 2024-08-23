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

import java.io.{File, FilenameFilter}

import scala.util.Random

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.sources.DeltaSQLConf._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.ColumnImplicitsShim._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.test.SharedSparkSession

class MultiDimClusteringSuite extends QueryTest
    with SharedSparkSession with DeltaSQLCommandTest {

  private lazy val sparkSession = spark
  // scalastyle:off sparkimplicits
  import sparkSession.implicits._
  // scalastyle:on sparkimplicits

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
          // "c1" -> "c2", // (rangeId_c1, rangeId_c2) -> ZOrder (decimal Z-Order)
          "a" -> 20, "a" -> 20, // (0, 0) -> 0b000000 (0)
          "b" -> 20, // (0, 0) -> 0b000000 (0)
          "c" -> 30, // (1, 1) -> 0b000011 (3)
          "d" -> 70, // (1, 2) -> 0b001011 (3)
          "e" -> 90, "e" -> 90, "e" -> 90, // (1, 2) -> 0b001001 (9)
          "f" -> 200, // (2, 3) -> 0b001110 (14)
          "g" -> 10, // (3, 0) -> 0b000101 (5)
          "h" -> 20) // (3, 0) -> 0b000101 (5)

        // Randomize the data. Use seed for deterministic input.
        val inputDf = new Random(seed = 101).shuffle(data)
            .toDF("c1", "c2")

        // Cluster the data and range partition into four partitions
        val outputDf = MultiDimClustering.cluster(
          inputDf,
          approxNumPartitions = 4,
          colNames = Seq("c1", "c2"),
          curve = "zorder")
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

  test("ensure records with close Hilbert curve values are close in the output") {
    withTempDir { tempDir =>
      withSQLConf(MDC_NUM_RANGE_IDS.key -> "4", MDC_ADD_NOISE.key -> "false") {
        val data = Seq(
          // "c1" -> "c2", // (rangeId_c1, rangeId_c2) -> Decimal Hilbert index
          "a" -> 20, "a" -> 20, // (0, 0) -> 0
          "b" -> 20, // (0, 0) -> 0
          "c" -> 30, // (1, 1) -> 2
          "d" -> 70, // (1, 2) -> 13
          "e" -> 90, "e" -> 90, "e" -> 90, // (1, 2) -> 13
          "f" -> 200, // (2, 3) -> 11
          "g" -> 10, // (3, 0) -> 5
          "h" -> 20) // (3, 0) -> 5

        // Randomize the data. Use seed for deterministic input.
        val inputDf = new Random(seed = 101)
          .shuffle(data)
          .toDF("c1", "c2")

        // Cluster the data and range partition into four partitions
        val outputDf = MultiDimClustering.cluster(
          inputDf,
          approxNumPartitions = 2,
          colNames = Seq("c1", "c2"),
          curve = "hilbert")
        outputDf.write.parquet(new File(tempDir, "source").getCanonicalPath)

        // Load the partition 0 and verify its records.
        checkAnswer(
          Seq("a" -> 20, "a" -> 20, "b" -> 20, "c" -> 30, "g" -> 10, "h" -> 20).toDF("c1", "c2"),
          sparkSession.read.parquet(new File(tempDir, "source/part-00000*").getCanonicalPath)
        )

        // partition 1
        checkAnswer(
          Seq("d" -> 70, "e" -> 90, "e" -> 90, "e" -> 90, "f" -> 200).toDF("c1", "c2"),
          sparkSession.read.parquet(new File(tempDir, "source/part-00001*").getCanonicalPath)
        )
      }
    }
  }

  test("noise is helpful in skew handling") {
    Seq("zorder", "hilbert").foreach { curve =>
      Seq("true", "false").foreach { addNoise =>
        withTempDir { tempDir =>
          withSQLConf(
            MDC_NUM_RANGE_IDS.key -> "4",
            MDC_ADD_NOISE.key -> addNoise) {
            val data = Array.fill(100)(20, 20) // all records have the same values
            val inputDf = data.toSeq.toDF("c1", "c2")

            // Cluster the data and range partition into four partitions
            val outputDf = MultiDimClustering.cluster(
              inputDf,
              approxNumPartitions = 4,
              colNames = Seq("c1", "c2"),
              curve)

            outputDf.write.parquet(new File(tempDir, "source").getCanonicalPath)

            // If there is no noise added, expect only one partition, otherwise four partition
            // as mentioned in the cluster command above.
            val partCount = new File(tempDir, "source").listFiles(new FilenameFilter {
              override def accept(dir: File, name: String): Boolean = {
                name.startsWith("part-0000")
              }
            }).length

            if ("true".equals(addNoise)) {
              assert(4 === partCount, s"Incorrect number of partitions when addNoise=$addNoise")
            } else {
              assert(1 === partCount, s"Incorrect number of partitions when addNoise=$addNoise")
            }
          }
        }
      }
    }
  }

  test(s"try clustering with different ranges and noise flag on/off") {
    Seq("zorder", "hilbert").foreach { curve =>
      Seq("true", "false").foreach { addNoise =>
        Seq("20", "100", "200", "1000").foreach { numRanges =>
          withSQLConf(MDC_NUM_RANGE_IDS.key -> numRanges, MDC_ADD_NOISE.key -> addNoise) {
            val data = Seq.range(0, 100)
            val inputDf = Random.shuffle(data).map(x => (x, x * 113 % 101)).toDF("col1", "col2")
            val outputDf = MultiDimClustering.cluster(
              inputDf,
              approxNumPartitions = 10,
              colNames = Seq("col1", "col2"),
              curve)
            // Underlying data shouldn't change
            checkAnswer(outputDf, inputDf)
          }
        }
      }
    }
  }
}
