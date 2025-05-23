/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults

import io.delta.kernel.Table
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestUtils}
import io.delta.kernel.expressions.Literal.ofInt
import io.delta.kernel.utils.PartitionUtils

import org.apache.spark.sql.functions.{col => sparkCol}
import org.scalatest.funsuite.AnyFunSuite

class PartitionUtilsSuite extends AnyFunSuite with TestUtils with ExpressionTestUtils {

  private def createTableWithPartCols(path: String): Unit = {
    spark.range(100)
      .withColumn("part1", sparkCol("id") % 2)
      .withColumn("part2", sparkCol("id") % 5)
      .withColumn("col1", sparkCol("id"))
      .withColumn("col2", sparkCol("id"))
      .drop("id")
      .write
      .format("delta")
      .partitionBy("part1", "part2")
      .save(path)
  }

  test("partitionExists - input validation") {
    withTempDirAndEngine { (path, engine) =>
      createTableWithPartCols(path)

      val snapshot = Table.forPath(engine, path).getLatestSnapshot(engine)

      {
        val badPredicate = and(
          predicate("=", col("part1"), ofInt(0)),
          predicate("=", col("col1"), ofInt(0)))
        val exMsg = intercept[IllegalArgumentException] {
          PartitionUtils.partitionExists(engine, snapshot, badPredicate)
        }.getMessage
        assert(exMsg.contains("Partition predicate must contain only partition columns"))
      }

      {
        val badPredicate = predicate("=", col("col1"), ofInt(0))
        val exMsg = intercept[IllegalArgumentException] {
          PartitionUtils.partitionExists(engine, snapshot, badPredicate)
        }.getMessage
        assert(exMsg.contains("Partition predicate must contain at least one partition column"))
      }
    }
  }

  test("partitionExists - simple case using latest table snapshot") {
    withTempDirAndEngine { (path, engine) =>
      createTableWithPartCols(path)

      val snapshot = Table.forPath(engine, path).getLatestSnapshot(engine)

      // ===== Simple Cases =====
      {
        val partPredicate = predicate("=", col("part1"), ofInt(1)) // Yes
        assert(PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
      {
        val partPredicate = predicate(">=", col("part1"), ofInt(500)) // No
        assert(!PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
      {
        val partPredicate = predicate("<", col("part1"), ofInt(100)) // Yes
        assert(PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }

      // ===== Conjunction and Disjunction Cases =====
      {
        val partPredicate = and(
          predicate("=", col("part1"), ofInt(0)), // Yes
          predicate("=", col("part2"), ofInt(4)) // Yes
        )
        assert(PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
      {
        val partPredicate = and(
          predicate("=", col("part1"), ofInt(0)), // Yes
          predicate("=", col("part2"), ofInt(500)) // No
        )
        assert(!PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
      {
        val partPredicate = or(
          predicate("=", col("part1"), ofInt(500)), // No
          predicate("=", col("part2"), ofInt(3)) // N/A
        )
        assert(PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
    }
  }

  test("partitionExists - some or all data in partition was removed") {
    withTempDirAndEngine { (path, engine) =>
      createTableWithPartCols(path)
      spark.sql(s"DELETE FROM delta.`$path` WHERE part1 = 0")
      spark.sql(s"DELETE FROM delta.`$path` WHERE part1 = 1 AND part2 = 0")
      spark.sql(s"DELETE FROM delta.`$path` WHERE part1 = 1 AND part2 = 1 AND col1 < 50")

      val snapshot = Table.forPath(engine, path).getLatestSnapshot(engine)

      {
        val partPredicate = predicate("=", col("part1"), ofInt(0)) // No
        assert(!PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
      {
        val partPredicate = and(
          predicate("=", col("part1"), ofInt(1)), // Yes
          predicate("=", col("part2"), ofInt(0)) // No
        )
        assert(!PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
      {
        val partPredicate = and(
          predicate("=", col("part1"), ofInt(1)), // Yes
          predicate("=", col("part2"), ofInt(1)) // Has some data
        )
        assert(PartitionUtils.partitionExists(engine, snapshot, partPredicate))
      }
    }
  }

}
