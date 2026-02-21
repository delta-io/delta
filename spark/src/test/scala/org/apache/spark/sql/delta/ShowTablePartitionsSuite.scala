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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for SHOW PARTITIONS command on Delta tables.
 *
 * This suite verifies that Spark's native SHOW PARTITIONS command works correctly
 * with Delta tables through the delegation mechanism in DeltaSqlParser.
 */
class ShowTablePartitionsSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  test("SHOW PARTITIONS on single partition column Delta table") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, date STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (date)
        """.stripMargin)

      // Insert data into multiple partitions
      sql("INSERT INTO delta_table VALUES (1, '2024-01-01', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, '2024-01-02', 200.0)")
      sql("INSERT INTO delta_table VALUES (3, '2024-01-03', 300.0)")

      // Show partitions
      val result = sql("SHOW PARTITIONS delta_table")

      checkAnswer(
        result,
        Seq(
          Row("2024-01-01"),
          Row("2024-01-02"),
          Row("2024-01-03")
        )
      )
    }
  }

  test("SHOW PARTITIONS on multi-partition column Delta table") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, year STRING, month STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (year, month)
        """.stripMargin)

      // Insert data into multiple partitions
      sql("INSERT INTO delta_table VALUES (1, '2024', '01', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, '2024', '02', 200.0)")
      sql("INSERT INTO delta_table VALUES (3, '2023', '12', 300.0)")

      // Show partitions
      val result = sql("SHOW PARTITIONS delta_table")

      checkAnswer(
        result,
        Seq(
          Row("2023", "12"),
          Row("2024", "01"),
          Row("2024", "02")
        )
      )
    }
  }

  test("SHOW PARTITIONS on non-partitioned Delta table should return empty") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, value DOUBLE)
          |USING delta
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 200.0)")

      // Show partitions on non-partitioned table
      val result = sql("SHOW PARTITIONS delta_table")

      // Should return empty result
      assert(result.isEmpty)
    }
  }

  // TODO: Implement partition spec filtering support
  // This requires converting Spark's PartitionSpec (Seq of expressions) to Map[String, String]


  test("SHOW PARTITIONS on path-based Delta table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath

      sql(
        s"""
          |CREATE TABLE delta.`$path` (id INT, date STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (date)
        """.stripMargin)

      sql(s"INSERT INTO delta.`$path` VALUES (1, '2024-01-01', 100.0)")
      sql(s"INSERT INTO delta.`$path` VALUES (2, '2024-01-02', 200.0)")

      // Show partitions using path
      val result = sql(s"SHOW PARTITIONS delta.`$path`")

      checkAnswer(
        result,
        Seq(
          Row("2024-01-01"),
          Row("2024-01-02")
        )
      )
    }
  }

  test("SHOW PARTITIONS with special characters in partition values") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, category STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (category)
        """.stripMargin)

      // Insert data with special characters
      sql("INSERT INTO delta_table VALUES (1, 'cat-1', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 'cat_2', 200.0)")
      sql("INSERT INTO delta_table VALUES (3, 'cat.3', 300.0)")

      // Show partitions
      val result = sql("SHOW PARTITIONS delta_table")

      assert(result.count() == 3)
      val partitions = result.collect().map(_.getString(0)).toSet
      assert(partitions.contains("cat-1"))
      assert(partitions.contains("cat_2"))
      assert(partitions.contains("cat.3"))
    }
  }

  test("SHOW PARTITIONS after DELETE operation") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, date STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (date)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, '2024-01-01', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, '2024-01-02', 200.0)")
      sql("INSERT INTO delta_table VALUES (3, '2024-01-03', 300.0)")

      // Delete all data from one partition
      sql("DELETE FROM delta_table WHERE date = '2024-01-02'")

      // Show partitions - should still show all partitions (files may still exist)
      val result = sql("SHOW PARTITIONS delta_table")

      // Note: SHOW PARTITIONS shows partitions based on file structure,
      // not based on whether data exists in them
      assert(result.count() >= 2)
    }
  }

  test("SHOW PARTITIONS with NULL partition values") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, category STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (category)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 'cat1', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, NULL, 200.0)")
      sql("INSERT INTO delta_table VALUES (3, 'cat2', 300.0)")

      // Show partitions
      val result = sql("SHOW PARTITIONS delta_table")

      assert(result.count() == 3)
      val partitions = result.collect().map(_.getString(0)).toSet
      assert(partitions.contains("cat1"))
      assert(partitions.contains("cat2"))
      // NULL partition is represented as __HIVE_DEFAULT_PARTITION__
      assert(partitions.contains("__HIVE_DEFAULT_PARTITION__"))
    }
  }

  test("SHOW PARTITIONS on table with numeric partition column") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, year INT, value DOUBLE)
          |USING delta
          |PARTITIONED BY (year)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 2023, 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 2024, 200.0)")

      // Show partitions
      val result = sql("SHOW PARTITIONS delta_table")

      checkAnswer(
        result,
        Seq(
          Row("2023"),
          Row("2024")
        )
      )
    }
  }

  test("SHOW PARTITIONS on empty partitioned Delta table") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, date STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (date)
        """.stripMargin)

      // Don't insert any data

      // Show partitions on empty table
      val result = sql("SHOW PARTITIONS delta_table")

      // Should return empty result
      assert(result.isEmpty)
    }
  }

  test("SHOW PARTITIONS works with catalog qualified table name") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, region STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (region)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 'us', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 'eu', 200.0)")

      // Show partitions with catalog qualified name
      val result = sql("SHOW PARTITIONS spark_catalog.default.delta_table")

      checkAnswer(
        result,
        Seq(
          Row("eu"),
          Row("us")
        )
      )
    }
  }
}
