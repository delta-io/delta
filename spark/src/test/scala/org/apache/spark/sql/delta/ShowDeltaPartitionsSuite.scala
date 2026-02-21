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
 * with Delta tables through the analysis rule in DeltaAnalysis.
 */
class ShowDeltaPartitionsSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

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

  test("SHOW PARTITIONS on non-partitioned Delta table should fail") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, value DOUBLE)
          |USING delta
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 200.0)")

      val e = intercept[DeltaAnalysisException] {
        sql("SHOW PARTITIONS delta_table")
      }
      checkError(
        e,
        "DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_TABLE",
        parameters = Map("tableName" -> "spark_catalog.default.delta_table"))
    }
  }

  test("SHOW PARTITIONS with partition spec on Delta table") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, year INT, month STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (year, month)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 2024, '01', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 2024, '02', 200.0)")
      sql("INSERT INTO delta_table VALUES (3, 2023, '12', 300.0)")

      checkAnswer(
        sql("SHOW PARTITIONS delta_table PARTITION (year = 2024)"),
        Seq(
          Row(2024, "01"),
          Row(2024, "02")
        )
      )

      checkAnswer(
        sql("SHOW PARTITIONS delta_table PARTITION (year = 2024, month = '02')"),
        Seq(Row(2024, "02"))
      )
    }
  }

  test("SHOW PARTITIONS with invalid partition column name in partition spec") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, year INT, month STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (year, month)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 2024, '01', 100.0)")

      val e = intercept[DeltaAnalysisException] {
        sql("SHOW PARTITIONS delta_table PARTITION (day = 1)")
      }
      checkError(
        e,
        "DELTA_NON_PARTITION_COLUMN_SPECIFIED",
        parameters = Map(
          "columnList" -> "[`day`]",
          "fragment" -> "day=1"))
    }
  }

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

  test("SHOW PARTITIONS on column-mapping-enabled Delta table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath

      sql(
        s"""
           |CREATE TABLE delta.`$path` (id INT, date STRING, value DOUBLE)
           |USING delta
           |PARTITIONED BY (date)
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.minReaderVersion' = '2',
           |  'delta.minWriterVersion' = '5'
           |)
         """.stripMargin)

      sql(s"INSERT INTO delta.`$path` VALUES (1, '2024-01-01', 100.0)")
      sql(s"INSERT INTO delta.`$path` VALUES (2, '2024-01-02', 200.0)")

      checkAnswer(
        sql(s"SHOW PARTITIONS delta.`$path`"),
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

      checkAnswer(
        sql("SHOW PARTITIONS delta_table"),
        Seq(
          Row("cat-1"),
          Row("cat.3"),
          Row("cat_2")
        )
      )
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

      checkAnswer(
        sql("SHOW PARTITIONS delta_table"),
        Seq(
          Row("2024-01-01"),
          Row("2024-01-03")
        )
      )
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

      checkAnswer(
        sql("SHOW PARTITIONS delta_table"),
        Seq(
          Row("__HIVE_DEFAULT_PARTITION__"),
          Row("cat1"),
          Row("cat2")
        )
      )

      checkAnswer(
        sql("SHOW PARTITIONS delta_table PARTITION (category = 'cat1')"),
        Seq(Row("cat1"))
      )
    }
  }

  test("SHOW PARTITIONS on table with numeric partition columns") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, year INT, value DOUBLE)
          |USING delta
          |PARTITIONED BY (year)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 2023, 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 2024, 200.0)")

      checkAnswer(
        sql("SHOW PARTITIONS delta_table"),
        Seq(
          Row(2023),
          Row(2024)
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
