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

import java.math.{BigDecimal => JBigDecimal}
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for Delta's SHOW PARTITIONS support on Delta tables.
 *
 * This suite verifies that DeltaAnalysis rewrites SHOW PARTITIONS on Delta tables to
 * ShowDeltaPartitionsCommand, which returns typed partition values from Delta metadata.
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

      checkAnswer(
        sql("SHOW PARTITIONS delta_table PARTITION (year = 2025)"),
        Seq.empty
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
        "DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_COLUMN",
        parameters = Map("badCols" -> "[day]"))
    }
  }

  test("SHOW PARTITIONS deduplicates partitions written across multiple inserts") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (id INT, date STRING, value DOUBLE)
          |USING delta
          |PARTITIONED BY (date)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, '2024-01-01', 100.0)")
      sql("INSERT INTO delta_table VALUES (2, '2024-01-01', 200.0)")
      sql("INSERT INTO delta_table VALUES (3, '2024-01-02', 300.0)")

      checkAnswer(
        sql("SHOW PARTITIONS delta_table"),
        Seq(
          Row("2024-01-01"),
          Row("2024-01-02")
        )
      )
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
          |CREATE TABLE delta_table (id INT, category STRING, part INT, value DOUBLE)
          |USING delta
          |PARTITIONED BY (category, part)
        """.stripMargin)

      sql("INSERT INTO delta_table VALUES (1, 'cat1', 1, 100.0)")
      sql("INSERT INTO delta_table VALUES (2, NULL, 2, 200.0)")
      sql("INSERT INTO delta_table VALUES (3, 'cat2', NULL, 300.0)")

      checkAnswer(
        sql("SHOW PARTITIONS delta_table"),
        Seq(
          Row("cat1", 1),
          Row("cat2", null),
          Row(null, 2)
        )
      )

      checkAnswer(
        sql("SHOW PARTITIONS delta_table PARTITION (category = 'cat1', part = 1)"),
        Seq(Row("cat1", 1))
      )

      checkAnswer(
        sql("SHOW PARTITIONS delta_table PARTITION (part = null)"),
        Seq(Row("cat2", null))
      )

      checkAnswer(
        sql("SHOW PARTITIONS delta_table PARTITION (category = null)"),
        Seq(Row(null, 2))
      )
    }
  }

  test("SHOW PARTITIONS preserves typed partition values for DATE, TIMESTAMP, and DECIMAL") {
    withTable("delta_table") {
      sql(
        """
          |CREATE TABLE delta_table (
          |  id INT,
          |  part_date DATE,
          |  part_ts TIMESTAMP,
          |  part_decimal DECIMAL(10, 2),
          |  value DOUBLE)
          |USING delta
          |PARTITIONED BY (part_date, part_ts, part_decimal)
        """.stripMargin)

      sql(
        """
          |INSERT INTO delta_table VALUES
          |(1, DATE '2024-01-02', TIMESTAMP '2024-01-02 03:04:05', CAST(123.45 AS DECIMAL(10, 2)),
          | 100.0)
        """.stripMargin)

      val result = sql("SHOW PARTITIONS delta_table").collect()
      assert(result.length === 1)

      val row = result.head
      assert(row.get(0).isInstanceOf[Date], s"Expected DATE partition value, got ${row.get(0)}")
      assert(
        row.get(1).isInstanceOf[Timestamp],
        s"Expected TIMESTAMP partition value, got ${row.get(1)}")
      assert(
        row.get(2).isInstanceOf[JBigDecimal],
        s"Expected DECIMAL partition value, got ${row.get(2)}")

      assert(row === Row(
        Date.valueOf("2024-01-02"),
        Timestamp.valueOf("2024-01-02 03:04:05"),
        new JBigDecimal("123.45")))
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

      sql("INSERT INTO delta_table VALUES (1, 10, 100.0)")
      sql("INSERT INTO delta_table VALUES (2, 2, 200.0)")
      sql("INSERT INTO delta_table VALUES (3, 20, 300.0)")

      checkAnswer(
        sql("SHOW PARTITIONS delta_table"),
        Seq(Row(2), Row(10), Row(20))
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
