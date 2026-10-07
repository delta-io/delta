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

package io.delta.spark.internal.v2.read

import java.io.File

import org.apache.spark.sql.Row

private[read] trait DeltaV2ReadE2ETests {
  self: DeltaV2ScanE2ETestUtils =>

  test("plain catalog read preserves the requested schema and rows") {
    withDeltaTable("v2_scan_e2e_plain") {
      checkRead("plain catalog read")(sql("SELECT id, value FROM v2_scan_e2e_plain")) { df =>
        assert(df.schema.fieldNames.toSeq == Seq("id", "value"))
        checkAnswer(df, Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
      }
    }
  }

  test("partition-filtered read returns matching partition rows") {
    withTable("v2_scan_e2e_partitioned") {
      withV1Mode {
        sql(
          s"CREATE TABLE v2_scan_e2e_partitioned (id LONG, part STRING) " +
            s"USING $tableProvider PARTITIONED BY (part)")
        sql("INSERT INTO v2_scan_e2e_partitioned VALUES (1, 'x'), (2, 'y'), (3, 'x')")
      }
      checkRead("partition-filtered read") {
        sql("SELECT id, part FROM v2_scan_e2e_partitioned WHERE part = 'x'")
      } { df =>
        checkAnswer(df, Seq(Row(1L, "x"), Row(3L, "x")))
      }
    }
  }

  test("projection with a residual data filter returns matching rows") {
    withDeltaTable("v2_scan_e2e_projection") {
      checkRead("projected residual-filtered read") {
        sql("SELECT value FROM v2_scan_e2e_projection WHERE id > 1")
      } { df =>
        checkAnswer(df, Seq(Row("b"), Row("c")))
      }
    }
  }

  test("a deletion-vector-capable table without selected vectors preserves live rows") {
    withDeltaTable(
        "v2_scan_e2e_dv_unused",
        "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')") {
      checkRead("deletion-vector-capable read without selected vectors") {
        sql("SELECT id, value FROM v2_scan_e2e_dv_unused")
      } { df =>
        checkAnswer(df, Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
      }
    }
  }

  test("a selected deletion vector preserves only live rows") {
    val table = "v2_scan_e2e_dv"
    withDeltaTable(table, "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')") {
      withV1Mode {
        sql(s"DELETE FROM $table WHERE id = 2")
      }
      val query = s"SELECT id, value FROM $table"
      assertAnySelectedFileHasDeletionVector(
        sql(query),
        "selected deletion-vector read")
      val df = sql(query)
      checkAnswer(df, Seq(Row(1L, "a"), Row(3L, "c")))
      assertExpectedScan(df, "selected deletion-vector read")
    }
  }

  test("mixed deletion-vector and plain files preserve only live rows") {
    val table = "v2_scan_e2e_dv_mixed"
    withDeltaTable(table, "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')") {
      withV1Mode {
        sql(s"INSERT INTO $table VALUES (4, 'd'), (5, 'e')")
        sql(s"DELETE FROM $table WHERE id = 2")
      }
      val query = s"SELECT id, value FROM $table"
      assertMixedSelectedFileDeletionVectors(
        sql(query),
        "mixed deletion-vector and plain read")
      val df = sql(query)
      checkAnswer(df, Seq(Row(1L, "a"), Row(3L, "c"), Row(4L, "d"), Row(5L, "e")))
      assertExpectedScan(df, "mixed deletion-vector and plain read")
    }
  }

  test("partitioned deletion vectors preserve live rows and partition pruning") {
    val table = "v2_scan_e2e_dv_partitioned"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id LONG, part STRING) USING $tableProvider " +
            "PARTITIONED BY (part) " +
            "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        sql(s"INSERT INTO $table VALUES (1, 'x'), (2, 'x'), (3, 'y'), (4, 'y')")
        sql(s"DELETE FROM $table WHERE id IN (2, 3)")
      }
      val numFiles = fileCount(table)
      assert(numFiles == 2L)
      val query = s"SELECT id, part FROM $table"
      assertAllSelectedFilesHaveDeletionVectors(
        sql(query),
        "partitioned deletion-vector read")
      val df = sql(query)
      checkAnswer(df, Seq(Row(1L, "x"), Row(4L, "y")))
      assertExpectedScan(df, "partitioned deletion-vector read")

      val pruned = sql(s"SELECT id, part FROM $table WHERE part = 'y'")
      checkAnswer(pruned, Seq(Row(4L, "y")))
      assertExpectedScan(pruned, "partitioned deletion-vector read with partition filter")
    }
  }

  test("a second delete on the same file reads through the rewritten vector") {
    val table = "v2_scan_e2e_dv_rewritten"
    withDeltaTable(table, "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')") {
      withV1Mode {
        sql(s"DELETE FROM $table WHERE id = 1")
        sql(s"DELETE FROM $table WHERE id = 2")
      }
      val query = s"SELECT id, value FROM $table"
      assertAnySelectedFileHasDeletionVector(
        sql(query),
        "rewritten deletion-vector read")
      val df = sql(query)
      checkAnswer(df, Seq(Row(3L, "c")))
      assertExpectedScan(df, "rewritten deletion-vector read")
    }
  }

  test("row-tracking protocol and deletion vectors preserve ordinary-column rows") {
    val table = "v2_scan_e2e_dv_row_tracking"
    withDeltaTable(
        table,
        "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true', " +
          "'delta.enableRowTracking' = 'true')") {
      withV1Mode {
        sql(s"DELETE FROM $table WHERE id = 2")
      }
      val query = s"SELECT id, value FROM $table"
      assertAnySelectedFileHasDeletionVector(
        sql(query),
        "row-tracking protocol DV read")
      val df = sql(query)
      checkAnswer(df, Seq(Row(1L, "a"), Row(3L, "c")))
      assertExpectedScan(df, "row-tracking protocol DV read")
    }
  }

  test("a row-tracking table preserves ordinary-column rows") {
    withDeltaTable(
        "v2_scan_e2e_row_tracking",
        "TBLPROPERTIES ('delta.enableRowTracking' = 'true')") {
      checkRead("row-tracking table ordinary-column read") {
        sql("SELECT id, value FROM v2_scan_e2e_row_tracking")
      } { df =>
        checkAnswer(df, Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))
      }
    }
  }

  test("partition column in the middle preserves DDL schema order") {
    withPartitionColumnInMiddleTable("v2_scan_e2e_partition_middle") {
      checkRead("partition column in the middle") {
        sql("SELECT * FROM v2_scan_e2e_partition_middle ORDER BY id")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("id", "part", "col3"))
        checkAnswer(
          df,
          Seq(Row(1L, 10L, 100), Row(2L, 20L, 200), Row(3L, 30L, 300)))
      }
    }
  }

  test("partition column in the middle preserves pruned projections") {
    val table = "v2_scan_e2e_partition_middle_pruning"
    withPartitionColumnInMiddleTable(table) {
      checkRead("reordered partition and data projection") {
        sql(s"SELECT part, id FROM $table ORDER BY id")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("part", "id"))
        checkAnswer(df, Seq(Row(10L, 1L), Row(20L, 2L), Row(30L, 3L)))
      }

      checkRead("partition-only projection") {
        sql(s"SELECT part FROM $table ORDER BY part")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("part"))
        checkAnswer(df, Seq(Row(10L), Row(20L), Row(30L)))
      }

      checkRead("data-only projection") {
        sql(s"SELECT col3 FROM $table ORDER BY col3")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("col3"))
        checkAnswer(df, Seq(Row(100), Row(200), Row(300)))
      }
    }
  }

  test("partition column at the end preserves DDL schema order") {
    val table = "v2_scan_e2e_partition_end"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id LONG, col3 INT, part LONG) USING $tableProvider " +
            "PARTITIONED BY (part)")
        sql(
          s"INSERT INTO $table VALUES " +
            "(1, 100, 10), (2, 200, 20), (3, 300, 30)")
      }
      checkRead("partition column at the end") {
        sql(s"SELECT * FROM $table ORDER BY id")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("id", "col3", "part"))
        checkAnswer(
          df,
          Seq(Row(1L, 100, 10L), Row(2L, 200, 20L), Row(3L, 300, 30L)))
      }
    }
  }

  test("multiple interleaved partition columns preserve DDL schema order") {
    val table = "v2_scan_e2e_multiple_partitions"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (a LONG, p1 STRING, b INT, p2 STRING, c DOUBLE) " +
            s"USING $tableProvider PARTITIONED BY (p2, p1)")
        sql(
          s"INSERT INTO $table VALUES " +
            "(1, 'x', 10, 'y', 1.5), " +
            "(2, 'x', 20, 'z', 2.5), " +
            "(3, 'w', 30, 'y', 3.5)")
      }
      checkRead("multiple interleaved partition columns") {
        sql(s"SELECT * FROM $table ORDER BY a")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("a", "p1", "b", "p2", "c"))
        checkAnswer(
          df,
          Seq(
            Row(1L, "x", 10, "y", 1.5),
            Row(2L, "x", 20, "z", 2.5),
            Row(3L, "w", 30, "y", 3.5)))
      }
    }
  }

  test("deletion vectors with a middle partition column preserve rows and DDL order") {
    val table = "v2_scan_e2e_dv_partition_middle"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id LONG, part LONG, val INT) USING $tableProvider " +
            "PARTITIONED BY (part) " +
            "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
        spark
          .range(2000)
          .selectExpr("id", "id % 2 AS part", "cast(id * 10 AS INT) AS val")
          .write
          .mode("append")
          .insertInto(table)
        sql(s"DELETE FROM $table WHERE id < 100")
      }

      val query = s"SELECT * FROM $table ORDER BY id"
      assertAnySelectedFileHasDeletionVector(
        sql(query),
        "deletion-vector read with a middle partition column")
      val df = sql(query)
      assert(df.schema.fieldNames.toSeq == Seq("id", "part", "val"))
      val rows = df.collect()
      assert(rows.length == 1900, "expected 1900 surviving rows after delete")
      assert(rows.head == Row(100L, 0L, 1000))
      assertExpectedScan(
        df,
        "deletion-vector read with a middle partition column")
    }
  }

  test("deletion vectors resolve against a table location containing a space") {
    withTempDir { dir =>
      val location = new File(dir, "my table").getCanonicalPath
      val table = "v2_scan_e2e_dv_location_with_space"
      withTable(table) {
        withV1Mode {
          sql(
            s"CREATE TABLE $table (id LONG, value STRING) USING $tableProvider " +
              s"LOCATION '$location' " +
              "TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')")
          spark
            .range(1000)
            .selectExpr("id", "cast(id as string) as value")
            .write
            .mode("append")
            .insertInto(table)
          sql(s"DELETE FROM $table WHERE id % 2 = 0")
        }

        val query = s"SELECT id FROM $table"
        assertAnySelectedFileHasDeletionVector(
          sql(query),
          "deletion-vector read from a location containing a space")
        val df = sql(query)
        checkAnswer(df, (1L until 1000L by 2L).map(Row(_)))
        assertExpectedScan(df, "deletion-vector read from a location containing a space")
      }
    }
  }

  test("an empty table read preserves the table schema") {
    val table = "v2_scan_e2e_empty"
    withTable(table) {
      withV1Mode {
        sql(s"CREATE TABLE $table (id INT, name STRING, value DOUBLE) USING $tableProvider")
      }

      checkRead("empty table read")(sql(s"SELECT * FROM $table")) { df =>
        assert(df.schema.fieldNames.toSeq == Seq("id", "name", "value"))
        checkAnswer(df, Seq.empty)
      }
    }
  }

  test("a name-mode column-mapped table preserves logical names and rows") {
    val table = "v2_scan_e2e_column_mapping_name"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id INT, user_name STRING, amount DOUBLE) " +
            s"USING $tableProvider " +
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        sql(s"INSERT INTO $table VALUES (1, 'Alice', 100.0), (2, 'Bob', 200.0)")
      }

      checkRead("name-mode column-mapping read")(sql(s"SELECT * FROM $table ORDER BY id")) {
        df =>
          assert(df.schema.fieldNames.toSeq == Seq("id", "user_name", "amount"))
          checkAnswer(df, Seq(Row(1, "Alice", 100.0), Row(2, "Bob", 200.0)))
      }
    }
  }

  test("an id-mode column-mapped middle partition column preserves DDL order") {
    val table = "v2_scan_e2e_partition_middle_column_mapping_id"
    withPartitionColumnInMiddleTable(
        table,
        "TBLPROPERTIES ('delta.columnMapping.mode' = 'id')") {
      checkRead("id-mode column-mapping read")(sql(s"SELECT * FROM $table ORDER BY id")) {
        df =>
          assert(df.schema.fieldNames.toSeq == Seq("id", "part", "col3"))
          checkAnswer(
            df,
            Seq(Row(1L, 10L, 100), Row(2L, 20L, 200), Row(3L, 30L, 300)))
      }

      checkRead("projected id-mode column-mapping read") {
        sql(s"SELECT part, id FROM $table ORDER BY id")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("part", "id"))
        checkAnswer(df, Seq(Row(10L, 1L), Row(20L, 2L), Row(30L, 3L)))
      }
    }
  }

  test("renamed column-mapped partition and data columns preserve rows") {
    val table = "v2_scan_e2e_partition_middle_after_rename"
    withTable(table) {
      withV1Mode {
        sql(
          s"CREATE TABLE $table (id LONG, part LONG, original_col INT) " +
            s"USING $tableProvider " +
            "PARTITIONED BY (part) " +
            "TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
        sql(s"INSERT INTO $table VALUES (1, 10, 100)")
        sql(s"ALTER TABLE $table RENAME COLUMN original_col TO renamed_col")
        sql(s"INSERT INTO $table VALUES (2, 20, 200)")
        sql(s"ALTER TABLE $table RENAME COLUMN part TO renamed_part")
        sql(s"INSERT INTO $table VALUES (3, 30, 300)")
      }

      checkRead("renamed name-mode column-mapping read") {
        sql(s"SELECT * FROM $table ORDER BY id")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("id", "renamed_part", "renamed_col"))
        checkAnswer(
          df,
          Seq(Row(1L, 10L, 100), Row(2L, 20L, 200), Row(3L, 30L, 300)))
      }

      checkRead("projected renamed column-mapping read") {
        sql(s"SELECT renamed_part, id FROM $table ORDER BY id")
      } { df =>
        assert(df.schema.fieldNames.toSeq == Seq("renamed_part", "id"))
        checkAnswer(df, Seq(Row(10L, 1L), Row(20L, 2L), Row(30L, 3L)))
      }

      checkRead("renamed partition-column filter") {
        sql(s"SELECT id, renamed_col FROM $table WHERE renamed_part = 20")
      } { df =>
        checkAnswer(df, Seq(Row(2L, 200)))
      }
    }
  }
}

/** Covers read, schema, column-mapping, and deletion-vector semantics shared across Delta V2
 * scan paths.
 */
class DeltaV2ReadE2ESuite
  extends DeltaV2ScanE2ETestUtils
  with DeltaV2ReadE2ETests
{
}
