/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that DML commands work on tables that have a data column whose name conflicts with the
 * file source metadata column (`_metadata`).
 *
 * Delta supports such tables (see RowIdSuite "Base Row IDs can be read with conflicting metadata
 * column name"). When a data column named `_metadata` exists, Spark renames the file source
 * metadata column, and a plain `col("_metadata.file_path")` lookup resolves to the *user* column
 * instead. Commands must therefore look the metadata column up by its logical name.
 */
class DMLWithConflictingMetadataColumnSuite
  extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  /** Writes 2 files so that the commands have to identify which file(s) to rewrite. */
  private def writeTableWithMetadataColumn(path: String): Unit = {
    spark.range(start = 0, end = 5).toDF("_metadata")
      .withColumn("value", col("_metadata") * 10)
      .repartition(1)
      .write.format("delta").save(path)
    spark.range(start = 5, end = 10).toDF("_metadata")
      .withColumn("value", col("_metadata") * 10)
      .repartition(1)
      .write.format("delta").mode("append").save(path)
  }

  // Deletion vectors take a different code path, so cover the classic path explicitly.
  private def withoutDeletionVectors(thunk: => Unit): Unit =
    withSQLConf(DeltaSQLConf.DELETE_USE_PERSISTENT_DELETION_VECTORS.key -> "false",
      DeltaSQLConf.UPDATE_USE_PERSISTENT_DELETION_VECTORS.key -> "false") {
      thunk
    }

  test("UPDATE on a table with a conflicting _metadata column") {
    withoutDeletionVectors {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        writeTableWithMetadataColumn(path)

        sql(s"UPDATE delta.`$path` SET value = -1 WHERE _metadata = 3")

        checkAnswer(
          spark.read.format("delta").load(path).select("_metadata", "value"),
          (0 until 10).map(i => Row(i, if (i == 3) -1 else i * 10)))
      }
    }
  }

  test("DELETE on a table with a conflicting _metadata column") {
    withoutDeletionVectors {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        writeTableWithMetadataColumn(path)

        sql(s"DELETE FROM delta.`$path` WHERE _metadata = 3")

        checkAnswer(
          spark.read.format("delta").load(path).select("_metadata", "value"),
          (0 until 10).filter(_ != 3).map(i => Row(i, i * 10)))
      }
    }
  }

  test("MERGE on a table with a conflicting _metadata column") {
    import testImplicits._
    withoutDeletionVectors {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        writeTableWithMetadataColumn(path)

        withTempView("source") {
          Seq((3L, -1L), (100L, 1000L)).toDF("_metadata", "value")
            .createOrReplaceTempView("source")

          sql(
            s"""MERGE INTO delta.`$path` t
               |USING source s ON t._metadata = s._metadata
               |WHEN MATCHED THEN UPDATE SET t.value = s.value
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        }

        checkAnswer(
          spark.read.format("delta").load(path).select("_metadata", "value"),
          (0 until 10).map(i => Row(i, if (i == 3) -1 else i * 10)) :+ Row(100, 1000))
      }
    }
  }

  test("CONVERT TO DELTA with stats on a table with a conflicting _metadata column") {
    withTempDir { dir =>
      val path = new java.io.File(dir, "parquet_table").getAbsolutePath
      spark.range(start = 0, end = 10).toDF("_metadata")
        .withColumn("value", col("_metadata") * 10)
        .repartition(2)
        .write.format("parquet").save(path)

      sql(s"CONVERT TO DELTA parquet.`$path`")

      checkAnswer(
        spark.read.format("delta").load(path).select("_metadata", "value"),
        (0 until 10).map(i => Row(i, i * 10)))
    }
  }
}
