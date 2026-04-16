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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.{col, concat, count, lit}
import org.apache.spark.sql.internal.SQLConf

/**
 * Common tests for replaceUsing option via DataFrame APIs (save(), insertInto(),
 * and saveAsTable()).
 */
trait DeltaInsertReplaceUsingDFWriterTests
  extends DeltaInsertReplaceOnOrUsingTestUtils {
  import testImplicits._

  /**
   * Write the sourceDF to the target with replaceUsing.
   */
  protected def writeReplaceUsingDF(
      sourceDF: DataFrame,
      target: String,
      replaceUsingCols: String,
      writeMode: String = "overwrite",
      mergeSchema: Boolean = false,
      options: Map[String, String] = Map.empty): Unit

  // Basic replaceUsing operations

  test("replaceUsing with single column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (2, "source"), (4, "source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") ::
          Row(2, "source") ::
          Row(3, "target") ::
          Row(4, "source") ::
          Nil)
    }
  }

  test("replaceUsing with multiple columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "a", "target"), (1, "b", "target"), (2, "a", "target"))
        .toDF("id", "key", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "a", "source"), (2, "b", "source"), (3, "a", "source"))
          .toDF("id", "key", "value"),
        target = path,
        replaceUsingCols = "id, key")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a", "source") ::
          Row(1, "b", "target") ::
          Row(2, "a", "target") ::
          Row(2, "b", "source") ::
          Row(3, "a", "source") ::
          Nil)
    }
  }

  test("replaceUsing with CDF enabled") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val numRowsPerFile = 10
      val numFiles = 10
      spark.range(0, numFiles * numRowsPerFile, 1, numFiles).toDF("id")
        .write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, true)
        .save(path)

      writeReplaceUsingDF(
        sourceDF = spark.range(0, 15).toDF("id"),
        target = path,
        replaceUsingCols = "id")

      val replaceRowIds = Seq.range(0, 15, 1).toSet
      val preRowIds = Seq.range(0, numFiles * numRowsPerFile).toSet
      val postRowIDs = (preRowIds -- replaceRowIds).toSeq ++ replaceRowIds.toSeq
      checkAnswer(spark.read.format("delta").load(path).select("id"), postRowIDs.toDF("id"))

      val deltaLog = DeltaLog.forTable(spark, path)
      val version = deltaLog.update().version
      val changesDF = CDCReader.changesToBatchDF(deltaLog, version, version, spark)
        .select("id", CDCReader.CDC_TYPE_COLUMN_NAME)
      val expectedAnswerDF =
        replaceRowIds.toSeq.toDF("id")
          .withColumn(CDCReader.CDC_TYPE_COLUMN_NAME, lit(CDCReader.CDC_TYPE_DELETE))
          .union(
            replaceRowIds.toSeq.toDF("id")
              .withColumn(CDCReader.CDC_TYPE_COLUMN_NAME, lit(CDCReader.CDC_TYPE_INSERT)))
      checkAnswer(changesDF, expectedAnswerDF)
    }
  }


  test("replaceUsing with CDF enabled and no matching rows") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, true)
        .save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((3, "source"), (4, "source")).toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(Row(1, "target"), Row(2, "target"), Row(3, "source"), Row(4, "source")))

      val deltaLog = DeltaLog.forTable(spark, path)
      val version = deltaLog.update().version
      val changesDF = CDCReader.changesToBatchDF(deltaLog, version, version, spark)
        .select("id", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      // No deletes because no rows matched, only inserts
      val expectedCDC =
        Seq((3, "source", CDCReader.CDC_TYPE_INSERT),
            (4, "source", CDCReader.CDC_TYPE_INSERT))
          .toDF("id", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      checkAnswer(changesDF, expectedCDC)
    }
  }

  test("replaceUsing with CDF enabled and all rows matching") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, true)
        .save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (2, "source")).toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(Row(1, "source"), Row(2, "source")))

      val deltaLog = DeltaLog.forTable(spark, path)
      val version = deltaLog.update().version
      val changesDF = CDCReader.changesToBatchDF(deltaLog, version, version, spark)
        .select("id", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      val expectedCDC =
        Seq((1, "target", CDCReader.CDC_TYPE_DELETE_STRING),
            (2, "target", CDCReader.CDC_TYPE_DELETE_STRING),
            (1, "source", CDCReader.CDC_TYPE_INSERT),
            (2, "source", CDCReader.CDC_TYPE_INSERT))
          .toDF("id", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      checkAnswer(changesDF, expectedCDC)
    }
  }

  test("replaceUsing with CDF enabled on multi-column key") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "a", "target"), (1, "b", "target"), (2, "a", "target"))
        .toDF("id", "key", "value")
        .write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, true)
        .save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "a", "source")).toDF("id", "key", "value"),
        target = path,
        replaceUsingCols = "id, key")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id", "key"),
        Seq(Row(1, "a", "source"), Row(1, "b", "target"), Row(2, "a", "target")))

      val deltaLog = DeltaLog.forTable(spark, path)
      val version = deltaLog.update().version
      val changesDF = CDCReader.changesToBatchDF(deltaLog, version, version, spark)
        .select("id", "key", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      val expectedCDC =
        Seq((1, "a", "target", CDCReader.CDC_TYPE_DELETE_STRING),
            (1, "a", "source", CDCReader.CDC_TYPE_INSERT))
          .toDF("id", "key", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      checkAnswer(changesDF, expectedCDC)
    }
  }


  test("replaceUsing with CDF enabled and duplicate source rows matching same target") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, true)
        .save(path)

      // Source has two rows with id=1, both match target row id=1
      writeReplaceUsingDF(
        sourceDF = Seq((1, "source_a"), (1, "source_b")).toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      val result = spark.read.format("delta").load(path).orderBy("id", "value")
      checkAnswer(result,
        Seq(Row(1, "source_a"), Row(1, "source_b"), Row(2, "target")))

      val deltaLog = DeltaLog.forTable(spark, path)
      val version = deltaLog.update().version
      val changesDF = CDCReader.changesToBatchDF(deltaLog, version, version, spark)
        .select("id", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      // Target row id=1 should appear as DELETE exactly once
      val deleteCount = changesDF
        .filter(col(CDCReader.CDC_TYPE_COLUMN_NAME) === CDCReader.CDC_TYPE_DELETE_STRING)
        .count()
      assert(deleteCount === 1, "Expected exactly one DELETE CDC record for the matched target row")
    }
  }

  test("replaceUsing with CDF enabled and NULL values in matching columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((Some(1), "target"), (None, "target_null"), (Some(2), "target"))
        .toDF("id", "value")
        .write.format("delta")
        .option(DeltaConfigs.CHANGE_DATA_FEED.key, true)
        .save(path)

      // Source has a NULL id; EXISTS with NULL = NULL is false, so NULL row is not matched
      writeReplaceUsingDF(
        sourceDF = Seq((Some(1), "source"), (None, "source_null")).toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      val result = spark.read.format("delta").load(path).orderBy("value")
      // NULL rows are not matched by = comparison, so target_null stays
      checkAnswer(result,
        Seq(Row(1, "source"), Row(null, "source_null"),
            Row(2, "target"), Row(null, "target_null")))

      val deltaLog = DeltaLog.forTable(spark, path)
      val version = deltaLog.update().version
      val changesDF = CDCReader.changesToBatchDF(deltaLog, version, version, spark)
        .select("id", "value", CDCReader.CDC_TYPE_COLUMN_NAME)
      // Only id=1 should have DELETE CDC, NULL rows should not be deleted
      val deleteCDC = changesDF
        .filter(col(CDCReader.CDC_TYPE_COLUMN_NAME) === CDCReader.CDC_TYPE_DELETE_STRING)
      checkAnswer(deleteCDC.select("id", "value"),
        Seq(Row(1, "target")))
    }
  }

  test("replaceUsing with empty source") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq.empty[(Int, String)].toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "target") :: Row(2, "target") :: Nil)
    }
  }

  test("replaceUsing on empty but existing target") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq.empty[(Int, String)].toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (2, "source"), (3, "source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") :: Row(2, "source") :: Row(3, "source") :: Nil)
    }
  }

  // Data matching semantics

  test("replaceUsing with NULL values in matching column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((Some(1), "target"), (None, "target_null"), (Some(3), "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((Some(1), "source"), (None, "source_null"), (Some(4), "source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(null, "target_null") ::
          Row(null, "source_null") ::
          Row(1, "source") ::
          Row(3, "target") ::
          Row(4, "source") ::
          Nil)
    }
  }

  test("replaceUsing with NULL values in multiple matching columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq[(Option[Int], Option[Int], String)](
        (None, None, "target"),
        (Some(1), None, "target"),
        (Some(1), Some(2), "target"),
        (Some(1), Some(3), "target"))
        .toDF("col1", "col2", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq[(Option[Int], Option[Int], String)](
          (None, None, "source"),
          (Some(1), None, "source"),
          (None, Some(1), "source"),
          (Some(1), Some(2), "source"),
          (Some(2), Some(3), "source"))
          .toDF("col1", "col2", "value"),
        target = path,
        replaceUsingCols = "col1, col2")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(null, null, "target") ::
          Row(null, null, "source") ::
          Row(1, null, "target") ::
          Row(1, null, "source") ::
          Row(null, 1, "source") ::
          Row(1, 2, "source") ::
          Row(1, 3, "target") ::
          Row(2, 3, "source") ::
          Nil)
    }
  }

  test("replaceUsing with string matching column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(("US", "Dylan"), ("UK", "Jennie"), ("IT", "Julia"))
        .toDF("country", "name")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq(("US", "Sophie"), ("UK", "Oliver"), ("JP", "Yuki"))
          .toDF("country", "name"),
        target = path,
        replaceUsingCols = "country")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row("US", "Sophie") ::
          Row("UK", "Oliver") ::
          Row("JP", "Yuki") ::
          Row("IT", "Julia") ::
          Nil)
    }
  }

  test("replaceUsing replaces all matching rows when target has duplicates") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target1"), (1, "target2"), (2, "target1"), (2, "target2"),
        (2, "target3"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (2, "source")).toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") :: Row(2, "source") :: Row(3, "target") :: Nil)
    }
  }

  test("replaceUsing with filtered source only replaces matching subset") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (2, "target"), (3, "target"),
        (3, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      val source = Seq((1, "source"), (2, "source"), (3, "source"))
        .toDF("id", "value")
        .filter($"id" <= 2)

      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") ::
          Row(2, "source") ::
          Row(3, "target") ::
          Row(3, "target") ::
          Row(3, "target") ::
          Nil)
    }
  }

  test("replaceUsing matches on non-adjacent columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, 10, 100, "target"),
        (1, 20, 200, "target"),
        (2, 30, 100, "target"),
        (3, 40, 300, "target")
      ).toDF("col1", "col2", "col3", "origin")
        .write.format("delta").save(path)

      val source = Seq(
        (1, 99, 100, "source"),
        (2, 99, 200, "source")
      ).toDF("col1", "col2", "col3", "origin")

      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "col1, col3")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, 99, 100, "source") ::
          Row(1, 20, 200, "target") ::
          Row(2, 30, 100, "target") ::
          Row(2, 99, 200, "source") ::
          Row(3, 40, 300, "target") :: Nil)
    }
  }

  // Clustered and partitioned tables

  test("replaceUsing on clustered table with matching on clustering column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "a", "target"), (1, "b", "target"), (2, "a", "target"), (3, "a", "target"))
        .toDF("cluster", "key", "value")
        .write.format("delta").clusterBy("cluster").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "a", "source"), (2, "b", "source"), (4, "a", "source"))
          .toDF("cluster", "key", "value"),
        target = path,
        replaceUsingCols = "cluster")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a", "source") ::
          Row(2, "b", "source") ::
          Row(4, "a", "source") ::
          Row(3, "a", "target") ::
          Nil)
    }
  }

  test("replaceUsing on clustered table with matching on non-clustering column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "a", "target"),
        (1, "b", "target"),
        (2, "a", "target"),
        (3, "c", "target")
      ).toDF("cluster", "key", "value")
        .write.format("delta").clusterBy("cluster").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "a", "source"), (2, "b", "source"), (4, "d", "source"))
          .toDF("cluster", "key", "value"),
        target = path,
        replaceUsingCols = "key")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a", "source") ::
          Row(2, "b", "source") ::
          Row(4, "d", "source") ::
          Row(3, "c", "target") ::
          Nil
      )
    }
  }

  test("replaceUsing on partitioned table with matching on partition column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "a", "target"), (1, "b", "target"), (2, "a", "target"), (3, "a", "target"))
        .toDF("part", "key", "value")
        .write.format("delta").partitionBy("part").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "a", "source"), (2, "b", "source"), (4, "a", "source"))
          .toDF("part", "key", "value"),
        target = path,
        replaceUsingCols = "part")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a", "source") ::
          Row(2, "b", "source") ::
          Row(4, "a", "source") ::
          Row(3, "a", "target") ::
          Nil)
    }
  }

  test("replaceUsing on partitioned table with matching on non-partition column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "a", "target"),
        (1, "b", "target"),
        (2, "a", "target"),
        (3, "c", "target")
      ).toDF("part", "key", "value")
        .write.format("delta").partitionBy("part").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "a", "source"), (2, "b", "source"), (4, "d", "source"))
          .toDF("part", "key", "value"),
        target = path,
        replaceUsingCols = "key")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a", "source") ::
          Row(2, "b", "source") ::
          Row(4, "d", "source") ::
          Row(3, "c", "target") ::
          Nil
      )
    }
  }

  // Column resolution and schema compatibility

  test("replaceUsing with case insensitive column resolution") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        Seq((1, "target"), (2, "target"), (3, "target"))
          .toDF("id", "value")
          .write.format("delta").save(path)

        writeReplaceUsingDF(
          sourceDF = Seq((1, "source"), (4, "source"))
            .toDF("ID", "VALUE"),
          target = path,
          replaceUsingCols = "ID")

        checkAnswer(
          spark.read.format("delta").load(path),
          Row(1, "source") ::
            Row(2, "target") ::
            Row(3, "target") ::
            Row(4, "source") ::
            Nil
        )
      }
    }
  }

  test("replaceUsing with case sensitive column resolution errors on casing mismatch") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        Seq((1, "target"), (2, "target"))
          .toDF("id", "value")
          .write.format("delta").save(path)

        checkError(
          exception = intercept[AnalysisException] {
            writeReplaceUsingDF(
              sourceDF = Seq((1, "source"), (4, "source"))
                .toDF("ID", "VALUE"),
              target = path,
              replaceUsingCols = "ID")
          },
          condition = "UNRESOLVED_INSERT_REPLACE_USING_COLUMN",
          parameters = Map(
            "colName" -> "`ID`",
            "relationType" -> ".*",
            "suggestion" -> ".*"),
          matchPVals = true
        )
      }
    }
  }

  test("replaceUsing with duplicate column references") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (3, "source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id, id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") ::
          Row(2, "target") ::
          Row(3, "source") ::
          Nil
      )
    }
  }

  test("replaceUsing errors when source has extra columns without mergeSchema") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "source", 100), (3, "source", 300))
              .toDF("id", "value", "extra"),
            target = path,
            replaceUsingCols = "id")
        },
        condition = "DELTA_METADATA_MISMATCH",
        parameters = Map.empty[String, String]
      )
    }
  }

  test("replaceUsing with fewer source columns fills missing target columns with null") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq(1, 3).toDF("id"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, null) ::
          Row(2, "target") ::
          Row(3, null) ::
          Nil
      )
    }
  }

  test("replaceUsing errors when source has completely different column names") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "source"), (3, "source"))
              .toDF("key", "data"),
            target = path,
            replaceUsingCols = "key")
        },
        condition = "DELTA_METADATA_MISMATCH",
        parameters = Map.empty[String, String]
      )
    }
  }

  test("replaceUsing with schema evolution") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source", 100), (3, "source", 300))
          .toDF("id", "value", "new_col"),
        target = path,
        replaceUsingCols = "id",
        mergeSchema = true)

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source", 100) ::
          Row(2, "target", null) ::
          Row(3, "source", 300) ::
          Nil
      )
    }
  }

  // Column validation errors

  test("replaceUsing column not in source and target errors") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "source"), (2, "source")).toDF("id", "value"),
            target = path,
            replaceUsingCols = "nonexistent_col")
        },
        condition = "UNRESOLVED_INSERT_REPLACE_USING_COLUMN",
        parameters = Map(
          "colName" -> "`nonexistent_col`",
          "relationType" -> "table",
          "suggestion" -> ".*"),
        matchPVals = true
      )
    }
  }

  test("replaceUsing column exists in table but not in source errors") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target", "us"), (2, "target", "eu"))
        .toDF("id", "value", "region")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "source"), (3, "source"))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "region")
        },
        condition = "UNRESOLVED_INSERT_REPLACE_USING_COLUMN",
        parameters = Map(
          "colName" -> "`region`",
          "relationType" -> "query",
          "suggestion" -> ".*"),
        matchPVals = true
      )

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "target", "us") ::
          Row(2, "target", "eu") ::
          Nil
      )
    }
  }

  test("replaceUsing on non-existent path errors") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath + "/new_table"
      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "a"), (2, "b"), (3, "c"))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "id")
        },
        condition = "DELTA_PATH_DOES_NOT_EXIST",
        parameters = Map("path" -> ".*"),
        matchPVals = true
      )
    }
  }

  // Type compatibility

  test("replaceUsing col with source having wider data type than target") {
    withSQLConf(
        DeltaConfigs.ENABLE_TYPE_WIDENING.defaultTablePropertyKey -> "true") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        Seq(125.toByte, 126.toByte, 127.toByte).toDF("col")
          .withColumn("value", lit("target"))
          .write.format("delta").save(path)

        writeReplaceUsingDF(
          sourceDF = Seq(126.toShort, 127.toShort, 10000.toShort).toDF("col")
            .withColumn("value", lit("source")),
          target = path,
          replaceUsingCols = "col",
          mergeSchema = true)

        checkAnswer(
          spark.read.format("delta").load(path),
          Row(125, "target") ::
            Row(126, "source") ::
            Row(127, "source") ::
            Row(10000, "source") ::
            Nil
        )
      }
    }
  }

  test("replaceUsing col with target having wider data type than source") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(126.toShort, 127.toShort, 10000.toShort).toDF("col")
        .withColumn("value", lit("target"))
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq(125.toByte, 126.toByte, 127.toByte).toDF("col")
          .withColumn("value", lit("source")),
        target = path,
        replaceUsingCols = "col")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(125, "source") ::
          Row(126, "source") ::
          Row(127, "source") ::
          Row(10000, "target") ::
          Nil
      )
    }
  }

  // Source DataFrame transformations

  test("replaceUsing with source from parquet table") {
    withTempDir { targetDir =>
      withTempDir { sourceDir =>
        val targetPath = targetDir.getAbsolutePath
        val sourcePath = sourceDir.getAbsolutePath

        Seq((1, "target"), (2, "target"), (3, "target"))
          .toDF("id", "value")
          .write.format("delta").save(targetPath)

        Seq((1, "source"), (4, "source"))
          .toDF("id", "value")
          .write.format("parquet").mode("overwrite").save(sourcePath)

        writeReplaceUsingDF(
          sourceDF = spark.read.format("parquet").load(sourcePath),
          target = targetPath,
          replaceUsingCols = "id")

        checkAnswer(
          spark.read.format("delta").load(targetPath),
          Row(1, "source") ::
            Row(2, "target") ::
            Row(3, "target") ::
            Row(4, "source") ::
            Nil
        )
      }
    }
  }

  test("replaceUsing self-referencing (read and write same table)") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = spark.read.format("delta").load(path)
          .filter($"id" <= 2)
          .withColumn("value", lit("self_updated")),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "self_updated") ::
          Row(2, "self_updated") ::
          Row(3, "target") ::
          Nil
      )
    }
  }

  test("DataFrame alias before write does not affect replaceUsing") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (4, "source"))
          .toDF("id", "value")
          .alias("some_alias"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Row(4, "source") ::
          Nil
      )
    }
  }

  test("replaceUsing with source from Delta table read") {
    withTempDir { targetDir =>
      withTempDir { sourceDir =>
        val targetPath = targetDir.getAbsolutePath
        val sourcePath = sourceDir.getAbsolutePath

        Seq((1, "target"), (2, "target"), (3, "target"))
          .toDF("id", "value")
          .write.format("delta").save(targetPath)

        Seq((1, "source"), (4, "source"))
          .toDF("id", "value")
          .write.format("delta").save(sourcePath)

        writeReplaceUsingDF(
          sourceDF = spark.read.format("delta").load(sourcePath),
          target = targetPath,
          replaceUsingCols = "id")

        checkAnswer(
          spark.read.format("delta").load(targetPath),
          Row(1, "source") ::
            Row(2, "target") ::
            Row(3, "target") ::
            Row(4, "source") ::
            Nil
        )
      }
    }
  }

  test("replaceUsing with source from join") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      val left = Seq((1, "left"), (4, "left")).toDF("id", "label")
      val right = Seq((1, "right"), (4, "right")).toDF("id", "tag")
      val source = left.join(right, "id")
        .select($"id", $"label".as("value"))

      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "left") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Row(4, "left") ::
          Nil
      )
    }
  }

  test("replaceUsing with source from groupBy aggregation") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      val source = Seq((1, "a"), (1, "b"), (4, "c"))
        .toDF("id", "item")
        .groupBy("id")
        // scalastyle:off countstring
        .agg(concat(lit("agg_"), count("*").cast("string")).as("value"))
        // scalastyle:on countstring

      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "agg_2") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Row(4, "agg_1") ::
          Nil
      )
    }
  }

  test("replaceUsing with source from union") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      val part1 = Seq((1, "source1")).toDF("id", "value")
      val part2 = Seq((4, "source2")).toDF("id", "value")
      val source = part1.union(part2)

      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source1") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Row(4, "source2") ::
          Nil
      )
    }
  }

  test("replaceUsing with source from select/withColumn") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      val source = Seq((1, 100), (4, 400))
        .toDF("id", "num")
        .withColumn("value", concat(lit("val_"), $"num".cast("string")))
        .select("id", "value")

      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "val_100") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Row(4, "val_400") ::
          Nil
      )
    }
  }

  test("replaceUsing with source from sql() query") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      Seq((1, "source"), (4, "source")).toDF("id", "value").createOrReplaceTempView("src")
      val source = sql("SELECT id, value FROM src")

      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Row(4, "source") ::
          Nil
      )
    }
  }

  test("replaceUsing should fail when CHECK constraint is violated") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, 10), (2, 20)).toDF("id", "value")
        .write.format("delta").save(path)
      sql(s"ALTER TABLE delta.`$path` ADD CONSTRAINT positive_value CHECK (value > 0)")

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, -5)).toDF("id", "value"),
            target = path,
            replaceUsingCols = "id")
        },
        condition = "DELTA_REPLACE_ON_OR_USING_TABLE_CONSTRAINT_VIOLATION",
        sqlState = Some("44000"),
        parameters = Map(
          "replaceExpression" -> ".*",
          "invariantViolationMessage" ->
            "(?s)\\[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES\\] .*"),
        matchPVals = true
      )
    }
  }

  test("source is materialized during replaceUsing") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value").write.format("delta").save(path)

      val source = Seq((1, "source"), (4, "source")).toDF("id", "value")
      writeReplaceUsingDF(
        sourceDF = source,
        target = path,
        replaceUsingCols = "id")
    }
  }

  // Table constraints and option handling

  test("replaceUsing is blocked on appendOnly table") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta")
        .option("delta.appendOnly", "true")
        .save(path)

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "target") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Nil
      )

      checkError(
        exception = intercept[DeltaUnsupportedOperationException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "source"), (4, "source"))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "id")
        },
        condition = "DELTA_CANNOT_MODIFY_APPEND_ONLY",
        parameters = Map("table_name" -> ".*", "config" -> "delta.appendOnly"),
        matchPVals = true
      )

      // Data unchanged after blocked operation.
      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "target") ::
          Row(2, "target") ::
          Row(3, "target") ::
          Nil
      )
    }
  }

  test("replaceUsing does not apply delta config options to existing table") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      val deltaLog = DeltaLog.forTable(spark, path)
      assert(!DeltaConfigs.IS_APPEND_ONLY.fromMetaData(deltaLog.update().metadata))

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (3, "source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id",
        options = Map("delta.appendOnly" -> "true"))

      // appendOnly is not enabled because replaceUsing doesn't update table metadata.
      assert(!DeltaConfigs.IS_APPEND_ONLY.fromMetaData(deltaLog.update().metadata))

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") ::
          Row(2, "target") ::
          Row(3, "source") ::
          Nil
      )
    }
  }

  test("DPO kill switch does not block replaceUsing") {
    withSQLConf(DeltaSQLConf.DYNAMIC_PARTITION_OVERWRITE_ENABLED.key -> "false") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        Seq((0, "target"), (1, "target"))
          .toDF("id", "value")
          .write.format("delta").save(path)

        writeReplaceUsingDF(
          sourceDF = Seq((1, "source"), (2, "source")).toDF("id", "value"),
          target = path,
          replaceUsingCols = "id")

        checkAnswer(
          spark.read.format("delta").load(path),
          Row(0, "target") ::
            Row(1, "source") ::
            Row(2, "source") ::
            Nil
        )
      }
    }
  }

  for (partitionOverwriteMode <- Seq("static", "dynamic")) {
    test(s"SQL Conf partitionOverwriteMode=$partitionOverwriteMode " +
        "does not affect replaceUsing") {
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> partitionOverwriteMode) {
        withTempDir { dir =>
          val path = dir.getAbsolutePath
          Seq((0, "target"), (1, "target"))
            .toDF("id", "value")
            .write.format("delta").save(path)

          writeReplaceUsingDF(
            sourceDF = Seq((1, "source"), (2, "source")).toDF("id", "value"),
            target = path,
            replaceUsingCols = "id")

          checkAnswer(
            spark.read.format("delta").load(path),
            Row(0, "target") :: Row(1, "source") :: Row(2, "source") :: Nil)
        }
      }
    }
  }

  test("replaceUsing with empty string errors") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[DeltaIllegalArgumentException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "source"), (3, "source"))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "")
        },
        condition = "DELTA_ILLEGAL_OPTION",
        sqlState = Some("42616"),
        parameters = Map(
          "name" -> "replaceUsing",
          "input" -> "",
          "explain" -> "must not contain empty column names")
      )
    }
  }

  for (optionValue <- Seq(",", "id,,value", "id,", ",id")) {
    test(s"replaceUsing with empty column name errors: '$optionValue'") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        Seq((1, "target"), (2, "target"))
          .toDF("id", "value")
          .write.format("delta").save(path)

        checkError(
          exception = intercept[DeltaIllegalArgumentException] {
            writeReplaceUsingDF(
              sourceDF = Seq((1, "source"), (3, "source"))
                .toDF("id", "value"),
              target = path,
              replaceUsingCols = optionValue)
          },
          condition = "DELTA_ILLEGAL_OPTION",
          sqlState = Some("42616"),
          parameters = Map(
            "name" -> "replaceUsing",
            "input" -> optionValue,
            "explain" -> "must not contain empty column names")
        )
      }
    }
  }

  // Option parsing robustness

  for (optionValue <- Seq(
      "id,key",
      "id ,key",
      "id, key",
      "id , key",
      " id , key ",
      "id,  key",
      "  id  ,  key  ")) {
    test(s"replaceUsing option parsing is robust: '$optionValue'") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        Seq((1, "a", "target"), (2, "b", "target"))
          .toDF("id", "key", "value")
          .write.format("delta").save(path)

        writeReplaceUsingDF(
          sourceDF = Seq((1, "a", "source"))
            .toDF("id", "key", "value"),
          target = path,
          replaceUsingCols = optionValue)

        checkAnswer(
          spark.read.format("delta").load(path),
          Row(1, "a", "source") ::
            Row(2, "b", "target") ::
            Nil
        )
      }
    }
  }

  test("replaceUsing preserves table properties") {
    withTable("target") {
      sql("""CREATE TABLE target (id INT, data STRING) USING delta
             TBLPROPERTIES ('custom.key' = 'value', 'delta.enableChangeDataFeed' = 'true')""")
      sql("INSERT INTO target VALUES (1, 'old')")

      val path = DeltaLog.forTable(spark, spark.sessionState.catalog
        .getTableMetadata(spark.sessionState.sqlParser
          .parseTableIdentifier("target"))).dataPath.toString

      writeReplaceUsingDF(
        sourceDF = Seq((1, "new")).toDF("id", "data"),
        target = path,
        replaceUsingCols = "id")

      val props = sql("SHOW TBLPROPERTIES target").collect()
        .map(r => r.getString(0) -> r.getString(1)).toMap
      assert(props("custom.key") === "value")
      assert(props("delta.enableChangeDataFeed") === "true")
    }
  }


  test("replaceUsing with userMetadata preserves metadata in commit") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target")).toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (3, "source")).toDF("id", "value"),
        target = path,
        replaceUsingCols = "id",
        options = Map(DeltaOptions.USER_METADATA_OPTION -> "replaceUsingMeta"))

      val history = io.delta.tables.DeltaTable.forPath(spark, path)
        .history(1).as[DeltaHistory].head
      assert(history.userMetadata === Some("replaceUsingMeta"))
    }
  }

  test("replaceUsing with targetAlias replaces data normally") {
    // User-provided targetAlias is silently overridden by the internal alias for replaceUsing,
    // since replaceUsing takes top-level column names, not expressions that could reference a
    // table alias.
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (3, "source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id",
        options = Map("targetAlias" -> "t"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(Row(1, "source"), Row(2, "target"), Row(3, "source")))
    }
  }

  test("replaceUsing rejects multi-part columns - struct column") {
    // replaceUsing only accepts top-level column names. Multi-part names like "x.id"
    // (struct field references) are not supported and fail during column resolution.
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      spark.sql(
        """SELECT named_struct('id', 1) x, 'a' value
          |UNION ALL
          |SELECT named_struct('id', 2) x, 'b' value""".stripMargin)
        .write.format("delta").save(path)

      val sourceDF = spark.sql("SELECT named_struct('id', 1) x, 'c' value")

      val e = intercept[NoSuchElementException] {
        writeReplaceUsingDF(
          sourceDF = sourceDF,
          target = path,
          replaceUsingCols = "x.id")
      }
      assert(e.getMessage.contains("None.get"))
    }
  }

  test("replaceUsing rejects multi-part columns with targetAlias") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "a"), (2, "b")).toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "c")).toDF("id", "value").alias("t"),
            target = path,
            replaceUsingCols = "t.id",
            options = Map("targetAlias" -> "t"))
        },
        condition = "UNRESOLVED_INSERT_REPLACE_USING_COLUMN",
        parameters = Map(
          "colName" -> "`t`.`id`",
          "relationType" -> "table",
          "suggestion" -> "`id`, `value`")
      )
    }
  }

  test("replaceUsing option key is case insensitive") {
    Seq("REPLACEUSING", "replaceusing", "ReplaceUsing", "rEpLaCeUsInG").foreach { key =>
      withClue(s"option key: $key") {
        withTempDir { dir =>
          val path = dir.getAbsolutePath
          Seq((1, "original"), (2, "original"))
            .toDF("id", "value")
            .write.format("delta").save(path)

          Seq((1, "updated"))
            .toDF("id", "value")
            .write.format("delta")
            .mode("overwrite")
            .option(key, "id")
            .save(path)

          checkAnswer(
            spark.read.format("delta").load(path),
            Seq(Row(1, "updated"), Row(2, "original")))
        }
      }
    }
  }

  test("replaceUsing in Append mode does not do selective overwrite") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (3, "source")).toDF("id", "value"),
        target = path,
        replaceUsingCols = "id",
        writeMode = "append")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id", "value"),
        Row(1, "source") ::
          Row(1, "target") ::
          Row(2, "target") ::
          Row(3, "source") ::
          Nil
      )
    }
  }
}
