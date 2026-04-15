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

import com.databricks.spark.util.{Log4jUsageLogger, UsageRecord}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
 * Tests replaceUsing via DataFrameWriterV1 insertInto() API.
 * Reuses shared tests from [[DeltaInsertReplaceUsingDFWriterTests]] using
 * delta.`/path` syntax for insertInto, and adds insertInto-specific tests.
 */
class DeltaInsertReplaceUsingDFWriterV1InsertIntoSuite
  extends DeltaInsertReplaceUsingDFWriterTests {
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.REPLACE_USING_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")

  override protected def writeReplaceUsingDF(
      sourceDF: DataFrame,
      target: String,
      replaceUsingCols: String,
      writeMode: String = "overwrite",
      mergeSchema: Boolean = false,
      options: Map[String, String] = Map.empty): Unit = {
    var writer = sourceDF
      .write.format("delta")
      .mode(writeMode)
      .option("replaceUsing", replaceUsingCols)
    if (mergeSchema) {
      writer = writer.option("mergeSchema", "true")
    }
    options.foreach { case (k, v) => writer = writer.option(k, v) }
    writer.insertInto(s"delta.`$target`")
  }

  override protected def excluded: Seq[String] = super.excluded ++ Seq(
    // insertInto requires exact column arity match. These tests use source
    // with fewer columns than target, which is not supported by insertInto.
    "replaceUsing with fewer source columns fills missing target columns with null",
    "replaceUsing column exists in table but not in source errors",
    // This test relies on by name column resolution while insertInto is
    // by position.
    "replaceUsing errors when source has completely different column names"
  )

  test("insertInto: replaceUsing with same schema and aligned USING columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "a_target"), (2, "b_target"), (3, "c_target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "a_source"), (4, "d_source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "a_source"),
          Row(2, "b_target"),
          Row(3, "c_target"),
          Row(4, "d_source")))
    }
  }

  test("insertInto: replaceUsing with misaligned column names fails") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, 0, "target"))
        .toDF("match_col", "non_match_col", "row_origin")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((2, 1, "source"))
              .toDF("non_match_col", "match_col", "row_origin"),
            target = path,
            replaceUsingCols = "match_col")
        },
        condition = "INSERT_REPLACE_USING_DISALLOW_MISALIGNED_COLUMNS",
        parameters = Map("misalignedReplaceUsingCols" -> "`match_col`"))
    }
  }

  test("insertInto: replaceUsing with misaligned USING columns, disallow conf disabled") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, 0, "target"), (2, 0, "target"))
        .toDF("match_col", "non_match_col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((10, 1, "source"))
          .toDF("non_match_col", "match_col", "row_origin"),
        target = path,
        replaceUsingCols = "match_col")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("match_col"),
        Seq(
          Row(2, 0, "target"),
          Row(10, 1, "source")))
    }
  }

  test("insertInto: replaceUsing with aligned USING column but mismatched " +
      "non-USING column names, which triggers schemaAdjustment") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "target_data")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (4, "source"))
          .toDF("id", "source_data"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "target"),
          Row(4, "source")))
    }
  }

  test("insertInto: replaceUsing with implicit type widening (Int source to Long target)") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1L, "target"), (2L, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source"), (3, "source"))
          .toDF("id", "value"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "source")))
    }
  }
}
