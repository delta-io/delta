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

import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.internal.SQLConf

class DeltaInsertReplaceUsingDFWriterV1SaveSuite
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
    writer.save(target)
  }

  test("save with replaceUsing: inserts by name with misaligned column positions") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      Seq((1, "a_target", "b_target"), (2, "a_target", "b_target"))
        .toDF("id", "col_a", "col_b")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "b_source", "a_source"), (3, "b_source", "a_source"))
          .toDF("id", "col_b", "col_a"),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "a_source", "b_source") ::
          Row(2, "a_target", "b_target") ::
          Row(3, "a_source", "b_source") ::
          Nil
      )
    }
  }

  test("save with replaceUsing: inserts struct fields by name") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target_a", "target_b"))
        .toDF("id", "a", "b")
        .select($"id", struct($"a", $"b").as("info"))
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, "source_b", "source_a"))
          .toDF("id", "b", "a")
          .select($"id", struct($"a", $"b").as("info")),
        target = path,
        replaceUsingCols = "id")

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, Row("source_a", "source_b")) ::
          Nil
      )
    }
  }

  test("save with replaceUsing: rejects mismatched column types without implicit casting") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, 100), (3, 300))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "id")
        },
        condition = "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map("currentField" -> ".*", "updateField" -> ".*"),
        matchPVals = true
      )
    }
  }

  test("save with replaceUsing: rejects Long source to Int target (no implicit narrowing)") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1L, "source"), (3L, "source"))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "id")
        },
        condition = "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map("currentField" -> ".*", "updateField" -> ".*"),
        matchPVals = true
      )
    }
  }

  test("save with replaceUsing: rejects type mismatch on non-replaceUsing column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1L, "source"), (3L, "source"))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "value")
        },
        condition = "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map("currentField" -> ".*", "updateField" -> ".*"),
        matchPVals = true
      )
    }
  }

  test("save with replaceUsing: rejects Int source to Long target (no implicit widening)") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1L, "target"), (2L, "target"))
        .toDF("id", "value")
        .write.format("delta").save(path)

      checkError(
        exception = intercept[AnalysisException] {
          writeReplaceUsingDF(
            sourceDF = Seq((1, "source"), (3, "source"))
              .toDF("id", "value"),
            target = path,
            replaceUsingCols = "id")
        },
        condition = "DELTA_FAILED_TO_MERGE_FIELDS",
        parameters = Map("currentField" -> ".*", "updateField" -> ".*"),
        matchPVals = true
      )
    }
  }

  test("save: replaceUsing succeeds with misaligned columns because save() is by name") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath

      Seq((0, 1, "target"))
        .toDF("non_match_col", "match_col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceUsingDF(
        sourceDF = Seq((1, 2, "source"))
          .toDF("match_col", "non_match_col", "row_origin"),
        target = path,
        replaceUsingCols = "match_col")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("match_col"),
        Seq(
          Row(2, 1, "source")))
    }
  }
}
