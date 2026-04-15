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
 * Tests replaceOn via DataFrameWriterV1 insertInto() API.
 * Reuses shared tests from [[DeltaInsertReplaceOnDFWriterTests]] using
 * delta.`/path` syntax for insertInto, and adds insertInto-specific tests.
 */
class DeltaInsertReplaceOnDFWriterV1InsertIntoSuite
  extends DeltaInsertReplaceOnDFWriterTests {
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(DeltaSQLConf.REPLACE_ON_OPTION_IN_DATAFRAME_WRITER_ENABLED.key, "true")

  override protected def writeReplaceOnDF(
      sourceDF: DataFrame,
      target: String,
      replaceOnCond: String,
      targetAlias: Option[String] = None,
      mergeSchema: Boolean = false): Unit = {
    var writer = sourceDF
      .write.format("delta")
      .mode("overwrite")
      .option("replaceOn", replaceOnCond)
    targetAlias.foreach { alias =>
      writer = writer.option("targetAlias", alias)
    }
    if (mergeSchema) {
      writer = writer.option("mergeSchema", "true")
    }
    writer.insertInto(s"delta.`$target`")
  }

  override protected def excluded: Seq[String] = super.excluded ++ Seq(
    // insertInto requires exact column arity match; fewer source columns is
    // not allowed unlike save() which fills missing columns with null.
    "replaceOn with fewer source columns fills missing target columns with null",
    // insertInto resolves columns by position and does implicit casting,
    // so type mismatch doesn't trigger DELTA_FAILED_TO_MERGE_FIELDS.
    "same column name with different types fails schema merge",
    // This test relies on by name column resolution while insertInto is
    // by position.
    "replaceOn with different schemas, without mergeSchema fails",
    // This test relies on by name column resolution while insertInto is
    // by position. The new column is aligned positionally with the
    // table column, while it is different by name.
    "replaceOn null-safe equality matching between existing columns " +
      "with NULL values and new columns with NULL values"
  )

  test("insertInto: replaceOn on empty table via path") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq.empty[(Int, String)].toDF("id", "data")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "new1"),
          (2, "new2"))
          .toDF("id", "data"),
        target = path,
        replaceOnCond = "true")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "new1"),
          Row(2, "new2")))
    }
  }

  test("insertInto: replaceOn on empty named table") {
    withTable("target") {
      sql("CREATE TABLE target (id INT, data STRING) USING delta")

      Seq((1, "new1"), (2, "new2")).toDF("id", "data")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceOn", "true")
        .insertInto("target")

      checkAnswer(
        spark.table("target").orderBy("id"),
        Seq(
          Row(1, "new1"),
          Row(2, "new2")))
    }
  }

  test("insertInto: replaceOn on non-empty named table") {
    withTable("target") {
      Seq(
        (1, "original1"),
        (2, "original2"),
        (3, "original3"))
        .toDF("id", "data")
        .write.format("delta").saveAsTable("target")

      Seq((1, "updated1"), (4, "new4")).toDF("id", "data").as("s")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceOn", "t.id = s.id")
        .option("targetAlias", "t")
        .insertInto("target")

      checkAnswer(
        spark.table("target").orderBy("id"),
        Seq(
          Row(1, "updated1"),
          Row(2, "original2"),
          Row(3, "original3"),
          Row(4, "new4")))
    }
  }

  test("insertInto: replaceOn on non-existing named table fails") {
    checkError(
      exception = intercept[AnalysisException] {
        Seq((1, "data")).toDF("id", "data")
          .write.format("delta")
          .mode("overwrite")
          .option("replaceOn", "true")
          .insertInto("nonexistent_table")
      },
      condition = "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map("relationName" -> "`nonexistent_table`")
    )
  }

  test("insertInto: replaceOn with swapped column order") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, 100), (2, 200), (3, 300))
        .toDF("col1", "col2")
        .write.format("delta").save(path)
      writeReplaceOnDF(
        sourceDF = Seq((10, 1), (40, 4))
          .toDF("col2", "col1").as("s"),
        target = path,
        replaceOnCond = "t.col1 = s.col1",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("col1"),
        Seq(
          Row(2, 200),
          Row(3, 300),
          Row(10, 1),
          Row(40, 4)))
    }
  }


  test("insertInto: same column name with different types succeeds with implicit casting") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target")).toDF("id", "data").write.format("delta").save(path)
      // insertInto resolves by position and does implicit casting (String "1" -> Int 1).
      writeReplaceOnDF(
        sourceDF = Seq(("1", "source"))
          .toDF("id", "data")
          .as("s"),
        target = path,
        replaceOnCond = "t.id = s.id",
        targetAlias = Some("t")
      )
      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(Row(1, "source"))
      )
    }
  }

}
