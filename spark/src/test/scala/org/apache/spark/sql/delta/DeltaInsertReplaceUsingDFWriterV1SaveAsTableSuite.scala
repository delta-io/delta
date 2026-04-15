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
import org.apache.spark.sql.{DataFrame, Row}

class DeltaInsertReplaceUsingDFWriterV1SaveAsTableSuite
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
    writer.saveAsTable(s"delta.`$target`")
  }

  test("saveAsTable: replaceUsing with single column") {
    withTable("target") {
      Seq((1, "target"), (2, "target"), (3, "target"))
        .toDF("id", "value")
        .write.format("delta").saveAsTable("target")

      Seq((1, "source"), (4, "source")).toDF("id", "value")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceUsing", "id")
        .saveAsTable("target")

      checkAnswer(
        spark.table("target").orderBy("id"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "target"),
          Row(4, "source")))
    }
  }

  test("saveAsTable: replaceUsing on empty named table") {
    withTable("target") {
      sql("CREATE TABLE target (id INT, value STRING) USING delta")

      Seq((1, "source"), (2, "source")).toDF("id", "value")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceUsing", "id")
        .saveAsTable("target")

      checkAnswer(
        spark.table("target").orderBy("id"),
        Seq(
          Row(1, "source"),
          Row(2, "source")))
    }
  }

  test("saveAsTable: replaceUsing on non-existent table errors") {
    intercept[DeltaAnalysisException] {
      Seq((1, "data1"), (2, "data2")).toDF("id", "value")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceUsing", "id")
        .saveAsTable("new_table")
    }
  }

  test("saveAsTable: replaceUsing with schema evolution") {
    withTable("target") {
      Seq((1, "target"), (2, "target"))
        .toDF("id", "value")
        .write.format("delta").saveAsTable("target")

      Seq((1, "source", 100), (3, "source", 300))
        .toDF("id", "value", "extra")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceUsing", "id")
        .option("mergeSchema", "true")
        .saveAsTable("target")

      checkAnswer(
        spark.table("target").orderBy("id"),
        Seq(
          Row(1, "source", 100),
          Row(2, "target", null),
          Row(3, "source", 300)))
    }
  }
}
