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
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests replaceOn via DataFrameWriterV1 save() API.
 */
class DeltaInsertReplaceOnDFWriterV1SaveSuite
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
    writer.save(target)
  }

  // Misaligned column blocking only applies for replaceUsing, not replaceOn.
  test("save - replaceOn with misaligned column positions between source and target") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      // Target schema: (id, data), id at position 0
      Seq(
        (1, "target"),
        (2, "target"),
        (3, "target"))
        .toDF("id", "data")
        .write.format("delta").save(path)
      // Source schema: (data, id), id at position 1. save() inserts by name,
      // and replaceOn also matches by name to detect rows to be deleted.
      writeReplaceOnDF(
        sourceDF = Seq(
          ("source", 1),
          ("source", 4))
          .toDF("data", "id").as("s"),
        target = path,
        replaceOnCond = "t.id = s.id",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "target"),
          Row(4, "source")))
    }
  }
}
