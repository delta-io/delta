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

import scala.collection.mutable
import scala.util.control.NonFatal

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions, UsageRecord}
import org.apache.spark.sql.delta.commands.{InsertReplaceOnOrUsingMaterializeSourceError, InsertReplaceOnOrUsingStats}
import org.apache.spark.sql.delta.commands.merge.{MergeIntoMaterializeSourceError, MergeIntoMaterializeSourceErrorType, MergeIntoMaterializeSourceReason}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.{lit, rand, udf}
import org.apache.spark.storage.StorageLevel

/**
 * Common tests for replaceOn option via DataFrame APIs
 * (save(), insertInto(), and saveAsTable()).
 */
trait DeltaInsertReplaceOnDFWriterTests
  extends DeltaInsertReplaceOnOrUsingTestUtils {
  import testImplicits._

  /**
   * Write the sourceDF to the target with replaceOn.
   */
  protected def writeReplaceOnDF(
      sourceDF: DataFrame,
      target: String,
      replaceOnCond: String,
      targetAlias: Option[String] = None,
      mergeSchema: Boolean = false): Unit

  // Basic replaceOn behavior.
  test("replaceOn with always-true condition replaces all rows") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "original1"),
        (2, "original2"))
        .toDF("id", "data")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (10, "new1"),
          (20, "new2"))
          .toDF("id", "data"),
        target = path,
        replaceOnCond = "true")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(10, "new1"),
          Row(20, "new2")))
    }
  }

  test("replaceOn with always-false condition appends all rows") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "original1"),
        (2, "original2"))
        .toDF("id", "data")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (10, "new1"),
          (20, "new2"))
          .toDF("id", "data"),
        target = path,
        replaceOnCond = "false")

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "original1"),
          Row(2, "original2"),
          Row(10, "new1"),
          Row(20, "new2")))
    }
  }

  test("basic replaceOn with single matching column") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target1"),
        (1, "target2"),
        (3, "target"))
        .toDF("col1", "row_origin")
        .write.format("delta").save(path)

      val sourceDF = Seq(
        ("source1"),
        ("source2"))
        .toDF("row_origin")
        .withColumn("col1", lit(null).cast("int"))
        .select($"col1", $"row_origin")
        .union(
          Seq(
            (1, "source"),
            (2, "source1"),
            (2, "source2"),
            (2, "source3"))
            .toDF("col1", "row_origin"))

      writeReplaceOnDF(
        sourceDF = sourceDF.as("s"),
        target = path,
        replaceOnCond = "t.col1 = s.col1",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path),
        Seq(
          Row(null, "source1"),
          Row(null, "source2"),
          Row(1, "source"),
          Row(2, "source1"),
          Row(2, "source2"),
          Row(2, "source3"),
          Row(3, "target")))
    }
  }

  test("replaceOn with multiple matching columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, 1, "target"),
        (1, 2, "target"),
        (2, 1, "target"))
        .toDF("col1", "col2", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (1, 1, "source"),
          (1, 2, "source"),
          (3, 3, "source"))
          .toDF("col1", "col2", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.col1 = s.col1 AND t.col2 = s.col2",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, 1, "source") ::
          Row(1, 2, "source") ::
          Row(2, 1, "target") ::
          Row(3, 3, "source") ::
          Nil)
    }
  }

  test("replaceOn with duplicate rows in source") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"))
        .toDF("col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source1"),
          (1, "source2"),
          (1, "source3"),
          (3, "source"))
          .toDF("col", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.col = s.col",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source1") ::
          Row(1, "source2") ::
          Row(1, "source3") ::
          Row(2, "target") ::
          Row(3, "source") ::
          Nil)
    }
  }

  test("replaceOn with empty target table") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq.empty[(Int, String)].toDF("col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (2, "source"))
          .toDF("col", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.col = s.col",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "source") ::
          Row(2, "source") ::
          Nil)
    }
  }

  test("replaceOn with empty source") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"),
        (2, "target"))
        .toDF("col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq.empty[(Int, String)].toDF("col", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.col = s.col",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "target") ::
          Row(2, "target") ::
          Row(2, "target") ::
          Nil)
    }
  }

  test("replaceOn with filtered source") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"),
        (2, "target"),
        (3, "target"),
        (3, "target"),
        (3, "target"))
        .toDF("col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (2, "source"),
          (3, "source"))
          .toDF("col", "row_origin").filter($"col" <= 2).as("s"),
        target = path,
        replaceOnCond = "t.col = s.col",
        targetAlias = Some("t"))

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

  // Data layout: partitioned, clustered, normal tables.
  for (dataLayoutType <- DataLayoutType.values) {
    test(s"replaceOn with ${dataLayoutType.toString} table") {
      withTempDir { dir =>
        val path = dir.getAbsolutePath
        val targetData = Seq(
          (0, 0, 0, "target"),
          (2, 2, 2, "target"),
          (2, 2, 2, "target"),
          (3, 3, 3, "target"),
          (3, 3, 3, "target"),
          (3, 3, 3, "target"))
          .toDF("col1", "col2", "col3", "row_origin")

        dataLayoutType match {
          case DataLayoutType.PARTITIONED =>
            targetData.write.format("delta")
              .partitionBy("col1", "col2").save(path)
          case DataLayoutType.CLUSTERED =>
            targetData.write.format("delta")
              .clusterBy("col1", "col2").save(path)
          case DataLayoutType.NORMAL =>
            targetData.write.format("delta").save(path)
        }

        writeReplaceOnDF(
          sourceDF = Seq(
            (1, 1, 1, "source"),
            (2, 2, 2, "source"),
            (3, 3, 3, "source"))
            .toDF("col1", "col2", "col3", "row_origin").as("s"),
          target = path,
          replaceOnCond = "t.col1 = s.col1",
          targetAlias = Some("t"))

        checkAnswer(
          spark.read.format("delta").load(path),
          Row(0, 0, 0, "target") ::
            Row(1, 1, 1, "source") ::
            Row(2, 2, 2, "source") ::
            Row(3, 3, 3, "source") ::
            Nil)
      }
    }
  }

  // Condition variations: NULL-safe equality, one-sided, IN predicate, functions.
  test("replaceOn with NULL-safe equality condition") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq[(Option[Int], Option[Int], String)](
        (None, None, "target"),
        (None, Some(1), "target"),
        (Some(1), None, "target"),
        (Some(2), Some(3), "target"))
        .toDF("col1", "col2", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq[(Option[Int], Option[Int], String)](
          (None, None, "source"),
          (None, Some(2), "source"),
          (Some(1), None, "source"),
          (Some(3), None, "source"))
          .toDF("col1", "col2", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.col1 <=> s.col1",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(null, null, "source") ::
          Row(null, 2, "source") ::
          Row(1, null, "source") ::
          Row(2, 3, "target") ::
          Row(3, null, "source") ::
          Nil)
    }
  }

  test("replaceOn with condition referencing only target side") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (-1, "target"),
        (1, "target"),
        (2, "target"),
        (2, "target"),
        (3, "target"),
        (3, "target"),
        (3, "target"))
        .toDF("col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (0, "source"),
          (1, "source"),
          (2, "source"),
          (3, "source"))
          .toDF("col", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.col >= 1",
        targetAlias = Some("t"))
      // All target rows with col >= 1 are replaced; col = -1 is kept.
      checkAnswer(
        spark.read.format("delta").load(path),
        Row(-1, "target") ::
          Row(0, "source") ::
          Row(1, "source") ::
          Row(2, "source") ::
          Row(3, "source") ::
          Nil)
    }
  }

  test("replaceOn with condition referencing only source side") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (-1, "target"),
        (1, "target"),
        (2, "target"),
        (2, "target"),
        (3, "target"),
        (3, "target"),
        (3, "target"))
        .toDF("col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (0, "source"),
          (1, "source"),
          (2, "source"),
          (3, "source"))
          .toDF("col", "row_origin").as("s"),
        target = path,
        replaceOnCond = "s.col >= 1",
        targetAlias = Some("t"))
      // Source has rows with col >= 1, so the condition is true for every
      // target row. All target rows are replaced.
      checkAnswer(
        spark.read.format("delta").load(path),
        Row(0, "source") ::
          Row(1, "source") ::
          Row(2, "source") ::
          Row(3, "source") ::
          Nil)
    }
  }

  test("replaceOn with IN predicate in condition") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"),
        (2, "target"),
        (3, "target"),
        (3, "target"),
        (3, "target"))
        .toDF("col", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (2, "source"),
          (3, "source"))
          .toDF("col", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.col IN (2)",
        targetAlias = Some("t"))
      // Only target rows with col = 2 are replaced. Others are kept, and
      // non-matching source rows are appended.
      checkAnswer(
        spark.read.format("delta").load(path),
        Row(1, "target") ::
          Row(1, "source") ::
          Row(2, "source") ::
          Row(3, "target") ::
          Row(3, "target") ::
          Row(3, "target") ::
          Row(3, "source") ::
          Nil)
    }
  }

  test("replaceOn condition with function (e.g., abs)") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (-1, "target"),
        (2, "target"))
        .toDF("id", "data")
        .write.format("delta").save(path)
      // abs(t.id) = abs(s.id): rows with id=1 and id=-1 both match source id=1
      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (3, "source"))
          .toDF("id", "data").as("s"),
        target = path,
        replaceOnCond = "abs(t.id) = abs(s.id)",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "source")))
    }
  }

  // Schema evolution: new columns, missing columns, different column names.
  test("replaceOn with mergeSchema adds new columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"))
        .toDF("id", "data")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source", 100),
          (3, "source", 300))
          .toDF("id", "data", "extra").as("s"),
        target = path,
        replaceOnCond = "t.id = s.id",
        targetAlias = Some("t"),
        mergeSchema = true)

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "source", 100),
          Row(2, "target", null),
          Row(3, "source", 300)))
    }
  }

  test("replaceOn with fewer source columns fills missing target columns with null") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target", 10),
        (2, "target", 20))
        .toDF("id", "data", "extra")
        .write.format("delta").save(path)
      // Source has fewer columns (id, data), missing "extra" gets null
      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (3, "source"))
          .toDF("id", "data").as("s"),
        target = path,
        replaceOnCond = "t.id = s.id",
        targetAlias = Some("t"),
        mergeSchema = true)

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "source", null),
          Row(2, "target", 20),
          Row(3, "source", null)))
    }
  }

  test("replaceOn with mergeSchema referencing new columns in condition") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (-1, -1, "target"),
        (1, 1, "target"),
        (2, 2, "target"),
        (2, 2, "target"),
        (4, 4, "target"),
        (4, 4, "target"),
        (4, 4, "target"),
        (4, 4, "target"))
        .toDF("col1", "col2", "row_origin")
        .write.format("delta").save(path)
      // Source adds two new columns (col3, col4) and the condition
      // references them.
      writeReplaceOnDF(
        sourceDF = Seq(
          (0, 0, "source", 0, 0),
          (1, 1, "source", 1, 1),
          (2, 2, "source", 2, 2),
          (3, 3, "source", 3, 3))
          .toDF("col1", "col2", "row_origin", "col3", "col4").as("s"),
        target = path,
        replaceOnCond = "t.col1 = s.col3 AND t.col2 = s.col4",
        targetAlias = Some("t"),
        mergeSchema = true)

      checkAnswer(
        spark.read.format("delta").load(path),
        Row(-1, -1, "target", null, null) ::
          Row(0, 0, "source", 0, 0) ::
          Row(1, 1, "source", 1, 1) ::
          Row(2, 2, "source", 2, 2) ::
          Row(3, 3, "source", 3, 3) ::
          Row(4, 4, "target", null, null) ::
          Row(4, 4, "target", null, null) ::
          Row(4, 4, "target", null, null) ::
          Row(4, 4, "target", null, null) ::
          Nil)
    }
  }

  test("replaceOn null-safe equality matching between existing columns " +
      "with NULL values and new columns with NULL values") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      // Target has (id, row_origin). Some rows have id=null.
      // Source also has (id, row_origin) with id=null.
      // Using <=> so null matches null.
      Seq[(Option[Int], String)](
        (Some(1), "target"),
        (None, "target_null"),
        (Some(3), "target"))
        .toDF("id", "row_origin")
        .write.format("delta").save(path)

      writeReplaceOnDF(
        sourceDF = Seq[(Option[Int], String)](
          (None, "source_null"),
          (Some(1), "source"))
          .toDF("id", "row_origin").as("s"),
        target = path,
        replaceOnCond = "t.id <=> s.id",
        targetAlias = Some("t"))

      // id=null in source matches id=null in target via <=>.
      // id=1 in source matches id=1 in target.
      // id=3 in target is not matched, so it remains.
      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(null, "source_null"),
          Row(1, "source"),
          Row(3, "target")))
    }
  }

  // Source data variety: spark.table(), spark.sql().
  test("replaceOn with source from spark.table()") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      withTable("source") {
        Seq(
          (1, "target"),
          (2, "target"),
          (3, "target"))
          .toDF("id", "data")
          .write.format("delta").save(path)

        sql("CREATE TABLE source (id INT, data STRING) USING parquet")
        sql(
          "INSERT INTO source VALUES " +
            "(1, 'source'), (2, 'source'), (4, 'source')")

        writeReplaceOnDF(
          sourceDF = spark.table("source").as("s"),
          target = path,
          replaceOnCond = "t.id = s.id",
          targetAlias = Some("t"))

        checkAnswer(
          spark.read.format("delta").load(path).orderBy("id"),
          Seq(
            Row(1, "source"),
            Row(2, "source"),
            Row(3, "target"),
            Row(4, "source")))
      }
    }
  }

  test("replaceOn with source from spark.sql()") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      withTable("source") {
        Seq(
          (1, "target"),
          (2, "target"),
          (3, "target"))
          .toDF("id", "data")
          .write.format("delta").save(path)

        sql("CREATE TABLE source (id INT, data STRING) USING parquet")
        sql("INSERT INTO source VALUES (1, 'source'), (4, 'source')")

        writeReplaceOnDF(
          sourceDF = spark.sql("SELECT * FROM source").as("s"),
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

  // Alias resolution: targetAlias, compound conditions.
  test("replaceOn with targetAlias and source .as resolves ambiguous columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "original1"),
        (2, "original2"),
        (3, "original3"))
        .toDF("id", "data")
        .write.format("delta").save(path)
      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "updated"),
          (4, "new"))
          .toDF("id", "data").as("s"),
        target = path,
        replaceOnCond = "t.id = s.id",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "updated"),
          Row(2, "original2"),
          Row(3, "original3"),
          Row(4, "new")))
    }
  }

  test("targetAlias resolves ambiguity with compound AND condition") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "a", "target"),
        (2, "b", "target"))
        .toDF("id", "key", "data")
        .write.format("delta").save(path)
      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "a", "source"),
          (3, "c", "source"))
          .toDF("id", "key", "data").as("s"),
        target = path,
        replaceOnCond = "t.id = s.id AND t.key = s.key",
        targetAlias = Some("t"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("id"),
        Seq(
          Row(1, "a", "source"),
          Row(2, "b", "target"),
          Row(3, "c", "source")))
    }
  }

  // Error cases: ambiguous columns, unresolvable columns, type/schema mismatches.
  test("ambiguous: identical alias for target and source") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"))
        .toDF("col1", "row_origin")
        .write.format("delta").save(path)
      // Both target and source aliased as "s". s.col1 resolves to both.
      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"),
            (2, "source"))
            .toDF("col1", "row_origin").as("s"),
          target = path,
          replaceOnCond = "s.col1 = s.col1",
          targetAlias = Some("s"))
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_AMBIGUOUS_COLUMNS_IN_CONDITION",
        sqlState = Some("42702"),
        parameters = Map("columnNames" -> "`s`.`col1`"))
    }
  }

  test("ambiguous: no source alias, bare 'id' on both sides") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"),
        (3, "target"))
        .toDF("id", "data")
        .write.format("delta").save(path)
      // Even without .as(), bare 'id' on both sides is ambiguous.
      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"),
            (4, "source"))
            .toDF("id", "data"),
          target = path,
          replaceOnCond = "id = id")
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_AMBIGUOUS_COLUMNS_IN_CONDITION",
        sqlState = Some("42702"),
        parameters = Map("columnNames" -> "`id`"))
    }
  }

  test("replaceOn with unresolvable column errors") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, 10),
        (2, 20))
        .toDF("col1", "col2")
        .write.format("delta").save(path)

      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq((1, 100)).toDF("col1", "col2"),
          target = path,
          replaceOnCond = "nonexistent_col = 1")
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_UNRESOLVED_COLUMNS_IN_CONDITION",
        sqlState = Some("42703"),
        parameters = Map("columnNames" -> "`nonexistent_col`"))
    }
  }

  test("replaceOn condition uses aliases for both sides but user forgot targetAlias option") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target1"),
        (2, "target2"))
        .toDF("col1", "data")
        .write.format("delta").save(path)

      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"))
            .toDF("col1", "data").as("s"),
          target = path,
          replaceOnCond = "t.col1 = s.col1")
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_UNRESOLVED_COLUMNS_IN_CONDITION",
        sqlState = Some("42703"),
        parameters = Map("columnNames" -> "`t`.`col1`"))
    }
  }

  test("replaceOn condition uses aliases for both sides but user forgot source .as alias") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target1"),
        (2, "target2"))
        .toDF("col1", "data")
        .write.format("delta").save(path)

      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"))
            .toDF("col1", "data"),
          target = path,
          replaceOnCond = "t.col1 = s.col1",
          targetAlias = Some("t"))
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_UNRESOLVED_COLUMNS_IN_CONDITION",
        sqlState = Some("42703"),
        parameters = Map("columnNames" -> "`s`.`col1`"))
    }
  }

  test("replaceOn condition uses aliases for both sides but user forgot both aliases") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target1"),
        (2, "target2"))
        .toDF("col1", "data")
        .write.format("delta").save(path)

      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"))
            .toDF("col1", "data"),
          target = path,
          replaceOnCond = "t.col1 = s.col1")
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_UNRESOLVED_COLUMNS_IN_CONDITION",
        sqlState = Some("42703"),
        parameters = Map("columnNames" -> "`t`.`col1`, `s`.`col1`"))
    }
  }

  test("ambiguous: compound condition, all columns overlap") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "a"),
        (2, "b"))
        .toDF("id", "key")
        .write.format("delta").save(path)
      // Source has identical schema (id, key), both without alias
      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq((1, "x")).toDF("id", "key").as("s"),
          target = path,
          replaceOnCond = "id = s.id AND key = s.key",
          targetAlias = Some("t"))
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_AMBIGUOUS_COLUMNS_IN_CONDITION",
        sqlState = Some("42702"),
        parameters = Map("columnNames" -> "`id`, `key`"))
    }
  }

  test("ambiguous: multiple overlapping columns, only one without alias") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "a", 10)).toDF("id", "key", "value")
        .write.format("delta").save(path)
      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq((1, "a", 99))
            .toDF("id", "key", "amount").as("s"),
          target = path,
          replaceOnCond = "t.id = s.id AND key = s.key",
          targetAlias = Some("t"),
          mergeSchema = true)
      }
      checkError(exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_AMBIGUOUS_COLUMNS_IN_CONDITION",
        sqlState = Some("42702"),
        parameters = Map("columnNames" -> "`key`"))
    }
  }

  test("same column name with different types fails schema merge") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq((1, "target")).toDF("id", "data")
        .write.format("delta").save(path)
      // Source has (id: string, data: string), type mismatch on 'id'
      // fails before reaching the ambiguity check
      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(("1", "source"))
            .toDF("id", "data").as("s"),
          target = path,
          replaceOnCond = "id = s.id",
          targetAlias = Some("t"))
      }
      checkError(exception = e,
        condition = "DELTA_FAILED_TO_MERGE_FIELDS",
        sqlState = Some("22005"),
        parameters = Map(
          "currentField" -> "id",
          "updateField" -> "id"))
    }
  }

  test("replaceOn with different schemas, without mergeSchema fails") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      // Target has (col2, row_origin), source has (col1, row_origin).
      // Without mergeSchema, the schema mismatch is rejected.
      Seq(
        (1, "target"),
        (2, "target"),
        (3, "target"))
        .toDF("col2", "row_origin")
        .write.format("delta").save(path)

      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"),
            (4, "source"))
            .toDF("col1", "row_origin").as("s"),
          target = path,
          replaceOnCond = "t.col2 = s.col1",
          targetAlias = Some("t"))
      }
      checkError(exception = e,
        condition = "DELTA_METADATA_MISMATCH",
        sqlState = Some("42KDG"))
    }
  }

  // targetAlias edge cases: wrong alias, case sensitivity, weird alias values.
  test("replaceOn with wrong targetAlias errors on unresolved columns") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"))
        .toDF("col1", "data")
        .write.format("delta").save(path)
      // Condition references "t" but targetAlias is set to "y",
      // so t.col1 cannot be resolved against either side.
      val e = intercept[DeltaAnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"))
            .toDF("col1", "data").as("s"),
          target = path,
          replaceOnCond = "t.col1 = s.col1",
          targetAlias = Some("y"))
      }
      checkError(
        exception = e,
        condition =
          "DELTA_INSERT_REPLACE_ON_UNRESOLVED_COLUMNS_IN_CONDITION",
        sqlState = Some("42703"),
        parameters = Map("columnNames" -> "`t`.`col1`"))
    }
  }

  test("replaceOn targetAlias is case insensitive") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"),
        (3, "target"))
        .toDF("col1", "data")
        .write.format("delta").save(path)
      // Condition uses lowercase "t" but targetAlias is uppercase "T".
      // Spark resolves aliases case insensitively by default.
      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (4, "source"))
          .toDF("col1", "data").as("s"),
        target = path,
        replaceOnCond = "t.col1 = s.col1",
        targetAlias = Some("T"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("col1"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "target"),
          Row(4, "source")))
    }
  }

  test("replaceOn with numeric targetAlias '22' resolves with backticks") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"),
        (3, "target"))
        .toDF("col1", "data")
        .write.format("delta").save(path)
      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (4, "source"))
          .toDF("col1", "data").as("s"),
        target = path,
        replaceOnCond = "`22`.col1 = s.col1",
        targetAlias = Some("22"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("col1"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "target"),
          Row(4, "source")))
    }
  }

  test("replaceOn with numeric targetAlias '22' without backticks fails on type extraction") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"))
        .toDF("col1", "data")
        .write.format("delta").save(path)
      // Without backticks, "22.col1" is parsed as integer literal 22 with
      // a field extraction of "col1", which fails because INT is not a complex type.
      val e = intercept[org.apache.spark.sql.AnalysisException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"))
            .toDF("col1", "data").as("s"),
          target = path,
          replaceOnCond = "22.col1 = s.col1",
          targetAlias = Some("22"))
      }
      checkError(
        exception = e,
        condition = "INVALID_EXTRACT_BASE_FIELD_TYPE",
        sqlState = Some("42000"),
        parameters = Map("base" -> "\"22\"", "other" -> "\"INT\""))
    }
  }

  test("replaceOn with space containing targetAlias 'hello world' resolves with backticks") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"),
        (3, "target"))
        .toDF("col1", "data")
        .write.format("delta").save(path)
      writeReplaceOnDF(
        sourceDF = Seq(
          (1, "source"),
          (4, "source"))
          .toDF("col1", "data").as("s"),
        target = path,
        replaceOnCond = "`hello world`.col1 = s.col1",
        targetAlias = Some("hello world"))

      checkAnswer(
        spark.read.format("delta").load(path).orderBy("col1"),
        Seq(
          Row(1, "source"),
          Row(2, "target"),
          Row(3, "target"),
          Row(4, "source")))
    }
  }

  test("replaceOn with space containing targetAlias without backticks fails to parse") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      Seq(
        (1, "target"),
        (2, "target"))
        .toDF("col1", "data")
        .write.format("delta").save(path)
      // Without backticks, "hello world.col1" cannot be parsed as a valid expression.
      val e = intercept[ParseException] {
        writeReplaceOnDF(
          sourceDF = Seq(
            (1, "source"))
            .toDF("col1", "data").as("s"),
          target = path,
          replaceOnCond = "hello world.col1 = s.col1",
          targetAlias = Some("hello world"))
      }
      checkError(
        exception = e,
        condition = "PARSE_SYNTAX_ERROR",
        sqlState = Some("42601"),
        parameters = Map("error" -> "'.'", "hint" -> ""))
    }
  }


  private def testMaterializedSourceUnpersist(
      targetPath: String, sourceViewName: String, numKills: Int): Seq[UsageRecord] = {
    val maxAttempts = spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS)
    val killerThreadJoinTimeoutMs = 10000
    val killerIntervalMs = 1

    Seq.tabulate(100)(i => (i, "target")).toDF("id", "row_origin")
      .write.format("delta").save(targetPath)
    Seq.tabulate(20)(i => (i + 100, "source")).toDF("id", "row_origin")
      .createOrReplaceTempView(sourceViewName)

    @volatile var finished = false
    @volatile var invalidStorageLevel: Option[String] = None
    val killerThread = new Thread() {
      override def run(): Unit = {
        val seenSources = mutable.Set[Int]()
        while (!finished) {
          sparkContext.getPersistentRDDs.foreach { case (rddId, rdd) =>
            if (rdd.name == "mergeMaterializedSource") {
              if (!seenSources.contains(rddId)) {
                seenSources.add(rddId)
              }
              if (seenSources.size > numKills) {
                finished = true
              } else if (rdd.isCheckpointed) {
                val expectedStorageLevel = StorageLevel.fromString(
                  if (seenSources.size == 1) {
                    spark.conf.get(DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL)
                  } else if (seenSources.size == 2) {
                    spark.conf.get(
                      DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_FIRST_RETRY)
                  } else {
                    spark.conf.get(
                      DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_RDD_STORAGE_LEVEL_RETRY)
                  })
                val rddStorageLevel = rdd.getStorageLevel
                if (rddStorageLevel != expectedStorageLevel) {
                  invalidStorageLevel =
                    Some(s"For attempt ${seenSources.size} expected " +
                      s"$expectedStorageLevel but got $rddStorageLevel")
                  finished = true
                }
                rdd.unpersist(blocking = false)
              }
            }
          }
          Thread.sleep(killerIntervalMs)
        }
      }
    }
    killerThread.start()

    val events = Log4jUsageLogger.track {
      try {
        writeReplaceOnDF(
          sourceDF = spark.table(sourceViewName).as("s"),
          target = targetPath,
          replaceOnCond = "t.id = s.id",
          targetAlias = Some("t"))
      } catch {
        case NonFatal(ex) =>
          if (numKills < maxAttempts) throw ex
      } finally {
        finished = true
        killerThread.join(killerThreadJoinTimeoutMs)
        assert(!killerThread.isAlive)
      }
    }.filter(_.metric == MetricDefinitions.EVENT_TAHOE.name)

    assert(invalidStorageLevel.isEmpty, invalidStorageLevel.toString)
    events
  }

  {
    val maxAttempts = DeltaSQLConf.MERGE_MATERIALIZE_SOURCE_MAX_ATTEMPTS.defaultValue.get

    for (kills <- 1 to maxAttempts - 1) {
      test(s"materialize source unpersist with $kills kill attempts succeeds") {
        withTempDir { dir =>
          withView("source") {
            val allDeltaEvents = testMaterializedSourceUnpersist(
              targetPath = dir.getAbsolutePath, sourceViewName = "source", numKills = kills)
            val events = allDeltaEvents.filter(
              _.tags.get("opType").contains("delta.insertReplaceOnOrUsing.stats"))
            assert(events.length === 1, s"allDeltaEvents:\n$allDeltaEvents")
            val stats =
              JsonUtils.fromJson[InsertReplaceOnOrUsingStats](events(0).blob)
            assert(stats.materializeSourceAttempts.isDefined,
              s"stats:\n$stats")
            assert(stats.materializeSourceAttempts.get === kills + 1,
              s"stats:\n$stats")

            checkAnswer(
              spark.read.format("delta").load(dir.getAbsolutePath).orderBy("id"),
              (0 until 120).map(id =>
                Row(id, if (id < 100) "target" else "source")))
          }
        }
      }
    }

    test(s"materialize source unpersist with $maxAttempts kill attempts fails") {
      withTempDir { dir =>
        withView("source") {
          val allDeltaEvents = testMaterializedSourceUnpersist(
            targetPath = dir.getAbsolutePath, sourceViewName = "source",
            numKills = maxAttempts)
          val events = allDeltaEvents.filter(_.tags.get("opType")
            .contains(InsertReplaceOnOrUsingMaterializeSourceError.OP_TYPE))
          assert(events.length === 1, s"allDeltaEvents:\n$allDeltaEvents")
          val error =
            JsonUtils.fromJson[MergeIntoMaterializeSourceError](events(0).blob)
          assert(error.errorType ===
            MergeIntoMaterializeSourceErrorType.RDD_BLOCK_LOST.toString)
          assert(error.attempt === maxAttempts)
        }
      }
    }
  }
}
