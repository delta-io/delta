/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.tables

import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.test.DeltaQueryTest

class DeltaTableSuite extends DeltaQueryTest with RemoteSparkSession {
  private lazy val testData = spark.range(100).toDF("value")

  test("forPath") {
    withTempPath { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forPath(spark, dir.getAbsolutePath).toDF,
        testData.collect().toSeq
      )
    }
  }

  test("forName") {
    withTable("deltaTable") {
      testData.write.format("delta").saveAsTable("deltaTable")
      checkAnswer(
        DeltaTable.forName(spark, "deltaTable").toDF,
        testData.collect().toSeq
      )
    }
  }

  test("as") {
    withTempPath { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forPath(spark, dir.getAbsolutePath).as("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq
      )
    }
  }

  test("history") {
    val session = spark
    import session.implicits._

    withTempPath { dir =>
      Seq(1, 2, 3).toDF().write.format("delta").save(dir.getAbsolutePath)
      Seq(4, 5).toDF().write.format("delta").mode("append").save(dir.getAbsolutePath)

      val table = DeltaTable.forPath(spark, dir.getAbsolutePath)
      checkAnswer(
        table.history().select("version"),
        Seq(Row(0L), Row(1L))
      )
    }
  }

  test("detail") {
    val session = spark
    import session.implicits._

    withTempPath { dir =>
      Seq(1, 2, 3).toDF().write.format("delta").save(dir.getAbsolutePath)

      val deltaTable = DeltaTable.forPath(spark, dir.getAbsolutePath)
      checkAnswer(
        deltaTable.detail().select("format"),
        Seq(Row("delta"))
      )
    }
  }

  test("isDeltaTable - path - with _delta_log dir") {
    withTempPath { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      assert(DeltaTable.isDeltaTable(spark, dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with empty _delta_log dir") {
    withTempPath { dir =>
      new File(dir, "_delta_log").mkdirs()
      assert(!DeltaTable.isDeltaTable(spark, dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with no _delta_log dir") {
    withTempPath { dir =>
      assert(!DeltaTable.isDeltaTable(spark, dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with non-existent dir") {
    withTempPath { dir =>
      assert(!DeltaTable.isDeltaTable(spark, dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - with non-Delta table path") {
    withTempPath { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      assert(!DeltaTable.isDeltaTable(spark, dir.getAbsolutePath))
    }
  }

  test("delete") {
    val session = spark
    import session.implicits._
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)).toDF("key", "value")
        .write.format("delta").save(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.delete("key = 'a'")
      checkAnswer(deltaTable.toDF, Seq(Row("b", 2), Row("c", 3), Row("d", 4)))

      deltaTable.delete(col("key") === lit("b"))
      checkAnswer(deltaTable.toDF, Seq(Row("c", 3), Row("d", 4)))

      deltaTable.delete()
      checkAnswer(deltaTable.toDF, Nil)
    }
  }

  test("update") {
    val session = spark
    import session.implicits._
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4)).toDF("key", "value")
        .write.format("delta").save(path)
      val deltaTable = DeltaTable.forPath(spark, path)

      deltaTable.updateExpr("key = 'a' or key = 'b'", Map("value" -> "1"))
      checkAnswer(deltaTable.toDF, Seq(Row("a", 1), Row("b", 1), Row("c", 3), Row("d", 4)))

      deltaTable.update(col("key") === lit("a") || col("key") === lit("b"), Map("value" -> lit(0)))
      checkAnswer(deltaTable.toDF, Seq(Row("a", 0), Row("b", 0), Row("c", 3), Row("d", 4)))

      deltaTable.updateExpr(Map("value" -> "-1"))
      checkAnswer(deltaTable.toDF, Seq(Row("a", -1), Row("b", -1), Row("c", -1), Row("d", -1)))

      deltaTable.update(Map("value" -> lit(37)))
      checkAnswer(deltaTable.toDF, Seq(Row("a", 37), Row("b", 37), Row("c", 37), Row("d", 37)))
    }
  }

  private def writeOptimizeTestData(path: String): Unit = {
    testData
      .withColumn("col1", col("value") % 7)
      .withColumn("col2", col("value") % 27)
      .withColumn("p", col("value") % 10)
      .repartition(numPartitions = 4)
      .write.partitionBy("p").format("delta").save(path)
  }

  private def checkOptimizeMetrics(
      result: DataFrame, numFilesAdded: Long, numFilesRemoved: Long): Unit = {
    val metrics = result.select("metrics.*").head()
    assert(metrics.getLong(0) === numFilesAdded)
    assert(metrics.getLong(1) === numFilesRemoved)
  }

  private def checkOptimizeHistory(
      table: DeltaTable, expectedPredicates: Seq[String], expectedZorderCols: Seq[String]): Unit = {
    val session = table.toDF.sparkSession
    import session.implicits._

    val (operation, operationParameters) = table.history()
      .select("operation", "operationParameters")
      .as[(String, Map[String, String])]
      .head()
    assert(operation === "OPTIMIZE")
    assert(operationParameters("predicate") ===
      expectedPredicates.map(p => s"""\"($p)\"""").mkString(start = "[", sep = ",", end = "]"))
    assert(operationParameters("zOrderBy") ===
      expectedZorderCols.map(c => s"""\"$c\"""").mkString(start = "[", sep = ",", end = "]"))
  }

  test("optimize - compaction") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      writeOptimizeTestData(path)

      val numDataFilesPreCompaction =
        spark.read.format("delta").load(path)
          .select("_metadata.file_path")
          .distinct().count()

      val table = io.delta.tables.DeltaTable.forPath(spark, path)
      val result = table.optimize().executeCompaction()

      checkOptimizeMetrics(result, numFilesAdded = 10, numFilesRemoved = numDataFilesPreCompaction)
      checkOptimizeHistory(table, expectedPredicates = Nil, expectedZorderCols = Nil)
    }
  }

  test("optimize - compaction - with partition filter") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      writeOptimizeTestData(path)

      val numDataFilesPreCompaction =
        spark.read.format("delta").load(path)
          .where("p = 2")
          .select("_metadata.file_path")
          .distinct().count()

      val table = io.delta.tables.DeltaTable.forPath(spark, path)
      val result = table.optimize().where("p = 2").executeCompaction()

      checkOptimizeMetrics(result, numFilesAdded = 1, numFilesRemoved = numDataFilesPreCompaction)
      checkOptimizeHistory(table, expectedPredicates = Seq("'p = 2"), expectedZorderCols = Nil)
    }
  }

  test("optimize - zorder") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      writeOptimizeTestData(path)

      val numDataFilesPreZOrder =
        spark.read.format("delta").load(path)
          .select("_metadata.file_path")
          .distinct().count()

      val table = io.delta.tables.DeltaTable.forPath(spark, path)
      val result = table.optimize().executeZOrderBy("col1", "col2")

      checkOptimizeMetrics(result, numFilesAdded = 10, numFilesRemoved = numDataFilesPreZOrder)
      checkOptimizeHistory(
        table, expectedPredicates = Nil, expectedZorderCols = Seq("col1", "col2"))
    }
  }

  test("optimize - zorder - with partition filter") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath

      writeOptimizeTestData(path)

      val numDataFilesPreZOrder =
        spark.read.format("delta").load(path)
          .where("p = 2")
          .select("_metadata.file_path")
          .distinct().count()

      val table = io.delta.tables.DeltaTable.forPath(spark, path)
      val result = table.optimize().where("p = 2").executeZOrderBy("col1", "col2")

      checkOptimizeMetrics(result, numFilesAdded = 1, numFilesRemoved = numDataFilesPreZOrder)
      checkOptimizeHistory(
        table, expectedPredicates = Seq("'p = 2"), expectedZorderCols = Seq("col1", "col2"))
    }
  }

  private def getTimestampForVersion(path: String, version: Long): String = {
    val logPath = new File(path, "_delta_log")
    val file = new File(logPath, f"$version%020d.json")
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    sdf.format(file.lastModified())
  }

  test("restore") {
    val session = spark
    import session.implicits._
    withTempPath { dir =>
      val path = dir.getPath

      val df1 = Seq(1, 2, 3).toDF("id")
      val df2 = Seq(4, 5).toDF("id")
      val df3 = Seq(6, 7).toDF("id")

      // version 0.
      df1.write.format("delta").save(path)
      // version 1.
      df2.write.format("delta").mode("append").save(path)
      // version 2.
      df3.write.format("delta").mode("append").save(path)

      checkAnswer(
        spark.read.format("delta").load(path),
        df1.union(df2).union(df3))

      val deltaTable = io.delta.tables.DeltaTable.forPath(spark, path)
      deltaTable.restoreToVersion(1)

      checkAnswer(
        spark.read.format("delta").load(path),
        df1.union(df2))

      val deltaTable2 = io.delta.tables.DeltaTable.forPath(spark, path)
      val timestamp = getTimestampForVersion(path, version = 0)
      deltaTable2.restoreToTimestamp(timestamp)

      checkAnswer(
        spark.read.format("delta").load(path),
        df1)
    }
  }
}
