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

import org.apache.spark.sql.delta.tables.RemoteSparkSession
import java.io.File
import java.text.SimpleDateFormat

import org.apache.spark.sql.Row
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
