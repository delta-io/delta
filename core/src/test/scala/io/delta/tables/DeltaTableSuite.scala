/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import java.util.Locale

import scala.language.postfixOps

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkException
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.test.SharedSparkSession

class DeltaTableSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  test("forPath") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forPath(spark, dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
      checkAnswer(
        DeltaTable.forPath(dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
    }
  }

  test("forPath - with non-Delta table path") {
    val msg = "not a delta table"
    withTempDir { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      testError(msg) { DeltaTable.forPath(spark, dir.getAbsolutePath) }
      testError(msg) { DeltaTable.forPath(dir.getAbsolutePath) }
    }
  }

  test("forName") {
    withTempDir { dir =>
      withTable("deltaTable") {
        testData.write.format("delta").saveAsTable("deltaTable")

        checkAnswer(
          DeltaTable.forName(spark, "deltaTable").toDF,
          testData.collect().toSeq)
        checkAnswer(
          DeltaTable.forName("deltaTable").toDF,
          testData.collect().toSeq)

      }
    }
  }

  def testForNameOnNonDeltaName(tableName: String): Unit = {
    val msg = "not a Delta table"
    testError(msg) { DeltaTable.forName(spark, tableName) }
    testError(msg) { DeltaTable.forName(tableName) }
  }

  test("forName - with non-Delta table name") {
    withTempDir { dir =>
      withTable("notADeltaTable") {
        testData.write.format("parquet").mode("overwrite").saveAsTable("notADeltaTable")
        testForNameOnNonDeltaName("notADeltaTable")
      }
    }
  }

  test("forName - with temp view name") {
    withTempDir { dir =>
      withTempView("viewOnDeltaTable") {
        testData.write.format("delta").save(dir.getAbsolutePath)
        spark.read.format("delta").load(dir.getAbsolutePath)
          .createOrReplaceTempView("viewOnDeltaTable")
        testForNameOnNonDeltaName("viewOnDeltaTable")
      }
    }
  }

  test("forName - with delta.`path`") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      testForNameOnNonDeltaName(s"delta.`$dir`")
    }
  }

  test("as") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forPath(dir.getAbsolutePath).as("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq)
    }
  }

  test("isDeltaTable - path - with _delta_log dir") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      assert(DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with empty _delta_log dir") {
    withTempDir { dir =>
      new File(dir, "_delta_log").mkdirs()
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with no _delta_log dir") {
    withTempDir { dir =>
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - path - with non-existent dir") {
    withTempDir { dir =>
      JavaUtils.deleteRecursively(dir)
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - with non-Delta table path") {
    withTempDir { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  def testError(expectedMsg: String)(thunk: => Unit): Unit = {
    val e = intercept[AnalysisException] { thunk }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
  }

  test("DeltaTable is Java Serializable but cannot be used in executors") {
    import testImplicits._

    // DeltaTable can be passed to executor without method calls.
    withTempDir { dir =>
      testData.write.format("delta").mode("append").save(dir.getAbsolutePath)
      val dt: DeltaTable = DeltaTable.forPath(dir.getAbsolutePath)
      spark.range(5).as[Long].map{ row: Long =>
        val foo = dt
        row + 3
      }.count()
    }

    // DeltaTable can be passed to executor but method call causes exception.
    val e = intercept[SparkException] {
      withTempDir { dir =>
        testData.write.format("delta").mode("append").save(dir.getAbsolutePath)
        val dt: DeltaTable = DeltaTable.forPath(dir.getAbsolutePath)
        spark.range(5).as[Long].map{ row: Long =>
          dt.toDF
          row + 3
        }.count()
      }
    }.getMessage
    assert(e.contains("DeltaTable cannot be used in executors"))
  }
}
