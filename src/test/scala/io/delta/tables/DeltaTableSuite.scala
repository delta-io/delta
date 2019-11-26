/*
 * Copyright 2019 Databricks, Inc.
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

import java.util.Locale


import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class DeltaTableSuite extends QueryTest
  with SharedSparkSession {

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


  test("as") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      checkAnswer(
        DeltaTable.forPath(dir.getAbsolutePath).as("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq)
    }
  }

  test("isDeltaTable - path") {
    withTempDir { dir =>
      testData.write.format("delta").save(dir.getAbsolutePath)
      assert(DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  test("isDeltaTable - with non-Delta table path") {
    val msg = "not a delta table"
    withTempDir { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      assert(!DeltaTable.isDeltaTable(dir.getAbsolutePath))
    }
  }

  def testError(expectedMsg: String)(thunk: => Unit): Unit = {
    val e = intercept[AnalysisException] { thunk }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
  }
}
