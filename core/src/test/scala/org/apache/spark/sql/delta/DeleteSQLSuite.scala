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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.Row

class DeleteSQLSuite extends DeleteSuiteBase  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def executeDelete(target: String, where: String = null): Unit = {
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    sql(s"DELETE FROM $target $whereClause")
  }

  // For EXPLAIN, which is not supported in OSS
  test("explain") {
    append(Seq((2, 2)).toDF("key", "value"))
    val df = sql(s"EXPLAIN DELETE FROM delta.`$tempPath` WHERE key = 2")
    val outputs = df.collect().map(_.mkString).mkString
    assert(outputs.contains("Delta"))
    assert(!outputs.contains("index") && !outputs.contains("ActionLog"))
    // no change should be made by explain
    checkAnswer(readDeltaTable(tempPath), Row(2, 2))
  }

  test("delete from a temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((1, 1), (0, 3), (1, 5)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        spark.table("tab").as("name").createTempView("v")
        sql("DELETE FROM v WHERE key = 1")
        checkAnswer(spark.table("tab"), Row(0, 3))
      }
    }
  }

  test("delete from a SQL temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((1, 1), (0, 3), (1, 5)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        sql("CREATE TEMP VIEW v AS SELECT * FROM tab")
        sql("DELETE FROM v WHERE key = 1 AND VALUE = 5")
        checkAnswer(spark.table("tab"), Seq(Row(1, 1), Row(0, 3)))
      }
    }
  }
}
