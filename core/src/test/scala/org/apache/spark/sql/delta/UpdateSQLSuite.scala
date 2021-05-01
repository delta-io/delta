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

class UpdateSQLSuite extends UpdateSuiteBase  with DeltaSQLCommandTest {

  import testImplicits._

  test("explain") {
    append(Seq((2, 2)).toDF("key", "value"))
    val df = sql(s"EXPLAIN UPDATE delta.`$tempPath` SET key = 1, value = 2 WHERE key = 2")
    val outputs = df.collect().map(_.mkString).mkString
    assert(outputs.contains("Delta"))
    assert(!outputs.contains("index") && !outputs.contains("ActionLog"))
    // no change should be made by explain
    checkAnswer(readDeltaTable(tempPath), Row(2, 2))
  }

  test("SC-11376: Update command should check target columns during analysis, same key") {
    val targetDF = spark.read.json(
      """
        {"a": {"c": {"d": 'random', "e": 'str'}, "g": 1}, "z": 10}
        {"a": {"c": {"d": 'random2', "e": 'str2'}, "g": 2}, "z": 20}"""
        .split("\n").toSeq.toDS())

    testAnalysisException(
      targetDF,
      set = "z = 30" :: "z = 40" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)

    testAnalysisException(
      targetDF,
      set = "a.c.d = 'rand'" :: "a.c.d = 'RANDOM2'" :: Nil,
      errMsgs = "There is a conflict from these SET columns" :: Nil)
  }

  test("update a dataset temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((0, 3)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        spark.table("tab").as("name").createTempView("v")
        sql("UPDATE v SET key = 1 WHERE key = 0 AND value = 3")
        checkAnswer(spark.table("tab"), Row(1, 3))
      }
    }
  }

  test("update a SQL temp view") {
    withTable("tab") {
      withTempView("v") {
        Seq((0, 3)).toDF("key", "value").write.format("delta").saveAsTable("tab")
        sql("CREATE TEMP VIEW v AS SELECT * FROM tab")
        sql("UPDATE v SET key = 1 WHERE key = 0 AND value = 3")
        checkAnswer(spark.table("tab"), Row(1, 3))
      }
    }
  }

  override protected def executeUpdate(
      target: String,
      set: String,
      where: String = null): Unit = {
    val whereClause = Option(where).map(c => s"WHERE $c").getOrElse("")
    sql(s"UPDATE $target SET $set $whereClause")
  }
}
