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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}

class UpdateSQLSuite extends UpdateSuiteBase  with DeltaSQLCommandTest {

  import testImplicits._

  test("explain") {
    append(Seq((2, 2)).toDF("key", "value"))
    val df = sql(s"EXPLAIN UPDATE delta.`$tempPath` SET key = 1, value = 2 WHERE key = 2")
    val outputs = df.collect().map(_.mkString).mkString
    assert(outputs.contains("Delta"))
    assert(!outputs.contains("index") && !outputs.contains("ActionLog"))
    // no change should be made by explain
    checkAnswer(readDeltaTableByPath(tempPath), Row(2, 2))
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
        QueryTest.checkAnswer(sql("UPDATE v SET key = 1 WHERE key = 0 AND value = 3"), Seq(Row(1)))
        checkAnswer(spark.table("tab"), Row(1, 3))
      }
    }
  }

  Seq(true, false).foreach { partitioned =>
    test(s"User defined _change_type column doesn't get dropped - partitioned=$partitioned") {
      withTable("tab") {
        sql(
          s"""CREATE TABLE tab USING DELTA
             |${if (partitioned) "PARTITIONED BY (part) " else ""}
             |TBLPROPERTIES (delta.enableChangeDataFeed = false)
             |AS SELECT id, int(id / 10) AS part, 'foo' as _change_type
             |FROM RANGE(1000)
             |""".stripMargin)
        val rowsToUpdate = (1 to 1000 by 42).mkString("(", ", ", ")")
        executeUpdate("tab", "_change_type = 'bar'", s"id in $rowsToUpdate")
        sql("SELECT id, _change_type FROM tab").collect().foreach { row =>
          val _change_type = row.getString(1)
          assert(_change_type === "foo" || _change_type === "bar",
            s"Invalid _change_type for id=${row.get(0)}")
        }
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
