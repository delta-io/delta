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

import org.apache.spark.sql.delta.test.{DeltaExcludedTestMixin, DeltaSQLCommandTest}

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
        val rowsToDelete = (1 to 1000 by 42).mkString("(", ", ", ")")
        executeDelete("tab", s"id in $rowsToDelete")
        sql("SELECT id, _change_type FROM tab").collect().foreach { row =>
          val _change_type = row.getString(1)
          assert(_change_type === "foo", s"Invalid _change_type for id=${row.get(0)}")
        }
      }
    }
  }
}


class DeleteSQLNameColumnMappingSuite extends DeleteSQLSuite
  with DeltaColumnMappingEnableNameMode {


  protected override def runOnlyTests: Seq[String] = Seq(true, false).map { isPartitioned =>
    s"basic case - delete from a Delta table by name - Partition=$isPartitioned"
  } ++ Seq(true, false).flatMap { isPartitioned =>
    Seq(
      s"where key columns - Partition=$isPartitioned",
      s"where data columns - Partition=$isPartitioned")
  }

}

class DeleteSQLWithDeletionVectorsSuite extends DeleteSQLSuite
  with DeltaExcludedTestMixin
  with DeletionVectorsTestUtils {
  override def beforeAll(): Unit = {
    super.beforeAll()
    enableDeletionVectorsForDeletes(spark)
  }

  override def excluded: Seq[String] = super.excluded ++
    Seq(
      // The following two tests must fail when DV is used. Covered by another test case:
      // "throw error when non-pinned TahoeFileIndex snapshot is used".
      "data and partition columns - Partition=true Skipping=false",
      "data and partition columns - Partition=false Skipping=false",
      // The scan schema contains additional row index filter columns.
      "nested schema pruning on data condition"
  )

  // This works correctly with DVs, but fails in classic DELETE.
  override def testSuperSetColsTempView(): Unit = {
    testComplexTempViews("superset cols")(
      text = "SELECT key, value, 1 FROM tab",
      expectResult = Row(0, 3, 1) :: Nil)
  }

}
