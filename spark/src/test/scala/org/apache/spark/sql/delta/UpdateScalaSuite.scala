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

import org.apache.spark.sql.{functions, Row}

class UpdateScalaSuite extends UpdateSuiteBase
  with DeltaSQLCommandTest
  with DeltaExcludedTestMixin {

  import testImplicits._

  override def excluded: Seq[String] = super.excluded ++ Seq(
    // Exclude tempViews, because DeltaTable.forName does not resolve them correctly, so no one can
    // use them anyway with the Scala API.
    // scalastyle:off line.size.limit
    "different variations of column references - TempView",
    "test update on temp view - basic - Partition=true - SQL TempView",
    "test update on temp view - basic - Partition=true - Dataset TempView",
    "test update on temp view - basic - Partition=false - SQL TempView",
    "test update on temp view - basic - Partition=false - Dataset TempView",
    "test update on temp view - subset cols - SQL TempView",
    "test update on temp view - subset cols - Dataset TempView",
    "test update on temp view - superset cols - SQL TempView",
    "test update on temp view - superset cols - Dataset TempView",
    "test update on temp view - nontrivial projection - SQL TempView",
    "test update on temp view - nontrivial projection - Dataset TempView",
    "test update on temp view - view with too many internal aliases - SQL TempView",
    "test update on temp view - view with too many internal aliases - Dataset TempView",
    "test update on temp view - nontrivial projection with write amplification reduction - SQL TempView",
    "test update on temp view - nontrivial projection with write amplification reduction - Dataset TempView",
    "test update on temp view - view with too many internal aliases with write amplification reduction - SQL TempView",
    "test update on temp view - view with too many internal aliases with write amplification reduction - Dataset TempView",
    "test update on temp view - view with too many internal aliases with write amplification reduction - Dataset TempView"
    // scalastyle:on line.size.limit
  )

  test("update usage test - without condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = io.delta.tables.DeltaTable.forPath(tempPath)
    table.updateExpr(Map("key" -> "100"))
    checkAnswer(readDeltaTable(tempPath),
      Row(100, 10) :: Row(100, 20) :: Row(100, 30) :: Row(100, 40) :: Nil)
  }

  test("update usage test - without condition, using Column") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = io.delta.tables.DeltaTable.forPath(tempPath)
    table.update(Map("key" -> functions.expr("100")))
    checkAnswer(readDeltaTable(tempPath),
      Row(100, 10) :: Row(100, 20) :: Row(100, 30) :: Row(100, 40) :: Nil)
  }

  test("update usage test - with condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = io.delta.tables.DeltaTable.forPath(tempPath)
    table.updateExpr("key = 1 or key = 2", Map("key" -> "100"))
    checkAnswer(readDeltaTable(tempPath),
      Row(100, 10) :: Row(100, 20) :: Row(3, 30) :: Row(4, 40) :: Nil)
  }

  test("update usage test - with condition, using Column") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = io.delta.tables.DeltaTable.forPath(tempPath)
    table.update(functions.expr("key = 1 or key = 2"),
      Map("key" -> functions.expr("100"), "value" -> functions.expr("101")))
    checkAnswer(readDeltaTable(tempPath),
      Row(100, 101) :: Row(100, 101) :: Row(3, 30) :: Row(4, 40) :: Nil)
  }

  override protected def executeUpdate(
      target: String,
      set: String,
      where: String = null): Unit = {
    executeUpdate(target, set.split(","), where)
  }

  override protected def executeUpdate(
      target: String,
      set: Seq[String],
      where: String): Unit = {

    val deltaTable = DeltaTestUtils.getDeltaTableForIdentifierOrPath(
      spark,
      DeltaTestUtils.getTableIdentifierOrPath(target))

    val setColumns = set.map { assign =>
      val kv = assign.split("=")
      require(kv.size == 2)
      kv(0).trim -> kv(1).trim
    }.toMap

    if (where == null) {
      deltaTable.updateExpr(setColumns)
    } else {
      deltaTable.updateExpr(where, setColumns)
    }
  }
}
