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

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.tables.DeltaTableTestUtils

import org.apache.spark.sql.{functions, Row}

class DeleteScalaSuite extends DeleteSuiteBase with DeltaSQLCommandTest {

  import testImplicits._

  test("delete usage test - without condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = io.delta.tables.DeltaTable.forPath(tempPath)
    table.delete()
    checkAnswer(readDeltaTable(tempPath), Nil)
  }

  test("delete usage test - with condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = io.delta.tables.DeltaTable.forPath(tempPath)
    table.delete("key = 1 or key = 2")
    checkAnswer(readDeltaTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  test("delete usage test - with Column condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = io.delta.tables.DeltaTable.forPath(tempPath)
    table.delete(functions.expr("key = 1 or key = 2"))
    checkAnswer(readDeltaTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  override protected def executeDelete(target: String, where: String = null): Unit = {
    import DeltaTestUtils.TableIdentifierOrPath
    val deltaTable: io.delta.tables.DeltaTable =
      DeltaTestUtils.getTableIdentifierOrPath(target) match {
        case TableIdentifierOrPath.Identifier(id, optionalAlias) =>
          val table = DeltaTableTestUtils.createTable(
            spark.table(id.unquotedString),
            DeltaLog.forTable(spark, id.unquotedString))
          optionalAlias.map(table.as(_)).getOrElse(table)
        case TableIdentifierOrPath.Path(path, optionalAlias) =>
          val table = io.delta.tables.DeltaTable.forPath(spark, path)
          optionalAlias.map(table.as(_)).getOrElse(table)
      }

    if (where != null) {
      deltaTable.delete(where)
    } else {
      deltaTable.delete()
    }
  }
}
