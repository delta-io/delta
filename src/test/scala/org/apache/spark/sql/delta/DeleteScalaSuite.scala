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

package org.apache.spark.sql.delta

import io.delta.DeltaTable

class DeleteScalaSuite extends DeleteSuiteBase
{

  override protected def executeDelete(target: String, where: String = null): Unit = {

    def parse(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil => tableName -> None // just table name
        case tableName :: alias :: Nil => // tablename SPACE alias OR tab SPACE lename
          val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
          if (!alias.forall(ordinary.contains(_))) {
            (tableName + " " + alias) -> None
          } else {
            tableName -> Some(alias)
          }
        case _ =>
          fail(s"Could not build parse '$tableNameWithAlias' for table and optional alias")
      }
    }

    val deltaTable: io.delta.DeltaTable = {
      val (tableNameOrPath, optionalAlias) = parse(target)
      val isPath: Boolean = tableNameOrPath.startsWith("delta.")
      val table = if (isPath) {
        val path = tableNameOrPath.stripPrefix("delta.`").stripSuffix("`")
        io.delta.DeltaTable.forPath(spark, path)
      } else {
        new DeltaTable(spark.table(tableNameOrPath))
      }
      optionalAlias.map(table.as(_)).getOrElse(table)
    }

    if (where != null) {
      deltaTable.delete(where)
    } else {
      deltaTable.delete()
    }
  }
}
