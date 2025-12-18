/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.connector.catalog.TableChange.AddColumn

/**
 * In Spark 4.2 QualifiedColType stores `default` as a DefaultValueExpression (same as Spark 4.1)
 */
object QualifiedColTypeShims {

  def getDefaultValueArgFromAddColumn(col: AddColumn): Option[DefaultValueExpression] = {
    Option(col.defaultValue).map(v =>
    DefaultValueExpression(
      org.apache.spark.sql.catalyst.parser.CatalystSqlParser.parseExpression(
        v.getSql()),
      v.getSql()))
  }

  def getDefaultValueStr(col: QualifiedColType): Option[String] = {
    col.default.map { value =>
      value match {
        case DefaultValueExpression(_, originalSQL, _) => originalSQL
        case _ => value.toString
      }
    }
  }
}
