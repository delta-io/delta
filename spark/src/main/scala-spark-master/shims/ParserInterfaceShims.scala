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

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

trait ParserInterfaceShims extends ParserInterface

object ParserInterfaceShims {
  def apply(parser: ParserInterface): ParserInterfaceShims = new ParserInterfaceShims {
    override def parsePlan(sqlText: String): LogicalPlan = parser.parsePlan(sqlText)

    override def parseQuery(sqlText: String): LogicalPlan = parser.parseQuery(sqlText)

    override def parseExpression(sqlText: String): Expression = parser.parseExpression(sqlText)

    override def parseTableIdentifier(sqlText: String): TableIdentifier =
      parser.parseTableIdentifier(sqlText)

    override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
      parser.parseFunctionIdentifier(sqlText)

    override def parseMultipartIdentifier (sqlText: String): Seq[String] =
      parser.parseMultipartIdentifier(sqlText)

    override def parseTableSchema(sqlText: String): StructType = parser.parseTableSchema(sqlText)

    override def parseDataType(sqlText: String): DataType = parser.parseDataType(sqlText)

    override def parseScript(sqlScriptText: String): CompoundBody =
      parser.parseScript(sqlScriptText)
  }
}
