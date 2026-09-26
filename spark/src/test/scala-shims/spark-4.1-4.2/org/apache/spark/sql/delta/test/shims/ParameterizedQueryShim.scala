/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.test.shims

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.{NamedParameterContext, ParserInterface, PositionalParameterContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Shim for testing `ParserInterface.parsePlanWithParameters`, which only exists in Spark 4.1+.
 * This Spark 4.1/4.2 variant supports parsing with named parameters; the Spark 4.0 variant
 * reports that parser parameter substitution is unsupported so tests can cancel.
 */
object ParameterizedQueryShim {

  /** Whether this Spark version substitutes SQL parameters in the parser. */
  def supportsParserParameterSubstitution: Boolean = true

  /** Parses `sqlText` after substituting the given named parameter values. */
  def parsePlanWithNamedParameters(
      parser: ParserInterface,
      sqlText: String,
      params: Map[String, Any]): LogicalPlan = {
    val context = NamedParameterContext(params.map { case (name, value) => name -> Literal(value) })
    parser.parsePlanWithParameters(sqlText, context)
  }

  /** Parses `sqlText` after substituting the given positional parameter values. */
  def parsePlanWithPositionalParameters(
      parser: ParserInterface,
      sqlText: String,
      params: Seq[Any]): LogicalPlan = {
    val context = PositionalParameterContext(params.map(Literal(_)))
    parser.parsePlanWithParameters(sqlText, context)
  }

  /** Parses `sqlText` with an empty parameter context, as `SparkSession.sql(text)` does. */
  def parsePlanWithEmptyParameters(
      parser: ParserInterface,
      sqlText: String): LogicalPlan = {
    parser.parsePlanWithParameters(sqlText, NamedParameterContext(Map.empty))
  }
}
