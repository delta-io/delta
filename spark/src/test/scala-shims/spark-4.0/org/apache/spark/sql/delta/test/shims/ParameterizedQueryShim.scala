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

import io.delta.sql.parser.DeltaSqlParser

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Shim for testing `ParserInterface.parsePlanWithParameters`, which only exists in Spark 4.1+.
 * This Spark 4.0 variant reports that parser parameter substitution is unsupported (Spark 4.0
 * substitutes SQL parameters during analysis instead), so tests can cancel.
 */
object ParameterizedQueryShim {

  /** Whether this Spark version substitutes SQL parameters in the parser. */
  def supportsParserParameterSubstitution: Boolean = false

  /** Not supported on Spark 4.0; tests must check [[supportsParserParameterSubstitution]] first. */
  def parsePlanWithNamedParameters(
      parser: DeltaSqlParser,
      sqlText: String,
      params: Map[String, Any]): LogicalPlan = {
    throw new UnsupportedOperationException(
      "Spark 4.0 substitutes SQL parameters during analysis, not in the parser")
  }

  /** Not supported on Spark 4.0; tests must check [[supportsParserParameterSubstitution]] first. */
  def parsePlanWithPositionalParameters(
      parser: DeltaSqlParser,
      sqlText: String,
      params: Seq[Any]): LogicalPlan = {
    throw new UnsupportedOperationException(
      "Spark 4.0 substitutes SQL parameters during analysis, not in the parser")
  }

  /** Not supported on Spark 4.0; tests must check [[supportsParserParameterSubstitution]] first. */
  def parsePlanWithEmptyParameters(
      parser: DeltaSqlParser,
      sqlText: String): LogicalPlan = {
    throw new UnsupportedOperationException(
      "Spark 4.0 substitutes SQL parameters during analysis, not in the parser")
  }
}
