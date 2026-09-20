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

package io.delta.sql.parser

import org.apache.spark.sql.catalyst.parser.ParameterContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Version-gated implementation of `ParserInterface.parsePlanWithParameters` for Spark 4.1+.
 *
 * Delta keeps its own grammar dispatch and only forwards the parameter context on the paths that
 * delegate to Spark's parser, so Spark's substitution order, legacy parameter binding and
 * parameter-aware error positions are preserved.
 */
trait DeltaParserWithParametersShim { self: DeltaSqlParser =>

  override def parsePlanWithParameters(
      sqlText: String,
      parameterContext: ParameterContext): LogicalPlan = {
    parsePlanWithFallback(
      sqlText,
      substitutedSql => delegate.parsePlanWithParameters(substitutedSql, parameterContext))
  }
}
