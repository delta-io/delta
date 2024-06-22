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

package org.apache.spark.sql.catalyst

// scalastyle:off import.ordering.noEmptyLine

import com.databricks.spark.util.DatabricksLogging

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

/**
 * A logical node used to time travel the child relation to the given `timestamp` or `version`.
 * The `child` must support time travel, e.g. Delta, and cannot be a view, subquery or stream.
 * The timestamp expression cannot be a subquery. It must be a timestamp expression.
 * @param creationSource The API used to perform time travel, e.g. `atSyntax`, `dfReader` or SQL
 */
case class TimeTravel(
    relation: LogicalPlan,
    timestamp: Option[Expression],
    version: Option[Long],
    creationSource: Option[String]) extends LeafNode with DatabricksLogging {

  assert(version.isEmpty ^ timestamp.isEmpty,
    "Either the version or timestamp should be provided for time travel")

  override def output: Seq[Attribute] = Nil

  override lazy val resolved: Boolean = false
}
