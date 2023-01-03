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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * CLONE TABLE statement, as parsed from SQL
 *
 * @param source source plan for table to be cloned
 * @param target target path or table name where clone should be instantiated
 * @param ifNotExists if a table exists at the target, we should not go through with the clone
 * @param isReplaceCommand when true, replace the target table if one exists
 * @param isCreateCommand when true, create the target table if none exists
 * @param tablePropertyOverrides user-defined table properties that should override any properties
 *                        with the same key from the source table
 * @param targetLocation if target is a table name then user can provide a targetLocation to
 *                       create an external table with this location
 */
case class CloneTableStatement(
    source: LogicalPlan,
    target: LogicalPlan,
    ifNotExists: Boolean,
    isReplaceCommand: Boolean,
    isCreateCommand: Boolean,
    tablePropertyOverrides: Map[String, String],
    targetLocation: Option[String]) extends BinaryNode {
  override def output: Seq[Attribute] = Nil

  override def left: LogicalPlan = source
  override def right: LogicalPlan = target
  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): CloneTableStatement =
    copy(source = newLeft, target = newRight)
}
