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

import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.delta.constraints.{AddConstraint, DropConstraint}

/**
 * The logical plan of the ALTER TABLE ... ADD CONSTRAINT command.
 */
case class AlterTableAddConstraint(
    table: LogicalPlan, constraintName: String, expr: String) extends AlterTableCommand {

  override def changes: Seq[TableChange] = Seq(AddConstraint(constraintName, expr))

  override protected def withNewChildInternal(newChildren: LogicalPlan): AlterTableAddConstraint =
    copy(table = newChildren)
}

/**
 * The logical plan of the ALTER TABLE ... DROP CONSTRAINT command.
 */
case class AlterTableDropConstraint(
    table: LogicalPlan, constraintName: String) extends AlterTableCommand {

  override def changes: Seq[TableChange] = Seq(DropConstraint(constraintName))

  override protected def withNewChildInternal(newChildren: LogicalPlan): AlterTableDropConstraint =
    copy(table = newChildren)
}
