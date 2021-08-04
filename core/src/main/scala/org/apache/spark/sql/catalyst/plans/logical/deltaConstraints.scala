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

/**
 * The logical plan of the ALTER TABLE ... ADD CONSTRAINT command.
 */
case class AlterTableAddConstraint(
    table: LogicalPlan, constraintName: String, expr: String) extends Command {
  // TODO: extend UnaryCommand when new Spark version released, now fails on OSS Delta build
  override def children: Seq[LogicalPlan] = Seq(table)
  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}

/**
 * The logical plan of the ALTER TABLE ... DROP CONSTRAINT command.
 */
case class AlterTableDropConstraint(
    table: LogicalPlan, constraintName: String) extends Command {
  // TODO: extend UnaryCommand when new Spark version released, now fails on OSS Delta build
  override def children: Seq[LogicalPlan] = Seq(table)
  // TODO: remove when the new Spark version is releases that has the withNewChildInternal method
}
