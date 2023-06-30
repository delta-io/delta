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

import org.apache.spark.sql.catalyst.TimeTravel

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * RESTORE TABLE statement as parsed from SQL
 *
 * @param table - logical node of the table that will be restored, internally contains either
 *              version or timestamp.
 */
case class RestoreTableStatement(table: TimeTravel) extends UnaryNode {

  override def child: LogicalPlan = table

  override def output: Seq[Attribute] = Nil

  override protected def withNewChildInternal(newChild: LogicalPlan): RestoreTableStatement =
    copy(table = newChild.asInstanceOf[TimeTravel])
}
