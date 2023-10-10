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

package org.apache.spark.sql.delta.commands

import org.apache.spark.sql.delta.catalog.DeltaTableV2

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.UnaryLike
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.command.RunnableCommand

/**
 * The column format of the result returned by the `SHOW COLUMNS` command.
 */
case class TableColumns(col_name: String)

/**
 * A command for listing all column names of a Delta table.
 *
 * @param child The resolved Delta table
 */
case class ShowDeltaTableColumnsCommand(child: LogicalPlan)
  extends RunnableCommand with UnaryLike[LogicalPlan] with DeltaCommand {

  override val output: Seq[Attribute] = toAttributes(ExpressionEncoder[TableColumns]().schema)

  override protected def withNewChildInternal(newChild: LogicalPlan): ShowDeltaTableColumnsCommand =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Return the schema from snapshot if it is an Delta table. Or raise
    // `DeltaErrors.notADeltaTableException` if it is a non-Delta table.
    val deltaLog = getDeltaTable(child, "SHOW COLUMNS").deltaLog
    recordDeltaOperation(deltaLog, "delta.ddl.showColumns") {
      deltaLog.update().schema.fieldNames.map { x => Row(x) }.toSeq
    }
  }
}
