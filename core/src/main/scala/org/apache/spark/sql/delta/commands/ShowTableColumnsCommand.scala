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

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.execution.command.LeafRunnableCommand

/**
 * The column format of the result returned by the `SHOW COLUMNS` command.
 */
case class TableColumns(col_name: String)

/**
 * A command listing all column names of a table.
 *
 * The syntax of using this command in SQL is:
 * {{{
 *   SHOW COLUMNS (FROM | IN) tableName [(FROM | IN) schemaName];
 * }}}
 *
 * @param tableID  the identifier of the Delta table
 */
case class ShowTableColumnsCommand(tableID: DeltaTableIdentifier)
  extends LeafRunnableCommand with DeltaCommand {

  override val output: Seq[Attribute] = ExpressionEncoder[TableColumns]().schema.toAttributes

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Return the schema from snapshot if it is an Delta table. Or raise
    // `DeltaErrors.notADeltaTableException` if it is a non-Delta table.
    val (deltaLog, snapshot) = DeltaLog.forTableWithSnapshot(sparkSession, tableID)
    recordDeltaOperation(deltaLog, "delta.ddl.showColumns") {
      if (snapshot.version < 0) {
        throw DeltaErrors.notADeltaTableException("SHOW COLUMNS")
      } else {
        snapshot.schema.fieldNames.map { x => Row(x) }.toSeq
      }
    }
  }
}
