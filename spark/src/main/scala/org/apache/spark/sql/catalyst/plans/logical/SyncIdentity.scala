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

import org.apache.spark.sql.catalyst.analysis.FieldName
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableChange.ColumnChange

/**
 * A `ColumnChange` to model `ALTER TABLE ... ALTER (CHANGE) COLUMN ... SYNC IDENTITY` command.
 *
 * @param fieldNames The (potentially nested) column name.
 */
case class SyncIdentity(fieldNames: Array[String]) extends ColumnChange {
  require(fieldNames.size == 1, "IDENTITY column cannot be a nested column.")
}

case class AlterColumnSyncIdentity(table: LogicalPlan, column: FieldName)
    extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    require(column.resolved, "FieldName should be resolved before it's converted to TableChange.")
    val colName = column.name.toArray
    Seq(SyncIdentity(colName))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}
