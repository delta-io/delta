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

package org.apache.spark.sql.delta.skipping.clustering.temp

import org.apache.spark.sql.catalyst.plans.logical.{AlterTableCommand, LogicalPlan}
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.expressions.NamedReference

/**
 * The logical plan of the following commands:
 *  - ALTER TABLE ... CLUSTER BY (col1, col2, ...)
 *  - ALTER TABLE ... CLUSTER BY NONE
 */
case class AlterTableClusterBy(
    table: LogicalPlan, clusterBySpec: Option[ClusterBySpec]) extends AlterTableCommand {
  override def changes: Seq[TableChange] =
    Seq(ClusterBy(clusterBySpec
      .map(_.columnNames) // CLUSTER BY (col1, col2, ...)
      .getOrElse(Seq.empty))) // CLUSTER BY NONE

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(table = newChild)
}

/** A TableChange to alter clustering columns for a table. */
case class ClusterBy(clusteringColumns: Seq[NamedReference]) extends TableChange {}
