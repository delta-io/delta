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

/**
 * The logical plan of the following commands:
 *  - ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY (col1, col2, ...)
 *  - ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY NONE
 *  - ALTER TABLE ... REPLACE PARTITIONED BY WITH CLUSTER BY
 *
 * Converts a partitioned table into a clustered table (or an unpartitioned, unclustered table
 * with NONE) without rewriting data files. Without columns or NONE
 * (`clusterByPartitionColumns`), the partition columns become the clustering columns.
 */
case class AlterTableReplacePartitionedByWithClusterBy(
    table: LogicalPlan,
    clusterBySpec: Option[ClusterBySpec],
    clusterByPartitionColumns: Boolean = false) extends AlterTableCommand {
  override def changes: Seq[TableChange] =
    Seq(ReplacePartitionedByWithClusterBy(
      clusterBySpec.map(_.columnNames).getOrElse(Seq.empty), clusterByPartitionColumns))

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(table = newChild)
}

/**
 * A TableChange to replace the partitioning of a table with the given clustering columns, or
 * with the partition columns if `clusterByPartitionColumns` is set.
 */
case class ReplacePartitionedByWithClusterBy(
    clusteringColumns: Seq[NamedReference],
    clusterByPartitionColumns: Boolean = false) extends TableChange {}
