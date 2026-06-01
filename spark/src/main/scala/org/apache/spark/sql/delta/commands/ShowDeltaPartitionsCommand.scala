/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import org.apache.spark.sql.delta.{DataFrameUtils, DeltaErrors}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.DeltaDataSource

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation

/**
 * Command to show partitions for a Delta table.
 *
 * Unlike Spark's standard SHOW PARTITIONS output, which returns a single string column such as
 * `year=2024/month=01`, this Delta command returns one typed output column per partition column.
 * This matches Delta's runtime SHOW PARTITIONS behavior.
 *
 * The command executes through a Delta relation so that partition enumeration follows normal Delta
 * read semantics instead of directly scanning `snapshot.allFiles`.
 *
 * @param child The resolved Delta table
 * @param partitionSpec Optional partition spec to filter the returned partitions
 */
case class ShowDeltaPartitionsCommand(
    child: LogicalPlan,
    partitionSpec: Map[String, String] = Map.empty)
  extends RunnableCommand with UnaryNode with DeltaCommand {

  private lazy val deltaTable: DeltaTableV2 = getDeltaTable(child, "SHOW PARTITIONS")

  private lazy val snapshot = deltaTable.update()

  override def run(sparkSession: SparkSession): Seq[Row] = {
    recordDeltaOperation(deltaTable.deltaLog, "delta.ddl.showPartitions") {
      val partitionSchemaFields = snapshot.metadata.partitionSchema.fields.toSeq
      val partitionColumns = partitionSchemaFields.map(_.name)

      if (partitionColumns.isEmpty) {
        throw DeltaErrors.showPartitionInNotPartitionedTable(deltaTable.name())
      }

      val partitionFilters =
        if (partitionSpec.nonEmpty) {
          DeltaDataSource.verifyAndCreatePartitionFilters(
            deltaTable.path.toString,
            snapshot,
            partitionSpec.toSeq)
        } else {
          Nil
        }

      val baseRelation = deltaTable.deltaLog.createRelation(
        partitionFilters = partitionFilters,
        snapshotToUseOpt = Some(snapshot),
        catalogTableOpt = deltaTable.catalogTable)
      val filteredDf = DataFrameUtils.ofRows(sparkSession, LogicalRelation(baseRelation))

      filteredDf.select(partitionColumns.map(new Column(_)): _*)
        .distinct()
        .orderBy(partitionColumns.map(new Column(_)): _*)
        .collect()
        .map { row =>
          Row.fromSeq(partitionColumns.indices.map { idx =>
            val value = row.get(idx)
            if (value != null) {
              value
            } else if (partitionSchemaFields(idx).dataType ==
                org.apache.spark.sql.types.StringType) {
              ExternalCatalogUtils.DEFAULT_PARTITION_NAME
            } else {
              null
            }
          })
        }
        .toSeq
    }
  }

  override lazy val output: Seq[Attribute] = {
    snapshot.metadata.partitionSchema.fields.map { field =>
      AttributeReference(field.name, field.dataType, nullable = field.nullable)()
    }
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowDeltaPartitionsCommand = {
    copy(child = newChild, partitionSpec = partitionSpec)
  }
}
