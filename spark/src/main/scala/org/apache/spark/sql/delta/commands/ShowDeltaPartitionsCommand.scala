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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaTableIdentifier}
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{PartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types.StringType

/**
 * Command to show partitions for a Delta table.
 * This command reads partition information from the Delta transaction log.
 *
 * @param table The resolved Delta table
 * @param partitionSpec Optional partition specification from Spark's ShowPartitions command
 */
case class ShowDeltaPartitionsCommand(
    table: ResolvedTable,
    partitionSpec: Option[Map[String, String]] = None)
  extends LeafRunnableCommand with DeltaLogging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaTable = table.table match {
      case dt: DeltaTableV2 => dt
      case _ =>
        throw DeltaErrors.notADeltaTableException(
          "SHOW PARTITIONS",
          DeltaTableIdentifier(path = Some(table.identifier.toString))
        )
    }

    val deltaLog = DeltaLog.forTable(sparkSession, deltaTable.path)
    val snapshot = deltaLog.update()
    val partitionColumns = deltaTable.partitioning.map(_.references.head.fieldNames.head)

    // Return empty if table is not partitioned
    if (partitionColumns.isEmpty) {
      return Seq.empty[Row]
    }

    // Get all distinct partition values from the Delta log
    // scalastyle:off sparkimplicits
    import sparkSession.implicits._
    // scalastyle:on sparkimplicits

    val allFiles = snapshot.allFiles
    val partitionDF = allFiles
      .select(partitionColumns.map(col => $"partitionValues.$col"): _*)
      .distinct()

    // Apply partition spec filter if provided
    val filteredDF = partitionSpec match {
      case Some(spec) =>
        spec.foldLeft(partitionDF) { case (df, (key, value)) =>
          df.filter($"$key" === value)
        }
      case None => partitionDF
    }

    val partitions = filteredDF.collect().map { row =>
      Row.fromSeq(partitionColumns.indices.map { idx =>
        Option(row.get(idx)).map(_.toString).getOrElse("__HIVE_DEFAULT_PARTITION__")
      })
    }

    partitions.toSeq.sortBy(_.toString)
  }

  override val output: Seq[Attribute] = {
    // Dynamically create output schema based on partition columns
    val deltaTable = table.table.asInstanceOf[DeltaTableV2]
    val partitionColumns = deltaTable.partitioning.map(_.references.head.fieldNames.head)

    partitionColumns.map { colName =>
      AttributeReference(colName, StringType, nullable = false)()
    }
  }
}
