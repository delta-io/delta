/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.spark.catalog.SparkTable
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Extractor for matching DataSourceV2Relation containing a SparkTable.
 * Returns the DataSourceV2Relation and the SparkTable if matched.
 */
private object SparkTableRelation {
  def unapply(relation: LogicalPlan): Option[(DataSourceV2Relation, SparkTable)] = {
    relation match {
      case dsv2 @ DataSourceV2Relation(sparkTable: SparkTable, _, _, _, _) =>
        Some((dsv2, sparkTable))
      case _ => None
    }
  }
}

/**
 * An analyzer rule that converts SparkTable (Kernel-based V2 connector) to DeltaTableV2
 * (legacy V1 connector) in DataSourceV2Relation nodes and V2 write commands.
 *
 * This conversion is necessary because SparkTable is a read-only implementation that
 * lacks many features supported by DeltaTableV2. By converting SparkTable to DeltaTableV2
 * early in the analysis phase, all existing Delta analysis rules (AppendDelta, OverwriteDelta,
 * DeltaRelation, etc.) will work seamlessly.
 *
 * The conversion handles:
 * - DataSourceV2Relation nodes (for reads and as children of other operators)
 * - V2 write commands (AppendData, OverwriteByExpression, OverwritePartitionsDynamic)
 *   where the table field is not a child and won't be transformed by resolveOperatorsDown
 */
class ConvertSparkTableToDeltaTableV2(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsDown {
      // Handle DataSourceV2Relation nodes (for reads)
      case SparkTableRelation(dsv2, sparkTable) =>
        dsv2.copy(table = convertToDeltaTableV2(sparkTable))

      // Handle AppendData (INSERT INTO)
      case a: AppendData if isDeltaKernelTable(a.table) =>
        val (dsv2, sparkTable) = extractSparkTable(a.table)
        a.copy(table = dsv2.copy(table = convertToDeltaTableV2(sparkTable)))

      // Handle OverwriteByExpression (INSERT OVERWRITE)
      case o: OverwriteByExpression if isDeltaKernelTable(o.table) =>
        val (dsv2, sparkTable) = extractSparkTable(o.table)
        o.copy(table = dsv2.copy(table = convertToDeltaTableV2(sparkTable)))

      // Handle OverwritePartitionsDynamic (dynamic partition overwrite)
      case o: OverwritePartitionsDynamic if isDeltaKernelTable(o.table) =>
        val (dsv2, sparkTable) = extractSparkTable(o.table)
        o.copy(table = dsv2.copy(table = convertToDeltaTableV2(sparkTable)))
    }
  }

  /** Checks if the relation contains a SparkTable */
  private def isDeltaKernelTable(relation: LogicalPlan): Boolean = {
    SparkTableRelation.unapply(relation).isDefined
  }

  /** Extracts DataSourceV2Relation and SparkTable from a relation */
  private def extractSparkTable(relation: LogicalPlan): (DataSourceV2Relation, SparkTable) = {
    SparkTableRelation.unapply(relation).get
  }

  /** Converts Java Optional to Scala Option */
  private def toScalaOption[T](opt: Optional[T]): Option[T] = {
    if (opt.isPresent) Some(opt.get()) else None
  }

  /**
   * Converts a SparkTable to DeltaTableV2.
   *
   * @param sparkTable the SparkTable to convert
   * @return a DeltaTableV2 instance with the same table location and metadata
   */
  private def convertToDeltaTableV2(sparkTable: SparkTable): DeltaTableV2 = {
    val path = new Path(sparkTable.getTablePath)
    val catalogTable: Option[CatalogTable] = toScalaOption(sparkTable.getCatalogTable)
    val options = sparkTable.getOptions.asScala.toMap

    // Create DeltaTableV2 with the same properties as SparkTable
    // The tableIdentifier is derived from the table name
    val tableIdentifier = catalogTable.map(_.identifier.unquotedString)

    DeltaTableV2(
      spark = session,
      path = path,
      catalogTable = catalogTable,
      tableIdentifier = tableIdentifier,
      options = options
    )
  }
}

