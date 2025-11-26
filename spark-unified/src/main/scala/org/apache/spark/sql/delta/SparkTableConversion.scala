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

import scala.collection.JavaConverters._

import io.delta.kernel.spark.catalog.SparkTable
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * An analyzer rule that converts SparkTable (Kernel-based V2 connector) to DeltaTableV2
 * (legacy V1 connector) in DataSourceV2Relation nodes.
 *
 * This conversion is necessary because SparkTable is a read-only implementation that
 * lacks many features supported by DeltaTableV2. By converting SparkTable to DeltaTableV2
 * early in the analysis phase, all existing Delta analysis rules (AppendDelta, OverwriteDelta,
 * DeltaRelation, etc.) will work seamlessly.
 *
 * The conversion happens for all SparkTable instances, allowing the existing Delta
 * infrastructure to handle all table operations.
 */
class ConvertSparkTableToDeltaTableV2(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsDown {
      case dsv2 @ DataSourceV2Relation(sparkTable: SparkTable, _, _, _, _) =>
        // Convert SparkTable to DeltaTableV2
        val deltaTableV2 = convertToDeltaTableV2(sparkTable)
        dsv2.copy(table = deltaTableV2)
    }
  }

  /**
   * Converts a SparkTable to DeltaTableV2.
   *
   * @param sparkTable the SparkTable to convert
   * @return a DeltaTableV2 instance with the same table location and metadata
   */
  private def convertToDeltaTableV2(sparkTable: SparkTable): DeltaTableV2 = {
    val path = new Path(sparkTable.getTablePath)
    val catalogTable = sparkTable.getCatalogTable.map(ct => ct).orElse(None)
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

/**
 * Extractor object for matching SparkTable in DataSourceV2Relation and converting to DeltaTableV2.
 * This can be used in pattern matching to extract the converted DeltaTableV2.
 */
object SparkTableToDeltaTableV2 {
  def unapply(dsv2: DataSourceV2Relation)(implicit session: SparkSession)
      : Option[(DataSourceV2Relation, DeltaTableV2)] = {
    dsv2.table match {
      case sparkTable: SparkTable =>
        val path = new Path(sparkTable.getTablePath)
        val catalogTable = sparkTable.getCatalogTable.map(ct => ct).orElse(None)
        val options = sparkTable.getOptions.asScala.toMap
        val tableIdentifier = catalogTable.map(_.identifier.unquotedString)

        val deltaTableV2 = DeltaTableV2(
          spark = session,
          path = path,
          catalogTable = catalogTable,
          tableIdentifier = tableIdentifier,
          options = options
        )
        Some((dsv2, deltaTableV2))
      case _ => None
    }
  }
}

