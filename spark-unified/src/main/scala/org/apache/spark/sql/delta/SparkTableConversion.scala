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
import org.apache.spark.sql.catalyst.analysis.{NamedRelation, ResolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.TimeTravel
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
 * (legacy V1 connector) in DataSourceV2Relation nodes and all operations that use Delta tables.
 *
 * This conversion is necessary because SparkTable is a read-only implementation that
 * lacks many features supported by DeltaTableV2 (streaming, DML operations, etc.).
 * By converting SparkTable to DeltaTableV2 early in the analysis phase, all existing
 * Delta analysis rules (AppendDelta, OverwriteDelta, DeltaRelation, MergeInto, etc.)
 * will work seamlessly.
 *
 * Implementation Notes:
 * - Uses `resolveOperatorsDown` which automatically traverses all children of LogicalPlan nodes
 * - Explicit cases are needed ONLY for non-child fields
 *   (e.g., `AppendData.table`, `MergeIntoTable.targetTable`)
 * - Operations where tables are children
 *   (e.g., `CloneTableStatement.source`, `RestoreTableStatement.table`,
 *   `DescribeDeltaHistory.child`, `WriteToStream.inputQuery`) don't need explicit cases
 *   because `resolveOperatorsDown` will traverse them automatically
 *
 * The conversion handles:
 * - DataSourceV2Relation nodes (for reads and as children of other operators)
 * - TimeTravel wrappers around DataSourceV2Relation (for time travel queries)
 * - V2 write commands (AppendData, OverwriteByExpression, OverwritePartitionsDynamic)
 *   - explicit cases needed for non-child table fields
 * - DML operations (DeleteFromTable, UpdateTable, MergeIntoTable)
 *   - explicit cases needed for non-child table fields
 * - ResolvedTable nodes (for catalog operations like ShowColumns, DeltaReorgTable)
 * - DDL operations (CloneTableStatement, RestoreTableStatement, DescribeDeltaHistory,
 *   WriteToStream) - handled automatically via child traversal
 *
 * Note: Streaming sources (readStream) are explicitly excluded as SparkTable doesn't support
 * streaming reads, but streaming writes are supported via DeltaSink.
 */
class ConvertSparkTableToDeltaTableV2(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperatorsDown {
      // Handle DataSourceV2Relation nodes (for reads and as children of operators)
      case SparkTableRelation(dsv2, sparkTable) =>
        dsv2.copy(table = convertToDeltaTableV2(sparkTable))

      // Handle TimeTravel wrapper around DataSourceV2Relation or ResolvedTable
      // PreprocessTimeTravel may resolve to either DataSourceV2Relation or ResolvedTable
      // After converting SparkTable to DeltaTableV2, we need to convert ResolvedTable to
      // DataSourceV2Relation so that DeltaAnalysis can match it (DeltaAnalysis expects
      // TimeTravel(DataSourceV2Relation(DeltaTableV2, ...)))
      case tt @ TimeTravel(relation, timestamp, version, creationSource) =>
        convertTimeTravelRelation(relation) match {
          case Some(convertedRelation) => tt.copy(relation = convertedRelation)
          case None => tt
        }

      // Handle AppendData (INSERT INTO)
      case a: AppendData if isDeltaKernelTable(a.table) =>
        a.copy(table = convertTableRelation(a.table))

      // Handle OverwriteByExpression (INSERT OVERWRITE)
      case o: OverwriteByExpression if isDeltaKernelTable(o.table) =>
        o.copy(table = convertTableRelation(o.table))

      // Handle OverwritePartitionsDynamic (dynamic partition overwrite)
      case o: OverwritePartitionsDynamic if isDeltaKernelTable(o.table) =>
        o.copy(table = convertTableRelation(o.table))

      // Handle DeleteFromTable (DELETE FROM)
      case d: DeleteFromTable if isDeltaKernelTable(d.table) =>
        d.copy(table = convertTableRelation(d.table))

      // Handle UpdateTable (UPDATE)
      case u: UpdateTable if isDeltaKernelTable(u.table) =>
        u.copy(table = convertTableRelation(u.table))

      // Handle MergeIntoTable (MERGE INTO) - convert target table
      case m: MergeIntoTable if isDeltaKernelTable(m.targetTable) =>
        m.copy(targetTable = convertTableRelation(m.targetTable))

      // Handle ResolvedTable (for catalog operations like ShowColumns, DeltaReorgTable)
      // Note: ResolvedTable.table is a Table object (not a LogicalPlan child),
      // so we need explicit handling
      case rt @ ResolvedTable(catalog, ident, sparkTable: SparkTable, output) =>
        val convertedTable = convertToDeltaTableV2(sparkTable)
        rt.copy(table = convertedTable)

      // Note: The following operations have table references as CHILDREN, so
      // resolveOperatorsDown will traverse them automatically. No explicit cases needed:
      // - CloneTableStatement: source and target are children (left/right of BinaryNode)
      // - RestoreTableStatement: table is the child (UnaryNode)
      // - DescribeDeltaHistory: child is a LogicalPlan (UnaryNode)
      // - WriteToStream: inputQuery is the child (UnaryNode)
      // The DataSourceV2Relation, TimeTravel, and ResolvedTable cases above will catch
      // SparkTable nodes when resolveOperatorsDown traverses these children.
    }
  }

  /** Checks if the relation contains a SparkTable */
  private def isDeltaKernelTable(relation: LogicalPlan): Boolean = {
    SparkTableRelation.unapply(relation).isDefined
  }

  /**
   * Converts a table relation containing SparkTable to DataSourceV2Relation with DeltaTableV2.
   * If the relation doesn't contain SparkTable, returns it unchanged.
   * The return type is NamedRelation because that's what the table fields expect.
   */
  private def convertTableRelation(relation: LogicalPlan): NamedRelation = {
    SparkTableRelation.unapply(relation) match {
      case Some((dsv2, sparkTable)) =>
        dsv2.copy(table = convertToDeltaTableV2(sparkTable))
      case None =>
        relation.asInstanceOf[NamedRelation]
    }
  }

  /**
   * Converts a relation inside TimeTravel to DataSourceV2Relation(DeltaTableV2) if it contains
   * SparkTable or DeltaTableV2. Returns None if the relation is not a Delta table.
   * This ensures DeltaAnalysis can match TimeTravel(DataSourceV2Relation(DeltaTableV2, ...)).
   */
  private def convertTimeTravelRelation(relation: LogicalPlan): Option[DataSourceV2Relation] = {
    relation match {
      // Case 1: DataSourceV2Relation(SparkTable) -> DataSourceV2Relation(DeltaTableV2)
      case SparkTableRelation(dsv2, sparkTable) =>
        Some(dsv2.copy(table = convertToDeltaTableV2(sparkTable)))

      // Case 2: ResolvedTable(SparkTable or DeltaTableV2) -> DataSourceV2Relation(DeltaTableV2)
      case rt @ ResolvedTable(catalog, ident, table, _) =>
        val deltaTableV2 = table match {
          case st: SparkTable => Some(convertToDeltaTableV2(st))
          case dt: DeltaTableV2 => Some(dt)
          case _ => None // Not a Delta table
        }
        deltaTableV2.map { dt =>
          import org.apache.spark.sql.util.CaseInsensitiveStringMap
          DataSourceV2Relation.create(
            dt,
            Some(catalog),
            Some(ident),
            CaseInsensitiveStringMap.empty
          )
        }

      // Case 3: Other relation types (not SparkTable/DeltaTableV2)
      case _ => None
    }
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

