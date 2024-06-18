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

package org.apache.spark.sql.delta

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

/**
 * This rule adds a Project on top of Delta tables that support the Row tracking table feature to
 * provide a default generated Row ID and row commit version for rows that don't have them
 * materialized in the data file.
 */
object GenerateRowIDs extends Rule[LogicalPlan] {

  /**
   * Matcher for a scan on a Delta table that has Row tracking enabled.
   */
  private object DeltaScanWithRowTrackingEnabled {
    def unapply(plan: LogicalPlan): Option[LogicalRelation] = plan match {
      case scan @ LogicalRelation(relation: HadoopFsRelation, _, _, _) =>
        relation.fileFormat match {
          case format: DeltaParquetFileFormat
            if RowTracking.isEnabled(format.protocol, format.metadata) => Some(scan)
          case _ => None
        }
      case _ => None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithNewOutput {
    case DeltaScanWithRowTrackingEnabled(
            scan @ LogicalRelation(baseRelation: HadoopFsRelation, _, _, _)) =>
      // While Row IDs and commit versions are non-nullable, we'll use the Row ID & commit
      // version attributes to read the materialized values from now on, which can be null. We make
      // the materialized Row ID & commit version attributes nullable in the scan here.

      // Update nullability in the scan `metadataOutput` by updating the delta file format.
      val newFileFormat = baseRelation.fileFormat match {
        case format: DeltaParquetFileFormat =>
          format.copy(nullableRowTrackingFields = true)
      }
      val newBaseRelation = baseRelation.copy(fileFormat = newFileFormat)(baseRelation.sparkSession)

      // Update the output metadata column's data type (now with nullable row tracking fields).
      val newOutput = scan.output.map {
        case MetadataAttributeWithLogicalName(metadata, FileFormat.METADATA_NAME) =>
          metadata.withDataType(newFileFormat.createFileMetadataCol().dataType)
        case other => other
      }
      val newScan = scan.copy(relation = newBaseRelation, output = newOutput)
      newScan.copyTagsFrom(scan)

      // Add projection with row tracking column expressions.
      val updatedAttributes = mutable.Buffer.empty[(Attribute, Attribute)]
      val projectList = newOutput.map {
        case MetadataAttributeWithLogicalName(metadata, FileFormat.METADATA_NAME) =>
          val updatedMetadata = metadataWithRowTrackingColumnsProjection(metadata)
          updatedAttributes += metadata -> updatedMetadata.toAttribute
          updatedMetadata
        case other => other
      }
      Project(projectList = projectList, child = newScan) -> updatedAttributes.toSeq
    case o =>
      val newPlan = o.transformExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
        // Recurse into subquery plans. Similar to how [[transformUpWithSubqueries]] works except
        // that it allows us to still use [[transformUpWithNewOutput]] on subquery plans to
        // correctly update references to the metadata attribute when going up the plan.
        // Get around type erasure by explicitly checking the plan type and removing warning.
        case planExpression: PlanExpression[LogicalPlan @unchecked]
          if planExpression.plan.isInstanceOf[LogicalPlan] =>
            planExpression.withNewPlan(apply(planExpression.plan))
      }
      newPlan -> Nil
  }

  /**
   * Expression that reads the Row IDs from the materialized Row ID column if the value is
   * present and returns the default generated Row ID using the file's base Row ID and current row
   * index if not:
   *    coalesce(_metadata.row_id, _metadata.base_row_id + _metadata.row_index).
   */
  private def rowIdExpr(metadata: AttributeReference): Expression = {
    Coalesce(Seq(
      getField(metadata, RowId.ROW_ID),
      Add(
        getField(metadata, RowId.BASE_ROW_ID),
        getField(metadata, ParquetFileFormat.ROW_INDEX))))
  }

  /**
   * Expression that reads the Row commit versions from the materialized Row commit version column
   * if the value is present and returns the default Row commit version from the file if not:
   *    coalesce(_metadata.row_commit_Version, _metadata.default_row_commit_version).
   */
  private def rowCommitVersionExpr(metadata: AttributeReference): Expression = {
    Coalesce(Seq(
      getField(metadata, RowCommitVersion.METADATA_STRUCT_FIELD_NAME),
      getField(metadata, DefaultRowCommitVersion.METADATA_STRUCT_FIELD_NAME)))
  }

  /**
   * Extract a field from the metadata column.
   */
  private def getField(metadata: AttributeReference, name: String): GetStructField = {
    ExtractValue(metadata, Literal(name), conf.resolver) match {
      case field: GetStructField => field
      case _ =>
        throw new IllegalStateException(s"The metadata column '${metadata.name}' is not a struct.")
    }
  }

  /**
   * Create a new metadata struct where the Row ID and row commit version values are populated using
   * the materialized values if present, or the default Row ID / row commit version values if not.
   */
  private def metadataWithRowTrackingColumnsProjection(
      metadata: AttributeReference): NamedExpression = {
    val metadataFields = metadata.dataType.asInstanceOf[StructType].map {
      case field if field.name == RowId.ROW_ID =>
        field -> rowIdExpr(metadata)
      case field if field.name == RowCommitVersion.METADATA_STRUCT_FIELD_NAME =>
        field -> rowCommitVersionExpr(metadata)
      case field =>
        field -> getField(metadata, field.name)
    }.flatMap { case (oldField, newExpr) =>
      // Propagate the type metadata from the old fields to the new fields.
      val newField = Alias(newExpr, oldField.name)(explicitMetadata = Some(oldField.metadata))
      Seq(Literal(oldField.name), newField)
    }
    Alias(CreateNamedStruct(metadataFields), metadata.name)()
  }
}
