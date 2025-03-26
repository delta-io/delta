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
import org.apache.spark.sql.execution.datasources.{FileFormat, HadoopFsRelation, LogicalRelation, LogicalRelationWithTable}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

/**
 * This rule adds a Project on top of Delta tables that support the Row tracking table feature to
 * provide a default generated Row ID and row commit version for rows that don't have them
 * materialized in the data file.
 *
 * Note: This rule adds a Project node on top of the scan that will prevent _metadata column
 * propagation through SubQueryAlias nodes. For that reason, it must be run after analysis
 * completed.
 */
object GenerateRowIDs extends Rule[LogicalPlan] {

  /**
   * Matcher for a scan on a Delta table that has Row tracking enabled.
   */
  private object DeltaScanWithRowTrackingEnabled {
    def unapply(plan: LogicalPlan): Option[(LogicalRelation, DeltaParquetFileFormat)] = plan match {
      case scan @ LogicalRelationWithTable(relation: HadoopFsRelation, _) =>
        relation.fileFormat match {
          case format: DeltaParquetFileFormat
            if RowTracking.isEnabled(format.protocol, format.metadata) => Some(scan, format)
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Helper method to check if the given scan returns either `_metadata.row_id` or
   * `_metadata.row_commit_version` metadata fields. Use to skip applying the rule if we don't
   * need to generate expressions for these fields.
   */
  private def usesRowTrackingFields(scan: LogicalRelation): Boolean = {
    scan.output.exists {
      case MetadataAttributeWithLogicalName(a, FileFormat.METADATA_NAME) =>
        a.dataType.asInstanceOf[StructType].fieldNames.exists {
          case RowId.ROW_ID | RowCommitVersion.METADATA_STRUCT_FIELD_NAME => true
          case _ => false
        }
      case _ => false
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithNewOutput {
    case node @ DeltaScanWithRowTrackingEnabled(scan, fileFormat)
      if node.resolved && usesRowTrackingFields(scan) && !fileFormat.rowTrackingFieldsUpdated =>
      // Set rowTrackingFieldsUpdated so that:
      // 1. We don't apply this rule again on the same plan.
      // 2. Row tracking metadata fields `_metadata.row_id` and `_metadata.row_commit_version`
      //    become nullable, since these will reference the materialized fields from now on.
      val newFileFormat = fileFormat.copy(rowTrackingFieldsUpdated = true)

      // Keep track of the updated metadata attribute to replace all references to it in the plan
      // with [[transformUpWithNewOutput]].
      val updatedAttributes = mutable.Buffer.empty[(Attribute, Attribute)]

      // Generate default Row ID and row commit version values:
      // 1. Update the `_metadata` attribute returned from the scan so that it includes fields that
      //    the expressions will reference. See [[metadataWithRequiredRowTrackingColumns]].
      // 2. Add a Project on top of the scan that will apply expressions to generate the default
      //    Row ID and row commit version values. See [[metadataWithRowTrackingColumnsProjection]].
      val (scanOutput, projectList) = scan.output.map {
        case MetadataAttributeWithLogicalName(originalMetadata, FileFormat.METADATA_NAME) =>
          val newScanMetadata =
            metadataWithRequiredRowTrackingColumns(originalMetadata, newFileFormat)
          val projectedMetadata =
            metadataWithRowTrackingColumnsProjection(originalMetadata, newScanMetadata)
          updatedAttributes += originalMetadata -> projectedMetadata.toAttribute
          (newScanMetadata, projectedMetadata)
        // Not a metadata field, return as is from scan and projection.
        case other => (other, other)
      }.unzip

      val newScan = DeltaTableUtils.replaceFileFormat(scan.copy(output = scanOutput), newFileFormat)
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
   * Creates a new metadata attribute with additional row tracking columns that are required to
   * generate the default row ID and row commit version values:
   * - For `_metadata.row_id`, we need `_metadata.base_row_id` and `_metadata.row_index`.
   * - For `_metadata.row_commit_version`, we need `_metadata.default_row_commit_version`.
   */
  private def metadataWithRequiredRowTrackingColumns(
      originalMetadata: AttributeReference,
      fileFormat: DeltaParquetFileFormat): AttributeReference = {
    def originalMetadataContains(fieldName: String): Boolean =
      originalMetadata.dataType.asInstanceOf[StructType].exists(_.name == fieldName)

    // Create a new metadata struct with all fields, then prune the ones we don't need.
    val newMetadataType = fileFormat.createFileMetadataCol().dataType.asInstanceOf[StructType]

    val metadataFieldNames = newMetadataType.fieldNames.filter {
      case RowId.BASE_ROW_ID if originalMetadataContains(RowId.ROW_ID) => true
      case ParquetFileFormat.ROW_INDEX if originalMetadataContains(RowId.ROW_ID) => true
      case DefaultRowCommitVersion.METADATA_STRUCT_FIELD_NAME
        if originalMetadataContains(RowCommitVersion.METADATA_STRUCT_FIELD_NAME) => true
      case other => originalMetadataContains(other)
    }
    val metadataFields = newMetadataType.filter(f => metadataFieldNames.contains(f.name))
    // Update the original metadata to include all required fields.
    originalMetadata.withDataType(StructType(metadataFields))
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
      originalMetadata: AttributeReference, scanMetadata: AttributeReference): NamedExpression = {
    val metadataFields = originalMetadata.dataType.asInstanceOf[StructType].map {
      case field if field.name == RowId.ROW_ID =>
        field -> rowIdExpr(scanMetadata)
      case field if field.name == RowCommitVersion.METADATA_STRUCT_FIELD_NAME =>
        field -> rowCommitVersionExpr(scanMetadata)
      case field =>
        field -> getField(scanMetadata, field.name)
    }.flatMap { case (oldField, newExpr) =>
      // Propagate the type metadata from the old fields to the new fields.
      val newField = Alias(newExpr, oldField.name)(explicitMetadata = Some(oldField.metadata))
      Seq(Literal(oldField.name), newField)
    }
    Alias(CreateNamedStruct(metadataFields), originalMetadata.name)(
      explicitMetadata = Some(originalMetadata.metadata))
  }
}
