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

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.{types, Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, FileSourceGeneratedMetadataStructField}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types.{DataType, LongType, MetadataBuilder, StructField}

object RowCommitVersion {

  val METADATA_STRUCT_FIELD_NAME = "row_commit_version"

  val QUALIFIED_COLUMN_NAME = s"${FileFormat.METADATA_NAME}.$METADATA_STRUCT_FIELD_NAME"

  def createMetadataStructField(
      protocol: Protocol,
      metadata: Metadata,
      nullable: Boolean = false): Option[StructField] =
    MaterializedRowCommitVersion.getMaterializedColumnName(protocol, metadata)
      .map(MetadataStructField(_, nullable))

  /**
   * Add a new column to `dataFrame` that has the name of the materialized Row Commit Version column
   * and holds Row Commit Versions. The column also is tagged with the appropriate metadata such
   * that it can be used to write materialized Row Commit Versions.
   */
  private[delta] def preserveRowCommitVersions(
      dataFrame: DataFrame,
      snapshot: SnapshotDescriptor): DataFrame = {
    if (!RowTracking.isEnabled(snapshot.protocol, snapshot.metadata)) {
      return dataFrame
    }

    val materializedColumnName = MaterializedRowCommitVersion.getMaterializedColumnNameOrThrow(
      snapshot.protocol, snapshot.metadata, snapshot.deltaLog.tableId)

    val rowCommitVersionColumn =
      DeltaTableUtils.getFileMetadataColumn(dataFrame).getField(METADATA_STRUCT_FIELD_NAME)
    preserveRowCommitVersionsUnsafe(dataFrame, materializedColumnName, rowCommitVersionColumn)
  }

  private[delta] def preserveRowCommitVersionsUnsafe(
      dataFrame: DataFrame,
      materializedColumnName: String,
      rowCommitVersionColumn: Column): DataFrame = {
    dataFrame
      .withColumn(materializedColumnName, rowCommitVersionColumn)
      .withMetadata(materializedColumnName, MetadataStructField.metadata(materializedColumnName))
  }

  object MetadataStructField {
    private val METADATA_COL_ATTR_KEY = "__row_commit_version_metadata_col"

    def apply(materializedColumnName: String, nullable: Boolean = false): StructField =
      StructField(
        METADATA_STRUCT_FIELD_NAME,
        LongType,
        // The Row commit version field is used to read the materialized Row commit version value
        // which is nullable. The actual Row commit version expression is created using a projection
        // injected before the optimizer pass by the [[GenerateRowIDs] rule at which point the Row
        // commit version field is non-nullable.
        nullable,
        metadata = metadata(materializedColumnName))

    def unapply(field: StructField): Option[StructField] =
      Option.when(isValid(field.dataType, field.metadata))(field)

    def metadata(materializedColumnName: String): types.Metadata = new MetadataBuilder()
      .withMetadata(
        FileSourceGeneratedMetadataStructField.metadata(
          METADATA_STRUCT_FIELD_NAME, materializedColumnName))
      .putBoolean(METADATA_COL_ATTR_KEY, value = true)
      .build()

    /** Return true if the column is a Row Commit Version column. */
    def isRowCommitVersionColumn(structField: StructField): Boolean =
      isValid(structField.dataType, structField.metadata)

    private[delta] def isValid(dataType: DataType, metadata: types.Metadata): Boolean = {
      FileSourceGeneratedMetadataStructField.isValid(dataType, metadata) &&
        metadata.contains(METADATA_COL_ATTR_KEY) &&
        metadata.getBoolean(METADATA_COL_ATTR_KEY)
    }
  }

  def columnMetadata(materializedColumnName: String): types.Metadata =
    MetadataStructField.metadata(materializedColumnName)

  object MetadataAttribute {
    def apply(materializedColumnName: String): AttributeReference =
      DataTypeUtils.toAttribute(MetadataStructField(materializedColumnName))
        .withName(materializedColumnName)

    def unapply(attr: Attribute): Option[Attribute] =
      if (isRowCommitVersionColumn(attr)) Some(attr) else None

    /** Return true if the column is a Row Commit Version column. */
    def isRowCommitVersionColumn(attr: Attribute): Boolean =
      MetadataStructField.isValid(attr.dataType, attr.metadata)
  }
}
