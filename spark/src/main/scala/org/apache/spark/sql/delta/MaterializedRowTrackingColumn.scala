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

import java.util.UUID

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Represents a materialized row tracking column. Concrete implementations are [[MaterializedRowId]]
 * and [[MaterializedRowCommitVersion]].
 */
abstract class MaterializedRowTrackingColumn {
  /**
   * Table metadata configuration property name storing the name of this materialized row tracking
   * column.
   */
  val MATERIALIZED_COLUMN_NAME_PROP: String

  /** Prefix to use for the name of this materialized row tracking column */
  val MATERIALIZED_COLUMN_NAME_PREFIX: String

  /**
   * Returns the exception to throw when the materialized column name is not set in the table
   * metadata. The table name is passed as argument.
   */
  def missingMetadataException: String => Throwable

  /**
   * Generate a random name for a materialized row tracking column. The generated name contains a
   * unique UUID, we assume it shall not conflict with existing column.
   */
  private def generateMaterializedColumnName: String =
    MATERIALIZED_COLUMN_NAME_PREFIX + UUID.randomUUID().toString

  /**
   * Update this materialized row tracking column name in the metadata.
   *   - If row tracking is not allowed or not supported, this operation is a noop.
   *   - If row tracking is supported on the table and no name is assigned to the old metadata, we
   *   assign a name. If a name was already assigned, we copy over this name.
   * Throws in case the assignment of a new name fails due to a conflict.
   */
  private[delta] def updateMaterializedColumnName(
      protocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata): Metadata = {
    if (!RowTracking.isSupported(protocol)) {
      // During a CLONE we might not enable row tracking, but still receive the materialized column
      // name from the source. In this case, we need to remove the column name to not have the same
      // column name in two different tables.
      return newMetadata.copy(
        configuration = newMetadata.configuration - MATERIALIZED_COLUMN_NAME_PROP)
    }

    // Take the materialized column name from the old metadata, as this is the materialized column
    // name of the current table. We overwrite the materialized column name of the new metadata as
    // it could contain a materialized column name from another table, e.g. the source table during
    // a CLONE.
    val materializedColumnName = oldMetadata.configuration
      .getOrElse(MATERIALIZED_COLUMN_NAME_PROP, generateMaterializedColumnName)
    newMetadata.copy(configuration = newMetadata.configuration +
      (MATERIALIZED_COLUMN_NAME_PROP -> materializedColumnName))
  }

  /**
   * Throws an exception if row tracking is allowed and the materialized column name conflicts with
   * another column name.
   */
  private[delta] def throwIfMaterializedColumnNameConflictsWithSchema(metadata: Metadata): Unit = {
    val logicalColumnNames = metadata.schema.fields.map(_.name)
    val physicalColumnNames = metadata.schema.fields
      .map(field => DeltaColumnMapping.getPhysicalName(field))

    metadata.configuration.get(MATERIALIZED_COLUMN_NAME_PROP).foreach { columnName =>
      if (logicalColumnNames.contains(columnName) || physicalColumnNames.contains(columnName)) {
        throw DeltaErrors.addingColumnWithInternalNameFailed(columnName)
      }
    }
  }

  /** Extract the materialized column name from the [[Metadata]] of a [[DeltaLog]]. */
  def getMaterializedColumnName(protocol: Protocol, metadata: Metadata): Option[String] = {
    if (RowTracking.isEnabled(protocol, metadata)) {
      metadata.configuration.get(MATERIALIZED_COLUMN_NAME_PROP)
    } else {
      None
    }
  }

  /** Convenience method that throws if the materialized column name cannot be extracted. */
  def getMaterializedColumnNameOrThrow(
      protocol: Protocol, metadata: Metadata, tableId: String): String = {
    getMaterializedColumnName(protocol, metadata).getOrElse {
      throw missingMetadataException(tableId)
    }
  }

  /**
   * If Row tracking is enabled, return an Expression referencing this Row tracking column Attribute
   * in 'dataFrame' if one is available. Otherwise returns None.
   */
  private[delta] def getAttribute(
      snapshot: Snapshot, dataFrame: DataFrame): Option[Attribute] = {
    if (!RowTracking.isEnabled(snapshot.protocol, snapshot.metadata)) {
      return None
    }

    val materializedColumnName = getMaterializedColumnNameOrThrow(
      snapshot.protocol, snapshot.metadata, snapshot.deltaLog.tableId)

    val analyzedPlan = dataFrame.queryExecution.analyzed
    analyzedPlan.outputSet.view.find(attr => materializedColumnName == attr.name)
  }
}

object MaterializedRowId extends MaterializedRowTrackingColumn {
  /**
   * Table metadata configuration property name storing the name of the column in which the
   * Row IDs are materialized.
   */
  val MATERIALIZED_COLUMN_NAME_PROP = "delta.rowTracking.materializedRowIdColumnName"

  /** Prefix to use for the name of the materialized Row ID column */
  val MATERIALIZED_COLUMN_NAME_PREFIX = "_row-id-col-"

  def missingMetadataException: String => Throwable = DeltaErrors.materializedRowIdMetadataMissing
}

object MaterializedRowCommitVersion extends MaterializedRowTrackingColumn {
  /**
   * Table metadata configuration property name storing the name of the column in which the
   * Row commit versions are materialized.
   */
  val MATERIALIZED_COLUMN_NAME_PROP = "delta.rowTracking.materializedRowCommitVersionColumnName"

  /** Prefix to use for the name of the materialized Row commit version column */
  val MATERIALIZED_COLUMN_NAME_PREFIX = "_row-commit-version-col-"

  def missingMetadataException: String => Throwable =
    DeltaErrors.materializedRowCommitVersionMetadataMissing
}
