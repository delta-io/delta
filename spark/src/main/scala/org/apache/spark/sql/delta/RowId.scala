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

import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.propertyKey
import org.apache.spark.sql.util.ScalaExtensions._

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, FileSourceConstantMetadataStructField, FileSourceGeneratedMetadataStructField}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{DataType, LongType, MetadataBuilder, StructField}

/**
 * Collection of helpers to handle Row IDs.
 *
 * This file includes the following Row ID features:
 * - Enabling Row IDs using table feature and table property.
 * - Assigning fresh Row IDs.
 * - Reading back Row IDs.
 * - Preserving stable Row IDs.
 */
object RowId {
  /**
   * Metadata domain for the high water mark stored using a [[DomainMetadata]] action.
   */
  case class RowTrackingMetadataDomain(rowIdHighWaterMark: Long)
      extends JsonMetadataDomain[RowTrackingMetadataDomain] {
    override val domainName: String = RowTrackingMetadataDomain.domainName
  }

  object RowTrackingMetadataDomain extends JsonMetadataDomainUtils[RowTrackingMetadataDomain] {
    override protected val domainName = "delta.rowTracking"

    def unapply(action: Action): Option[RowTrackingMetadataDomain] = action match {
      case d: DomainMetadata if d.domain == domainName => Some(fromJsonConfiguration(d))
      case _ => None
    }
  }

  val MISSING_HIGH_WATER_MARK: Long = -1L

  /**
   * Returns whether the protocol version supports the Row ID table feature. Whenever Row IDs are
   * supported, fresh Row IDs must be assigned to all newly committed files, even when Row IDs are
   * disabled in the current table version.
   */
  def isSupported(protocol: Protocol): Boolean = RowTracking.isSupported(protocol)

  /**
   * Returns whether Row IDs are enabled on this table version. Checks that Row IDs are supported,
   * which is a pre-requisite for enabling Row IDs, throws an error if not.
   */
  def isEnabled(protocol: Protocol, metadata: Metadata): Boolean = {
    val isEnabled = DeltaConfigs.ROW_TRACKING_ENABLED.fromMetaData(metadata)
    if (isEnabled && !isSupported(protocol)) {
      throw new IllegalStateException(
        s"Table property '${DeltaConfigs.ROW_TRACKING_ENABLED.key}' is " +
        s"set on the table but this table version doesn't support table feature " +
        s"'${propertyKey(RowTrackingFeature)}'.")
    }
    isEnabled
  }

  /**
   * Assigns fresh row IDs to all AddFiles inside `actions` that do not have row IDs yet and emits
   * a [[RowIdHighWaterMark]] action with the new high-water mark.
   */
  private[delta] def assignFreshRowIds(
      protocol: Protocol,
      snapshot: Snapshot,
      actions: Iterator[Action]): Iterator[Action] = {
    if (!isSupported(protocol)) return actions

    val oldHighWatermark = extractHighWatermark(snapshot).getOrElse(MISSING_HIGH_WATER_MARK)

    var newHighWatermark = oldHighWatermark

    val actionsWithFreshRowIds = actions.map {
      case a: AddFile if a.baseRowId.isEmpty =>
        val baseRowId = newHighWatermark + 1L
        newHighWatermark += a.numPhysicalRecords.getOrElse {
          throw DeltaErrors.rowIdAssignmentWithoutStats
        }
        a.copy(baseRowId = Some(baseRowId))
      case d: DomainMetadata if RowTrackingMetadataDomain.isSameDomain(d) =>
        throw new IllegalStateException(
          "Manually setting the Row ID high water mark is not allowed")
      case other => other
    }

    val newHighWatermarkAction: Iterator[Action] = new Iterator[Action] {
      // Iterators are lazy, so the first call to `hasNext` won't happen until after we
      // exhaust the remapped actions iterator. At that point, the watermark (changed or not)
      // decides whether the iterator is empty or infinite; take(1) below to bound it.
      override def hasNext: Boolean = newHighWatermark != oldHighWatermark
      override def next(): Action = RowTrackingMetadataDomain(newHighWatermark).toDomainMetadata
    }
    actionsWithFreshRowIds ++ newHighWatermarkAction.take(1)
  }

  /**
   * Extracts the high watermark of row IDs from a snapshot.
   */
  private[delta] def extractHighWatermark(snapshot: Snapshot): Option[Long] =
    RowTrackingMetadataDomain.fromSnapshot(snapshot).map(_.rowIdHighWaterMark)

  /** Base Row ID column name */
  val BASE_ROW_ID = "base_row_id"

  /*
   * A specialization of [[FileSourceConstantMetadataStructField]] used to represent base RowId
   * columns.
   */
  object BaseRowIdMetadataStructField {
    private val BASE_ROW_ID_METADATA_COL_ATTR_KEY = s"__base_row_id_metadata_col"

    def metadata: types.Metadata = new MetadataBuilder()
      .withMetadata(FileSourceConstantMetadataStructField.metadata(BASE_ROW_ID))
      .putBoolean(BASE_ROW_ID_METADATA_COL_ATTR_KEY, value = true)
      .build()

    def apply(): StructField =
      StructField(
        BASE_ROW_ID,
        LongType,
        nullable = false,
        metadata = metadata)

    def unapply(field: StructField): Option[StructField] =
      Some(field).filter(isBaseRowIdColumn)

    /** Return true if the column is a base Row ID column. */
    def isBaseRowIdColumn(structField: StructField): Boolean =
      isValid(structField.dataType, structField.metadata)

    def isValid(dataType: DataType, metadata: types.Metadata): Boolean = {
      FileSourceConstantMetadataStructField.isValid(dataType, metadata) &&
        metadata.contains(BASE_ROW_ID_METADATA_COL_ATTR_KEY) &&
        metadata.getBoolean(BASE_ROW_ID_METADATA_COL_ATTR_KEY)
    }
  }

  /**
   * The field readers can use to access the base row id column.
   */
  def createBaseRowIdField(protocol: Protocol, metadata: Metadata): Option[StructField] =
    Option.when(RowId.isEnabled(protocol, metadata)) {
      BaseRowIdMetadataStructField()
    }

  /** Row ID column name */
  val ROW_ID = "row_id"

  val QUALIFIED_COLUMN_NAME = s"${FileFormat.METADATA_NAME}.${ROW_ID}"

  /** Column metadata to be used in conjunction [[QUALIFIED_COLUMN_NAME]] to mark row id columns */
  def columnMetadata(materializedColumnName: String): types.Metadata =
    RowIdMetadataStructField.metadata(materializedColumnName)

  /**
   * The field readers can use to access the generated row id column. The scanner's internal column
   * name is obtained from the table's metadata.
   */
  def createRowIdField(protocol: Protocol, metadata: Metadata, nullable: Boolean)
  : Option[StructField] =
    MaterializedRowId.getMaterializedColumnName(protocol, metadata)
      .map(RowIdMetadataStructField(_, nullable))

  /*
   * A specialization of [[FileSourceGeneratedMetadataStructField]] used to represent RowId columns.
   *
   * - Row ID columns can be read by adding '_metadata.row_id' to the read schema
   * - To write to the materialized Row ID column
   *     - use the materialized Row ID column name which can be obtained using
   *       [[getMaterializedColumnName]]
   *     - add [[COLUMN_METADATA]] which is part of [[RowId]] as metadata to the column
   *     - nulls are replaced with fresh Row IDs
   */
  object RowIdMetadataStructField {

    val ROW_ID_METADATA_COL_ATTR_KEY = "__row_id_metadata_col"

    def metadata(materializedColumnName: String): types.Metadata = new MetadataBuilder()
      .withMetadata(
        FileSourceGeneratedMetadataStructField.metadata(RowId.ROW_ID, materializedColumnName))
      .putBoolean(ROW_ID_METADATA_COL_ATTR_KEY, value = true)
      .build()

    def apply(materializedColumnName: String, nullable: Boolean = false): StructField =
      StructField(
        RowId.ROW_ID,
        LongType,
        // The Row ID field is used to read the materialized Row ID value which is nullable. The
        // actual Row ID expression is created using a projection injected before the optimizer pass
        // by the [[GenerateRowIDs] rule at which point the Row ID field is non-nullable.
        nullable,
        metadata = metadata(materializedColumnName))

    def unapply(field: StructField): Option[StructField] =
      if (isRowIdColumn(field)) Some(field) else None

    /** Return true if the column is a Row Id column. */
    def isRowIdColumn(structField: StructField): Boolean =
      isValid(structField.dataType, structField.metadata)

    def isValid(dataType: DataType, metadata: types.Metadata): Boolean = {
      FileSourceGeneratedMetadataStructField.isValid(dataType, metadata) &&
        metadata.contains(ROW_ID_METADATA_COL_ATTR_KEY) &&
        metadata.getBoolean(ROW_ID_METADATA_COL_ATTR_KEY)
    }
  }

  object RowIdMetadataAttribute {
    /** Creates an attribute for writing out the materialized column name */
    def apply(materializedColumnName: String): AttributeReference =
      DataTypeUtils.toAttribute(RowIdMetadataStructField(materializedColumnName))
        .withName(materializedColumnName)

    def unapply(attr: Attribute): Option[Attribute] =
      if (isRowIdColumn(attr)) Some(attr) else None

    /** Return true if the column is a Row Id column. */
    def isRowIdColumn(attr: Attribute): Boolean =
      RowIdMetadataStructField.isValid(attr.dataType, attr.metadata)
  }

  /**
   * Throw if row tracking is supported and columns in the write schema tagged as materialized row
   * IDs do not reference the materialized row id column name.
   */
  private[delta] def throwIfMaterializedRowIdColumnNameIsInvalid(
      data: DataFrame, metadata: Metadata, protocol: Protocol, tableId: String): Unit = {
    if (!RowTracking.isEnabled(protocol, metadata)) {
      return
    }

    val materializedColumnName =
      metadata.configuration.get(MaterializedRowId.MATERIALIZED_COLUMN_NAME_PROP)

    if (materializedColumnName.isEmpty) {
      // If row tracking is enabled, a missing materialized column name is a bug and we need to
      // throw an error. If row tracking is only supported, we should just return, as it's fine
      // for the materialized column to not be assigned.
      if (RowTracking.isEnabled(protocol, metadata)) {
        throw DeltaErrors.materializedRowIdMetadataMissing(tableId)
      }
      return
    }

    toAttributes(data.schema).foreach {
      case RowIdMetadataAttribute(attribute) =>
        if (attribute.name != materializedColumnName.get) {
          throw new UnsupportedOperationException("Materialized Row IDs column name " +
            s"${attribute.name} is invalid. Must be ${materializedColumnName.get}.")
        }
      case _ =>
    }
  }

  /**
   * Add a new column to 'dataFrame' that has the name of the materialized Row ID column and holds
   * Row IDs. The column also is tagged with the appropriate metadata such that it can be used to
   * write materialized Row IDs.
   */
  private[delta] def preserveRowIds(
      dataFrame: DataFrame,
      snapshot: SnapshotDescriptor): DataFrame = {
    if (!isEnabled(snapshot.protocol, snapshot.metadata)) {
      return dataFrame
    }

    val materializedColumnName = MaterializedRowId.getMaterializedColumnNameOrThrow(
      snapshot.protocol, snapshot.metadata, snapshot.deltaLog.tableId)

    val rowIdColumn = DeltaTableUtils.getFileMetadataColumn(dataFrame).getField(ROW_ID)
    preserveRowIdsUnsafe(dataFrame, materializedColumnName, rowIdColumn)
  }

  /**
   * Add a new column to 'dataFrame' that has 'materializedColumnName' and holds Row IDs. The column
   * is also tagged with the appropriate metadata so it can be used to write materialized Row IDs.
   *
   * Internal method, exposed only for testing.
   */
  private[delta] def preserveRowIdsUnsafe(
      dataFrame: DataFrame,
      materializedColumnName: String,
      rowIdColumn: Column): DataFrame = {
    dataFrame
      .withColumn(materializedColumnName, rowIdColumn)
      .withMetadata(materializedColumnName, columnMetadata(materializedColumnName))
  }
}
