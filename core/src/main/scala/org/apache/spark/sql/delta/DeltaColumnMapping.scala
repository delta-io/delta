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

import java.util.{Locale, UUID}

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.schema.SchemaMergingUtils

import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}

object DeltaColumnMapping {
  val MIN_WRITER_VERSION = 5
  val MIN_READER_VERSION = 2
  val MIN_PROTOCOL_VERSION = Protocol(MIN_READER_VERSION, MIN_WRITER_VERSION)

  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"
  val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"
  val COLUMN_MAPPING_METADATA_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"
  val COLUMN_MAPPING_METADATA_DISPLAY_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "displayName"

  def requiresNewProtocol(metadata: Metadata): Boolean =
    DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(metadata) match {
      case IdMapping => true
      case NoMapping => false
    }

  def satisfyColumnMappingProtocol(protocol: Protocol): Boolean =
    protocol.minWriterVersion >= MIN_WRITER_VERSION &&
      protocol.minReaderVersion >= MIN_READER_VERSION

  /**
   * If the table is already on the column mapping protocol, we block:
   *     - all schema changes
   *     - setting of column mapping config
   * otherwise, we block
   *     - upgrading to the column mapping Protocol through configurations
   */
  def verifyMetadataChange(
      oldProtocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Unit = {
    // Force the generation of physicalSchema, which throws exception if it fails,
    // e.g., some columns don't have IDs in ID mode
    newMetadata.physicalSchema
    if (isCreatingNewTable) return
    val oldMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(oldMetadata)
    val newMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(newMetadata)
    if (satisfyColumnMappingProtocol(oldProtocol)) {
      if (oldMetadata.schemaString != newMetadata.schemaString && oldMappingMode != NoMapping) {
        // This is only temporary, until we implement the schema change logic on the new protocol
        throw DeltaErrors.schemaChangeInColumnMappingProtocolNotSupported(
          oldMetadata.schema, newMetadata.schema, oldMappingMode)
      }
      if (oldMappingMode != newMappingMode) {
        throw DeltaErrors.changeColumnMappingModeNotSupported
      }
    } else {
      if (oldMappingMode != newMappingMode) {
        throw DeltaErrors.setColumnMappingModeOnOldProtocol(oldProtocol)
      }
    }
  }

  def hasColumnId(field: StructField): Boolean =
    field.metadata.contains(COLUMN_MAPPING_METADATA_ID_KEY)

  def getColumnId(field: StructField): Int =
    field.metadata.getLong(COLUMN_MAPPING_METADATA_ID_KEY).toInt

  def hasPhysicalName(field: StructField): Boolean =
    field.metadata.contains(COLUMN_MAPPING_METADATA_PHYSICAL_NAME_KEY)

  def getPhysicalName(field: StructField): String =
    field.metadata.getString(COLUMN_MAPPING_METADATA_PHYSICAL_NAME_KEY)

  /**
   * Prepares the table schema, to be used by the readers and writers of the table.
   *
   * In the new Delta protocol that supports column mapping, we persist various column mapping
   * metadata in the serialized schema of the Delta log. This method performs the necessary
   * transformation and filtering on these metadata based on the column mapping mode set for the
   * table.
   *
   * @param schema the raw schema directly deserialized from the Delta log, with various column
   *               mapping metadata.
   * @param mode column mapping mode of the table
   *
   * @return the table schema for the readers and writers
   */
  def createPhysicalSchema(schema: StructType, mode: DeltaColumnMappingMode): StructType = {
    if (mode == NoMapping) return schema
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      mode match {
        case IdMapping =>
          if (!hasColumnId(field)) {
            throw DeltaErrors.missingColumnId(IdMapping.name, field)
          }
          if (!hasPhysicalName(field)) {
            throw DeltaErrors.missingPhysicalName(IdMapping.name, field)
          }
          val metadata = new MetadataBuilder()
            .withMetadata(field.metadata)
            .putLong(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY, getColumnId(field))
            .putString(DeltaColumnMapping.COLUMN_MAPPING_METADATA_DISPLAY_NAME_KEY, field.name)
            .build()
          // TODO: replace name with getPhysicalName(field)
          field.copy(metadata = metadata)
        case mode =>
          throw DeltaErrors.unknownColumnMappingMode(mode.name)
      }
    }
  }

  def assignPhysicalNames(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      val metadata = new MetadataBuilder()
        .withMetadata(field.metadata)
        .putString(COLUMN_MAPPING_METADATA_PHYSICAL_NAME_KEY, generatePhysicalName)
        .build()
      field.copy(metadata = metadata)
    }
  }

  def generatePhysicalName: String = "col-" + UUID.randomUUID()
}

/**
 * A trait for Delta column mapping modes.
 */
sealed trait DeltaColumnMappingMode {
  def name: String
}

/**
 * No mapping mode uses a column's display name as its true identifier to
 * read and write data.
 *
 * This is the default mode and is the same mode as Delta always has been.
 */
case object NoMapping extends DeltaColumnMappingMode {
  val name = "none"
}

/**
 * Id Mapping uses column ID as the true identifier of a column. Column IDs are stored as
 * StructField metadata in the schema and will be used when reading and writing Parquet files.
 * The Parquet files in this mode will also have corresponding field Ids for each column in their
 * file schema.
 *
 * This mode is used for tables converted from Iceberg.
 */
case object IdMapping extends DeltaColumnMappingMode {
  val name = "id"
}

object DeltaColumnMappingMode {
  def apply(name: String): DeltaColumnMappingMode = {
    name.toLowerCase(Locale.ROOT) match {
      case NoMapping.name => NoMapping
      case IdMapping.name => IdMapping
      case mode => throw DeltaErrors.unknownColumnMappingMode(mode)
    }
  }
}
