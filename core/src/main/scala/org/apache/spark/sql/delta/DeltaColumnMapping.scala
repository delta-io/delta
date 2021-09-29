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

import scala.collection.mutable

import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.schema.SchemaMergingUtils

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{DataType, Metadata => SparkMetadata, MetadataBuilder, StructField, StructType}

object DeltaColumnMapping {
  val MIN_WRITER_VERSION = 5
  val MIN_READER_VERSION = 2
  val MIN_PROTOCOL_VERSION = Protocol(MIN_READER_VERSION, MIN_WRITER_VERSION)

  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"
  val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"
  val COLUMN_MAPPING_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"

  def requiresNewProtocol(metadata: Metadata): Boolean =
    DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(metadata) match {
      case IdMapping => true
      case NameMapping => true
      case NoMapping => false
    }

  def satisfyColumnMappingProtocol(protocol: Protocol): Boolean =
    protocol.minWriterVersion >= MIN_WRITER_VERSION &&
      protocol.minReaderVersion >= MIN_READER_VERSION

  /**
   * If the table is already on the column mapping protocol, we block:
   *     - changing column mapping config
   * otherwise, we block
   *     - upgrading to the column mapping Protocol through configurations
   */
  def verifyAndUpdateMetadataChange(
      oldProtocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean): Metadata = {
    // field in new metadata should have been dropped
    val oldMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(oldMetadata)
    val newMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(newMetadata)
    var updatedMetadata = newMetadata
    if (satisfyColumnMappingProtocol(oldProtocol)) {
      if (oldMappingMode != newMappingMode && !isCreatingNewTable) {
        // block changing modes on new protocol
        throw DeltaErrors.changeColumnMappingModeNotSupported
      }
    } else {
      if (oldMappingMode != newMappingMode && !isCreatingNewTable) {
        // block changing modes on old protocol
        throw DeltaErrors.setColumnMappingModeOnOldProtocol(oldProtocol)
      }
    }
    updatedMetadata = tryFixMetadata(newMetadata, newMappingMode)
    // Force the generation of the physical schema, which throws an exception if it fails,
    // e.g., some columns don't have IDs in ID mode
    updatedMetadata.schema
    updatedMetadata
  }

  def hasColumnId(field: StructField): Boolean =
    field.metadata.contains(COLUMN_MAPPING_METADATA_ID_KEY)

  def getColumnId(field: StructField): Int =
    field.metadata.getLong(COLUMN_MAPPING_METADATA_ID_KEY).toInt

  def hasPhysicalName(field: StructField): Boolean =
    field.metadata.contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)

  /**
   * Gets the required column metadata for each column based on the column mapping mode.
   */
  def getColumnMappingMetadata(field: StructField, mode: DeltaColumnMappingMode): SparkMetadata = {
    mode match {
      case NoMapping =>
        // drop all column mapping related fields
        new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .remove(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)
          .build()

      case IdMapping =>
        if (!hasColumnId(field)) {
          throw DeltaErrors.missingColumnId(IdMapping, field.name)
        }
        if (!hasPhysicalName(field)) {
          throw DeltaErrors.missingPhysicalName(IdMapping, field.name)
        }
        new MetadataBuilder()
          .withMetadata(field.metadata)
          .putLong(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY, getColumnId(field))
          .build()

      case NameMapping =>
        if (!hasPhysicalName(field)) {
          throw DeltaErrors.missingPhysicalName(NameMapping, field.name)
        }
        new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .build()

      case mode =>
        throw DeltaErrors.unknownColumnMappingMode(mode.name)
    }
  }


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
   * @return the table schema for the readers and writers. Columns will need to be renamed
   *         by using the `renameColumns` function.
   */
  def setColumnMetadata(schema: StructType, mode: DeltaColumnMappingMode): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.copy(metadata = getColumnMappingMetadata(field, mode))
    }
  }

  /** Recursively renames columns in the given schema with their physical schema. */
  def renameColumns(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.copy(name = getPhysicalName(field))
    }
  }

  def assignPhysicalNames(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      val existingName = if (hasPhysicalName(field)) Option(getPhysicalName(field)) else None
      val metadata = new MetadataBuilder()
        .withMetadata(field.metadata)
        .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, existingName.getOrElse(generatePhysicalName))
        .build()
      field.copy(metadata = metadata)
    }
  }

  def generatePhysicalName: String = "col-" + UUID.randomUUID()

  def getPhysicalName(field: StructField): String = {
    if (field.metadata.contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
      field.metadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
    } else {
      field.name
    }
  }

  def getPhysicalName(field: Attribute): String = {
    if (field.metadata.contains(COLUMN_MAPPING_PHYSICAL_NAME_KEY)) {
      field.metadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
    } else {
      field.name
    }
  }

  def tryFixMetadata(
      metadata: Metadata,
      mappingMode: DeltaColumnMappingMode): Metadata = {
    mappingMode match {
      case IdMapping =>
        assignColumnIdAndPhysicalName(metadata)
      case NoMapping =>
        metadata
      case mode =>
         throw DeltaErrors.unknownColumnMappingMode(mode.name)
    }
  }

  def findMaxColumnId(schema: StructType): Long = {
    var maxId: Long = 0
    SchemaMergingUtils.transformColumns(schema)((_, f, _) => {
      if (hasColumnId(f)) {
        maxId = maxId max getColumnId(f)
      }
      f
    })
    maxId
  }

  def checkColumnIdAndPhysicalNameAssignments(
      schema: StructType,
      mode: DeltaColumnMappingMode): Unit = {
    // physical name/column id -> full field path
    val columnIds = mutable.Map[Int, String]()
    val physicalNames = mutable.Map[String, String]()

    SchemaMergingUtils.transformColumns(schema) ( (parentPath, field, _) => {
      val curFullPath = (parentPath :+ field.name).mkString(".")
      if (!hasColumnId(field)) {
        throw DeltaErrors.missingColumnId(IdMapping, field.name)
      }
      if (!hasPhysicalName(field)) {
        throw DeltaErrors.missingPhysicalName(IdMapping, field.name)
      }
      val columnId = getColumnId(field)
      if (columnIds.contains(columnId)) {
        throw DeltaErrors.duplicatedColumnId(mode, curFullPath, columnIds(columnId))
      }
      columnIds.update(columnId, curFullPath)
      val physicalName = getPhysicalName(field)
      if (physicalNames.contains(physicalName)) {
        throw DeltaErrors.duplicatedPhysicalName(mode, curFullPath, physicalNames(physicalName))
      }
      physicalNames.update(physicalName, curFullPath)
      field
    })

  }

  /**
   * For each column/field in a Metadata's schema, assign id using the current maximum id
   * as the basis and increment from there, and assign physical name using UUID
   * @param metadata Metadata whose schema to modify
   * @return updated metadata
   */
  private def assignColumnIdAndPhysicalName(metadata: Metadata): Metadata = {
    // Cannot use metadata.schema here, as it will likely fail our column mapping checks
    val rawSchema = Option(metadata.schemaString)
      .map(s => DataType.fromJson(s).asInstanceOf[StructType])
      .getOrElse(new StructType())
    var maxId = DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetaData(metadata) max
                findMaxColumnId(rawSchema)

    val newSchema =
      SchemaMergingUtils.transformColumns(rawSchema)((_, field, _) => {
        val builder = new MetadataBuilder()
          .withMetadata(field.metadata)
        if (!hasColumnId(field)) {
          maxId += 1
          builder.putLong(COLUMN_MAPPING_METADATA_ID_KEY, maxId)
        }
        if (!hasPhysicalName(field)) {
          builder.putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, generatePhysicalName)
        }
        field.copy(metadata = builder.build())
      })

    metadata.copy(
      schemaString = newSchema.json,
      configuration =
        metadata.configuration ++
          Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> maxId.toString)
    )
  }
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

/**
 * Name Mapping uses the physical column name as the true identifier of a column. The physical name
 * is stored as part of StructField metadata in the schema and will be used when reading and writing
 * Parquet files. Even if id mapping can be used for reading the physical files, name mapping is
 * used for reading statistics and partition values in the DeltaLog.
 */
case object NameMapping extends DeltaColumnMappingMode {
  val name = "name"
}

object DeltaColumnMappingMode {
  def apply(name: String): DeltaColumnMappingMode = {
    name.toLowerCase(Locale.ROOT) match {
      case NoMapping.name => NoMapping
      case IdMapping.name => IdMapping
      case NameMapping.name => NameMapping
      case mode => throw DeltaErrors.unknownColumnMappingMode(mode)
    }
  }
}
