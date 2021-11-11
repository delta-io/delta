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
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.types.{Metadata => SparkMetadata, MetadataBuilder, StructField, StructType}

object DeltaColumnMapping
{
  val MIN_WRITER_VERSION = 5
  val MIN_READER_VERSION = 2
  val MIN_PROTOCOL_VERSION = Protocol(MIN_READER_VERSION, MIN_WRITER_VERSION)

  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"
  val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"
  val COLUMN_MAPPING_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"

  def requiresNewProtocol(metadata: Metadata): Boolean =
    metadata.columnMappingMode match {
      case IdMapping => true
      case NameMapping => true
      case NoMapping => false
    }

  def satisfyColumnMappingProtocol(protocol: Protocol): Boolean =
    protocol.minWriterVersion >= MIN_WRITER_VERSION &&
      protocol.minReaderVersion >= MIN_READER_VERSION

  /**
   * The only allowed mode change is from NoMapping to NameMapping. Other changes
   * would require re-writing Parquet files and are not supported right now.
   */
  private def allowMappingModeChange(
      oldMode: DeltaColumnMappingMode,
      newMode: DeltaColumnMappingMode): Boolean = {
    if (oldMode == newMode) true
    else oldMode == NoMapping && newMode == NameMapping
  }

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
    val oldMappingMode = oldMetadata.columnMappingMode
    val newMappingMode = newMetadata.columnMappingMode

    val isChangingModeOnExistingTable = oldMappingMode != newMappingMode && !isCreatingNewTable
    if (isChangingModeOnExistingTable) {
      if (!allowMappingModeChange(oldMappingMode, newMappingMode)) {
        throw DeltaErrors.changeColumnMappingModeNotSupported(
          oldMappingMode.name, newMappingMode.name)
      } else {
        // legal mode change, now check if protocol is upgraded before or part of this txn
        val caseInsensitiveMap = CaseInsensitiveMap(newMetadata.configuration)
        val newProtocol = new Protocol(
          minReaderVersion = caseInsensitiveMap
            .get(Protocol.MIN_READER_VERSION_PROP).map(_.toInt)
            .getOrElse(oldProtocol.minReaderVersion),
          minWriterVersion = caseInsensitiveMap
            .get(Protocol.MIN_WRITER_VERSION_PROP).map(_.toInt)
            .getOrElse(oldProtocol.minWriterVersion))

        if (!satisfyColumnMappingProtocol(newProtocol)) {
          throw DeltaErrors.changeColumnMappingModeOnOldProtocol(oldProtocol)
        }
      }
    }
    tryFixMetadata(oldMetadata, newMetadata, isChangingModeOnExistingTable)
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

  def tryFixMetadata(
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean): Metadata = {
    val newMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(newMetadata)
    newMappingMode match {
      case IdMapping | NameMapping =>
        assignColumnIdAndPhysicalName(newMetadata, oldMetadata, isChangingModeOnExistingTable)
      case NoMapping =>
        newMetadata
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
      val columnId = getColumnId(field)
      if (columnIds.contains(columnId)) {
        throw DeltaErrors.duplicatedColumnId(mode, curFullPath, columnIds(columnId))
      }
      columnIds.update(columnId, curFullPath)

      if (!hasPhysicalName(field)) {
        throw DeltaErrors.missingPhysicalName(IdMapping, field.name)
      }
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
   * @param newMetadata The new metadata to assign Ids and physical names
   * @param oldMetadata The old metadata
   * @param isChangingModeOnExistingTable whether this is part of a commit that changes the
   *                                      mapping mode on a existing table
   * @return new metadata with Ids and physical names assigned
   */
  def assignColumnIdAndPhysicalName(
      newMetadata: Metadata,
      oldMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean): Metadata = {
    val rawSchema = newMetadata.schema
    var maxId = DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetaData(newMetadata) max
                findMaxColumnId(rawSchema)
    val newSchema =
      SchemaMergingUtils.transformColumns(rawSchema)((path, field, _) => {
        val builder = new MetadataBuilder()
          .withMetadata(field.metadata)
        if (!hasColumnId(field)) {
          maxId += 1
          builder.putLong(COLUMN_MAPPING_METADATA_ID_KEY, maxId)
        }
        if (!hasPhysicalName(field)) {
          val physicalName = if (isChangingModeOnExistingTable) {
            val fullName = path :+ field.name
            val existingField =
              SchemaUtils.findNestedFieldIgnoreCase(
                oldMetadata.schema, fullName, includeCollections = true)
            if (existingField.isEmpty) {
              throw DeltaErrors.schemaChangeDuringMappingModeChangeNotSupported(
                oldMetadata.schema, newMetadata.schema)
            } else {
              // When changing from NoMapping to NameMapping mode, we directly use old display names
              // as physical names. This is by design: 1) We don't need to rewrite the
              // existing Parquet files, and 2) display names in no-mapping mode have all the
              // properties required for physical names: unique, stable and compliant with Parquet
              // column naming restrictions.
              existingField.get.name
            }
          } else {
            generatePhysicalName
          }

          builder.putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
        }
        field.copy(metadata = builder.build())
      })

    newMetadata.copy(
      schemaString = newSchema.json,
      configuration =
        newMetadata.configuration ++ Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> maxId.toString)
    )
  }

  def dropColumnMappingMetadata(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.copy(
        metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(DeltaColumnMapping.COLUMN_MAPPING_METADATA_ID_KEY)
          .remove(DeltaColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)
          .remove(DeltaColumnMapping.PARQUET_FIELD_ID_METADATA_KEY)
          .build()
      )
    }
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
