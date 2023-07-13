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
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, Metadata => SparkMetadata, MetadataBuilder, StructField, StructType}

trait DeltaColumnMappingBase extends DeltaLogging {
  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"
  val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"
  val COLUMN_MAPPING_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"

  /**
   * This list of internal columns (and only this list) is allowed to have missing
   * column mapping metadata such as field id and physical name because
   * they might not be present in user's table schema.
   *
   * These fields, if materialized to parquet, will always be matched by their display name in the
   * downstream parquet reader even under column mapping modes.
   *
   * For future developers who want to utilize additional internal columns without generating
   * column mapping metadata, please add them here.
   *
   * This list is case-insensitive.
   */
  protected val DELTA_INTERNAL_COLUMNS: Set[String] =
    (CDCReader.CDC_COLUMNS_IN_DATA ++ Seq(
      CDCReader.CDC_COMMIT_VERSION,
      CDCReader.CDC_COMMIT_TIMESTAMP,
      DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME,
      DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME)
    ).map(_.toLowerCase(Locale.ROOT)).toSet

  val supportedModes: Set[DeltaColumnMappingMode] =
    Set(IdMapping, NoMapping, NameMapping)

  def isInternalField(field: StructField): Boolean = DELTA_INTERNAL_COLUMNS
    .contains(field.name.toLowerCase(Locale.ROOT))

  def satisfiesColumnMappingProtocol(protocol: Protocol): Boolean =
    protocol.isFeatureSupported(ColumnMappingTableFeature)

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

  def isColumnMappingUpgrade(
      oldMode: DeltaColumnMappingMode,
      newMode: DeltaColumnMappingMode): Boolean = {
    oldMode == NoMapping && newMode != NoMapping
  }

  /**
   * If the table is already on the column mapping protocol, we block:
   *     - changing column mapping config
   * otherwise, we block
   *     - upgrading to the column mapping Protocol through configurations
   */
  def verifyAndUpdateMetadataChange(
      deltaLog: DeltaLog,
      oldProtocol: Protocol,
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isCreatingNewTable: Boolean,
      isOverwriteSchema: Boolean): Metadata = {
    // field in new metadata should have been dropped
    val oldMappingMode = oldMetadata.columnMappingMode
    val newMappingMode = newMetadata.columnMappingMode

    if (!supportedModes.contains(newMappingMode)) {
      throw DeltaErrors.unsupportedColumnMappingMode(newMappingMode.name)
    }

    val isChangingModeOnExistingTable = oldMappingMode != newMappingMode && !isCreatingNewTable
    if (isChangingModeOnExistingTable) {
      if (!allowMappingModeChange(oldMappingMode, newMappingMode)) {
        throw DeltaErrors.changeColumnMappingModeNotSupported(
          oldMappingMode.name, newMappingMode.name)
      } else {
        // legal mode change, now check if protocol is upgraded before or part of this txn
        val caseInsensitiveMap = CaseInsensitiveMap(newMetadata.configuration)
        val minReaderVersion = caseInsensitiveMap
          .get(Protocol.MIN_READER_VERSION_PROP).map(_.toInt)
          .getOrElse(oldProtocol.minReaderVersion)
        val minWriterVersion = caseInsensitiveMap
          .get(Protocol.MIN_WRITER_VERSION_PROP).map(_.toInt)
          .getOrElse(oldProtocol.minWriterVersion)
        var newProtocol = Protocol(minReaderVersion, minWriterVersion)
        val satisfiesWriterVersion = minWriterVersion >= ColumnMappingTableFeature.minWriterVersion
        val satisfiesReaderVersion = minReaderVersion >= ColumnMappingTableFeature.minReaderVersion
        // This is an OR check because `readerFeatures` and `writerFeatures` can independently
        // support table features.
        if ((newProtocol.supportsReaderFeatures && satisfiesWriterVersion) ||
            (newProtocol.supportsWriterFeatures && satisfiesReaderVersion)) {
          newProtocol = newProtocol.withFeature(ColumnMappingTableFeature)
        }

        if (!satisfiesColumnMappingProtocol(newProtocol)) {
          throw DeltaErrors.changeColumnMappingModeOnOldProtocol(oldProtocol)
        }
      }
    }

    val updatedMetadata = updateColumnMappingMetadata(
      oldMetadata, newMetadata, isChangingModeOnExistingTable, isOverwriteSchema)

    // record column mapping table creation/upgrade
    if (newMappingMode != NoMapping) {
      if (isCreatingNewTable) {
        recordDeltaEvent(deltaLog, "delta.columnMapping.createTable")
      } else if (oldMappingMode != newMappingMode) {
        recordDeltaEvent(deltaLog, "delta.columnMapping.upgradeTable")
      }
    }

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
          .remove(COLUMN_MAPPING_METADATA_ID_KEY)
          .remove(PARQUET_FIELD_ID_METADATA_KEY)
          .remove(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
          .build()

      case IdMapping | NameMapping =>
        if (!hasColumnId(field)) {
          throw DeltaErrors.missingColumnId(mode, field.name)
        }
        if (!hasPhysicalName(field)) {
          throw DeltaErrors.missingPhysicalName(mode, field.name)
        }
        // Delta spec requires writer to always write field_id in parquet schema for column mapping
        // Reader strips PARQUET_FIELD_ID_METADATA_KEY in
        // DeltaParquetFileFormat:prepareSchemaForRead
        new MetadataBuilder()
          .withMetadata(field.metadata)
          .putLong(PARQUET_FIELD_ID_METADATA_KEY, getColumnId(field))
          .build()

      case mode =>
        throw DeltaErrors.unsupportedColumnMappingMode(mode.name)
    }
  }

  /** Recursively renames columns in the given schema with their physical schema. */
  def renameColumns(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.copy(name = getPhysicalName(field))
    }
  }

  def assignPhysicalName(field: StructField, physicalName: String): StructField = {
    field.copy(metadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .putString(COLUMN_MAPPING_PHYSICAL_NAME_KEY, physicalName)
      .build())
  }

  def assignPhysicalNames(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      if (hasPhysicalName(field)) field else assignPhysicalName(field, generatePhysicalName)
    }
  }

  /** Set physical name based on field path, skip if field path not found in the map */
  def setPhysicalNames(
      schema: StructType,
      fieldPathToPhysicalName: Map[Seq[String], String]): StructType = {
    if (fieldPathToPhysicalName.isEmpty) {
      schema
    } else {
      SchemaMergingUtils.transformColumns(schema) { (parent, field, _) =>
        val path = parent :+ field.name
        if (fieldPathToPhysicalName.contains(path)) {
          assignPhysicalName(field, fieldPathToPhysicalName(path))
        } else {
          field
        }
      }
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

  private def updateColumnMappingMetadata(
      oldMetadata: Metadata,
      newMetadata: Metadata,
      isChangingModeOnExistingTable: Boolean,
      isOverwritingSchema: Boolean): Metadata = {
    val newMappingMode = DeltaConfigs.COLUMN_MAPPING_MODE.fromMetaData(newMetadata)
    newMappingMode match {
      case IdMapping | NameMapping =>
        assignColumnIdAndPhysicalName(
          newMetadata, oldMetadata, isChangingModeOnExistingTable, isOverwritingSchema)
      case NoMapping =>
        newMetadata
      case mode =>
         throw DeltaErrors.unsupportedColumnMappingMode(mode.name)
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

  /**
   * Verify the metadata for valid column mapping metadata assignment. This is triggered for every
   * commit as a last defense.
   *
   * 1. Ensure column mapping metadata is set for the appropriate mode
   * 2. Ensure no duplicate column id/physical names set
   * 3. Ensure max column id is in a good state (set, and greater than all field ids available)
   */
  def checkColumnIdAndPhysicalNameAssignments(metadata: Metadata): Unit = {
    val schema = metadata.schema
    val mode = metadata.columnMappingMode

    // physical name/column id -> full field path
    val columnIds = mutable.Set[Int]()
    val physicalNames = mutable.Set[String]()
    // use id mapping to keep all column mapping metadata
    // this method checks for missing physical name & column id already
    val physicalSchema = createPhysicalSchema(schema, schema, IdMapping, checkSupportedMode = false)

    // Check id / physical name duplication
    SchemaMergingUtils.transformColumns(physicalSchema) ((parentPhysicalPath, field, _) => {
      // field.name is now physical name
      // We also need to apply backticks to column paths with dots in them to prevent a possible
      // false alarm in which a column `a.b` is duplicated with `a`.`b`
      val curFullPhysicalPath = UnresolvedAttribute(parentPhysicalPath :+ field.name).name
      val columnId = getColumnId(field)
      if (columnIds.contains(columnId)) {
        throw DeltaErrors.duplicatedColumnId(mode, columnId, schema)
      }
      columnIds.add(columnId)

      // We should check duplication by full physical name path, because nested fields
      // such as `a.b.c` shouldn't conflict with `x.y.c` due to same column name.
      if (physicalNames.contains(curFullPhysicalPath)) {
        throw DeltaErrors.duplicatedPhysicalName(mode, curFullPhysicalPath, schema)
      }
      physicalNames.add(curFullPhysicalPath)

      field
    })

    // Check assignment of the max id property
    if (SQLConf.get.getConf(DeltaSQLConf.DELTA_COLUMN_MAPPING_CHECK_MAX_COLUMN_ID)) {
      if (!metadata.configuration.contains(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key)) {
        throw DeltaErrors.maxColumnIdNotSet
      }
      val fieldMaxId = DeltaColumnMapping.findMaxColumnId(schema)
      if (metadata.columnMappingMaxId < DeltaColumnMapping.findMaxColumnId(schema)) {
        throw DeltaErrors.maxColumnIdNotSetCorrectly(metadata.columnMappingMaxId, fieldMaxId)
      }
    }
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
      isChangingModeOnExistingTable: Boolean,
      isOverwritingSchema: Boolean): Metadata = {
    val rawSchema = newMetadata.schema
    var maxId = DeltaConfigs.COLUMN_MAPPING_MAX_ID.fromMetaData(newMetadata) max
                findMaxColumnId(rawSchema)
    val newSchema =
      SchemaMergingUtils.transformColumns(rawSchema)((path, field, _) => {
        val builder = new MetadataBuilder().withMetadata(field.metadata)

        lazy val fullName = path :+ field.name
        lazy val existingFieldOpt =
          SchemaUtils.findNestedFieldIgnoreCase(
            oldMetadata.schema, fullName, includeCollections = true)
        lazy val canReuseColumnMappingMetadataDuringOverwrite = {
          val canReuse =
            isOverwritingSchema &&
              SparkSession.getActiveSession.exists(
                _.conf.get(DeltaSQLConf.REUSE_COLUMN_MAPPING_METADATA_DURING_OVERWRITE)) &&
              existingFieldOpt.exists { existingField =>
                // Ensure data type & nullability are compatible
                DataType.equalsIgnoreCompatibleNullability(
                  from = existingField.dataType,
                  to = field.dataType
                )
              }
          if (canReuse) {
            require(!isChangingModeOnExistingTable,
              "Cannot change column mapping mode while overwriting the table")
            assert(hasColumnId(existingFieldOpt.get) && hasPhysicalName(existingFieldOpt.get))
          }
          canReuse
        }

        if (!hasColumnId(field)) {
          val columnId = if (canReuseColumnMappingMetadataDuringOverwrite) {
            getColumnId(existingFieldOpt.get)
          } else {
            maxId += 1
            maxId
          }

          builder.putLong(COLUMN_MAPPING_METADATA_ID_KEY, columnId)
        }
        if (!hasPhysicalName(field)) {
          val physicalName = if (isChangingModeOnExistingTable) {
            if (existingFieldOpt.isEmpty) {
              if (oldMetadata.schema.isEmpty) {
                // We should relax the check for tables that have both an empty schema
                // and no data. Assumption: no schema => no data
                generatePhysicalName
              } else throw DeltaErrors.schemaChangeDuringMappingModeChangeNotSupported(
                oldMetadata.schema, newMetadata.schema)
            } else {
              // When changing from NoMapping to NameMapping mode, we directly use old display names
              // as physical names. This is by design: 1) We don't need to rewrite the
              // existing Parquet files, and 2) display names in no-mapping mode have all the
              // properties required for physical names: unique, stable and compliant with Parquet
              // column naming restrictions.
              existingFieldOpt.get.name
            }
          } else if (canReuseColumnMappingMetadataDuringOverwrite) {
            // Copy the physical name metadata over from the existing field if possible
            getPhysicalName(existingFieldOpt.get)
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
          .remove(COLUMN_MAPPING_METADATA_ID_KEY)
          .remove(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
          .remove(PARQUET_FIELD_ID_METADATA_KEY)
          .build()
      )
    }
  }

  def filterColumnMappingProperties(properties: Map[String, String]): Map[String, String] = {
    properties.filterKeys(_ != DeltaConfigs.COLUMN_MAPPING_MAX_ID.key).toMap
  }

  // Verify the values of internal column mapping properties are the same in two sets of config
  // ONLY if the config is present in both sets of properties.
  def verifyInternalProperties(one: Map[String, String], two: Map[String, String]): Boolean = {
    val key = DeltaConfigs.COLUMN_MAPPING_MAX_ID.key
    one.get(key).forall(value => value == two.getOrElse(key, value))
  }

  /**
   * Create a physical schema for the given schema using the Delta table schema as a reference.
   *
   * @param schema the given logical schema (potentially without any metadata)
   * @param referenceSchema the schema from the delta log, which has all the metadata
   * @param columnMappingMode column mapping mode of the delta table, which determines which
   *                          metadata to fill in
   * @param checkSupportedMode whether we should check of the column mapping mode is supported
   */
  def createPhysicalSchema(
      schema: StructType,
      referenceSchema: StructType,
      columnMappingMode: DeltaColumnMappingMode,
      checkSupportedMode: Boolean = true): StructType = {
    if (columnMappingMode == NoMapping) {
      return schema
    }

    // createPhysicalSchema is the narrow-waist for both read/write code path
    // so we could check for mode support here
    if (checkSupportedMode && !supportedModes.contains(columnMappingMode)) {
      throw DeltaErrors.unsupportedColumnMappingMode(columnMappingMode.name)
    }

    SchemaMergingUtils.transformColumns(schema) { (path, field, _) =>
      val fullName = path :+ field.name
      val inSchema = SchemaUtils
        .findNestedFieldIgnoreCase(referenceSchema, fullName, includeCollections = true)
      inSchema.map { refField =>
        val sparkMetadata = getColumnMappingMetadata(refField, columnMappingMode)
        field.copy(metadata = sparkMetadata, name = getPhysicalName(refField))
      }.getOrElse {
        if (isInternalField(field)) {
          field
        } else {
          throw DeltaErrors.columnNotFound(fullName, referenceSchema)
        }
      }
    }
  }

  /**
   * Create a list of physical attributes for the given attributes using the table schema as a
   * reference.
   *
   * @param output the list of attributes (potentially without any metadata)
   * @param referenceSchema   the table schema with all the metadata
   * @param columnMappingMode column mapping mode of the delta table, which determines which
   *                          metadata to fill in
   */
  def createPhysicalAttributes(
      output: Seq[Attribute],
      referenceSchema: StructType,
      columnMappingMode: DeltaColumnMappingMode): Seq[Attribute] = {
    // Assign correct column mapping info to columns according to the schema
    val struct = createPhysicalSchema(output.toStructType, referenceSchema, columnMappingMode)
    output.zip(struct).map { case (attr, field) =>
      attr.withDataType(field.dataType) // for recursive column names and metadata
        .withMetadata(field.metadata)
        .withName(field.name)
    }
  }

  /**
   * Returns a map of physicalNamePath -> field for the given `schema`, where
   * physicalNamePath is the [$parentPhysicalName, ..., $fieldPhysicalName] list of physical names
   * for every field (including nested) in the `schema`.
   *
   * Must be called after `checkColumnIdAndPhysicalNameAssignments`, so that we know the schema
   * is valid.
   */
  def getPhysicalNameFieldMap(schema: StructType): Map[Seq[String], StructField] = {
    val physicalSchema = renameColumns(schema)

    val physicalSchemaFieldPaths = SchemaMergingUtils.explode(physicalSchema).map(_._1)

    val originalSchemaFields = SchemaMergingUtils.explode(schema).map(_._2)

    physicalSchemaFieldPaths.zip(originalSchemaFields).toMap
  }

  /**
   * Returns true if Column Mapping mode is enabled and the newMetadata's schema, when compared to
   * the currentMetadata's schema, is indicative of a DROP COLUMN operation.
   *
   * We detect DROP COLUMNS by checking if any physical name in `currentSchema` is missing in
   * `newSchema`.
   */
  def isDropColumnOperation(newMetadata: Metadata, currentMetadata: Metadata): Boolean = {

    // We will need to compare the new schema's physical columns to the current schema's physical
    // columns. So, they both must have column mapping enabled.
    if (newMetadata.columnMappingMode == NoMapping ||
      currentMetadata.columnMappingMode == NoMapping) {
      return false
    }

    isDropColumnOperation(newSchema = newMetadata.schema, currentSchema = currentMetadata.schema)
  }

  def isDropColumnOperation(newSchema: StructType, currentSchema: StructType): Boolean = {
    val newPhysicalToLogicalMap = getPhysicalNameFieldMap(newSchema)
    val currentPhysicalToLogicalMap = getPhysicalNameFieldMap(currentSchema)

    // are any of the current physical names missing in the new schema?
    currentPhysicalToLogicalMap
      .keys
      .exists { k => !newPhysicalToLogicalMap.contains(k) }
  }

  /**
   * Returns true if Column Mapping mode is enabled and the newMetadata's schema, when compared to
   * the currentMetadata's schema, is indicative of a RENAME COLUMN operation.
   *
   * We detect RENAME COLUMNS by checking if any two columns with the same physical name have
   * different logical names
   */
  def isRenameColumnOperation(newMetadata: Metadata, currentMetadata: Metadata): Boolean = {

    // We will need to compare the new schema's physical columns to the current schema's physical
    // columns. So, they both must have column mapping enabled.
    if (newMetadata.columnMappingMode == NoMapping ||
      currentMetadata.columnMappingMode == NoMapping) {
      return false
    }

    isRenameColumnOperation(newSchema = newMetadata.schema, currentSchema = currentMetadata.schema)
  }

  def isRenameColumnOperation(newSchema: StructType, currentSchema: StructType): Boolean = {
    val newPhysicalToLogicalMap = getPhysicalNameFieldMap(newSchema)
    val currentPhysicalToLogicalMap = getPhysicalNameFieldMap(currentSchema)

    // do any two columns with the same physical name have different logical names?
    currentPhysicalToLogicalMap
      .exists { case (physicalPath, field) =>
        newPhysicalToLogicalMap.get(physicalPath).exists(_.name != field.name)
      }
  }

  /**
   * Compare the old metadata's schema with new metadata's schema for column mapping schema changes.
   *
   * newMetadata's snapshot version must be >= oldMetadata's snapshot version so we could reliably
   * detect the difference between ADD COLUMN and DROP COLUMN.
   *
   * As of now, `newMetadata` is column mapping read compatible with `oldMetadata` if
   * no rename column or drop column has happened in-between.
   */
  def hasNoColumnMappingSchemaChanges(newMetadata: Metadata, oldMetadata: Metadata): Boolean = {
    val (oldMode, newMode) = (oldMetadata.columnMappingMode, newMetadata.columnMappingMode)
    if (oldMode != NoMapping && newMode != NoMapping) {
      require(oldMode == newMode, "changing mode is not supported")
      // Both changes are post column mapping enabled
      !isRenameColumnOperation(newMetadata, oldMetadata) &&
        !isDropColumnOperation(newMetadata, oldMetadata)
    } else if (oldMode == NoMapping && newMode != NoMapping) {
      // The old metadata does not have column mapping while the new metadata does, in this case
      // we assume an upgrade has happened in between.
      // So we manually construct a post-upgrade schema for the old metadata and compare that with
      // the new metadata, as the upgrade would use the logical name as the physical name, we could
      // easily capture any difference in the schema using the same is{Drop,Rename}ColumnOperation
      // utils.
      var upgradedMetadata = assignColumnIdAndPhysicalName(
        oldMetadata, oldMetadata, isChangingModeOnExistingTable = true, isOverwritingSchema = false
      )
      // need to change to a column mapping mode too so the utils below can recognize
      upgradedMetadata = upgradedMetadata.copy(
        configuration = upgradedMetadata.configuration ++
          Map(DeltaConfigs.COLUMN_MAPPING_MODE.key -> newMetadata.columnMappingMode.name)
      )
      // use the same check
      !isRenameColumnOperation(newMetadata, upgradedMetadata) &&
        !isDropColumnOperation(newMetadata, upgradedMetadata)
    } else {
      // Not column mapping, don't block
      // TODO: support column mapping downgrade check once that's rolled out.
      true
    }
  }
}

object DeltaColumnMapping extends DeltaColumnMappingBase

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
      case mode => throw DeltaErrors.unsupportedColumnMappingMode(mode)
    }
  }
}
