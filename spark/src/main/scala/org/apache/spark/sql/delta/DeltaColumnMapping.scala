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

import org.apache.spark.sql.delta.RowId.RowIdMetadataStructField
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{SchemaMergingUtils, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, QuotingUtils}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, Metadata => SparkMetadata, MetadataBuilder, StructField, StructType}

trait DeltaColumnMappingBase extends DeltaLogging {
  val PARQUET_FIELD_ID_METADATA_KEY = "parquet.field.id"
  val PARQUET_FIELD_NESTED_IDS_METADATA_KEY = "parquet.field.nested.ids"
  val COLUMN_MAPPING_METADATA_PREFIX = "delta.columnMapping."
  val COLUMN_MAPPING_METADATA_ID_KEY = COLUMN_MAPPING_METADATA_PREFIX + "id"
  val COLUMN_MAPPING_PHYSICAL_NAME_KEY = COLUMN_MAPPING_METADATA_PREFIX + "physicalName"
  val COLUMN_MAPPING_METADATA_NESTED_IDS_KEY = COLUMN_MAPPING_METADATA_PREFIX + "nested.ids"
  val PARQUET_LIST_ELEMENT_FIELD_NAME = "element"
  val PARQUET_MAP_KEY_FIELD_NAME = "key"
  val PARQUET_MAP_VALUE_FIELD_NAME = "value"

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
      /**
       * Whenever `_metadata` column is selected, Spark adds the format generated metadata
       * columns to `ParquetFileFormat`'s required output schema. Column `_metadata` contains
       * constant value subfields metadata such as `file_path` and format specific custom metadata
       * subfields such as `row_index` in Parquet. Spark creates the file format object with
       * data schema plus additional custom metadata columns required from file format to fill up
       * the `_metadata` column.
       */
      ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME,
      DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME,
      DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME)
    ).map(_.toLowerCase(Locale.ROOT)).toSet

  val supportedModes: Set[DeltaColumnMappingMode] =
    Set(IdMapping, NoMapping, NameMapping)

  def isInternalField(field: StructField): Boolean =
    DELTA_INTERNAL_COLUMNS.contains(field.name.toLowerCase(Locale.ROOT)) ||
      RowIdMetadataStructField.isRowIdColumn(field) ||
      RowCommitVersion.MetadataStructField.isRowCommitVersionColumn(field)

  /**
   * Allow NameMapping -> NoMapping transition behind a feature flag.
   * Otherwise only NoMapping -> NameMapping is allowed.
   */
  private def allowMappingModeChange(
      oldMode: DeltaColumnMappingMode,
      newMode: DeltaColumnMappingMode): Boolean = {
    val removalAllowed = SparkSession.getActiveSession
      .exists(_.conf.get(DeltaSQLConf.ALLOW_COLUMN_MAPPING_REMOVAL))
    // No change.
    (oldMode == newMode) ||
      // Downgrade allowed with a flag.
      (removalAllowed && (oldMode != NoMapping && newMode == NoMapping)) ||
      // Upgrade always allowed.
      (oldMode == NoMapping && newMode == NameMapping)
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
    if (isChangingModeOnExistingTable && !allowMappingModeChange(oldMappingMode, newMappingMode)) {
      throw DeltaErrors.changeColumnMappingModeNotSupported(
        oldMappingMode.name, newMappingMode.name)
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

  def hasNestedColumnIds(field: StructField): Boolean =
    field.metadata.contains(COLUMN_MAPPING_METADATA_NESTED_IDS_KEY)

  def getNestedColumnIds(field: StructField): SparkMetadata =
    field.metadata.getMetadata(COLUMN_MAPPING_METADATA_NESTED_IDS_KEY)

  def getNestedColumnIdsAsLong(field: StructField): Iterable[Long] = {
    val nestedColumnMetadata = getNestedColumnIds(field)
    metadataToMap[Map[String, Long]](nestedColumnMetadata).values
  }

  private def metadataToMap[T <: Map[_, _]](metadata: SparkMetadata)(implicit m: Manifest[T]): T = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    parse(metadata.json).extract[T]
  }

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
          .remove(COLUMN_MAPPING_METADATA_NESTED_IDS_KEY)
          .remove(PARQUET_FIELD_ID_METADATA_KEY)
          .remove(PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
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
        val builder = new MetadataBuilder()
          .withMetadata(field.metadata)
          .putLong(PARQUET_FIELD_ID_METADATA_KEY, getColumnId(field))

        // Nested field IDs for the 'element' and 'key'/'value' fields of Arrays
        // and Maps are written when Uniform with IcebergCompatV2 is enabled on a table.
        if (hasNestedColumnIds(field)) {
          builder.putMetadata(PARQUET_FIELD_NESTED_IDS_METADATA_KEY, getNestedColumnIds(field))
        }

        builder.build()

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
        if (hasNestedColumnIds(f)) {
          val nestedIds = getNestedColumnIdsAsLong(f)
          maxId = maxId max (if (nestedIds.nonEmpty) nestedIds.max else 0)
        }
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
      SchemaMergingUtils.transformColumns(rawSchema, traverseStructsAtOnce = true)(
        (path, field, _) => {
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

    val (finalSchema, newMaxId) = if (IcebergCompatV2.isEnabled(newMetadata)) {
      rewriteFieldIdsForIceberg(newSchema, maxId)
    } else {
      (newSchema, maxId)
    }

    newMetadata.copy(
      schemaString = finalSchema.json,
      configuration = newMetadata.configuration
        ++ Map(DeltaConfigs.COLUMN_MAPPING_MAX_ID.key -> newMaxId.toString)
    )
  }

  def dropColumnMappingMetadata(schema: StructType): StructType = {
    SchemaMergingUtils.transformColumns(schema) { (_, field, _) =>
      field.copy(
        metadata = new MetadataBuilder()
          .withMetadata(field.metadata)
          .remove(COLUMN_MAPPING_METADATA_ID_KEY)
          .remove(COLUMN_MAPPING_METADATA_NESTED_IDS_KEY)
          .remove(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
          .remove(PARQUET_FIELD_ID_METADATA_KEY)
          .remove(PARQUET_FIELD_NESTED_IDS_METADATA_KEY)
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

    val referenceSchemaColumnMap: Map[String, StructField] =
      SchemaMergingUtils.explode(referenceSchema).map { case (path, field) =>
        QuotingUtils.quoteNameParts(path).toLowerCase(Locale.ROOT) -> field
      }.toMap

    SchemaMergingUtils.transformColumns(schema) { (path, field, _) =>
      val fullName = path :+ field.name
      val inSchema =
        referenceSchemaColumnMap.get(QuotingUtils.quoteNameParts(fullName).toLowerCase(Locale.ROOT))
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
   * Returns a map from the logical name paths to the physical name paths for the given schema.
   * The logical name path is the result of splitting a multi-part identifier, and the physical name
   * path is result of replacing all names in the logical name path with their physical names.
   */
  def getLogicalNameToPhysicalNameMap(schema: StructType): Map[Seq[String], Seq[String]] = {
    val physicalSchema = renameColumns(schema)
    val logicalSchemaFieldPaths = SchemaMergingUtils.explode(schema).map(_._1)
    val physicalSchemaFieldPaths = SchemaMergingUtils.explode(physicalSchema).map(_._1)
    logicalSchemaFieldPaths.zip(physicalSchemaFieldPaths).toMap
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
   * Also check for repartition because we need to fail fast when repartition detected.
   *
   * newMetadata's snapshot version must be >= oldMetadata's snapshot version so we could reliably
   * detect the difference between ADD COLUMN and DROP COLUMN.
   *
   * As of now, `newMetadata` is column mapping read compatible with `oldMetadata` if
   * no rename column or drop column has happened in-between.
   */
  def hasNoColumnMappingSchemaChanges(newMetadata: Metadata, oldMetadata: Metadata,
      allowUnsafeReadOnPartitionChanges: Boolean = false): Boolean = {
    // Helper function to check no column mapping schema change and no repartition
    def hasNoColMappingAndRepartitionSchemaChange(
       newMetadata: Metadata, oldMetadata: Metadata): Boolean = {
      isRenameColumnOperation(newMetadata, oldMetadata) ||
        isDropColumnOperation(newMetadata, oldMetadata) ||
        !SchemaUtils.isPartitionCompatible(
          // if allow unsafe row read for partition change, ignore the check
          if (allowUnsafeReadOnPartitionChanges) Seq.empty else newMetadata.partitionColumns,
          if (allowUnsafeReadOnPartitionChanges) Seq.empty else oldMetadata.partitionColumns)
    }

    val (oldMode, newMode) = (oldMetadata.columnMappingMode, newMetadata.columnMappingMode)
    if (oldMode != NoMapping && newMode != NoMapping) {
      require(oldMode == newMode, "changing mode is not supported")
      // Both changes are post column mapping enabled
      !hasNoColMappingAndRepartitionSchemaChange(newMetadata, oldMetadata)
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
      !hasNoColMappingAndRepartitionSchemaChange(newMetadata, upgradedMetadata)
    } else {
      // Prohibit reading across a downgrade.
      val isDowngrade = oldMode != NoMapping && newMode == NoMapping
      !isDowngrade
    }
  }

  /**
   * Adds the nested field IDs required by Iceberg.
   *
   * In parquet, list-type columns have a nested, implicitly defined [[element]] field and
   * map-type columns have implicitly defined [[key]] and [[value]] fields. By default,
   * Spark does not write field IDs for these fields in the parquet files. However, Iceberg
   * requires these *nested* field IDs to be present. This method rewrites the specified
   * Spark schema to add those nested field IDs.
   *
   * As list and map types are not [[StructField]]s themselves, nested field IDs are stored in
   * a map as part of the metadata of the *nearest* parent [[StructField]]. For example, consider
   * the following schema:
   *
   * col1 ARRAY(INT)
   * col2 MAP(INT, INT)
   * col3 STRUCT(a INT, b ARRAY(STRUCT(c INT, d MAP(INT, INT))))
   *
   * col1 is a list and so requires one nested field ID for the [[element]] field in parquet.
   * This nested field ID will be stored in a map that is part of col1's [[StructField.metadata]].
   * The same applies to the nested field IDs for col2's implicit [[key]] and [[value]] fields.
   * col3 itself is a Struct, consisting of an integer field and a list field named 'b'. The
   * nested field ID for the list of 'b' is stored in b's StructField metadata. Finally, the
   * list type itself is again a struct consisting of an integer field and a map field named 'd'.
   * The nested field IDs for the map of 'd' are stored in d's StructField metadata.
   *
   * @param schema  The schema to which nested field IDs should be added
   * @param startId The first field ID to use for the nested field IDs
   */
  def rewriteFieldIdsForIceberg(schema: StructType, startId: Long): (StructType, Long) = {
    var currFieldId = startId

    def initNestedIdsMetadata(field: StructField): MetadataBuilder = {
      if (hasNestedColumnIds(field)) {
        new MetadataBuilder().withMetadata(getNestedColumnIds(field))
      } else {
        new MetadataBuilder()
      }
    }

    /*
     * Helper to add the next field ID to the specified [[MetadataBuilder]] under
     * the specified key. This method first checks whether this is an existing nested
     * field or a newly added nested field. New field IDs are only assigned to newly
     * added nested fields.
     */
    def updateFieldId(metadata: MetadataBuilder, key: String): Unit = {
      if (!metadata.build().contains(key)) {
        currFieldId += 1
        metadata.putLong(key, currFieldId)
      }
    }

    /*
     * Recursively adds nested field IDs for the passed data type in pre-order,
     * ensuring uniqueness of field IDs.
     *
     * @param dt The data type that should be transformed
     * @param nestedIds A MetadataBuilder that keeps track of the nested field ID
     *                  assignment. This metadata is added to the parent field.
     * @param path The current field path relative to the parent field
     */
    def transform[E <: DataType](dt: E, nestedIds: MetadataBuilder, path: Seq[String]): E = {
      val newDt = dt match {
        case StructType(fields) =>
          StructType(fields.map { field =>
            val newNestedIds = initNestedIdsMetadata(field)
            val newDt = transform(field.dataType, newNestedIds, Seq(getPhysicalName(field)))
            val newFieldMetadata = new MetadataBuilder().withMetadata(field.metadata).putMetadata(
              COLUMN_MAPPING_METADATA_NESTED_IDS_KEY, newNestedIds.build()).build()
            field.copy(dataType = newDt, metadata = newFieldMetadata)
          })
        case ArrayType(elementType, containsNull) =>
          // update element type metadata and recurse into element type
          val elemPath = path :+ PARQUET_LIST_ELEMENT_FIELD_NAME
          updateFieldId(nestedIds, elemPath.mkString("."))
          val elementDt = transform(elementType, nestedIds, elemPath)
          // return new array type with updated metadata
          ArrayType(elementDt, containsNull)
        case MapType(keyType, valType, valueContainsNull) =>
          // update key type metadata and recurse into key type
          val keyPath = path :+ PARQUET_MAP_KEY_FIELD_NAME
          updateFieldId(nestedIds, keyPath.mkString("."))
          val keyDt = transform(keyType, nestedIds, keyPath)
          // update value type metadata and recurse into value type
          val valPath = path :+ PARQUET_MAP_VALUE_FIELD_NAME
          updateFieldId(nestedIds, valPath.mkString("."))
          val valDt = transform(valType, nestedIds, valPath)
          // return new map type with updated metadata
          MapType(keyDt, valDt, valueContainsNull)
        case other => other
      }
      newDt.asInstanceOf[E]
    }

    (transform(schema, new MetadataBuilder(), Seq.empty), currFieldId)
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
