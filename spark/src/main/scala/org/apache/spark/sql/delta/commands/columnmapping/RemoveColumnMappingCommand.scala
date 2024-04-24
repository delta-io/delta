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

package org.apache.spark.sql.delta.commands.columnmapping

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, SchemaUtils}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.types.StructType

/**
 * A command to remove the column mapping from a table.
 */
class RemoveColumnMappingCommand(
    val deltaLog: DeltaLog,
    val catalogOpt: Option[CatalogTable])
  extends ImplicitMetadataOperation {
  override protected val canMergeSchema: Boolean = false
  override protected val canOverwriteSchema: Boolean = true

  /**
   * Remove the column mapping from the table.
   * @param removeColumnMappingTableProperty - whether to remove the column mapping property from
   *                                         the table instead of setting it to 'none'
   */
  def run(spark: SparkSession, removeColumnMappingTableProperty: Boolean): Unit = {
    deltaLog.withNewTransaction(catalogOpt) { txn =>
      val originalFiles = txn.filterFiles()
      val originalData = buildDataFrame(txn, originalFiles)
      val originalSchema = txn.snapshot.schema
      val newSchema = DeltaColumnMapping.dropColumnMappingMetadata(originalSchema)
      verifySchemaFieldNames(newSchema)

      updateMetadata(removeColumnMappingTableProperty, txn, newSchema)

      val deltaOptions = getDeltaOptionsForWrite(spark)
      val addedFiles = writeData(txn, originalData, deltaOptions)
      val removeFileActions = originalFiles.map(_.removeWithTimestamp(dataChange = false))

      txn.commit(actions = removeFileActions ++ addedFiles,
        op = DeltaOperations.RemoveColumnMapping(),
        tags = RowTracking.addPreservedRowTrackingTagIfNotSet(txn.snapshot)
      )
    }
  }

  /**
   * Verify none of the schema fields contain invalid column names.
   */
  def verifySchemaFieldNames(schema: StructType): Unit = {
    val invalidColumnNames =
      SchemaUtils.findInvalidColumnNamesInSchema(schema)
    if (invalidColumnNames.nonEmpty) {
      throw DeltaErrors
        .foundInvalidColumnNamesWhenRemovingColumnMapping(invalidColumnNames)
    }
  }

  /**
   * Update the metadata to remove the column mapping table properties and
   * update the schema to remove the column mapping metadata.
   */
  def updateMetadata(
      removeColumnMappingTableProperty: Boolean,
      txn: OptimisticTransaction,
      newSchema: StructType): Unit = {
    val newConfiguration =
      getConfigurationWithoutColumnMapping(txn, removeColumnMappingTableProperty)
    val newMetadata = txn.metadata.copy(
      schemaString = newSchema.json,
      configuration = newConfiguration)
    txn.updateMetadata(newMetadata)
  }

  def getConfigurationWithoutColumnMapping(
      txn: OptimisticTransaction,
      removeColumnMappingTableProperty: Boolean): Map[String, String] = {
    // Scanned schema does not include the column mapping metadata and can be reused as is.
    val columnMappingPropertyKey = DeltaConfigs.COLUMN_MAPPING_MODE.key
    val columnMappingMaxIdPropertyKey = DeltaConfigs.COLUMN_MAPPING_MAX_ID.key
    // Unset or overwrite the column mapping mode to none and remove max id property
    // while keeping other properties.
    (if (removeColumnMappingTableProperty) {
      txn.metadata.configuration - columnMappingPropertyKey
    } else {
      txn.metadata.configuration + (columnMappingPropertyKey -> "none")
    }) - columnMappingMaxIdPropertyKey
  }

  def getDeltaOptionsForWrite(spark: SparkSession): DeltaOptions = {
    new DeltaOptions(
      // Prevent files from being split by writers.
      Map(DeltaOptions.MAX_RECORDS_PER_FILE -> "0"),
      spark.sessionState.conf)
  }

  def buildDataFrame(
      txn: OptimisticTransaction,
      originalFiles: Seq[AddFile]): DataFrame =
    recordDeltaOperation(txn.deltaLog, "delta.removeColumnMapping.setupDataFrame") {
      txn.deltaLog.createDataFrame(txn.snapshot, originalFiles)
    }

  def writeData(
      txn: OptimisticTransaction,
      data: DataFrame,
      deltaOptions: DeltaOptions): Seq[AddFile] = {
    txn.writeFiles(
      inputData = RowTracking.preserveRowTrackingColumns(data, txn.snapshot),
      writeOptions = Some(deltaOptions),
      isOptimize = true,
      additionalConstraints = Seq.empty)
      .asInstanceOf[Seq[AddFile]]
      // Mark as no data change to not generate CDC data. We are only removing column mapping.
      .map(_.copy(dataChange = false))
  }
}

object RemoveColumnMappingCommand {
  def apply(
      deltaLog: DeltaLog,
      catalogOpt: Option[CatalogTable]): RemoveColumnMappingCommand = {
    new RemoveColumnMappingCommand(deltaLog, catalogOpt)
  }
}
