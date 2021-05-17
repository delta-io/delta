/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.schema

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.PartitionUtils

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * A trait that writers into Delta can extend to update the schema and/or partitioning of the table.
 */
trait ImplicitMetadataOperation extends DeltaLogging {

  protected val canMergeSchema: Boolean
  protected val canOverwriteSchema: Boolean

  private def normalizePartitionColumns(
      spark: SparkSession,
      partitionCols: Seq[String],
      schema: StructType): Seq[String] = {
    partitionCols.map { columnName =>
      val colMatches = schema.filter(s => SchemaUtils.DELTA_COL_RESOLVER(s.name, columnName))
      if (colMatches.length > 1) {
        throw DeltaErrors.ambiguousPartitionColumnException(columnName, colMatches)
      } else if (colMatches.isEmpty) {
        throw DeltaErrors.partitionColumnNotFoundException(columnName, schema.toAttributes)
      }
      colMatches.head.name
    }
  }

  protected final def updateMetadata(
      txn: OptimisticTransaction,
      data: Dataset[_],
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      isOverwriteMode: Boolean,
      rearrangeOnly: Boolean = false): Unit = {
    updateMetadata(
      data.sparkSession, txn, data.schema, partitionColumns,
      configuration, isOverwriteMode, rearrangeOnly)
  }

  protected final def updateMetadata(
      spark: SparkSession,
      txn: OptimisticTransaction,
      schema: StructType,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      isOverwriteMode: Boolean,
      rearrangeOnly: Boolean): Unit = {
    val dataSchema = schema.asNullable
    val mergedSchema = if (isOverwriteMode && canOverwriteSchema) {
      dataSchema
    } else {
      val fixedTypeColumns =
        if (GeneratedColumn.satisfyGeneratedColumnProtocol(txn.protocol)) {
          txn.metadata.fixedTypeColumns
        } else {
          Set.empty[String]
        }
      SchemaUtils.mergeSchemas(
        txn.metadata.schema,
        dataSchema,
        fixedTypeColumns = fixedTypeColumns)
    }
    val normalizedPartitionCols =
      normalizePartitionColumns(spark, partitionColumns, dataSchema)
    // Merged schema will contain additional columns at the end
    def isNewSchema: Boolean = txn.metadata.schema != mergedSchema
    // We need to make sure that the partitioning order and naming is consistent
    // if provided. Otherwise we follow existing partitioning
    def isNewPartitioning: Boolean = normalizedPartitionCols.nonEmpty &&
      txn.metadata.partitionColumns != normalizedPartitionCols
    def isPartitioningChanged: Boolean = txn.metadata.partitionColumns != normalizedPartitionCols
    PartitionUtils.validatePartitionColumn(
      mergedSchema,
      normalizedPartitionCols,
      // Delta is case insensitive regarding internal column naming
      caseSensitive = false)

    if (!txn.deltaLog.tableExists) {
      if (dataSchema.isEmpty) {
        throw DeltaErrors.emptyDataException
      }
      recordDeltaEvent(txn.deltaLog, "delta.ddl.initializeSchema")
      // If this is the first write, configure the metadata of the table.
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException("Create a Delta table")
      }
      val description = configuration.get("comment").orNull
      val cleanedConfs = configuration.filterKeys(_ != "comment")
      txn.updateMetadata(
        Metadata(
          description = description,
          schemaString = dataSchema.json,
          partitionColumns = normalizedPartitionCols,
          configuration = cleanedConfs))
    } else if (isOverwriteMode && canOverwriteSchema && (isNewSchema || isPartitioningChanged)) {
      // Can define new partitioning in overwrite mode
      val newMetadata = txn.metadata.copy(
        schemaString = dataSchema.json,
        partitionColumns = normalizedPartitionCols
      )
      recordDeltaEvent(txn.deltaLog, "delta.ddl.overwriteSchema")
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException("Overwrite the Delta table schema or " +
          "change the partition schema")
      }
      txn.updateMetadata(newMetadata)
    } else if (isNewSchema && canMergeSchema && !isNewPartitioning) {
      logInfo(s"New merged schema: ${mergedSchema.treeString}")
      recordDeltaEvent(txn.deltaLog, "delta.ddl.mergeSchema")
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException("Change the Delta table schema")
      }
      txn.updateMetadata(txn.metadata.copy(schemaString = mergedSchema.json))
    } else if (isNewSchema || isNewPartitioning) {
      recordDeltaEvent(txn.deltaLog, "delta.schemaValidation.failure")
      val errorBuilder = new MetadataMismatchErrorBuilder
      if (isNewSchema) {
        errorBuilder.addSchemaMismatch(txn.metadata.schema, dataSchema, txn.metadata.id)
      }
      if (isNewPartitioning) {
        errorBuilder.addPartitioningMismatch(txn.metadata.partitionColumns, normalizedPartitionCols)
      }
      if (isOverwriteMode) {
        errorBuilder.addOverwriteBit()
      }
      errorBuilder.finalizeAndThrow(spark.sessionState.conf)
    }
  }
}
