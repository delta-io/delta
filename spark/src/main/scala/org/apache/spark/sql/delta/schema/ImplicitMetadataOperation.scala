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

package org.apache.spark.sql.delta.schema

import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.PartitionUtils

import org.apache.spark.internal.MDC
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.FileSourceGeneratedMetadataStructField
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A trait that writers into Delta can extend to update the schema and/or partitioning of the table.
 */
trait ImplicitMetadataOperation extends DeltaLogging {

  import ImplicitMetadataOperation._

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
        throw DeltaErrors.partitionColumnNotFoundException(columnName, toAttributes(schema))
      }
      colMatches.head.name
    }
  }

  /** Remove all file source generated metadata columns from the schema. */
  private def dropGeneratedMetadataColumns(structType: StructType): StructType = {
    val fields = structType.filter {
      case FileSourceGeneratedMetadataStructField(_, _) => false
      case _ => true
    }
    StructType(fields)
  }

  protected final def updateMetadata(
      spark: SparkSession,
      txn: OptimisticTransaction,
      schema: StructType,
      partitionColumns: Seq[String],
      configuration: Map[String, String],
      isOverwriteMode: Boolean,
      rearrangeOnly: Boolean
      ): Unit = {
    // To support the new column mapping mode, we drop existing metadata on data schema
    // so that all the column mapping related properties can be reinitialized in
    // OptimisticTransaction.updateMetadata
    var dataSchema =
      DeltaColumnMapping.dropColumnMappingMetadata(schema.asNullable)

    // File Source generated columns are not added to the stored schema.
    dataSchema = dropGeneratedMetadataColumns(dataSchema)

    val mergedSchema = mergeSchema(spark, txn, dataSchema, isOverwriteMode, canOverwriteSchema)
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
      // Filter out the property for clustering columns from Metadata action.
      val cleanedConfs = ClusteredTableUtils.removeClusteringColumnsProperty(
        configuration.filterKeys(_ != "comment").toMap)
      txn.updateMetadata(
        Metadata(
          description = description,
          schemaString = dataSchema.json,
          partitionColumns = normalizedPartitionCols,
          configuration = cleanedConfs
          ,
          createdTime = Some(System.currentTimeMillis())))
    } else if (isOverwriteMode && canOverwriteSchema && (isNewSchema || isPartitioningChanged
        )) {
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
      txn.updateMetadataForTableOverwrite(newMetadata)
    } else if (isNewSchema && canMergeSchema && !isNewPartitioning
        ) {
      logInfo(log"New merged schema: ${MDC(DeltaLogKeys.SCHEMA, mergedSchema.treeString)}")
      recordDeltaEvent(txn.deltaLog, "delta.ddl.mergeSchema")
      if (rearrangeOnly) {
        throw DeltaErrors.unexpectedDataChangeException("Change the Delta table schema")
      }

      val schemaWithTypeWideningMetadata = TypeWideningMetadata.addTypeWideningMetadata(
        txn,
        schema = mergedSchema,
        oldSchema = txn.metadata.schema
      )

      txn.updateMetadata(txn.metadata.copy(schemaString = schemaWithTypeWideningMetadata.json
      ))
    } else if (isNewSchema || isNewPartitioning
        ) {
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

  /**
   * Returns a sequence of new DomainMetadata if canUpdateMetadata is true and the operation is
   * either create table or replace the whole table (not replaceWhere operation). This is because
   * we only update Domain Metadata when creating or replacing table, and replace table for DDL
   * and DataFrameWriterV2 are already handled in CreateDeltaTableCommand. In that case,
   * canUpdateMetadata is false, so we don't update again.
   *
   * @param txn [[OptimisticTransaction]] being used to create or replace table.
   * @param canUpdateMetadata true if the metadata is not updated yet.
   * @param isReplacingTable true if the operation is replace table without replaceWhere option.
   * @param clusterBySpecOpt optional ClusterBySpec containing user-specified clustering columns.
   */
  protected final def getNewDomainMetadata(
      txn: OptimisticTransaction,
      canUpdateMetadata: Boolean,
      isReplacingTable: Boolean,
      clusterBySpecOpt: Option[ClusterBySpec] = None): Seq[DomainMetadata] = {
    if (canUpdateMetadata && (!txn.deltaLog.tableExists || isReplacingTable)) {
      val newDomainMetadata = Seq.empty[DomainMetadata] ++
        ClusteredTableUtils.getDomainMetadataFromTransaction(clusterBySpecOpt, txn)
      if (!txn.deltaLog.tableExists) {
        newDomainMetadata
      } else {
        // Handle domain metadata for replacing a table.
        DomainMetadataUtils.handleDomainMetadataForReplaceTable(
          txn.snapshot.domainMetadata, newDomainMetadata)
      }
    } else {
      Seq.empty
    }
  }
}

object ImplicitMetadataOperation {

  /**
   * Merge schemas based on transaction state and delta options
   * @param txn Target transaction
   * @param dataSchema New data schema
   * @param isOverwriteMode Whether we are overwriting
   * @param canOverwriteSchema Whether we can overwrite
   * @return Merged schema
   */
  private[delta] def mergeSchema(
      spark: SparkSession,
      txn: OptimisticTransaction,
      dataSchema: StructType,
      isOverwriteMode: Boolean,
      canOverwriteSchema: Boolean): StructType = {
    if (isOverwriteMode && canOverwriteSchema) {
      dataSchema
    } else {
      checkDependentExpressions(spark, txn.protocol, txn.metadata, dataSchema)

      SchemaMergingUtils.mergeSchemas(
        txn.metadata.schema,
        dataSchema,
        allowTypeWidening = TypeWidening.isEnabled(txn.protocol, txn.metadata))
    }
  }

  /**
   * Check whether there are dependant (CHECK) constraints for
   * the provided `currentDt`; if so, throw an error indicating
   * the constraint data type mismatch.
   *
   * @param spark the spark session used.
   * @param path the full column path for the current field.
   * @param metadata the metadata used for checking dependant (CHECK) constraints.
   * @param currentDt the current data type.
   * @param updateDt the updated data type.
   */
  private def checkDependentConstraints(
      spark: SparkSession,
      path: Seq[String],
      metadata: Metadata,
      currentDt: DataType,
      updateDt: DataType): Unit = {
    val dependentConstraints =
      Constraints.findDependentConstraints(spark, path, metadata)
    if (dependentConstraints.nonEmpty) {
      throw DeltaErrors.constraintDataTypeMismatch(
        path,
        currentDt,
        updateDt,
        dependentConstraints
      )
    }
  }

  /**
   * Check whether there are dependant generated columns for
   * the provided `currentDt`; if so, throw an error indicating
   * the generated columns data type mismatch.
   *
   * @param spark the spark session used.
   * @param path the full column path for the current field.
   * @param protocol the protocol used.
   * @param metadata the metadata used for checking dependant generated columns.
   * @param currentDt the current data type.
   * @param updateDt the updated data type.
   */
  private def checkDependentGeneratedColumns(
      spark: SparkSession,
      path: Seq[String],
      protocol: Protocol,
      metadata: Metadata,
      currentDt: DataType,
      updateDt: DataType): Unit = {
    val dependentGeneratedColumns = SchemaUtils.findDependentGeneratedColumns(
      spark, path, protocol, metadata.schema)
    if (dependentGeneratedColumns.nonEmpty) {
      throw DeltaErrors.generatedColumnsDataTypeMismatch(
        path,
        currentDt,
        updateDt,
        dependentGeneratedColumns
      )
    }
  }

  /**
   * Check whether the provided field is currently being referenced
   * by CHECK constraints or generated columns.
   * Note that we explicitly ignore the check for `StructType` in this
   * function by only inspecting its inner fields to relax the check;
   * plus, any `StructType` will be traversed in [[checkDependentExpressions]].
   *
   * @param spark the spark session used.
   * @param path the full column path for the current field.
   * @param protocol the protocol used.
   * @param metadata the metadata used for checking constraints and generated columns.
   * @param currentDt the current data type.
   * @param updateDt the updated data type.
   */
  private def checkConstraintsOrGeneratedColumnsOnStructField(
      spark: SparkSession,
      path: Seq[String],
      protocol: Protocol,
      metadata: Metadata,
      currentDt: DataType,
      updateDt: DataType): Unit = (currentDt, updateDt) match {
    // we explicitly ignore the check for `StructType` here.
    case (StructType(_), StructType(_)) =>

    // FIXME: we intentionally incorporate the pattern match for `ArrayType` and `MapType`
    //        here mainly due to the field paths for maps/arrays in constraints/generated columns
    //        are *NOT* consistent with regular field paths,
    //        e.g., `hash(a.arr[0].x)` vs. `hash(a.element.x)`.
    //        this makes it hard to recurse into maps/arrays and check for the corresponding
    //        fields - thus we can not actually block the operation even if the updated field
    //        is being referenced by any CHECK constraints or generated columns.
    case (from, to) =>
      if (currentDt != updateDt) {
        checkDependentConstraints(spark, path, metadata, from, to)
        checkDependentGeneratedColumns(spark, path, protocol, metadata, from, to)
      }
  }

  /**
   * Finds all fields that change between the current schema and the new data schema and fail if any
   * of them are referenced by check constraints or generated columns.
   */
  private def checkDependentExpressions(
      sparkSession: SparkSession,
      protocol: Protocol,
      metadata: actions.Metadata,
      dataSchema: StructType): Unit =
    SchemaMergingUtils.transformColumns(metadata.schema, dataSchema) {
      case (fieldPath, currentField, Some(updateField), _)
        if !SchemaMergingUtils.equalsIgnoreCaseAndCompatibleNullability(
          currentField.dataType,
          updateField.dataType
        ) =>
        checkConstraintsOrGeneratedColumnsOnStructField(
          spark = sparkSession,
          path = fieldPath :+ currentField.name,
          protocol = protocol,
          metadata = metadata,
          currentDt = currentField.dataType,
          updateDt = updateField.dataType
        )
        // We don't transform the schema but just perform checks,
        // the returned field won't be used anyway.
        updateField
      case (_, field, _, _) => field
    }
}
