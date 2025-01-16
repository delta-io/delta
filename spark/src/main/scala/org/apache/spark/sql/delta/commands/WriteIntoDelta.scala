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

package org.apache.spark.sql.delta.commands

import scala.collection.mutable

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.DMLUtils.TaggedCommitData
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.Constraint
import org.apache.spark.sql.delta.constraints.Constraints.Check
import org.apache.spark.sql.delta.constraints.Invariants.ArbitraryExpression
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.skipping.clustering.ClusteredTableUtils
import org.apache.spark.sql.delta.skipping.clustering.temp.ClusterBySpec
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CharVarcharUtils}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.functions.{array, col, explode, lit, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructType}

/**
 * Used to write a [[DataFrame]] into a delta table.
 *
 * New Table Semantics
 *  - The schema of the [[DataFrame]] is used to initialize the table.
 *  - The partition columns will be used to partition the table.
 *
 * Existing Table Semantics
 *  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
 *  - The schema of the DataFrame will be checked and if there are new columns present
 *    they will be added to the tables schema. Conflicting columns (i.e. a INT, and a STRING)
 *    will result in an exception
 *  - The partition columns, if present are validated against the existing metadata. If not
 *    present, then the partitioning of the table is respected.
 *
 * In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
 * replace data that matches a predicate.
 *
 * In combination with `Overwrite` dynamic partition overwrite mode (option `partitionOverwriteMode`
 * set to `dynamic`, or in spark conf `spark.sql.sources.partitionOverwriteMode` set to `dynamic`)
 * is also supported.
 *
 * Dynamic partition overwrite mode conflicts with `replaceWhere`:
 *   - If a `replaceWhere` option is provided, and dynamic partition overwrite mode is enabled in
 *   the DataFrameWriter options, an error will be thrown.
 *   - If a `replaceWhere` option is provided, and dynamic partition overwrite mode is enabled in
 *   the spark conf, data will be overwritten according to the `replaceWhere` expression
 *
 * @param catalogTableOpt Should explicitly be set when table is accessed from catalog
 * @param schemaInCatalog The schema created in Catalog. We will use this schema to update metadata
 *                        when it is set (in CTAS code path), and otherwise use schema from `data`.
 */
case class WriteIntoDelta(
    override val deltaLog: DeltaLog,
    mode: SaveMode,
    options: DeltaOptions,
    partitionColumns: Seq[String],
    override val configuration: Map[String, String],
    override val data: DataFrame,
    val catalogTableOpt: Option[CatalogTable] = None,
    schemaInCatalog: Option[StructType] = None
    )
  extends LeafRunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand
  with WriteIntoDeltaLike {

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  override protected val canOverwriteSchema: Boolean =
    options.canOverwriteSchema && isOverwriteOperation && options.replaceWhere.isEmpty


  override def run(sparkSession: SparkSession): Seq[Row] = {
    deltaLog.withNewTransaction(catalogTableOpt) { txn =>
      if (hasBeenExecuted(txn, sparkSession, Some(options))) {
        return Seq.empty
      }

      val taggedCommitData = writeAndReturnCommitData(
        txn, sparkSession
      )
      val operation = DeltaOperations.Write(
        mode, Option(partitionColumns),
        options.replaceWhere, options.userMetadata
      )
      txn.commitIfNeeded(taggedCommitData.actions, operation, tags = taggedCommitData.stringTags)
    }
    Seq.empty
  }

  override def writeAndReturnCommitData(
      txn: OptimisticTransaction,
      sparkSession: SparkSession,
      clusterBySpecOpt: Option[ClusterBySpec] = None,
      isTableReplace: Boolean = false): TaggedCommitData[Action] = {
    import org.apache.spark.sql.delta.implicits._
    if (txn.readVersion > -1) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw DeltaErrors.pathAlreadyExistsException(deltaLog.dataPath)
      } else if (mode == SaveMode.Ignore) {
        return TaggedCommitData.empty
      } else if (mode == SaveMode.Overwrite) {
        DeltaLog.assertRemovable(txn.snapshot)
      }
    }
    val isReplaceWhere = mode == SaveMode.Overwrite && options.replaceWhere.nonEmpty
    val finalClusterBySpecOpt =
      if (mode == SaveMode.Append || isReplaceWhere) {
        clusterBySpecOpt.foreach { clusterBySpec =>
          ClusteredTableUtils.validateClusteringColumnsInSnapshot(txn.snapshot, clusterBySpec)
        }
        // Append mode and replaceWhere cannot update the clustering columns.
        None
      } else {
        clusterBySpecOpt
      }
    val rearrangeOnly = options.rearrangeOnly
    val charPadding = sparkSession.conf.get(SQLConf.READ_SIDE_CHAR_PADDING)
    val charAsVarchar = sparkSession.conf.get(SQLConf.CHAR_AS_VARCHAR)
    val dataSchema = if (!charAsVarchar && charPadding) {
      data.schema
    } else {
      // If READ_SIDE_CHAR_PADDING is not enabled, CHAR type is the same as VARCHAR. The change
      // below makes DESC TABLE to show VARCHAR instead of CHAR.
      CharVarcharUtils.replaceCharVarcharWithStringInSchema(
        CharVarcharUtils.replaceCharWithVarchar(CharVarcharUtils.getRawSchema(data.schema))
          .asInstanceOf[StructType])
    }
    val finalSchema = schemaInCatalog.getOrElse(dataSchema)
    if (txn.metadata.schemaString != null) {
      // In cases other than CTAS (INSERT INTO, DataFrame write), block if values are provided for
      // GENERATED ALWAYS AS IDENTITY columns.
      IdentityColumn.blockExplicitIdentityColumnInsert(
        txn.metadata.schema,
        data.queryExecution.analyzed)
    }
    // We need to cache this canUpdateMetadata before calling updateMetadata, as that will update
    // it to true. This is unavoidable as getNewDomainMetadata uses information generated by
    // updateMetadata, so it needs to be run after that.
    val canUpdateMetadata = txn.canUpdateMetadata
    updateMetadata(data.sparkSession, txn, finalSchema,
      partitionColumns, configuration, isOverwriteOperation, rearrangeOnly
    )
    val newDomainMetadata = getNewDomainMetadata(
      txn,
      canUpdateMetadata,
      isReplacingTable = isOverwriteOperation && options.replaceWhere.isEmpty,
      finalClusterBySpecOpt
    )

    val replaceOnDataColsEnabled =
      sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED)

    val useDynamicPartitionOverwriteMode = {
      if (txn.metadata.partitionColumns.isEmpty) {
        // We ignore dynamic partition overwrite mode for non-partitioned tables
        false
      } else if (isTableReplace) {
        // A replace table command should always replace the table, not just some partitions.
        false
      } else if (options.replaceWhere.nonEmpty) {
        if (options.partitionOverwriteModeInOptions && options.isDynamicPartitionOverwriteMode) {
          // replaceWhere and dynamic partition overwrite conflict because they both specify which
          // data to overwrite. We throw an error when:
          // 1. replaceWhere is provided in a DataFrameWriter option
          // 2. partitionOverwriteMode is set to "dynamic" in a DataFrameWriter option
          throw DeltaErrors.replaceWhereUsedWithDynamicPartitionOverwrite()
        } else {
          // If replaceWhere is provided, we do not use dynamic partition overwrite, even if it's
          // enabled in the spark session configuration, since generally query-specific configs take
          // precedence over session configs
          false
        }
      } else {
        options.isDynamicPartitionOverwriteMode
      }
    }

    if (useDynamicPartitionOverwriteMode && canOverwriteSchema) {
      throw DeltaErrors.overwriteSchemaUsedWithDynamicPartitionOverwrite()
    }

    // Validate partition predicates
    var containsDataFilters = false
    val replaceWhere = options.replaceWhere.flatMap { replace =>
      val parsed = parsePredicates(sparkSession, replace)
      if (replaceOnDataColsEnabled) {
        // Helps split the predicate into separate expressions
        val (metadataPredicates, dataFilters) = DeltaTableUtils.splitMetadataAndDataPredicates(
          parsed.head, txn.metadata.partitionColumns, sparkSession)
        if (rearrangeOnly && dataFilters.nonEmpty) {
          throw DeltaErrors.replaceWhereWithFilterDataChangeUnset(dataFilters.mkString(","))
        }
        containsDataFilters = dataFilters.nonEmpty
        Some(metadataPredicates ++ dataFilters)
      } else if (mode == SaveMode.Overwrite) {
        verifyPartitionPredicates(sparkSession, txn.metadata.partitionColumns, parsed)
        Some(parsed)
      } else {
        None
      }
    }

    if (txn.readVersion < 0) {
      // Initialize the log path
      deltaLog.createLogDirectoriesIfNotExists()
    }

    val (newFiles, addFiles, deletedFiles) = (mode, replaceWhere) match {
      case (SaveMode.Overwrite, Some(predicates)) if !replaceOnDataColsEnabled =>
        // fall back to match on partition cols only when replaceArbitrary is disabled.
        val newFiles = txn.writeFiles(data, Some(options))
        val addFiles = newFiles.collect { case a: AddFile => a }
        // Check to make sure the files we wrote out were actually valid.
        val matchingFiles = DeltaLog.filterFileList(
          txn.metadata.partitionSchema, addFiles.toDF(sparkSession), predicates).as[AddFile]
          .collect()
        val invalidFiles = addFiles.toSet -- matchingFiles
        if (invalidFiles.nonEmpty) {
          val badPartitions = invalidFiles
            .map(_.partitionValues)
            .map { _.map { case (k, v) => s"$k=$v" }.mkString("/") }
            .mkString(", ")
          throw DeltaErrors.replaceWhereMismatchException(options.replaceWhere.get, badPartitions)
        }
        (newFiles, addFiles, txn.filterFiles(predicates).map(_.remove))
      case (SaveMode.Overwrite, Some(condition)) if txn.snapshot.version >= 0 =>
        val constraints = extractConstraints(sparkSession, condition)

        val removedFileActions = removeFiles(sparkSession, txn, condition)
        val cdcExistsInRemoveOp = removedFileActions.exists(_.isInstanceOf[AddCDCFile])

        // The above REMOVE will not produce explicit CDF data when persistent DV is enabled.
        // Therefore here we need to decide whether to produce explicit CDF for INSERTs, because
        // the CDF protocol requires either (i) all CDF data are generated explicitly as AddCDCFile,
        // or (ii) all CDF data can be deduced from [[AddFile]] and [[RemoveFile]].
        val dataToWrite =
          if (containsDataFilters &&
              CDCReader.isCDCEnabledOnTable(txn.metadata, sparkSession) &&
              sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_WITH_CDF_ENABLED) &&
              cdcExistsInRemoveOp) {
            var dataWithDefaultExprs = data
            // Add identity columns if they are not in `data`.
            // Column names for which we will track identity column high water marks.
            val trackHighWaterMarks = mutable.Set.empty[String]
            val topLevelOutputNames = CaseInsensitiveMap(data.schema.map(f => f.name -> f).toMap)
            val selectExprs = txn.metadata.schema.map { f =>
              if (ColumnWithDefaultExprUtils.isIdentityColumn(f) &&
                !topLevelOutputNames.contains(f.name)) {
                // Track high water marks for generated IDENTITY values.
                trackHighWaterMarks += f.name
                IdentityColumn.createIdentityColumnGenerationExprAsColumn(f)
              } else {
                SchemaUtils.fieldToColumn(f).alias(f.name)
              }
            }
            if (trackHighWaterMarks.nonEmpty) {
              txn.setTrackHighWaterMarks(trackHighWaterMarks.toSet)
              dataWithDefaultExprs = data.select(selectExprs: _*)
            }

            // pack new data and cdc data into an array of structs and unpack them into rows
            // to share values in outputCols on both branches, avoiding re-evaluating
            // non-deterministic expression twice.
            val outputCols = dataWithDefaultExprs.schema.map(SchemaUtils.fieldToColumn(_))
            val insertCols = outputCols :+
              lit(CDCReader.CDC_TYPE_INSERT).as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val insertDataCols = outputCols :+
              Column(CDCReader.CDC_TYPE_NOT_CDC)
                .as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val packedInserts = array(
              struct(insertCols: _*),
              struct(insertDataCols: _*)
            ).expr

            dataWithDefaultExprs
              .select(explode(Column(packedInserts)).as("packedData"))
              .select(
                (dataWithDefaultExprs.schema.map(_.name) :+ CDCReader.CDC_TYPE_COLUMN_NAME)
                  .map { n => col(s"packedData.`$n`").as(n) }: _*)
          } else {
            data
          }
        val newFiles = try txn.writeFiles(dataToWrite, Some(options), constraints) catch {
          case e: InvariantViolationException =>
            throw DeltaErrors.replaceWhereMismatchException(
              options.replaceWhere.get,
              e)
        }
        (newFiles,
          newFiles.collect { case a: AddFile => a },
          removedFileActions)
      case (SaveMode.Overwrite, None) =>
        val newFiles = writeFiles(
          txn, data, options
        )
        val addFiles = newFiles.collect { case a: AddFile => a }
        val deletedFiles = if (useDynamicPartitionOverwriteMode) {
          // with dynamic partition overwrite for any partition that is being written to all
          // existing data in that partition will be deleted.
          // the selection what to delete is on the next two lines
          val updatePartitions = addFiles.map(_.partitionValues).toSet
          txn.filterFiles(updatePartitions).map(_.remove)
        } else {
          txn.filterFiles().map(_.remove)
        }
        (newFiles, addFiles, deletedFiles)
      case _ =>
        val newFiles = writeFiles(
          txn, data, options
        )
        (newFiles, newFiles.collect { case a: AddFile => a }, Nil)
    }

    // Need to handle replace where metrics separately.
    if (replaceWhere.nonEmpty && replaceOnDataColsEnabled &&
        sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_METRICS_ENABLED)) {
      registerReplaceWhereMetrics(sparkSession, txn, newFiles, deletedFiles)
    }

    val fileActions = if (rearrangeOnly) {
      val changeFiles = newFiles.collect { case c: AddCDCFile => c }
      if (changeFiles.nonEmpty) {
        throw DeltaErrors.unexpectedChangeFilesFound(changeFiles.mkString("\n"))
      }
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map {
          case add: AddFile => add.copy(dataChange = !rearrangeOnly)
          case remove: RemoveFile => remove.copy(dataChange = !rearrangeOnly)
          case other => throw DeltaErrors.illegalFilesFound(other.toString)
        }
    } else {
      newFiles ++ deletedFiles
    }
    val allActions =
      newDomainMetadata ++
        createSetTransaction(sparkSession, deltaLog, Some(options)).toSeq ++
        fileActions
    TaggedCommitData(allActions)
  }

  private def writeFiles(
      txn: OptimisticTransaction,
      data: DataFrame,
      options: DeltaOptions
    ): Seq[FileAction] = {
    txn.writeFiles(data, Some(options))
  }

  private def removeFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      condition: Seq[Expression]): Seq[Action] = {
    val relation = LogicalRelation(
        txn.deltaLog.createRelation(snapshotToUseOpt = Some(txn.snapshot),
          catalogTableOpt = txn.catalogTable))
    val processedCondition = condition.reduceOption(And)
    val command = spark.sessionState.analyzer.execute(
      DeleteFromTable(relation, processedCondition.getOrElse(Literal.TrueLiteral)))
    spark.sessionState.analyzer.checkAnalysis(command)
    val (deleteActions, deleteMetrics) =
      command.asInstanceOf[DeleteCommand].performDelete(spark, txn.deltaLog, txn)
    recordDeltaEvent(
      deltaLog,
      "delta.dml.write.removeFiles.stats",
      data = deleteMetrics.copy(isWriteCommand = true)
    )
    deleteActions
  }

  override def withNewWriterConfiguration(updatedConfiguration: Map[String, String])
    : WriteIntoDeltaLike = this.copy(configuration = updatedConfiguration)
}
