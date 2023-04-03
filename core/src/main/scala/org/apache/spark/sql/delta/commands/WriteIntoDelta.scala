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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.Constraint
import org.apache.spark.sql.delta.constraints.Constraints.Check
import org.apache.spark.sql.delta.constraints.Invariants.ArbitraryExpression
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
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
 * @param schemaInCatalog The schema created in Catalog. We will use this schema to update metadata
 *                        when it is set (in CTAS code path), and otherwise use schema from `data`.
 */
case class WriteIntoDelta(
    deltaLog: DeltaLog,
    mode: SaveMode,
    options: DeltaOptions,
    partitionColumns: Seq[String],
    configuration: Map[String, String],
    data: DataFrame,
    schemaInCatalog: Option[StructType] = None)
  extends LeafRunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand {

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  override protected val canOverwriteSchema: Boolean =
    options.canOverwriteSchema && isOverwriteOperation && options.replaceWhere.isEmpty


  override def run(sparkSession: SparkSession): Seq[Row] = {
    deltaLog.withNewTransaction { txn =>
      if (hasBeenExecuted(txn, sparkSession, Some(options))) {
        return Seq.empty
      }

      val actions = write(txn, sparkSession)
      val operation = DeltaOperations.Write(mode, Option(partitionColumns),
        options.replaceWhere, options.userMetadata)
      txn.commit(actions, operation)
    }
    Seq.empty
  }

  // TODO: replace the method below with `CharVarcharUtils.replaceCharWithVarchar`, when 3.3 is out.
  import org.apache.spark.sql.types.{ArrayType, CharType, DataType, MapType, VarcharType}
  private def replaceCharWithVarchar(dt: DataType): DataType = dt match {
    case ArrayType(et, nullable) =>
      ArrayType(replaceCharWithVarchar(et), nullable)
    case MapType(kt, vt, nullable) =>
      MapType(replaceCharWithVarchar(kt), replaceCharWithVarchar(vt), nullable)
    case StructType(fields) =>
      StructType(fields.map { field =>
        field.copy(dataType = replaceCharWithVarchar(field.dataType))
      })
    case CharType(length) => VarcharType(length)
    case _ => dt
  }

  /**
   * Replace where operationMetrics need to be recorded separately.
   * @param newFiles - AddFile and AddCDCFile added by write job
   * @param deleteActions - AddFile, RemoveFile, AddCDCFile added by Delete job
   */
  private def registerReplaceWhereMetrics(
      spark: SparkSession,
      txn: OptimisticTransaction,
      newFiles: Seq[Action],
      deleteActions: Seq[Action]): Unit = {
    var numFiles = 0L
    var numCopiedRows = 0L
    var numOutputBytes = 0L
    var numNewRows = 0L
    var numAddedChangedFiles = 0L
    var hasRowLevelMetrics = true

    newFiles.foreach {
      case a: AddFile =>
        numFiles += 1
        numOutputBytes += a.size
        if (a.numLogicalRecords.isEmpty) {
          hasRowLevelMetrics = false
        } else {
          numNewRows += a.numLogicalRecords.get
        }
      case cdc: AddCDCFile =>
        numAddedChangedFiles += 1
      case _ =>
    }

    deleteActions.foreach {
      case a: AddFile =>
        numFiles += 1
        numOutputBytes += a.size
        if (a.numLogicalRecords.isEmpty) {
          hasRowLevelMetrics = false
        } else {
          numCopiedRows += a.numLogicalRecords.get
        }
      case cdc: AddCDCFile =>
        numAddedChangedFiles += 1
      // Remove metrics will be handled by the delete command.
      case _ =>
    }

    var sqlMetrics = Map(
      "numFiles" -> new SQLMetric("number of files written", numFiles),
      "numOutputBytes" -> new SQLMetric("number of output bytes", numOutputBytes),
      "numAddedChangeFiles" -> new SQLMetric(
        "number of change files added", numAddedChangedFiles)
    )
    if (hasRowLevelMetrics) {
      sqlMetrics ++= Map(
        "numOutputRows" -> new SQLMetric("number of rows added", numNewRows + numCopiedRows),
        "numCopiedRows" -> new SQLMetric("number of copied rows", numCopiedRows)
      )
    } else {
      // this will get filtered out in DeltaOperations.WRITE transformMetrics
      sqlMetrics ++= Map(
        "numOutputRows" -> new SQLMetric("number of rows added", 0L),
        "numCopiedRows" -> new SQLMetric("number of copied rows", 0L)
      )
    }
    txn.registerSQLMetrics(spark, sqlMetrics)
  }

  def write(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {
    import org.apache.spark.sql.delta.implicits._
    if (txn.readVersion > -1) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw DeltaErrors.pathAlreadyExistsException(deltaLog.dataPath)
      } else if (mode == SaveMode.Ignore) {
        return Nil
      } else if (mode == SaveMode.Overwrite) {
        DeltaLog.assertRemovable(txn.snapshot)
      }
    }
    val rearrangeOnly = options.rearrangeOnly
    // TODO: use `SQLConf.READ_SIDE_CHAR_PADDING` after Spark 3.4 is released.
    val charPadding = sparkSession.conf.get("spark.sql.readSideCharPadding", "false") == "true"
    val charAsVarchar = sparkSession.conf.get(SQLConf.CHAR_AS_VARCHAR)
    val dataSchema = if (!charAsVarchar && charPadding) {
      data.schema
    } else {
      // If READ_SIDE_CHAR_PADDING is not enabled, CHAR type is the same as VARCHAR. The change
      // below makes DESC TABLE to show VARCHAR instead of CHAR.
      CharVarcharUtils.replaceCharVarcharWithStringInSchema(
        replaceCharWithVarchar(CharVarcharUtils.getRawSchema(data.schema)).asInstanceOf[StructType])
    }
    val finalSchema = schemaInCatalog.getOrElse(dataSchema)
    updateMetadata(data.sparkSession, txn, finalSchema,
      partitionColumns, configuration, isOverwriteOperation, rearrangeOnly)

    val replaceOnDataColsEnabled =
      sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED)

    val useDynamicPartitionOverwriteMode = {
      if (txn.metadata.partitionColumns.isEmpty) {
        // We ignore dynamic partition overwrite mode for non-partitioned tables
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
      } else options.isDynamicPartitionOverwriteMode
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
      deltaLog.createLogDirectory()
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

            // pack new data and cdc data into an array of structs and unpack them into rows
            // to share values in outputCols on both branches, avoiding re-evaluating
            // non-deterministic expression twice.
            val outputCols = dataWithDefaultExprs.schema.map(SchemaUtils.fieldToColumn(_))
            val insertCols = outputCols :+
              lit(CDCReader.CDC_TYPE_INSERT).as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val insertDataCols = outputCols :+
              new Column(CDCReader.CDC_TYPE_NOT_CDC)
                .as(CDCReader.CDC_TYPE_COLUMN_NAME)
            val packedInserts = array(
              struct(insertCols: _*),
              struct(insertDataCols: _*)
            ).expr

            dataWithDefaultExprs
              .select(explode(new Column(packedInserts)).as("packedData"))
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
        val newFiles = txn.writeFiles(data, Some(options))
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
        val newFiles = writeFiles(txn, data, options)
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
    createSetTransaction(sparkSession, deltaLog, Some(options)).toSeq ++ fileActions
  }

  private def extractConstraints(
      sparkSession: SparkSession,
      expr: Seq[Expression]): Seq[Constraint] = {
    if (!sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_CONSTRAINT_CHECK_ENABLED)) {
      Seq.empty
    } else {
      expr.flatMap { e =>
        // While writing out the new data, we only want to enforce constraint on expressions
        // with UnresolvedAttribute, that is, containing column name. Because we parse a
        // predicate string without analyzing it, if there's a column name, it has to be
        // unresolved.
        e.collectFirst {
          case _: UnresolvedAttribute =>
            val arbitraryExpression = ArbitraryExpression(e)
            Check(arbitraryExpression.name, arbitraryExpression.expression)
        }
      }
    }
  }

  private def writeFiles(
      txn: OptimisticTransaction,
      data: DataFrame,
      options: DeltaOptions): Seq[FileAction] = {
    txn.writeFiles(data, Some(options))
  }

  private def removeFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      condition: Seq[Expression]): Seq[Action] = {
    val relation = LogicalRelation(
        txn.deltaLog.createRelation(snapshotToUseOpt = Some(txn.snapshot)))
    val processedCondition = condition.reduceOption(And)
    val command = spark.sessionState.analyzer.execute(
      DeleteFromTable(relation, processedCondition.getOrElse(Literal.TrueLiteral)))
    spark.sessionState.analyzer.checkAnalysis(command)
    command.asInstanceOf[DeleteCommand].performDelete(spark, txn.deltaLog, txn)
  }
}
