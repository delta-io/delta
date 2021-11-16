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
import org.apache.spark.sql.delta.constraints.Constraint
import org.apache.spark.sql.delta.constraints.Constraints.Check
import org.apache.spark.sql.delta.constraints.Invariants.ArbitraryExpression
import org.apache.spark.sql.delta.schema.{ImplicitMetadataOperation, InvariantViolationException}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, LogicalPlan}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.StructType

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

  def write(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {
    import sparkSession.implicits._
    if (txn.readVersion > -1) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw DeltaErrors.pathAlreadyExistsException(deltaLog.dataPath)
      } else if (mode == SaveMode.Ignore) {
        return Nil
      } else if (mode == SaveMode.Overwrite) {
        deltaLog.assertRemovable()
      }
    }
    val rearrangeOnly = options.rearrangeOnly
    // Delta does not support char padding and we should only have varchar type. This does not
    // change the actual behavior, but makes DESC TABLE to show varchar instead of char.
    val dataSchema = CharVarcharUtils.replaceCharVarcharWithStringInSchema(
      replaceCharWithVarchar(CharVarcharUtils.getRawSchema(data.schema)).asInstanceOf[StructType])
    updateMetadata(data.sparkSession, txn, schemaInCatalog.getOrElse(dataSchema),
      partitionColumns, configuration, isOverwriteOperation, rearrangeOnly)

    val replaceOnDataColsEnabled =
      sparkSession.conf.get(DeltaSQLConf.REPLACEWHERE_DATACOLUMNS_ENABLED)

    // Validate partition predicates
    val replaceWhere = options.replaceWhere.flatMap { replace =>
      val parsed = parsePredicates(sparkSession, replace)
      if (replaceOnDataColsEnabled) {
        // Helps split the predicate into separate expressions
        val (metadataPredicates, dataFilters) = DeltaTableUtils.splitMetadataAndDataPredicates(
          parsed.head, txn.metadata.partitionColumns, sparkSession)
        if (rearrangeOnly && dataFilters.nonEmpty) {
          throw new AnalysisException("'replaceWhere' cannot be used with data filters when " +
            s"'dataChange' is set to false. Filters: ${dataFilters.mkString(",")}")
        }
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
          txn.metadata.partitionSchema, addFiles.toDF(), predicates).as[AddFile].collect()
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
        val newFiles = try txn.writeFiles(data, Some(options), constraints) catch {
          case e: InvariantViolationException =>
            throw DeltaErrors.replaceWhereMismatchException(
              options.replaceWhere.get,
              e)
        }
        (newFiles,
          newFiles.collect { case a: AddFile => a },
          removeFiles(sparkSession, txn, condition))
      case (SaveMode.Overwrite, None) =>
        val newFiles = txn.writeFiles(data, Some(options))
        (newFiles, newFiles.collect { case a: AddFile => a }, txn.filterFiles().map(_.remove))
      case _ =>
        val newFiles = txn.writeFiles(data, Some(options))
        (newFiles, newFiles.collect { case a: AddFile => a }, Nil)
    }

    val fileActions = if (rearrangeOnly) {
      addFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map {
          case add: AddFile => add.copy(dataChange = !rearrangeOnly)
          case remove: RemoveFile => remove.copy(dataChange = !rearrangeOnly)
          case other =>
            throw new IllegalStateException(
              s"Illegal files found in a dataChange = false transaction. Files: $other")
        }
    } else {
      newFiles ++ deletedFiles
    }
    fileActions
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

  private def removeFiles(
      spark: SparkSession,
      txn: OptimisticTransaction,
      condition: Seq[Expression]): Seq[Action] = {
    val relation = LogicalRelation(
        txn.deltaLog.createRelation(snapshotToUseOpt = Some(txn.snapshot)))
    val processedCondition = condition.reduceOption(And)
    val command = spark.sessionState.analyzer.execute(DeleteFromTable(relation, processedCondition))
    spark.sessionState.analyzer.checkAnalysis(command)
    command.asInstanceOf[DeleteCommand].performDelete(spark, txn.deltaLog, txn)
  }
}
