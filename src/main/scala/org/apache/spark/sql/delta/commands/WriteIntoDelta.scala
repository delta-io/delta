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

package org.apache.spark.sql.delta.commands

// scalastyle:off import.ordering.noEmptyLine
import io.delta.tables.execution.DeltaTableOperations
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddFile, RemoveFile}
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{And, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.DeltaDelete
import org.apache.spark.sql.execution.command.RunnableCommand

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
 */
case class WriteIntoDelta(
    deltaLog: DeltaLog,
    mode: SaveMode,
    options: DeltaOptions,
    partitionColumns: Seq[String],
    configuration: Map[String, String],
    data: DataFrame)
  extends RunnableCommand
  with ImplicitMetadataOperation
  with DeltaCommand {

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  override protected val canOverwriteSchema: Boolean =
    options.canOverwriteSchema && isOverwriteOperation && options.replaceWhere.isEmpty

  override def run(sparkSession: SparkSession): Seq[Row] = {
    deltaLog.withNewTransaction { txn =>
      val actions = write(txn, sparkSession)
      val operation = DeltaOperations.Write(mode, Option(partitionColumns), options.replaceWhere)
      txn.commit(actions, operation)
    }
    Seq.empty
  }

  private def deleteFilesReplaceWhere(
    txn: OptimisticTransaction, sparkSession: SparkSession,
    predicates: Seq[Expression], newFiles: Seq[AddFile]): Seq[RemoveFile] = {
    import sparkSession.implicits._

    val filterExpression = predicates.reduceLeftOption(And).getOrElse(Literal(true))

    filterExpression.foreach { cond =>
      if (SubqueryExpression.hasSubquery(cond)) {
        throw DeltaErrors.subqueryNotSupportedException("DELETE", cond)
      }
    }

    // Not really sure how to get the logical plan for the target to pass to DeltaDelete
    val delete = DeltaDelete(
      deltaLog.createDataFrame(txn.snapshot, txn.filterFiles()).logicalPlan,
      Some(filterExpression))
    val qe = sparkSession.sessionState.executePlan(delete)
    val resolvedDelete = qe.analyzed.asInstanceOf[DeltaDelete]
    val deleteCommand = DeleteCommand(resolvedDelete)
    val actions = deleteCommand.gatherDeleteActions(sparkSession, deltaLog, txn)

    // Check to make sure the files we wrote out were actually valid.
    val matchingFiles = DeltaLog.filterFileList(
      txn.metadata.partitionSchema, newFiles.toDF(), predicates).as[AddFile].collect()
    val invalidFiles = newFiles.toSet -- matchingFiles
    if (invalidFiles.nonEmpty) {
      val badPartitions = invalidFiles
        .map(_.partitionValues)
        .map { _.map { case (k, v) => s"$k=$v" }.mkString("/") }
        .mkString(", ")
      throw DeltaErrors.replaceWhereMismatchException(options.replaceWhere.get, badPartitions)
    }

    txn.filterFiles(predicates).map(_.remove)
  }

  def write(txn: OptimisticTransaction, sparkSession: SparkSession): Seq[Action] = {

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
    updateMetadata(txn, data, partitionColumns, configuration, isOverwriteOperation, rearrangeOnly)

    // Validate partition predicates
    val replaceWhere = options.replaceWhere
    val replaceWherePredicates = if (replaceWhere.isDefined) {
      val predicates = parsePredicates(sparkSession, replaceWhere.get)
      Some(predicates)
    } else {
      None
    }

    if (txn.readVersion < 0) {
      // Initialize the log path
      deltaLog.fs.mkdirs(deltaLog.logPath)
    }

    // DeltaInvariantCheckerExec is called through writeFiles
    val newFiles = txn.writeFiles(data, Some(options))
    val deletedFiles: Seq[RemoveFile] = (mode, replaceWherePredicates) match {
      case (SaveMode.Overwrite, None) =>
        txn.filterFiles().map(_.remove)
      case (SaveMode.Overwrite, Some(predicates)) =>
        deleteFilesReplaceWhere(txn, sparkSession, predicates, newFiles)
      case _ => Nil
    }

    if (rearrangeOnly) {
      newFiles.map(_.copy(dataChange = !rearrangeOnly)) ++
        deletedFiles.map(_.copy(dataChange = !rearrangeOnly))
    } else {
      newFiles ++ deletedFiles
    }
  }
}
