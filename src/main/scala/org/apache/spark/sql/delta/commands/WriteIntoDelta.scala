/*
 * Copyright 2019 Databricks, Inc.
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
import org.apache.spark.sql.{AnalysisException, _}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{Action, AddFile}
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.delta.schema.ImplicitMetadataOperation
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions.input_file_name

/**
 * Used to write a [[DataFrame]] into a delta table.
 *
 * New Table Semantics
 *  - The schema of the [[DataFrame]] is used to initialize the table.
 *  - The partition columns will be used to partition the table.
 *
 * Existing Table Semantics
 *  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
 *  - The schema will of the DataFrame will be checked and if there are new columns present
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

  protected def fileActions(
    spark: SparkSession,
    partitionColumns: Seq[String],
    predicate: Option[Expression],
    txn: OptimisticTransaction): Seq[Action] = {
    import spark.implicits._

    val (metadataPredicates, otherPredicates) = predicate match {
        case Some(pred) =>
          DeltaTableUtils
            .splitMetadataAndDataPredicates(pred, txn.metadata.partitionColumns, spark)
        case None => (Seq.empty, Seq.empty)
      }


    if (otherPredicates.isEmpty) {
      val newFiles = txn.writeFiles(data, Some(options))
      val deletedFiles = (mode, predicate) match {
        case (SaveMode.Overwrite, None) =>
          txn.filterFiles().map(_.remove)
        case (SaveMode.Overwrite, Some(predicates)) =>
          // Check to make sure the files we wrote out were actually valid.
          val matchingFiles = DeltaLog.filterFileList(
            txn.metadata.partitionColumns, newFiles.toDF(), Seq(predicates)).as[AddFile].collect()
          val invalidFiles = newFiles.toSet -- matchingFiles
          if (invalidFiles.nonEmpty) {
            val badPartitions = invalidFiles
              .map(_.partitionValues)
              .map { _.map { case (k, v) => s"$k=$v" }.mkString("/") }
              .mkString(", ")
            throw DeltaErrors.replaceWhereMismatchException(options.replaceWhere.get, badPartitions)
          }

          txn.filterFiles(metadataPredicates).map(_.remove)
        case _ => Nil
      }

      newFiles ++ deletedFiles
    } else {
      val affectedFiles = txn.filterFiles(metadataPredicates ++ otherPredicates)
      val nameToAddFileMap = generateCandidateFileMap(deltaLog.dataPath, affectedFiles)

      if (affectedFiles.isEmpty) {
        // no rows qualify the partition predicates
        Nil
      } else {
        val affectedFileIndex = new TahoeBatchFileIndex(
          spark, "replaceWhere", affectedFiles, deltaLog, deltaLog.dataPath, txn.snapshot)

        val targetPlan = {
          txn.deltaLog.createDataFrame(txn.snapshot, affectedFiles).queryExecution.analyzed
        }

        val newTargetPlan = DeltaTableUtils.replaceFileIndex(targetPlan, affectedFileIndex)
        val targetDF = Dataset.ofRows(spark, newTargetPlan)

        val filesToRewrite: Array[String] = {
          targetDF.filter(new Column(predicate.get))
            .select(input_file_name()).distinct().as[String].collect()
        }

        if (filesToRewrite.isEmpty) {
          // Do nothing if no row qualifies the predicate
          Nil
        } else {

          val baseRelation = buildBaseRelation(
            spark, txn, "replaceWhere", affectedFileIndex.path, filesToRewrite, nameToAddFileMap)

          // Keep everything from the resolved target except a new TahoeFileIndex
          // that only involves the affected files instead of all files.
          val newTarget = DeltaTableUtils.replaceFileIndex(targetPlan, baseRelation.location)
          val targetDF = Dataset.ofRows(spark, newTarget)

          val rewrittenFiles = withStatusCode(
            "DELTA", s"Rewriting ${filesToRewrite.length} files for arbitraryReplace operation") {
            txn.writeFiles(getRevisedDataFrame(data, targetDF, predicate.get, txn))
          }

          val operationTimestamp = System.currentTimeMillis()
          val deleteActions =
            removeFilesFromPaths(deltaLog, nameToAddFileMap, filesToRewrite, operationTimestamp)

          deleteActions ++ rewrittenFiles
        }
      }
    }
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
    updateMetadata(txn, data, partitionColumns, configuration, isOverwriteOperation)

    // Validate partition predicates
    val replaceWhere = options.replaceWhere
    val partitionFilters = if (replaceWhere.isDefined) {

      // While the return is a `Seq`, we only ever get a single expression.
      val predicate = parsePredicates(sparkSession, replaceWhere.get).head
      if (mode == SaveMode.Overwrite) {
        if (DeltaTableUtils.containsSubquery(predicate)) {
          DeltaErrors.subqueryNotSupportedException("replaceWhere", predicate)
        }
      }
      Some(predicate)
    } else {
      None
    }

    if (txn.readVersion < 0) {
      // Initialize the log path
      deltaLog.fs.mkdirs(deltaLog.logPath)
    }

    fileActions(sparkSession, txn.metadata.partitionColumns, partitionFilters, txn)

  }
}
