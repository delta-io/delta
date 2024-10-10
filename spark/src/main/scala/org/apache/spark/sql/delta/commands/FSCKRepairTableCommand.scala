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

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.UnresolvedDeltaPathOrIdentifier
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{RemoveFile}
import org.apache.spark.sql.delta.metric.IncrementMetric
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DateTimeUtils.NANOS_PER_MILLIS
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Row, SparkSession, Column}
import org.apache.spark.util.SerializableConfiguration

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.catalyst.catalog.CatalogTable


trait FsckCommandMetrics { self: RunnableCommand =>
  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  // Create metrics
  def createMetrics: Map[String, SQLMetric] = Map[String, SQLMetric](
    "numMissingFiles" -> createMetric(sc, "number of files removed."),
    "executionTimeMs" -> createTimingMetric(sc, "time taken to execute the entire operation."),
    "numFilesScanned" -> createMetric(sc, "Number of files scanned.")
  )
}

/**
 * The `fsck repair table` command implementation for Spark SQL. Example SQL:
 * {{{
 * FSCK REPAIR TABLE (path=STRING | table=qualifiedName) (DRY RUN)
 * }}}
 */
case class FsckRepairTableCommand (
    child: LogicalPlan,
    dryRun: Boolean) extends RunnableCommand
                     with UnaryNode
                     with DeltaCommand
                     with FsckCommandMetrics {
  // Create output columns for the command
  override val output: Seq[Attribute] =
    Seq(AttributeReference("dataFilePath", StringType, nullable = false)(),
      AttributeReference("dataFileMissing", BooleanType, nullable = false)())
  override lazy val metrics = createMetrics

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(child = newChild)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val deltaTable = getDeltaTable(child, "FSCK")
    val deltaLog = deltaTable.deltaLog
    val catalogTable = deltaTable.catalogTable

    // Check if the table is a Delta table
    if (!deltaTable.tableExists || deltaTable.hasPartitionFilters) {
      throw DeltaErrors.notADeltaTableException(
        "FSCK REPAIR TABLE",
        DeltaTableIdentifier(path = Some(deltaTable.path.toString)))
    }

    // This chunk will call performFsck which executes the FSCK logic
    // and then will commit the changes to the Delta Log if needed.
    val (removedFiles, numMissingFiles) = performFsck(sparkSession, deltaLog, catalogTable)
    // Display the relevant log
    val limit =
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.FSCK_MAX_NUM_ENTRIES_IN_RESULT)
    val msg = if (dryRun) {
      if (numMissingFiles <= limit) {
        s"Found (${numMissingFiles}) file(s) to be removed from the delta log. Listing all rows"
      } else {
        s"Found (${numMissingFiles}) file(s) to be removed from the delta log."
      }
    } else {
      s"Removed (${numMissingFiles}) file(s) from the delta log."
    }
    logConsole(msg)
    logInfo(msg)
    // Format the output (the list of deleted files)
    removedFiles.map(action => Row(action.path, true))
  }

  def performFsck(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable]): (Seq[RemoveFile], Long) = {
    recordDeltaOperation(deltaLog, "delta.fsck") {
      deltaLog.withNewTransaction(catalogTable) { txn =>
        val startTime = System.nanoTime()
        // Needed to get the dataset of AddFile
        // scalastyle:off sparkimplicits
        import sparkSession.implicits._

        // scalastyle:on sparkimplicits

        // Get the files referenced in the current Delta Log
        val allFiles = deltaLog.snapshot.withStats
          .withColumn("stats", to_json(struct(col("stats.numRecords") as "numRecords")))
          .as[AddFile]

        val numFilesScanned = allFiles.count()

        // get table path
        val path = deltaLog.dataPath
        val deltaHadoopConf = deltaLog.newDeltaHadoopConf()
        val serializableHadoopConf = new SerializableConfiguration(deltaHadoopConf)

        // create the UDF to check if a file exists
        val fileExists = udf((dataPath: String, filePath: String, dryRun: Boolean) => {
          try {
            val absolutePath = DeltaFileOperations.absolutePath(dataPath, filePath)
              absolutePath.getFileSystem(serializableHadoopConf.value).exists(absolutePath)
          } catch {
            // We only catch the exception in dryRun mode so the users can
            // see the missing files. However without dryRun mode, if there are
            // any errors with reading files the command will terminate immediately.
            case e: Exception if dryRun => true
            case e => throw e
          }
        }).asNondeterministic()

        // Get the files that are in DeltaLogs but that don't exist in the filesystem.
        val missingFiles = allFiles.filter(fileExists(lit(deltaLog.dataPath.toString),
          col("path"), lit(dryRun)) === false)

        // Create remove actions based on the missing files and put them into a data set.
        val currentTimestamp = System.currentTimeMillis()
        val filesToRemoveDS = missingFiles.map(file => {
          file.removeWithTimestamp(currentTimestamp, false)
        })

        // Calculate metrics
        val executionTimeMs = (System.nanoTime() - startTime) / NANOS_PER_MILLIS
        val incrMissingFilesCountExpr = IncrementMetric(TrueLiteral, metrics("numMissingFiles"))

        // Only process the values based on the provided dryRun output limit
        val filesToRemove = if (dryRun) {
          filesToRemoveDS
            .filter(Column(incrMissingFilesCountExpr))
            .collect()
            .take(
            sparkSession.sessionState.conf.getConf(DeltaSQLConf.FSCK_MAX_NUM_ENTRIES_IN_RESULT))
            .toSeq
        } else {
          filesToRemoveDS
            .filter(Column(incrMissingFilesCountExpr))
            .collect()
            .toSeq
        }

        // Set SQL metrics
        metrics("executionTimeMs").set(executionTimeMs)
        metrics("numFilesScanned").set(numFilesScanned)

        txn.registerSQLMetrics(sparkSession, metrics)
        sendDriverMetrics(sparkSession, metrics)

        // Record delta event
        recordDeltaEvent(
          deltaLog,
          opType = "delta.fsck.stats",
          data = FsckMetric(
            numFilesScanned,
            metrics("numMissingFiles").value,
            executionTimeMs,
            dryRun)
        )

        // No commit if dryRun is true
        if (!dryRun && filesToRemove.nonEmpty) {
          txn.commitIfNeeded(filesToRemove, DeltaOperations.Fsck(dryRun))
        }
        (filesToRemove, metrics("numMissingFiles").value)
      }
    }
  }
}

object FsckRepairTableCommand{
  def apply(
      path: Option[String],
      table: Option[TableIdentifier],
      dryRun: Boolean): FsckRepairTableCommand = {
    val child = UnresolvedDeltaPathOrIdentifier(path, table, "FSCK")
    FsckRepairTableCommand(child, dryRun)
  }
}

case class FsckMetric (
  numFilesScanned: Long,
  numMissingFiles: Long,
  executionTimeMs: Long,
  dryRun: Boolean
)
