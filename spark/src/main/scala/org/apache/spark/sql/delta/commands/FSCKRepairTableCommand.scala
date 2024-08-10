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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, If, Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, UnaryNode}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.UnresolvedDeltaPathOrIdentifier
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.{RemoveFile}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DateTimeUtils.NANOS_PER_MILLIS
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.metric.SQLMetrics.{createMetric, createTimingMetric}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.NullType
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.SerializableConfiguration

import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor


trait FsckCommandMetrics { self: RunnableCommand =>
  @transient private lazy val sc: SparkContext = SparkContext.getOrCreate()
  // Create metrics
  def createMetrics: Map[String, SQLMetric] = Map[String, SQLMetric](
    "numMissingFiles" -> createMetric(sc, "number of files removed."),
    "executionTimeMs" -> createTimingMetric(sc, "time taken to execute the entire operation."),
    "numFilesScanned" -> createMetric(sc, "Number of files scanned."),
    "numMissingDVs" -> createMetric(sc, "Number of files with missing deletion vectors.")
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
      AttributeReference("dataFileMissing", BooleanType, nullable = false)(),
      AttributeReference("deletionVectorPath", StringType, nullable = true)(),
      AttributeReference("deletionVectorFileMissing", BooleanType, nullable = false)())
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
    val (removedFiles, numMissingFiles, numMissingDVs) =
      performFsck(sparkSession, deltaLog, catalogTable)
    val limit =
      sparkSession.sessionState.conf.getConf(DeltaSQLConf.FSCK_MAX_NUM_ENTRIES_IN_RESULT)
    // Display the relevant log
    val missingDVMessage = s"Found (${numMissingDVs}) file(s) with missing deletion vectors.\n"
    val msg = if (dryRun) {
      if (numMissingFiles <= limit) {
        s"Found (${numMissingFiles}) file(s) to be removed from the delta log. Listing all rows"
      } else {
        s"Found (${numMissingFiles}) file(s) to be removed from the delta log."
      }
    } else {
      s"Removed (${numMissingFiles}) file(s) from the delta log."
    }
    logConsole(missingDVMessage + msg)
    logInfo(missingDVMessage + msg)
    // Format the output (the list of deleted files)
    removedFiles.map(file => {
      Row(file.getAs[String]("path"),
        file.getAs[Boolean]("dataFileMissing"),
        file.getAs[String]("deletionVectorPath"),
        file.getAs[Boolean]("deletionVectorFileMissing"))
    })
  }

  def performFsck(
      sparkSession: SparkSession,
      deltaLog: DeltaLog,
      catalogTable: Option[CatalogTable]): (Seq[Row], Long, Long) = {
    recordDeltaOperation(deltaLog, "delta.fsck") {
      deltaLog.withNewTransaction(catalogTable) { txn =>
        val DVRemoveConf =
          sparkSession.sessionState.conf.getConf(DeltaSQLConf.FSCK_MISSING_DVS_MODE)
        val startTime = System.nanoTime()
        // scalastyle:off sparkimplicits
        import sparkSession.implicits._
        // scalastyle:on sparkimplicits

        // Get the display limit for dry run
        val limit =
        sparkSession.sessionState.conf.getConf(DeltaSQLConf.FSCK_MAX_NUM_ENTRIES_IN_RESULT)

        // Get the files referenced in the current Delta Log
        // We keep the numRecords stats for removeFile entry
        val allFiles = deltaLog.snapshot.withStats
          .withColumn("stats", to_json(struct(col("stats.numRecords") as "numRecords")))
          .as[AddFile]

        // Get the file count for metrics and current time.
        val numFilesScanned = allFiles.count()
        val currentTimestamp = System.currentTimeMillis()

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

        // Create a UDF to get the absolute path of a deletion vector and check if it exists
        val absoluteDVPathMissing = udf((deletionVec: Option[DeletionVectorDescriptor],
                                 tablePath: String) => {
          val output = deletionVec match {
            case None => (None, false)
            case Some(deletionVec) if deletionVec.isOnDisk =>
              val absolutePath = deletionVec.absolutePath(new Path(tablePath))
              val exists = absolutePath
                .getFileSystem(serializableHadoopConf.value).exists(absolutePath)
              (Some(absolutePath.toString()), !exists)
            case Some(deletionVec) => (None, false)
          }
          output
        }).asNondeterministic()

        val allMissingFiles = allFiles
          .withColumn("deletionVectorInfo",
          absoluteDVPathMissing(col("deletionVector"),
            lit(deltaLog.dataPath.toString)))
          .withColumn("dataFileMissing", fileExists(lit(deltaLog.dataPath.toString),
            col("path"), lit(dryRun)) === false)
          .select("deletionVectorInfo.*", "path", "dataFileMissing")
          .withColumnRenamed("_1", "deletionVectorPath")
          .withColumnRenamed("_2", "deletionVectorFileMissing")
          .filter(col("dataFileMissing") || col("deletionVectorFileMissing"))
          .collect()
        val filesToRemove = allMissingFiles.filter(row => row.getBoolean(3) || row.getBoolean(1))
          .map(row => {
            val file = allFiles.filter(col("path") === row.getString(2)).first()
            file.removeWithTimestamp(currentTimestamp, false)
        })

        val filesToCommit = DVRemoveConf match {
          case "exception" =>
            val missingDV = allMissingFiles
              .filter(row => !row.getBoolean(3) && row.getBoolean(1)).headOption
            missingDV match {
              case Some(value) =>
                if (!dryRun) {
                  val path = value.getString(2)
                  throw DeltaErrors.fileNotFoundException(path)
                }
                filesToRemove
              case _ => filesToRemove
            }
          case "removeDV" =>
            // In this implementation, only remove the deletion vector from the delta log
            // Filter out the files that only have DV missing
            // For files that only have the DV missing, we will add the file
            // with the deletion vector set to null
            val filesToAdd = allMissingFiles
              .filter(row => !row.getBoolean(3) && row.getBoolean(1))
              .map(row => {
                val file = allFiles.filter(col("path") === row.getString(2)).first()
                file.copy(deletionVector = null, modificationTime = currentTimestamp)
              })
            val filesToCommit = filesToRemove ++ filesToAdd
            val output = allMissingFiles
            filesToCommit
        }

        // Calculate metrics
        val executionTimeMs = (System.nanoTime() - startTime) / NANOS_PER_MILLIS
        val numMissingDVs = allMissingFiles.filter(row => row.getBoolean(1)).length
        val numMissingFiles = allMissingFiles.filter(row => row.getBoolean(3)).length

        // Set SQL metrics
        metrics("executionTimeMs").set(executionTimeMs)
        metrics("numFilesScanned").set(numFilesScanned)
        metrics("numMissingDVs").set(numMissingDVs)
        metrics("numMissingFiles").set(numMissingFiles)

        // Register the metrics
        txn.registerSQLMetrics(sparkSession, metrics)
        sendDriverMetrics(sparkSession, metrics)

        recordDeltaEvent(
          deltaLog,
          opType = "delta.fsck.stats",
          data = FsckMetric(
            numFilesScanned,
            metrics("numMissingFiles").value,
            executionTimeMs,
            metrics("numMissingDVs").value,
            dryRun)
        )

        if (!dryRun && filesToCommit.nonEmpty) {
          // Commit both the add and the remove actions
          txn.commitIfNeeded(filesToCommit,
            DeltaOperations.Fsck(dryRun))
        }
        // Prepare the output
        // Prepare the output

        val output = if (dryRun) {
          allMissingFiles.take(limit)
        } else {
          allMissingFiles
        }

        (output, metrics("numMissingFiles").value, metrics("numMissingDVs").value)
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
  numMissingDVs: Long,
  dryRun: Boolean
)