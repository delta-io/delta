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

package org.apache.spark.sql.delta.files

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints, DeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.util.SerializableConfiguration

/**
 * Adds the ability to write files out as part of a transaction. Checks
 * are performed to ensure that the data being written matches either the
 * current metadata or the new metadata being set by this transaction.
 */
trait TransactionalWrite extends DeltaLogging { self: OptimisticTransactionImpl =>

  def deltaLog: DeltaLog

  def protocol: Protocol

  protected def snapshot: Snapshot

  protected def metadata: Metadata

  protected var hasWritten = false

  protected def getCommitter(outputPath: Path): DelayedCommitProtocol =
    new DelayedCommitProtocol("delta", outputPath.toString, None)

  /** Makes the output attributes nullable, so that we don't write unreadable parquet files. */
  protected def makeOutputNullable(output: Seq[Attribute]): Seq[Attribute] = {
    output.map {
      case ref: AttributeReference =>
        val nullableDataType = SchemaUtils.typeAsNullable(ref.dataType)
        ref.copy(dataType = nullableDataType, nullable = true)(ref.exprId, ref.qualifier)
      case attr => attr.withNullability(true)
    }
  }

  /**
   * Normalize the schema of the query, and return the QueryExecution to execute. If the table has
   * generated columns and users provide these columns in the output, we will also return
   * constraints that should be respected. If any constraints are returned, the caller should apply
   * these constraints when writing data.
   *
   * Note: The output attributes of the QueryExecution may not match the attributes we return as the
   * output schema. This is because streaming queries create `IncrementalExecution`, which cannot be
   * further modified. We can however have the Parquet writer use the physical plan from
   * `IncrementalExecution` and the output schema provided through the attributes.
   */
  protected def normalizeData(
      deltaLog: DeltaLog,
      data: Dataset[_],
      partitionCols: Seq[String]): (QueryExecution, Seq[Attribute], Seq[Constraint]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(metadata.schema, data)
    val enforcesGeneratedColumns = GeneratedColumn.enforcesGeneratedColumns(protocol, metadata)
    val (dataWithGeneratedColumns, generatedColumnConstraints) =
      if (enforcesGeneratedColumns) {
        GeneratedColumn.addGeneratedColumnsOrReturnConstraints(
          deltaLog,
          // We need the original query execution if this is a streaming query, because
          // `normalizedData` may add a new projection and change its type.
          data.queryExecution,
          metadata.schema,
          normalizedData)
      } else {
        (normalizedData, Nil)
      }
    val cleanedData = SchemaUtils.dropNullTypeColumns(dataWithGeneratedColumns)
    val queryExecution = if (cleanedData.schema != dataWithGeneratedColumns.schema) {
      // This must be batch execution as DeltaSink doesn't accept NullType in micro batch DataFrame.
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData.queryExecution
    } else if (enforcesGeneratedColumns) {
      dataWithGeneratedColumns.queryExecution
    } else {
      assert(
        normalizedData == dataWithGeneratedColumns,
        "should not change data when there is no generate column")
      // Ideally, we should use `normalizedData`. But it may use `QueryExecution` rather than
      // `IncrementalExecution`. So we use the input `data` and leverage the `nullableOutput`
      // below to fix the column names.
      data.queryExecution
    }
    val nullableOutput = makeOutputNullable(cleanedData.queryExecution.analyzed.output)
    (queryExecution, nullableOutput, generatedColumnConstraints)
  }

  protected def getPartitioningColumns(
      partitionSchema: StructType,
      output: Seq[Attribute],
      colsDropped: Boolean): Seq[Attribute] = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map { col =>
      // schema is already normalized, therefore we can do an equality check
      output.find(f => f.name == col.name)
        .getOrElse {
          throw DeltaErrors.partitionColumnNotFoundException(col.name, output)
        }
    }
    if (partitionColumns.nonEmpty && partitionColumns.length == output.length) {
      throw DeltaErrors.nonPartitionColumnAbsentException(colsDropped)
    }
    partitionColumns
  }

  def writeFiles(data: Dataset[_], writeOptions: Option[DeltaOptions]): Seq[FileAction] = {
    writeFiles(data)
  }

  /**
   * Writes out the dataframe after performing schema validation. Returns a list of
   * actions to append these files to the reservoir.
   */
  def writeFiles(data: Dataset[_]): Seq[FileAction] = {
    if (DeltaConfigs.CHANGE_DATA_CAPTURE.fromMetaData(metadata) ||
        DeltaConfigs.CHANGE_DATA_CAPTURE_LEGACY.fromMetaData(metadata)) {
      throw DeltaErrors.cdcWriteNotAllowedInThisVersion()
    }

    hasWritten = true

    val spark = data.sparkSession
    val partitionSchema = metadata.partitionSchema
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints) =
      normalizeData(deltaLog, data, metadata.partitionColumns)
    val partitioningColumns =
      getPartitioningColumns(partitionSchema, output, output.length < data.schema.size)

    val committer = getCommitter(outputPath)

    val constraints = Constraints.getAll(metadata, spark) ++ generatedColumnConstraints

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(
        outputPath.toString,
        Map.empty,
        output)

      val physicalPlan = DeltaInvariantCheckerExec(queryExecution.executedPlan, constraints)

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(spark.sessionState.newHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        registerSQLMetrics(spark, basicWriteJobStatsTracker.metrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      try {
        FileFormatWriter.write(
          sparkSession = spark,
          plan = physicalPlan,
          fileFormat = snapshot.fileFormat, // TODO doesn't support changing formats.
          committer = committer,
          outputSpec = outputSpec,
          hadoopConf = spark.sessionState.newHadoopConfWithOptions(metadata.configuration),
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers = statsTrackers,
          options = Map.empty)
      } catch {
        case s: SparkException =>
          // Pull an InvariantViolationException up to the top level if it was the root cause.
          val violationException = ExceptionUtils.getRootCause(s)
          if (violationException.isInstanceOf[InvariantViolationException]) {
            throw violationException
          } else {
            throw s
          }
      }
    }

    committer.addedStatuses
  }
}
