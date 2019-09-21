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

package org.apache.spark.sql.delta.files

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema._
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.FileFormatWriter
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}

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
   * Normalize the schema of the query, and return the QueryExecution to execute. The output
   * attributes of the QueryExecution may not match the attributes we return as the output schema.
   * This is because streaming queries create `IncrementalExecution`, which cannot be further
   * modified. We can however have the Parquet writer use the physical plan from
   * `IncrementalExecution` and the output schema provided through the attributes.
   */
  protected def normalizeData(
      data: Dataset[_],
      partitionCols: Seq[String]): (QueryExecution, Seq[Attribute]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(metadata.schema, data)
    val cleanedData = SchemaUtils.dropNullTypeColumns(normalizedData)
    val queryExecution = if (cleanedData.schema != normalizedData.schema) {
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData.queryExecution
    } else {
      // For streaming workloads, we need to use the QueryExecution created from StreamExecution
      data.queryExecution
    }
    queryExecution -> makeOutputNullable(cleanedData.queryExecution.analyzed.output)
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

  def writeFiles(data: Dataset[_]): Seq[AddFile] = writeFiles(data, None, isOptimize = false)

  def writeFiles(data: Dataset[_], writeOptions: Option[DeltaOptions]): Seq[AddFile] =
    writeFiles(data, writeOptions, isOptimize = false)

  def writeFiles(data: Dataset[_], isOptimize: Boolean): Seq[AddFile] =
    writeFiles(data, None, isOptimize = isOptimize)

  /**
   * Writes out the dataframe after performing schema validation. Returns a list of
   * actions to append these files to the reservoir.
   */
  def writeFiles(
      data: Dataset[_],
      writeOptions: Option[DeltaOptions],
      isOptimize: Boolean): Seq[AddFile] = {
    hasWritten = true

    val spark = data.sparkSession
    val partitionSchema = metadata.partitionSchema
    val outputPath = deltaLog.dataPath

    val (queryExecution, output) = normalizeData(data, metadata.partitionColumns)
    val partitioningColumns =
      getPartitioningColumns(partitionSchema, output, output.length < data.schema.size)

    val committer = getCommitter(outputPath)

    val invariants = Invariants.getFromSchema(metadata.schema, spark)

    SQLExecution.withNewExecutionId(spark, queryExecution) {
      val outputSpec = FileFormatWriter.OutputSpec(
        outputPath.toString,
        Map.empty,
        output)

      val physicalPlan = DeltaInvariantCheckerExec(queryExecution.executedPlan, invariants)

      FileFormatWriter.write(
        sparkSession = spark,
        plan = physicalPlan,
        fileFormat = snapshot.fileFormat, // TODO doesn't support changing formats.
        committer = committer,
        outputSpec = outputSpec,
        hadoopConf = spark.sessionState.newHadoopConfWithOptions(metadata.configuration),
        partitionColumns = partitioningColumns,
        bucketSpec = None,
        statsTrackers = Nil,
        options = Map.empty)
    }

    committer.addedStatuses
  }
}
