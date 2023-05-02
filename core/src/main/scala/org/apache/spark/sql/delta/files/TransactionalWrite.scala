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

package org.apache.spark.sql.delta.files

import java.net.URI

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions._
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.constraints.{Constraint, Constraints, DeltaInvariantCheckerExec}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.{DeltaJobStatisticsTracker, StatisticsCollection}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{BasicWriteJobStatsTracker, FileFormatWriter, WriteJobStatsTracker}
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
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

  /** Replace the output attributes with the physical mapping information. */
  protected def mapColumnAttributes(
      output: Seq[Attribute],
      mappingMode: DeltaColumnMappingMode): Seq[Attribute] = {
    DeltaColumnMapping.createPhysicalAttributes(output, metadata.schema, mappingMode)
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
      data: Dataset[_]): (QueryExecution, Seq[Attribute], Seq[Constraint], Set[String]) = {
    val normalizedData = SchemaUtils.normalizeColumnNames(metadata.schema, data)
    val enforcesDefaultExprs = ColumnWithDefaultExprUtils.tableHasDefaultExpr(protocol, metadata)
    val (dataWithDefaultExprs, generatedColumnConstraints, trackHighWaterMarks) =
      if (enforcesDefaultExprs) {
        ColumnWithDefaultExprUtils.addDefaultExprsOrReturnConstraints(
          deltaLog,
          // We need the original query execution if this is a streaming query, because
          // `normalizedData` may add a new projection and change its type.
          data.queryExecution,
          metadata.schema,
          normalizedData)
      } else {
        (normalizedData, Nil, Set[String]())
      }
    val cleanedData = SchemaUtils.dropNullTypeColumns(dataWithDefaultExprs)
    val queryExecution = if (cleanedData.schema != dataWithDefaultExprs.schema) {
      // This must be batch execution as DeltaSink doesn't accept NullType in micro batch DataFrame.
      // For batch executions, we need to use the latest DataFrame query execution
      cleanedData.queryExecution
    } else if (enforcesDefaultExprs) {
      dataWithDefaultExprs.queryExecution
    } else {
      assert(
        normalizedData == dataWithDefaultExprs,
        "should not change data when there is no generate column")
      // Ideally, we should use `normalizedData`. But it may use `QueryExecution` rather than
      // `IncrementalExecution`. So we use the input `data` and leverage the `nullableOutput`
      // below to fix the column names.
      data.queryExecution
    }
    val nullableOutput = makeOutputNullable(cleanedData.queryExecution.analyzed.output)
    val columnMapping = metadata.columnMappingMode
    // Check partition column errors
    checkPartitionColumns(
      metadata.partitionSchema, nullableOutput, nullableOutput.length < data.schema.size
    )
    // Rewrite column physical names if using a mapping mode
    val mappedOutput = if (columnMapping == NoMapping) nullableOutput else {
      mapColumnAttributes(nullableOutput, columnMapping)
    }
    (queryExecution, mappedOutput, generatedColumnConstraints, trackHighWaterMarks)
  }

  protected def checkPartitionColumns(
      partitionSchema: StructType,
      output: Seq[Attribute],
      colsDropped: Boolean): Unit = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map { col =>
      // schema is already normalized, therefore we can do an equality check
      output.find(f => f.name == col.name).getOrElse(
        throw DeltaErrors.partitionColumnNotFoundException(col.name, output)
      )
    }
    if (partitionColumns.nonEmpty && partitionColumns.length == output.length) {
      throw DeltaErrors.nonPartitionColumnAbsentException(colsDropped)
    }
  }

  protected def getPartitioningColumns(
      partitionSchema: StructType,
      output: Seq[Attribute]): Seq[Attribute] = {
    val partitionColumns: Seq[Attribute] = partitionSchema.map { col =>
      // schema is already normalized, therefore we can do an equality check
      // we have already checked for missing columns, so the fields must exist
      output.find(f => f.name == col.name).get
    }
    partitionColumns
  }

  /**
   * If there is any string partition column and there are constraints defined, add a projection to
   * convert empty string to null for that column. The empty strings will be converted to null
   * eventually even without this convert, but we want to do this earlier before check constraints
   * so that empty strings are correctly rejected. Note that this should not cause the downstream
   * logic in `FileFormatWriter` to add duplicate conversions because the logic there checks the
   * partition column using the original plan's output. When the plan is modified with additional
   * projections, the partition column check won't match and will not add more conversion.
   *
   * @param plan The original SparkPlan.
   * @param partCols The partition columns.
   * @param constraints The defined constraints.
   * @return A SparkPlan potentially modified with an additional projection on top of `plan`
   */
  protected def convertEmptyToNullIfNeeded(
      plan: SparkPlan,
      partCols: Seq[Attribute],
      constraints: Seq[Constraint]): SparkPlan = {
    if (!spark.conf.get(DeltaSQLConf.CONVERT_EMPTY_TO_NULL_FOR_STRING_PARTITION_COL)) {
      return plan
    }
    // No need to convert if there are no constraints. The empty strings will be converted later by
    // FileFormatWriter and FileFormatDataWriter. Note that we might still do unnecessary convert
    // here as the constraints might not be related to the string partition columns. A precise
    // check will need to walk the constraints to see if such columns are really involved. It
    // doesn't seem to worth the effort.
    if (constraints.isEmpty) return plan

    val partSet = AttributeSet(partCols)
    var needConvert = false
    val projectList: Seq[NamedExpression] = plan.output.map {
      case p if partSet.contains(p) && p.dataType == StringType =>
        needConvert = true
        Alias(FileFormatWriter.Empty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) ProjectExec(projectList, plan) else plan
  }

  def writeFiles(
      data: Dataset[_],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    writeFiles(data, None, additionalConstraints)
  }

  def writeFiles(
      data: Dataset[_],
      writeOptions: Option[DeltaOptions]): Seq[FileAction] = {
    writeFiles(data, writeOptions, Nil)
  }

  def writeFiles(data: Dataset[_]): Seq[FileAction] = {
    writeFiles(data, Nil)
  }

  /**
   * Returns a tuple of (data, partition schema). For CDC writes, a `__is_cdc` column is added to
   * the data and `__is_cdc=true/false` is added to the front of the partition schema.
   */
  protected def performCDCPartition(inputData: Dataset[_]): (DataFrame, StructType) = {
    // If this is a CDC write, we need to generate the CDC_PARTITION_COL in order to properly
    // dispatch rows between the main table and CDC event records. This is a virtual partition
    // and will be stripped out later in [[DelayedCommitProtocolEdge]].
    // Note that the ordering of the partition schema is relevant - CDC_PARTITION_COL must
    // come first in order to ensure CDC data lands in the right place.
    if (CDCReader.isCDCEnabledOnTable(metadata, spark) &&
      inputData.schema.fieldNames.contains(CDCReader.CDC_TYPE_COLUMN_NAME)) {
      val augmentedData = inputData.withColumn(
        CDCReader.CDC_PARTITION_COL, col(CDCReader.CDC_TYPE_COLUMN_NAME).isNotNull)
      val partitionSchema = StructType(
        StructField(CDCReader.CDC_PARTITION_COL, StringType) +: metadata.physicalPartitionSchema)
      (augmentedData, partitionSchema)
    } else {
      (inputData.toDF(), metadata.physicalPartitionSchema)
    }
  }

  /**
   * Return a tuple of (statsDataSchema, statsCollectionSchema).
   * statsDataSchema is the data source schema from DataFrame used for stats collection. It
   * contains the columns in the DataFrame output, excluding the partition columns.
   * statsCollectionSchema is the schema to collect stats for. It contains the columns in the
   * table schema, excluding the partition columns.
   * Note: We only collect NULL_COUNT stats (as the number of rows) for the columns in
   * statsCollectionSchema but missing in statsDataSchema
   */
  protected def getStatsSchema(
    dataFrameOutput: Seq[Attribute],
    partitionSchema: StructType): (Seq[Attribute], Seq[Attribute]) = {
    val partitionColNames = partitionSchema.map(_.name).toSet

    // statsDataSchema comes from DataFrame output
    // schema should be normalized, therefore we can do an equality check
    val statsDataSchema = dataFrameOutput.filterNot(c => partitionColNames.contains(c.name))

    // statsCollectionSchema comes from table schema
    val statsTableSchema = metadata.schema.toAttributes
    val mappedStatsTableSchema = if (metadata.columnMappingMode == NoMapping) {
      statsTableSchema
    } else {
      mapColumnAttributes(statsTableSchema, metadata.columnMappingMode)
    }

    // It's important to first do the column mapping and then drop the partition columns
    val filteredStatsTableSchema = mappedStatsTableSchema
      .filterNot(c => partitionColNames.contains(c.name))

    (statsDataSchema, filteredStatsTableSchema)
  }

  protected def getStatsColExpr(
      statsDataSchema: Seq[Attribute],
      statsCollection: StatisticsCollection): Expression = {
    Dataset.ofRows(spark, LocalRelation(statsDataSchema))
      .select(to_json(statsCollection.statsCollector))
      .queryExecution.analyzed.expressions.head
  }


  /** Return the pair of optional stats tracker and stats collection class */
  protected def getOptionalStatsTrackerAndStatsCollection(
      output: Seq[Attribute],
      outputPath: Path,
      partitionSchema: StructType, data: DataFrame): (
        Option[DeltaJobStatisticsTracker],
        Option[StatisticsCollection]) = {
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_COLLECT_STATS)) {

      val (statsDataSchema, statsCollectionSchema) = getStatsSchema(output, partitionSchema)

      val indexedCols = DeltaConfigs.DATA_SKIPPING_NUM_INDEXED_COLS.fromMetaData(metadata)

      val statsCollection = new StatisticsCollection {
        override def tableDataSchema = {
          // If collecting stats using the table schema, then pass in statsCollectionSchema.
          // Otherwise pass in statsDataSchema to collect stats using the DataFrame schema.
          if (spark.sessionState.conf.getConf(DeltaSQLConf
            .DELTA_COLLECT_STATS_USING_TABLE_SCHEMA)) {
            statsCollectionSchema.toStructType
          } else {
            statsDataSchema.toStructType
          }
        }
        override def dataSchema = statsDataSchema.toStructType
        override val spark: SparkSession = data.sparkSession
        override val numIndexedCols = indexedCols
        override val protocol: Protocol = newProtocol.getOrElse(snapshot.protocol)
      }

      val statsColExpr = getStatsColExpr(statsDataSchema, statsCollection)

      (Some(new DeltaJobStatisticsTracker(
        deltaLog.newDeltaHadoopConf(),
        outputPath,
        statsDataSchema,
        statsColExpr)), Some(statsCollection))
    } else {
      (None, None)
    }
  }


  /**
   * Writes out the dataframe after performing schema validation. Returns a list of
   * actions to append these files to the reservoir.
   */
  def writeFiles(
      inputData: Dataset[_],
      writeOptions: Option[DeltaOptions],
      additionalConstraints: Seq[Constraint]): Seq[FileAction] = {
    hasWritten = true

    val spark = inputData.sparkSession
    val (data, partitionSchema) = performCDCPartition(inputData)
    val outputPath = deltaLog.dataPath

    val (queryExecution, output, generatedColumnConstraints, _) =
      normalizeData(deltaLog, data)
    val partitioningColumns = getPartitioningColumns(partitionSchema, output)

    val committer = getCommitter(outputPath)

    // If Statistics Collection is enabled, then create a stats tracker that will be injected during
    // the FileFormatWriter.write call below and will collect per-file stats using
    // StatisticsCollection
    val (optionalStatsTracker, _) = getOptionalStatsTrackerAndStatsCollection(output, outputPath,
      partitionSchema, data)


    val constraints =
      Constraints.getAll(metadata, spark) ++ generatedColumnConstraints ++ additionalConstraints

    SQLExecution.withNewExecutionId(queryExecution, Option("deltaTransactionalWrite")) {
      val outputSpec = FileFormatWriter.OutputSpec(
        outputPath.toString,
        Map.empty,
        output)

      val empty2NullPlan = convertEmptyToNullIfNeeded(queryExecution.executedPlan,
        partitioningColumns, constraints)
      val physicalPlan = DeltaInvariantCheckerExec(empty2NullPlan, constraints)

      val statsTrackers: ListBuffer[WriteJobStatsTracker] = ListBuffer()

      if (spark.conf.get(DeltaSQLConf.DELTA_HISTORY_METRICS_ENABLED)) {
        val basicWriteJobStatsTracker = new BasicWriteJobStatsTracker(
          new SerializableConfiguration(deltaLog.newDeltaHadoopConf()),
          BasicWriteJobStatsTracker.metrics)
        registerSQLMetrics(spark, basicWriteJobStatsTracker.driverSideMetrics)
        statsTrackers.append(basicWriteJobStatsTracker)
      }

      // Retain only a minimal selection of Spark writer options to avoid any potential
      // compatibility issues
      val options = writeOptions match {
        case None => Map.empty[String, String]
        case Some(writeOptions) =>
          writeOptions.options.filterKeys { key =>
            key.equalsIgnoreCase(DeltaOptions.MAX_RECORDS_PER_FILE) ||
              key.equalsIgnoreCase(DeltaOptions.COMPRESSION)
          }.toMap
      }

      try {
        FileFormatWriter.write(
          sparkSession = spark,
          plan = physicalPlan,
          fileFormat = deltaLog.fileFormat(protocol, metadata), // TODO support changing formats.
          committer = committer,
          outputSpec = outputSpec,
          // scalastyle:off deltahadoopconfiguration
          hadoopConf =
            spark.sessionState.newHadoopConfWithOptions(metadata.configuration ++ deltaLog.options),
          // scalastyle:on deltahadoopconfiguration
          partitionColumns = partitioningColumns,
          bucketSpec = None,
          statsTrackers = optionalStatsTracker.toSeq
            ++ statsTrackers,
          options = options)
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

    val resultFiles = committer.addedStatuses.map { a =>
      a.copy(stats = optionalStatsTracker.map(
        _.recordedStats(a.toPath.getName)).getOrElse(a.stats))
    }.filter {
      // In some cases, we can write out an empty `inputData`. Some examples of this (though, they
      // may be fixed in the future) are the MERGE command when you delete with empty source, or
      // empty target, or on disjoint tables. This is hard to catch before the write without
      // collecting the DF ahead of time. Instead, we can return only the AddFiles that
      // a) actually add rows, or
      // b) don't have any stats so we don't know the number of rows at all
      case a: AddFile => a.numLogicalRecords.forall(_ > 0)
      case _ => true
    }


    resultFiles.toSeq ++ committer.changeFiles
  }
}
