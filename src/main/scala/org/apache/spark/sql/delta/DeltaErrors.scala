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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.FileNotFoundException
import java.util.ConcurrentModificationException

import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{Invariant, InvariantViolationException}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{DataType, StructField, StructType}

trait DocsPath {
  /**
   * The URL for the base path of Delta's docs.
   */
  def baseDocsPath(conf: SparkConf): String = "https://docs.delta.io"
}

/**
 * A holder object for Delta errors.
 */
object DeltaErrors
    extends DocsPath
    with DeltaLogging {

  def baseDocsPath(spark: SparkSession): String = baseDocsPath(spark.sparkContext.getConf)

  val DeltaSourceIgnoreDeleteErrorMessage =
    "Detected deleted data from streaming source. This is currently not supported. If you'd like " +
      "to ignore deletes, set the option 'ignoreDeletes' to 'true'."

  val DeltaSourceIgnoreChangesErrorMessage =
    "Detected a data update in the source table. This is currently not supported. If you'd " +
      "like to ignore updates, set the option 'ignoreChanges' to 'true'. If you would like the " +
      "data update to be reflected, please restart this query with a fresh checkpoint directory."

  /**
   * File not found hint for Delta, replacing the normal one which is inapplicable.
   *
   * Note that we must pass in the docAddress as a string, because the config is not available on
   * executors where this method is called.
   */
  def deltaFileNotFoundHint(path: String, docAddress: String): String = {
    recordDeltaEvent(null, "delta.error.fileNotFound", data = path)
    val faq = docAddress + "/delta/delta-intro.html#frequently-asked-questions"
    "A file referenced in the transaction log cannot be found. This occurs when data has been " +
      "manually deleted from the file system rather than using the table `DELETE` statement. " +
      s"For more information, see $faq"
  }

  def formatColumn(colName: String): String = s"`$colName`"

  def formatColumnList(colNames: Seq[String]): String =
    colNames.map(formatColumn).mkString("[", ", ", "]")

  def formatSchema(schema: StructType): String = schema.treeString

  def analysisException(
      msg: String,
      line: Option[Int] = None,
      startPosition: Option[Int] = None,
      plan: Option[LogicalPlan] = None,
      cause: Option[Throwable] = None): AnalysisException = {
    new AnalysisException(msg, line, startPosition, plan, cause)
  }

  def notNullInvariantException(invariant: Invariant): Throwable = {
    new InvariantViolationException(s"Column ${UnresolvedAttribute(invariant.column).name}" +
      s", which is defined as ${invariant.rule.name}, is missing from the data being " +
      s"written into the table.")
  }

  def staticPartitionsNotSupportedException: Throwable = {
    new AnalysisException("Specifying static partitions in the partition spec is" +
      " currently not supported during inserts")
  }


  def operationNotSupportedException(
      operation: String, tableIdentifier: TableIdentifier): Throwable = {
    new AnalysisException(
        s"Operation not allowed: `$operation` is not supported " +
          s"for Delta tables: $tableIdentifier")
  }

  def operationNotSupportedException(operation: String): Throwable = {
    new AnalysisException(
      s"Operation not allowed: `$operation` is not supported for Delta tables")
  }

  def emptyDataException: Throwable = {
    new AnalysisException(
      "Data used in creating the Delta table doesn't have any columns.")
  }

  def notADeltaTableException(deltaTableIdentifier: DeltaTableIdentifier): Throwable = {
    new AnalysisException(s"$deltaTableIdentifier is not a Delta table.")
  }

  def notADeltaTableException(
      operation: String, deltaTableIdentifier: DeltaTableIdentifier): Throwable = {
    new AnalysisException(s"$deltaTableIdentifier is not a Delta table. " +
      s"$operation is only supported for Delta tables.")
  }

  def notADeltaTableException(operation: String): Throwable = {
    new AnalysisException(s"$operation is only supported for Delta tables.")
  }

  def notADeltaSourceException(command: String, plan: Option[LogicalPlan] = None): Throwable = {
    val planName = if (plan.isDefined) plan.toString else ""
    new AnalysisException(s"$command destination only supports Delta sources.\n$planName")
  }

  def missingTableIdentifierException(operationName: String): Throwable = {
    new AnalysisException(
      s"Please provide the path or table identifier for $operationName.")
  }

  def alterTableChangeColumnException(oldColumns: String, newColumns: String): Throwable = {
    new AnalysisException(
      "ALTER TABLE CHANGE COLUMN is not supported for changing column " + oldColumns + " to "
      + newColumns)
  }

  def alterTableReplaceColumnsException(
      oldSchema: StructType,
      newSchema: StructType,
      reason: String): Throwable = {
    new AnalysisException(
      s"""Unsupported ALTER TABLE REPLACE COLUMNS operation. Reason: $reason
         |
         |Failed to change schema from:
         |${formatSchema(oldSchema)}
         |to:
         |${formatSchema(newSchema)}""".stripMargin)
  }

  def unsetNonExistentPropertyException(
      propertyKey: String, deltaTableIdentifier: DeltaTableIdentifier): Throwable = {
    new AnalysisException(
      s"Attempted to unset non-existent property '$propertyKey' in table $deltaTableIdentifier")
  }

  def ambiguousPartitionColumnException(
      columnName: String, colMatches: Seq[StructField]): Throwable = {
    new AnalysisException(
      s"Ambiguous partition column ${formatColumn(columnName)} can be" +
        s" ${formatColumnList(colMatches.map(_.name))}.")
  }

  def vacuumBasePathMissingException(baseDeltaPath: Path): Throwable = {
    new AnalysisException(
      s"Please provide the base path ($baseDeltaPath) when Vacuuming Delta tables. " +
        "Vacuuming specific partitions is currently not supported.")
  }

  def unknownConfigurationKeyException(confKey: String): Throwable = {
    new AnalysisException(s"Unknown configuration was specified: $confKey")
  }

  def useDeltaOnOtherFormatPathException(
      operation: String, path: String, spark: SparkSession): Throwable = {
    new AnalysisException(
      s"""Incompatible format detected.
        |
        |You are trying to $operation `$path` using Delta Lake, but there is no
        |transaction log present. Check the upstream job to make sure that it is writing
        |using format("delta") and that you are trying to $operation the table base path.
        |
        |To disable this check, SET spark.databricks.delta.formatCheck.enabled=false
        |To learn more about Delta, see ${baseDocsPath(spark)}/delta/index.html
        |""".stripMargin)
  }

  def useOtherFormatOnDeltaPathException(
      operation: String,
      deltaRootPath: String,
      path: String,
      format: String,
      spark: SparkSession): Throwable = {
    new AnalysisException(
      s"""Incompatible format detected.
        |
        |A transaction log for Delta Lake was found at `$deltaRootPath/_delta_log`,
        |but you are trying to $operation `$path` using format("$format"). You must use
        |'format("delta")' when reading and writing to a delta table.
        |
        |To disable this check, SET spark.databricks.delta.formatCheck.enabled=false
        |To learn more about Delta, see ${baseDocsPath(spark)}/delta/index.html
        |""".stripMargin)
  }

  def pathNotSpecifiedException: Throwable = {
    new IllegalArgumentException("'path' is not specified")
  }

  def pathNotExistsException(path: String): Throwable = {
    new AnalysisException(s"$path doesn't exist")
  }

  def pathAlreadyExistsException(path: Path): Throwable = {
    new AnalysisException(s"$path already exists.")
  }

  def logFileNotFoundException(
      path: Path,
      version: Long,
      metadata: Metadata): Throwable = {
    val logRetention = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
    val checkpointRetention = DeltaConfigs.CHECKPOINT_RETENTION_DURATION.fromMetaData(metadata)
    new FileNotFoundException(s"$path: Unable to reconstruct state at version $version as the " +
      s"transaction log has been truncated due to manual deletion or the log retention policy " +
      s"(${DeltaConfigs.LOG_RETENTION.key}=$logRetention) and checkpoint retention policy " +
      s"(${DeltaConfigs.CHECKPOINT_RETENTION_DURATION.key}=$checkpointRetention)")
  }

  def logFileNotFoundExceptionForStreamingSource(e: FileNotFoundException): Throwable = {
    new FileNotFoundException(e.getMessage + " If you never deleted it, it's " +
      "likely your query is lagging behind. Please delete its checkpoint to restart" +
      " from scratch. To avoid this happening again, you can update your retention " +
      "policy of your Delta table").initCause(e)
  }

  def multipleLoadPathsException(paths: Seq[String]): Throwable = {
    throw new AnalysisException(
      s"""
        |Delta Lake does not support multiple input paths in the load() API.
        |paths: ${paths.mkString("[", ",", "]")}. To build a single DataFrame by loading
        |multiple paths from the same Delta table, please load the root path of
        |the Delta table with the corresponding partition filters. If the multiple paths
        |are from different Delta tables, please use Dataset's union()/unionByName() APIs
        |to combine the DataFrames generated by separate load() API calls.""".stripMargin)
  }

  def partitionColumnNotFoundException(colName: String, schema: Seq[Attribute]): Throwable = {
    new AnalysisException(
      s"Partition column ${formatColumn(colName)} not found in schema " +
        s"[${schema.map(_.name).mkString(", ")}]")
  }

  def partitionPathParseException(fragment: String): Throwable = {
    new AnalysisException(
      "A partition path fragment should be the form like `part1=foo/part2=bar`. "
        + s"The partition path: $fragment")
  }

  def partitionPathInvolvesNonPartitionColumnException(
      badColumns: Seq[String], fragment: String): Throwable = {

    new AnalysisException(
      s"Non-partitioning column(s) ${formatColumnList(badColumns)} are specified: $fragment")
  }

  def nonPartitionColumnAbsentException(colsDropped: Boolean): Throwable = {
    val msg = if (colsDropped) {
      " Columns which are of NullType have been dropped."
    } else {
      ""
    }
    new AnalysisException(
      s"Data written into Delta needs to contain at least one non-partitioned column.$msg")
  }

  def replaceWhereMismatchException(replaceWhere: String, badPartitions: String): Throwable = {
    new AnalysisException(
      s"""Data written out does not match replaceWhere '${replaceWhere}'.
         |Invalid data would be written to partitions $badPartitions.""".stripMargin)
  }

  def illegalDeltaOptionException(name: String, input: String, explain: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value '$input' for option '$name', $explain")
  }

  def modifyAppendOnlyTableException: Throwable = {
    new UnsupportedOperationException(
      "This table is configured to only allow appends. If you would like to permit " +
        s"updates or deletes, use 'ALTER TABLE <table_name> SET TBLPROPERTIES " +
        s"(${DeltaConfigs.IS_APPEND_ONLY.key}=false)'.")
  }

  def missingPartFilesException(c: CheckpointMetaData, ae: AnalysisException): Throwable = {
    new IllegalStateException(
      s"Couldn't find all part files of the checkpoint version: ${c.version}", ae)
  }

  def deltaVersionsNotContiguousException(deltaVersions: Seq[Long]): Throwable = {
    new IllegalStateException(s"versions (${deltaVersions}) are not contiguous")
  }

  def schemaChangedException(oldSchema: StructType, newSchema: StructType): Throwable = {
    new IllegalStateException(
      s"""Detected schema change:
        |old schema: ${formatSchema(oldSchema)}
        |
        |new schema: ${formatSchema(newSchema)}
        |
        |Please try restarting the query. If this issue repeats across query restarts without making
        |progress, you have made an incompatible schema change and need to start your query from
        |scratch using a new checkpoint directory.
      """.stripMargin)
  }

  def streamWriteNullTypeException: Throwable = {
    new AnalysisException(
      "Delta doesn't accept NullTypes in the schema for streaming writes.")
  }

  def schemaNotSetException: Throwable = {
    new AnalysisException(
      "Table schema is not set.  Write data into it or use CREATE TABLE to set the schema.")
  }

  def specifySchemaAtReadTimeException: Throwable = {
    new AnalysisException("Delta does not support specifying the schema at read time.")
  }

  def outputModeNotSupportedException(dataSource: String, outputMode: OutputMode): Throwable = {
    new AnalysisException(
      s"Data source $dataSource does not support $outputMode output mode")
  }

  def updateSetColumnNotFoundException(col: String, colList: Seq[String]): Throwable = {
    new AnalysisException(
      s"SET column ${formatColumn(col)} not found given columns: ${formatColumnList(colList)}.")
  }

  def updateSetConflictException(cols: Seq[String]): Throwable = {
    new AnalysisException(
      s"There is a conflict from these SET columns: ${formatColumnList(cols)}.")
  }

  def updateNonStructTypeFieldNotSupportedException(col: String, s: DataType): Throwable = {
    new AnalysisException(
      s"Updating nested fields is only supported for StructType, but you are trying to update " +
        s"a field of ${formatColumn(col)}, which is of type: $s.")
  }

  def truncateTablePartitionNotSupportedException: Throwable = {
    new AnalysisException(
      s"Operation not allowed: TRUNCATE TABLE on Delta tables does not support" +
        " partition predicates; use DELETE to delete specific partitions or rows.")
  }

  def bloomFilterOnPartitionColumnNotSupportedException(name: String): Throwable = {
    new AnalysisException(
      s"Creating a bloom filter index on a partitioning column is unsupported: $name")
  }

  def bloomFilterOnNestedColumnNotSupportedException(name: String): Throwable = {
    new AnalysisException(
      s"Creating a bloom filer index on a nested column is currently unsupported: $name")
  }

  def bloomFilterOnColumnTypeNotSupportedException(name: String, dataType: DataType): Throwable = {
    new AnalysisException(
      "Creating a bloom filter index on a column with type " +
        s"${dataType.catalogString} is unsupported: $name")
  }

  def bloomFilterMultipleConfForSingleColumnException(name: String): Throwable = {
    new AnalysisException(
      s"Multiple bloom filter index configurations passed to command for column: $name")
  }

  def bloomFilterCreateOnNonExistingColumnsException(unknownColumns: Seq[String]): Throwable = {
    new AnalysisException(
      "Cannot create bloom filter indices for the following non-existent column(s): "
        + unknownColumns.mkString(", "))
  }

  def bloomFilterInvalidParameterValueException(message: String): Throwable = {
    new AnalysisException(
      s"Cannot create bloom filter index, invalid parameter value: $message")
  }

  def bloomFilterDropOnNonIndexedColumnException(name: String): Throwable = {
    new AnalysisException(
      s"Cannot drop bloom filter index on a non indexed column: $name")
  }

  def bloomFilterDropOnNonExistingColumnsException(unknownColumns: Seq[String]): Throwable = {
    new AnalysisException(
      "Cannot drop bloom filter indices for the following non-existent column(s): "
        + unknownColumns.mkString(", "))
  }

  def multipleSourceRowMatchingTargetRowInMergeException: Throwable = {
    new UnsupportedOperationException("Cannot perform MERGE as multiple source rows " +
        "matched and attempted to update the same target row in the Delta table.")
  }

  def subqueryNotSupportedException(op: String, cond: Expression): Throwable = {
    new AnalysisException(s"Subqueries are not supported in the $op (condition = ${cond.sql}).")
  }

  def multiColumnInPredicateNotSupportedException(operation: String): Throwable = {
    new AnalysisException(
      s"Multi-column In predicates are not supported in the $operation condition.")
  }

  def nestedSubqueryNotSupportedException(operation: String): Throwable = {
    new AnalysisException(
      s"Nested subquery is not supported in the $operation condition.")
  }

  def nestedFieldNotSupported(operation: String, field: String): Throwable = {
    new AnalysisException(s"Nested field is not supported in the $operation (field = $field).")
  }

  def inSubqueryNotSupportedException(operation: String): Throwable = {
    new AnalysisException(
      s"In subquery is not supported in the $operation condition.")
  }

  def createExternalTableWithoutLogException(
      path: Path, tableName: String, spark: SparkSession): Throwable = {
    new AnalysisException(
      s"""
         |You are trying to create an external table $tableName
         |from `$path` using Delta Lake, but there is no transaction log present at
         |`$path/_delta_log`. Check the upstream job to make sure that it is writing using
         |format("delta") and that the path is the root of the table.
         |
         |To learn more about Delta, see ${baseDocsPath(spark)}/delta/index.html
       """.stripMargin)
  }

  def createExternalTableWithoutSchemaException(
      path: Path, tableName: String, spark: SparkSession): Throwable = {
    new AnalysisException(
      s"""
         |You are trying to create an external table $tableName
         |from `$path` using Delta Lake, but the schema is not specified when the
         |input path is empty.
         |
         |To learn more about Delta, see ${baseDocsPath(spark)}/delta/index.html
       """.stripMargin)
  }

  def createManagedTableWithoutSchemaException(
      tableName: String, spark: SparkSession): Throwable = {
    new AnalysisException(
      s"""
         |You are trying to create a managed table $tableName
         |using Delta Lake, but the schema is not specified.
         |
         |To learn more about Delta, see ${baseDocsPath(spark)}/delta/index.html
       """.stripMargin)
  }

  def aggsNotSupportedException(op: String, cond: Expression): Throwable = {
    val condStr = s"(condition = ${cond.sql})."
    new AnalysisException(s"Aggregate functions are not supported in the $op $condStr.")
  }

  def nonDeterministicNotSupportedException(op: String, cond: Expression): Throwable = {
    val condStr = s"(condition = ${cond.sql})."
    new AnalysisException(s"Non-deterministic functions are not supported in the $op $condStr.")
  }

  def noHistoryFound(logPath: Path): Throwable = {
    new AnalysisException(s"No commits found at $logPath")
  }

  def noReproducibleHistoryFound(logPath: Path): Throwable = {
    new AnalysisException(s"No reproducible commits found at $logPath")
  }

  def timestampEarlierThanCommitRetention(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp,
      timestampString: String): Throwable = {
    new AnalysisException(
      s"""The provided timestamp ($userTimestamp) is before the earliest version available to this
         |table ($commitTs). Please use a timestamp after $timestampString.
         """.stripMargin)
  }

  def temporallyUnstableInput(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp,
      timestampString: String,
      commitVersion: Long): Throwable = {
    new AnalysisException(
      s"""The provided timestamp: $userTimestamp is after the latest commit timestamp of
         |$commitTs. If you wish to query this version of the table, please either provide
         |the version with "VERSION AS OF $commitVersion" or use the exact timestamp
         |of the last commit: "TIMESTAMP AS OF '$timestampString'".
       """.stripMargin)
  }

  def versionNotExistException(userVersion: Long, earliest: Long, latest: Long): Throwable = {
    throw new AnalysisException(s"Cannot time travel Delta table to version $userVersion. " +
      s"Available versions: [$earliest, $latest].")
  }

  def timeTravelNotSupportedException: Throwable = {
    new AnalysisException("Cannot time travel views, subqueries or streams.")
  }

  def multipleTimeTravelSyntaxUsed: Throwable = {
    new AnalysisException("Cannot specify time travel in multiple formats.")
  }

  def provideOneOfInTimeTravel: Throwable = {
    new IllegalArgumentException(
      "Please either provide 'timestampAsOf' or 'versionAsOf' for time travel.")
  }

  def deltaLogAlreadyExistsException(path: String): Throwable = {
    new AnalysisException(s"A Delta Lake log already exists at $path")
  }

  // should only be used by fast import
  def commitAlreadyExistsException(version: Long, logPath: Path): Throwable = {
    new IllegalStateException(
      s"Commit of version $version already exists in the log: ${logPath.toUri.toString}")
  }

  def missingProviderForConvertException(path: String): Throwable = {
    new AnalysisException("CONVERT TO DELTA only supports parquet tables. " +
      s"Please rewrite your target as parquet.`$path` if it's a parquet directory.")
  }

  def convertNonParquetFilesException(path: String, sourceName: String): Throwable = {
    new AnalysisException("CONVERT TO DELTA only supports parquet files, but you are trying to " +
      s"convert a $sourceName source: `$sourceName`.`$path`")
  }

  def unexpectedPartitionColumnFromFileNameException(
      path: String, parsedCol: String, expectedCol: String): Throwable = {
    new AnalysisException(s"Expecting partition column ${formatColumn(expectedCol)}, but" +
      s" found partition column ${formatColumn(parsedCol)} from parsing the file name: $path")
  }

  def unexpectedNumPartitionColumnsFromFileNameException(
      path: String, parsedCols: Seq[String], expectedCols: Seq[String]): Throwable = {
    new AnalysisException(s"Expecting ${expectedCols.size} partition column(s): " +
      s"${formatColumnList(expectedCols)}, but found ${parsedCols.size} partition column(s): " +
      s"${formatColumnList(parsedCols)} from parsing the file name: $path")
  }

  def castPartitionValueException(partitionValue: String, dataType: DataType): Throwable = {
    new RuntimeException(
      s"Failed to cast partition value `$partitionValue` to $dataType")
  }

  def emptyDirectoryException(directory: String): Throwable = {
    new RuntimeException(s"No file found in the directory: $directory.")
  }

  def alterTableSetLocationSchemaMismatchException(
      original: StructType, destination: StructType): Throwable = {
    new AnalysisException(
      s"""
        |The schema of the new Delta location is different than the current table schema.
        |original schema:
        |  ${formatSchema(original)}
        |destination schema:
        |  ${formatSchema(destination)}
        |
        |If this is an intended change, you may turn this check off by running:
        |%sql set spark.databricks.delta.alterLocation.bypassSchemaCheck = true""".stripMargin)
  }

  def describeViewHistory: Throwable = {
    new AnalysisException("Cannot describe the history of a view.")
  }
}

/** The basic class for all Tahoe commit conflict exceptions. */
abstract class DeltaConcurrentModificationException(message: String)
  extends ConcurrentModificationException(message) {

  def this(baseMessage: String, conflictingCommit: Option[CommitInfo]) = this(
    baseMessage +
      conflictingCommit.map(ci => s"\nConflicting commit: ${JsonUtils.toJson(ci)}").getOrElse("") +
      s"\nRefer to ${DeltaErrors.baseDocsPath(SparkEnv.get.conf)}" +
      "/delta/isolation-level.html#optimistic-concurrency-control for more details."
  )

  /**
   * Type of the commit conflict.
   */
  def conflictType: String = this.getClass.getSimpleName.stripSuffix("Exception")
}

/**
 * Thrown when a concurrent transaction has written data after the current transaction read the
 * table.
 */
class ConcurrentWriteException(
    conflictingCommit: Option[CommitInfo]) extends DeltaConcurrentModificationException(
  s"A concurrent transaction has written new data since the current transaction " +
    s"read the table. Please try the operation again.", conflictingCommit)

/**
 * Thrown when the metadata of the Delta table has changed between the time of read
 * and the time of commit.
 */
class MetadataChangedException(
    conflictingCommit: Option[CommitInfo]) extends DeltaConcurrentModificationException(
  "The metadata of the Delta table has been changed by a concurrent update. " +
    "Please try the operation again.", conflictingCommit)

/**
 * Thrown when the protocol version has changed between the time of read
 * and the time of commit.
 */
class ProtocolChangedException(
    conflictingCommit: Option[CommitInfo]) extends DeltaConcurrentModificationException(
  "The protocol version of the Delta table has been changed by a concurrent update. " +
    "Please try the operation again.", conflictingCommit)

/** Thrown when files are added that would have been read by the current transaction. */
class ConcurrentAppendException(
    conflictingCommit: Option[CommitInfo],
    partition: String,
    customRetryMsg: Option[String] = None) extends DeltaConcurrentModificationException(
  s"Files were added to $partition by a concurrent update. " +
    customRetryMsg.getOrElse("Please try the operation again."), conflictingCommit)

/** Thrown when the current transaction reads data that was deleted by a concurrent transaction. */
class ConcurrentDeleteReadException(
    conflictingCommit: Option[CommitInfo],
    file: String) extends DeltaConcurrentModificationException(
  s"This transaction attempted to read one or more files that were deleted (for example $file) " +
    "by a concurrent update. Please try the operation again.", conflictingCommit)

/**
 * Thrown when the current transaction deletes data that was deleted by a concurrent transaction.
 */
class ConcurrentDeleteDeleteException(
    conflictingCommit: Option[CommitInfo],
    file: String) extends DeltaConcurrentModificationException(
  s"This transaction attempted to delete one or more files that were deleted (for example $file) " +
    "by a concurrent update. Please try the operation again.", conflictingCommit)

/** Thrown when concurrent transaction both attempt to update the same idempotent transaction. */
class ConcurrentTransactionException(
    conflictingCommit: Option[CommitInfo]) extends DeltaConcurrentModificationException(
  s"This error occurs when multiple streaming queries are using the same checkpoint to write " +
    "into this table. Did you run multiple instances of the same streaming query at the same " +
    "time?", conflictingCommit)

/** A helper class in building a helpful error message in case of metadata mismatches. */
class MetadataMismatchErrorBuilder {
  private var bits: Seq[String] = Nil

  private var mentionedOption = false

  def addSchemaMismatch(original: StructType, data: StructType): Unit = {
    bits ++=
      s"""A schema mismatch detected when writing to the Delta table.
         |To enable schema migration, please set:
         |'.option("${DeltaOptions.MERGE_SCHEMA_OPTION}", "true")'.
         |
         |Table schema:
         |${DeltaErrors.formatSchema(original)}
         |
         |Data schema:
         |${DeltaErrors.formatSchema(data)}
         """.stripMargin :: Nil
    mentionedOption = true
  }

  def addPartitioningMismatch(original: Seq[String], provided: Seq[String]): Unit = {
    bits ++=
      s"""Partition columns do not match the partition columns of the table.
         |Given: ${DeltaErrors.formatColumnList(provided)}
         |Table: ${DeltaErrors.formatColumnList(original)}
         """.stripMargin :: Nil
  }

  def addOverwriteBit(): Unit = {
    bits ++=
      s"""To overwrite your schema or change partitioning, please set:
         |'.option("${DeltaOptions.OVERWRITE_SCHEMA_OPTION}", "true")'.
         |
           |Note that the schema can't be overwritten when using
         |'${DeltaOptions.REPLACE_WHERE_OPTION}'.
         """.stripMargin :: Nil
    mentionedOption = true
  }

  def finalizeAndThrow(): Unit = {
    if (mentionedOption) {
      bits ++=
        """If Table ACLs are enabled, these options will be ignored. Please use the ALTER TABLE
          |command for changing the schema.
        """.stripMargin :: Nil
    }
    throw new AnalysisException(bits.mkString("\n"))
  }
}
