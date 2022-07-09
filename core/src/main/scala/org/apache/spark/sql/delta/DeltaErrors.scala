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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.{FileNotFoundException, IOException}
import java.nio.file.FileAlreadyExistsException
import java.util.ConcurrentModificationException

import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, InvariantViolationException, SchemaUtils, UnsupportedDataTypeInfo}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path
import org.json4s.JValue

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}


trait DocsPath {
  /**
   * The URL for the base path of Delta's docs. When changing this path, ensure that the new path
   * works with the error messages below.
   */
  protected def baseDocsPath(conf: SparkConf): String = "https://docs.delta.io/latest"

  def assertValidCallingFunction(): Unit = {
    val callingMethods = Thread.currentThread.getStackTrace
    callingMethods.foreach { method =>
      if (errorsWithDocsLinks.contains(method.getMethodName)) {
        return
      }
    }
    assert(assertion = false, "The method throwing the error which contains a doc link must be a " +
      s"part of DocsPath.errorsWithDocsLinks")
  }

  /**
   * Get the link to the docs for the given relativePath. Validates that the error generating the
   * link is added to docsLinks.
   *
   * @param relativePath the relative path after the base url to access.
   * @param skipValidation whether to validate that the function generating the link is
   *                       in the allowlist.
   * @return The entire URL of the documentation link
   */
  def generateDocsLink(
      conf: SparkConf,
      relativePath: String,
      skipValidation: Boolean = false): String = {
    if (!skipValidation) assertValidCallingFunction()
    baseDocsPath(conf) + relativePath
  }

  /**
   * List of error function names for all errors that have URLs. When adding your error to this list
   * remember to also add it to the list of errors in DeltaErrorsSuite
   *
   * @note add your error to DeltaErrorsSuiteBase after adding it to this list so that the url can
   *       be tested
   */
  def errorsWithDocsLinks: Seq[String] = Seq(
    "createExternalTableWithoutLogException",
    "createExternalTableWithoutSchemaException",
    "createManagedTableWithoutSchemaException",
    "multipleSourceRowMatchingTargetRowInMergeException",
    "faqRelativePath",
    "ignoreStreamingUpdatesAndDeletesWarning",
    "concurrentModificationExceptionMsg",
    "incorrectLogStoreImplementationException",
    "sourceNotDeterministicInMergeException",
    "columnMappingAdviceMessage"
  )
}

/**
 * A holder object for Delta errors.
 *
 *
 * IMPORTANT: Any time you add a test that references the docs, add to the Seq defined in
 * DeltaErrorsSuite so that the doc links that are generated can be verified to work in
 * docs.delta.io
 */
trait DeltaErrorsBase
    extends DocsPath
    with DeltaLogging {

  def baseDocsPath(spark: SparkSession): String = baseDocsPath(spark.sparkContext.getConf)

  val faqRelativePath: String = "/delta-intro.html#frequently-asked-questions"

  val EmptyCheckpointErrorMessage =
    s"""
       |Attempted to write an empty checkpoint without any actions. This checkpoint will not be
       |useful in recomputing the state of the table. However this might cause other checkpoints to
       |get deleted based on retention settings.
     """.stripMargin

  def deltaSourceIgnoreDeleteError(version: Long, removedFile: String): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_SOURCE_IGNORE_DELETE",
      messageParameters = Array(removedFile, version.toString)
    )
  }

  def deltaSourceIgnoreChangesError(version: Long, removedFile: String): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_SOURCE_TABLE_IGNORE_CHANGES",
      messageParameters = Array(removedFile, version.toString)
    )
  }

  /**
   * File not found hint for Delta, replacing the normal one which is inapplicable.
   *
   * Note that we must pass in the docAddress as a string, because the config is not available on
   * executors where this method is called.
   */
  def deltaFileNotFoundHint(faqPath: String, path: String): String = {
    recordDeltaEvent(null, "delta.error.fileNotFound", data = path)
    "A file referenced in the transaction log cannot be found. This occurs when data has been " +
      "manually deleted from the file system rather than using the table `DELETE` statement. " +
      s"For more information, see $faqPath"
  }

  def columnNotFound(path: Seq[String], schema: StructType): Throwable = {
    val name = UnresolvedAttribute(path).name
    cannotResolveColumn(name, schema)
  }

  def failedMergeSchemaFile(file: String, schema: String, cause: Throwable): Throwable = {
    new DeltaSparkException(
      errorClass = "DELTA_FAILED_MERGE_SCHEMA_FILE",
      messageParameters = Array(file, schema),
      cause = cause)
  }

  def failOnCheckpoint(src: Path, dest: Path): DeltaIllegalStateException = {
    failOnCheckpoint(src.toString, dest.toString)
  }

  def failOnCheckpoint(src: String, dest: String): DeltaIllegalStateException = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_CANNOT_RENAME_PATH",
      messageParameters = Array(s"$src", s"$dest"))
  }

  def checkpointMismatchWithSnapshot : Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_CHECKPOINT_SNAPSHOT_MISMATCH",
      messageParameters = Array.empty
    )
  }

  /**
   * Thrown when main table data contains columns that are reserved for CDF, such as `_change_type`.
   */
  def cdcColumnsInData(columns: Seq[String]): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "RESERVED_CDC_COLUMNS_ON_WRITE",
      messageParameters = Array(columns.mkString("[", ",", "]"), DeltaConfigs.CHANGE_DATA_FEED.key)
    )
  }

  /**
   * Thrown when main table data already contains columns that are reserved for CDF, such as
   * `_change_type`, but CDF is not yet enabled on that table.
   */
  def tableAlreadyContainsCDCColumns(columns: Seq[String]): Throwable = {
    new DeltaIllegalStateException(errorClass = "DELTA_TABLE_ALREADY_CONTAINS_CDC_COLUMNS",
      messageParameters = Array(columns.mkString("[", ",", "]")))
  }

  /**
   * Thrown when a CDC query contains conflict 'starting' or 'ending' options, e.g. when both
   * starting version and starting timestamp are specified.
   *
   * @param position Specifies which option was duplicated in the read. Values are "starting" or
   *                 "ending"
   */
  def multipleCDCBoundaryException(position: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MULTIPLE_CDC_BOUNDARY",
      messageParameters = Array(position, position, position)
    )
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

  def notNullColumnMissingException(constraint: Constraints.NotNull): Throwable = {
    new DeltaInvariantViolationException(
      errorClass = "DELTA_MISSING_NOT_NULL_COLUMN_VALUE",
      messageParameters = Array(s"${UnresolvedAttribute(constraint.column).name}"))
  }

  def nestedNotNullConstraint(
      parent: String, nested: DataType, nestType: String): AnalysisException = {
    new AnalysisException(s"The $nestType type of the field $parent contains a NOT NULL " +
      s"constraint. Delta does not support NOT NULL constraints nested within arrays or maps. " +
      s"To suppress this error and silently ignore the specified constraints, set " +
      s"${DeltaSQLConf.ALLOW_UNENFORCED_NOT_NULL_CONSTRAINTS.key} = true.\n" +
      s"Parsed $nestType type:\n${nested.prettyJson}")
  }

  def nullableParentWithNotNullNestedField : Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NOT_NULL_NESTED_FIELD",
      messageParameters = Array.empty
    )
  }

  def constraintAlreadyExists(name: String, oldExpr: String): AnalysisException = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CONSTRAINT_ALREADY_EXISTS",
      messageParameters = Array(name, oldExpr)
    )
  }

  def invalidConstraintName(name: String): AnalysisException = {
    new AnalysisException(s"Cannot use '$name' as the name of a CHECK constraint.")
  }

  def nonexistentConstraint(constraintName: String, tableName: String): AnalysisException = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CONSTRAINT_DOES_NOT_EXIST",
      messageParameters = Array(
        constraintName,
        tableName,
        DeltaSQLConf.DELTA_ASSUMES_DROP_CONSTRAINT_IF_EXISTS.key,
        "true"))
  }

  def checkConstraintNotBoolean(name: String, expr: String): AnalysisException = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NON_BOOLEAN_CHECK_CONSTRAINT",
      messageParameters = Array(name, expr)
    )
  }

  def newCheckConstraintViolated(num: Long, tableName: String, expr: String): AnalysisException = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
      messageParameters = Array(s"$num", tableName, expr)
    )
  }

  def newNotNullViolated(
      num: Long, tableName: String, col: UnresolvedAttribute): AnalysisException = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NEW_NOT_NULL_VIOLATION",
      messageParameters = Array(s"$num", tableName, col.name)
    )
  }

  def useAddConstraints: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_ADD_CONSTRAINTS",
      messageParameters = Array.empty)
  }

  def incorrectLogStoreImplementationException(
      sparkConf: SparkConf,
      cause: Throwable): Throwable = {
    new DeltaIOException(
      errorClass = "DELTA_INCORRECT_LOG_STORE_IMPLEMENTATION",
      messageParameters = Array(generateDocsLink(sparkConf, "/delta-storage.html")),
      cause = cause)
  }

  def failOnDataLossException(expectedVersion: Long, seenVersion: Long): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_MISSING_FILES_UNEXPECTED_VERSION",
      messageParameters = Array(s"$expectedVersion", s"$seenVersion",
        s"${DeltaOptions.FAIL_ON_DATA_LOSS_OPTION}")
    )
  }

  def staticPartitionsNotSupportedException: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_STATIC_PARTITIONS",
      messageParameters = Array.empty
    )
  }

  def zOrderingOnPartitionColumnException(colName: String): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_ZORDERING_ON_PARTITION_COLUMN",
      messageParameters = Array(colName)
    )
  }

  def zOrderingOnColumnWithNoStatsException(
      colNames: Seq[String],
      spark: SparkSession): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_ZORDERING_ON_COLUMN_WITHOUT_STATS",
      messageParameters = Array(colNames.mkString("[", ", ", "]"),
        DeltaSQLConf.DELTA_OPTIMIZE_ZORDER_COL_STAT_CHECK.key)
    )
  }

  def zOrderingColumnDoesNotExistException(colName: String): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_ZORDERING_COLUMN_DOES_NOT_EXIST",
      messageParameters = Array(colName))
  }

  /**
   * Throwable used when CDC options contain no 'start'.
   */
  def noStartVersionForCDC(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NO_START_FOR_CDC_READ",
      messageParameters = Array.empty
    )
  }

  /**
   * Throwable used when CDC is not enabled according to table metadata.
   */
  def changeDataNotRecordedException(version: Long, start: Long, end: Long): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MISSING_CHANGE_DATA",
      messageParameters = Array(start.toString, end.toString, version.toString,
        DeltaConfigs.CHANGE_DATA_FEED.key))
  }

  /**
   * Throwable used for invalid CDC 'start' and 'end' options, where end < start
   */
  def endBeforeStartVersionInCDC(start: Long, end: Long): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_INVALID_CDC_RANGE",
      messageParameters = Array(start.toString, end.toString)
    )
  }

  /**
   * Throwable used for invalid CDC 'start' and 'latest' options, where latest < start
   */
  def startVersionAfterLatestVersion(start: Long, latest: Long): Throwable = {
    new IllegalArgumentException(
      s"Provided Start version($start) for reading change data is invalid. " +
        s"Start version cannot be greater than the latest version of the table($latest).")
  }

  def unexpectedChangeFilesFound(changeFiles: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_UNEXPECTED_CHANGE_FILES_FOUND",
      messageParameters = Array(changeFiles))
  }

  def addColumnAtIndexLessThanZeroException(pos: String, col: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_ADD_COLUMN_AT_INDEX_LESS_THAN_ZERO",
      messageParameters = Array(pos, col))
  }

  def columnNameNotFoundException(colName: String, scheme: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_COLUMN_NOT_FOUND",
      messageParameters = Array(colName, scheme))
  }

  def foundDuplicateColumnsException(colType: String, duplicateCols: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_DUPLICATE_COLUMNS_FOUND",
      messageParameters = Array(colType, duplicateCols))
  }

  def addColumnStructNotFoundException(pos: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_ADD_COLUMN_STRUCT_NOT_FOUND",
      messageParameters = Array(pos))
  }

  def operationNotSupportedException(
      operation: String, tableIdentifier: TableIdentifier): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_OPERATION_NOT_ALLOWED_DETAIL",
      messageParameters = Array(operation, tableIdentifier.toString))
  }

  def operationNotSupportedException(operation: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_OPERATION_NOT_ALLOWED",
      messageParameters = Array(operation))
  }

  def emptyDataException: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_EMPTY_DATA", messageParameters = Array.empty)
  }

  def fileNotFoundException(path: String): Throwable = {
    new DeltaFileNotFoundException(
      errorClass = "DELTA_FILE_NOT_FOUND",
      messageParameters = Array(path))
  }

  def fileOrDirectoryNotFoundException(path: String): Throwable = {
    new DeltaFileNotFoundException(
      errorClass = "DELTA_FILE_OR_DIR_NOT_FOUND",
      messageParameters = Array(path))
  }

  def excludeRegexOptionException(regexOption: String, cause: Throwable = null): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_REGEX_OPT_SYNTAX_ERROR",
      messageParameters = Array(regexOption),
      cause = cause)
  }

  def notADeltaTableException(deltaTableIdentifier: DeltaTableIdentifier): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MISSING_DELTA_TABLE",
      messageParameters = Array(s"$deltaTableIdentifier"))
  }

  def notADeltaTableException(
      operation: String, deltaTableIdentifier: DeltaTableIdentifier): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_TABLE_ONLY_OPERATION",
      messageParameters = Array(s"$deltaTableIdentifier", s"$operation"))
  }

  def notADeltaTableException(operation: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_ONLY_OPERATION",
      messageParameters = Array(operation)
    )
  }

  def notADeltaSourceException(command: String, plan: Option[LogicalPlan] = None): Throwable = {
    val planName = if (plan.isDefined) plan.toString else ""
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_SOURCE",
      messageParameters = Array(command, s"$planName")
    )
  }

  def partitionColumnCastFailed(
      columnValue: String,
      dataType: String,
      columnName: String): Throwable = {
    new DeltaRuntimeException(
      errorClass = "DELTA_PARTITION_COLUMN_CAST_FAILED",
      messageParameters = Array(columnValue, dataType, columnName))
  }

  def schemaChangedSinceAnalysis(
      atAnalysis: StructType,
      latestSchema: StructType,
      mentionLegacyFlag: Boolean = false): Throwable = {
    val schemaDiff = SchemaUtils.reportDifferences(atAnalysis, latestSchema)
      .map(_.replace("Specified", "Latest"))
    val legacyFlagMessage = if (mentionLegacyFlag) {
      s"""
         |This check can be turned off by setting the session configuration key
         |${DeltaSQLConf.DELTA_SCHEMA_ON_READ_CHECK_ENABLED.key} to false.""".stripMargin
    } else {
      ""
    }
    new DeltaAnalysisException(
      errorClass = "DELTA_SCHEMA_CHANGE_SINCE_ANALYSIS",
      messageParameters = Array(schemaDiff.mkString("\n"), legacyFlagMessage)
    )
  }

  def incorrectArrayAccess(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INCORRECT_ARRAY_ACCESS",
      messageParameters = Array.empty)
  }
  def invalidColumnName(name: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_CHARACTERS_IN_COLUMN_NAME",
      messageParameters = Array(name))
  }

  def invalidIsolationLevelException(s: String): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_INVALID_ISOLATION_LEVEL",
      messageParameters = Array(s))
  }

  def invalidPartitionColumn(col: String, tbl: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_PARTITION_COLUMN",
      messageParameters = Array(col, tbl))
  }

  def invalidPartitionColumn(e: AnalysisException): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_PARTITION_COLUMN_NAME",
      messageParameters = Array.empty,
      cause = Option(e))
  }

  def invalidTimestampFormat(
      ts: String,
      format: String,
      cause: Option[Throwable] = None): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_TIMESTAMP_FORMAT",
      messageParameters = Array(ts, format),
      cause = cause)
  }

  def missingTableIdentifierException(operationName: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_OPERATION_MISSING_PATH",
      messageParameters = Array(operationName)
    )
  }

  def viewInDescribeDetailException(view: TableIdentifier): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_DESCRIBE_DETAIL_VIEW",
      messageParameters = Array(s"$view")
    )
  }

  def alterTableChangeColumnException(oldColumns: String, newColumns: String): Throwable = {
    new AnalysisException(
      "ALTER TABLE CHANGE COLUMN is not supported for changing column " + oldColumns + " to "
      + newColumns)
  }

  def notADeltaTable(table: String): Throwable = {
    new DeltaAnalysisException(errorClass = "DELTA_NOT_A_DELTA_TABLE",
      messageParameters = Array(table))
  }

  def unsupportedWriteStagedTable(tableName: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_WRITES_STAGED_TABLE",
      messageParameters = Array(tableName)
    )
  }

  def notEnoughColumnsInInsert(
      table: String,
      query: Int,
      target: Int,
      nestedField: Option[String] = None): Throwable = {
    val nestedFieldStr = nestedField.map(f => s"not enough nested fields in $f")
      .getOrElse("not enough data columns")
    new DeltaAnalysisException(
      errorClass = "DELTA_INSERT_COLUMN_ARITY_MISMATCH",
      messageParameters = Array(table, nestedFieldStr, target.toString, query.toString))
  }

  def notFoundFileToBeRewritten(absolutePath: String, candidates: Iterable[String]): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_FILE_TO_OVERWRITE_NOT_FOUND",
      messageParameters = Array(absolutePath, candidates.mkString("\n")))
  }

  def cannotFindSourceVersionException(json: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_CANNOT_FIND_VERSION",
      messageParameters = Array(json))
  }

  def cannotInsertIntoColumn(
      tableName: String,
      source: String,
      target: String,
      targetType: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_COLUMN_STRUCT_TYPE_MISMATCH",
      messageParameters = Array(source, targetType, target, tableName))
  }

  def alterTableReplaceColumnsException(
      oldSchema: StructType,
      newSchema: StructType,
      reason: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_ALTER_TABLE_REPLACE_COL_OP",
      messageParameters = Array(reason, formatSchema(oldSchema), formatSchema(newSchema))
    )
  }

  def ambiguousPartitionColumnException(
      columnName: String, colMatches: Seq[StructField]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_AMBIGUOUS_PARTITION_COLUMN",
      messageParameters = Array(formatColumn(columnName).toString,
        formatColumnList(colMatches.map(_.name)))
    )
  }

  def tableNotSupportedException(operation: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_TABLE_NOT_SUPPORTED_IN_OP",
      messageParameters = Array(operation)
    )
  }

  def vacuumBasePathMissingException(baseDeltaPath: Path): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_VACUUM_SPECIFIC_PARTITION",
      messageParameters = Array(s"$baseDeltaPath")
    )
  }

  def unexpectedDataChangeException(op: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_DATA_CHANGE_FALSE",
      messageParameters = Array(op)
    )
  }

  def unknownConfigurationKeyException(confKey: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNKNOWN_CONFIGURATION",
      messageParameters = Array(confKey))
  }

  def cdcNotAllowedInThisVersion(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CDC_NOT_ALLOWED_IN_THIS_VERSION",
      messageParameters = Array.empty
    )
  }

  def cdcWriteNotAllowedInThisVersion(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CHANGE_TABLE_FEED_DISABLED",
      messageParameters = Array.empty
    )
  }

  def pathNotSpecifiedException: Throwable = {
    new IllegalArgumentException("'path' is not specified")
  }

  def pathNotExistsException(path: String): Throwable = {
    new DeltaAnalysisException(errorClass = "DELTA_PATH_DOES_NOT_EXIST",
      messageParameters = Array(path))
  }

  def directoryNotFoundException(path: String): Throwable = {
    new FileNotFoundException(s"$path doesn't exist")
  }

  def pathAlreadyExistsException(path: Path): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_PATH_EXISTS",
      messageParameters = Array(s"$path")
    )
  }

  def logFileNotFoundException(
      path: Path,
      version: Long,
      metadata: Metadata): Throwable = {
    val logRetention = DeltaConfigs.LOG_RETENTION.fromMetaData(metadata)
    val checkpointRetention = DeltaConfigs.CHECKPOINT_RETENTION_DURATION.fromMetaData(metadata)
    new DeltaFileNotFoundException(
      errorClass = "DELTA_TRUNCATED_TRANSACTION_LOG",
      messageParameters = Array(
        path.toString,
        version.toString,
        DeltaConfigs.LOG_RETENTION.key,
        logRetention.toString,
        DeltaConfigs.CHECKPOINT_RETENTION_DURATION.key,
        checkpointRetention.toString)
    )
  }

  def logFileNotFoundExceptionForStreamingSource(e: FileNotFoundException): Throwable = {
    new FileNotFoundException(e.getMessage + " If you never deleted it, it's " +
      "likely your query is lagging behind. Please delete its checkpoint to restart" +
      " from scratch. To avoid this happening again, you can update your retention " +
      "policy of your Delta table").initCause(e)
  }

  def logFailedIntegrityCheck(version: Long, mismatchOption: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_TXN_LOG_FAILED_INTEGRITY",
      messageParameters = Array(version.toString, mismatchOption)
    )
  }

  def checkpointNonExistTable(path: Path): Throwable = {
    new IllegalStateException(s"Cannot checkpoint a non-exist table $path. Did you manually " +
      s"delete files in the _delta_log directory?")
  }

  def multipleLoadPathsException(paths: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "MULTIPLE_LOAD_PATH",
      messageParameters = Array(paths.mkString("[", ",", "]")))
  }

  def partitionColumnNotFoundException(colName: String, schema: Seq[Attribute]): Throwable = {
    new AnalysisException(
      s"Partition column ${formatColumn(colName)} not found in schema " +
        s"[${schema.map(_.name).mkString(", ")}]")
  }

  def partitionPathParseException(fragment: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_PARTITION_PATH",
      messageParameters = Array(fragment))
  }

  def partitionPathInvolvesNonPartitionColumnException(
      badColumns: Seq[String], fragment: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NON_PARTITION_COLUMN_SPECIFIED",
      messageParameters = Array(formatColumnList(badColumns), fragment)
    )
  }

  def nonPartitionColumnAbsentException(colsDropped: Boolean): Throwable = {
    val msg = if (colsDropped) {
      " Columns which are of NullType have been dropped."
    } else {
      ""
    }
    new DeltaAnalysisException(
      errorClass = "DELTA_NON_PARTITION_COLUMN_ABSENT",
      messageParameters = Array(msg)
    )
  }

  def replaceWhereMismatchException(
      replaceWhere: String,
      invariantViolation: InvariantViolationException): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_REPLACE_WHERE_MISMATCH",
      messageParameters = Array(replaceWhere, invariantViolation.getMessage),
      cause = Some(invariantViolation))
  }

  def replaceWhereMismatchException(replaceWhere: String, badPartitions: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_REPLACE_WHERE_MISMATCH",
      messageParameters = Array(replaceWhere,
        s"Invalid data would be written to partitions $badPartitions."))
  }

  def illegalFilesFound(file: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_ILLEGAL_FILE_FOUND",
      messageParameters = Array(file))
  }

  def illegalDeltaOptionException(name: String, input: String, explain: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value '$input' for option '$name', $explain")
  }

  def invalidIdempotentWritesOptionsException(explain: String): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_INVALID_IDEMPOTENT_WRITES_OPTIONS",
      messageParameters = Array(explain))
  }

  def invalidInterval(interval: String): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_INVALID_INTERVAL",
      messageParameters = Array(interval)
      )
  }

  def invalidTableValueFunction(function: String) : Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_TABLE_VALUE_FUNCTION",
      messageParameters = Array(function)
    )
  }

  def startingVersionAndTimestampBothSetException(
      versionOptKey: String,
      timestampOptKey: String): Throwable = {
    new IllegalArgumentException(s"Please either provide '$versionOptKey' or '$timestampOptKey'")
  }

  def unrecognizedLogFile(path: Path): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_UNRECOGNIZED_LOGFILE",
      messageParameters = Array(s"$path")
    )
  }

  def modifyAppendOnlyTableException(tableName: String): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_CANNOT_MODIFY_APPEND_ONLY",
      messageParameters = Array(tableName, DeltaConfigs.IS_APPEND_ONLY.key)
    )
  }

  def missingPartFilesException(version: Long, ae: Exception): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_MISSING_PART_FILES",
      messageParameters = Array(s"$version"),
      cause = ae
    )
  }

  def deltaVersionsNotContiguousException(
      spark: SparkSession, deltaVersions: Seq[Long]): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_VERSIONS_NOT_CONTIGUOUS",
      messageParameters = Array(deltaVersions.toString())
    )
  }

  def actionNotFoundException(action: String, version: Long): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_STATE_RECOVER_ERROR",
      messageParameters = Array(action, version.toString,
        DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key))
  }

  def schemaChangedException(
      oldSchema: StructType,
      newSchema: StructType,
      retryable: Boolean): Throwable = {
    val msg =
      s"""Detected schema change:
        |old schema: ${formatSchema(oldSchema)}
        |
        |new schema: ${formatSchema(newSchema)}
        |
        |Please try restarting the query. If this issue repeats across query restarts without making
        |progress, you have made an incompatible schema change and need to start your query from
        |scratch using a new checkpoint directory.
      """.stripMargin
    new IllegalStateException(msg)
  }

  def streamWriteNullTypeException: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NULL_SCHEMA_IN_STREAMING_WRITE",
      messageParameters = Array.empty
    )
  }

  def schemaNotSetException: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_SCHEMA_NOT_SET",
      messageParameters = Array.empty
    )
  }

  def specifySchemaAtReadTimeException: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_SCHEMA_DURING_READ",
      messageParameters = Array.empty
    )
  }

  def schemaNotProvidedException: Throwable = {
    new AnalysisException(
      "Table schema is not provided. Please provide the schema of the table when using " +
        "REPLACE table and an AS SELECT query is not provided.")
  }

  def outputModeNotSupportedException(dataSource: String, outputMode: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_OUTPUT_MODE",
      messageParameters = Array(dataSource, outputMode)
    )
  }

  def updateSetColumnNotFoundException(col: String, colList: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MISSING_SET_COLUMN",
      messageParameters = Array(formatColumn(col), formatColumnList(colList)))
  }

  def updateSetConflictException(cols: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CONFLICT_SET_COLUMN",
      messageParameters = Array(formatColumnList(cols)))
  }

  def updateNonStructTypeFieldNotSupportedException(col: String, s: DataType): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_FIELD_UPDATE_NON_STRUCT",
      messageParameters = Array(s"${formatColumn(col)}", s"$s")
    )
  }

  def truncateTablePartitionNotSupportedException: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_TRUNCATE_TABLE_PARTITION_NOT_SUPPORTED", messageParameters = Array.empty
    )
  }

  def blockStreamingReadsOnColumnMappingEnabledTable: Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_UNSUPPORTED_COLUMN_MAPPING_STREAMING_READS")
  }

  def bloomFilterOnPartitionColumnNotSupportedException(name: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_PARTITION_COLUMN_IN_BLOOM_FILTER",
      messageParameters = Array(name))
  }

  def bloomFilterOnNestedColumnNotSupportedException(name: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_NESTED_COLUMN_IN_BLOOM_FILTER",
      messageParameters = Array(name))
  }

  def bloomFilterOnColumnTypeNotSupportedException(name: String, dataType: DataType): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_COLUMN_TYPE_IN_BLOOM_FILTER",
      messageParameters = Array(s"${dataType.catalogString}", name))
  }

  def bloomFilterMultipleConfForSingleColumnException(name: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MULTIPLE_CONF_FOR_SINGLE_COLUMN_IN_BLOOM_FILTER",
      messageParameters = Array(name))
  }

  def bloomFilterCreateOnNonExistingColumnsException(unknownColumns: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_CREATE_BLOOM_FILTER_NON_EXISTING_COL",
      messageParameters = Array(unknownColumns.mkString(", ")))
  }

  def bloomFilterInvalidParameterValueException(message: String): Throwable = {
    new AnalysisException(
      s"Cannot create bloom filter index, invalid parameter value: $message")
  }

  def bloomFilterDropOnNonIndexedColumnException(name: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_DROP_BLOOM_FILTER_ON_NON_INDEXED_COLUMN",
      messageParameters = Array(name))
  }

  def bloomFilterDropOnNonExistingColumnsException(unknownColumns: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_BLOOM_FILTER_DROP_ON_NON_EXISTING_COLUMNS",
      messageParameters = Array(unknownColumns.mkString(", "))
    )
  }

  def cannotRenamePath(tempPath: String, path: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_CANNOT_RENAME_PATH", messageParameters = Array(tempPath, path))
  }

  def cannotSpecifyBothFileListAndPatternString(): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_FILE_LIST_AND_PATTERN_STRING_CONFLICT",
      messageParameters = null)
  }

  def cannotUpdateArrayField(table: String, field: String): Throwable = {
    new DeltaAnalysisException(errorClass = "DELTA_CANNOT_UPDATE_ARRAY_FIELD",
      messageParameters = Array(table, field))
  }

  def cannotUpdateMapField(table: String, field: String): Throwable = {
    new DeltaAnalysisException(errorClass = "DELTA_CANNOT_UPDATE_MAP_FIELD",
      messageParameters = Array(table, field))
  }

  def cannotUpdateStructField(table: String, field: String): Throwable = {
    new DeltaAnalysisException(errorClass = "DELTA_CANNOT_UPDATE_STRUCT_FIELD",
      messageParameters = Array(table, field))
  }

  def cannotUseDataTypeForPartitionColumnError(field: StructField): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_PARTITION_COLUMN_TYPE",
      messageParameters = Array(s"${field.name}", s"${field.dataType}")
    )
  }

  def multipleSourceRowMatchingTargetRowInMergeException(spark: SparkSession): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE",
      messageParameters = Array(generateDocsLink(spark.sparkContext.getConf,
        "/delta-update.html#upsert-into-a-table-using-merge"))
    )
  }

  def sourceNotDeterministicInMergeException(spark: SparkSession): Throwable = {
    new UnsupportedOperationException(
      s"""Cannot perform Merge because the source dataset is not deterministic. Please refer to
         |${generateDocsLink(spark.sparkContext.getConf,
        "/delta-update.html#operation-semantics")} for more information.""".stripMargin
    )
  }

  def columnOfTargetTableNotFoundInMergeException(targetCol: String,
      colNames: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_COLUMN_NOT_FOUND_IN_MERGE",
      messageParameters = Array(targetCol, colNames)
    )
  }

  def subqueryNotSupportedException(op: String, cond: Expression): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_SUBQUERY",
      messageParameters = Array(op, cond.sql)
    )
  }

  def multiColumnInPredicateNotSupportedException(operation: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_MULTI_COL_IN_PREDICATE",
      messageParameters = Array(operation)
    )
  }

  def nestedFieldNotSupported(operation: String, field: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_NESTED_FIELD_IN_OPERATION",
      messageParameters = Array(operation, field)
    )
  }

  def nestedFieldsNeedRename(columns : Set[String], baseSchema : StructType): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NESTED_FIELDS_NEED_RENAME",
      messageParameters = Array(columns.mkString("[", ", ", "]"), formatSchema(baseSchema))
    )
  }

  def inSubqueryNotSupportedException(operation: String): Throwable = {
    new AnalysisException(
      s"In subquery is not supported in the $operation condition.")
  }

  def convertMetastoreMetadataMismatchException(
      tableProperties: Map[String, String],
      deltaConfiguration: Map[String, String]): Throwable = {
    def prettyMap(m: Map[String, String]): String = {
      m.map(e => s"${e._1}=${e._2}").mkString("[", ", ", "]")
    }
    new AnalysisException(
      s"""You are trying to convert a table which already has a delta log where the table
         |properties in the catalog don't match the configuration in the delta log.
         |Table properties in catalog: ${prettyMap(tableProperties)}
         |Delta configuration: ${prettyMap{deltaConfiguration}}
         |If you would like to merge the configurations (update existing fields and insert new
         |ones), set the SQL configuration
         |spark.databricks.delta.convert.metadataCheck.enabled to false.
       """.stripMargin)
  }

  def createExternalTableWithoutLogException(
      path: Path, tableName: String, spark: SparkSession): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CREATE_EXTERNAL_TABLE_WITHOUT_TXN_LOG",
      messageParameters = Array(tableName, path.toString,
      generateDocsLink(spark.sparkContext.getConf, "/index.html")))
  }

  def createExternalTableWithoutSchemaException(
      path: Path, tableName: String, spark: SparkSession): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CREATE_EXTERNAL_TABLE_WITHOUT_SCHEMA",
      messageParameters = Array(tableName, path.toString,
        generateDocsLink(spark.sparkContext.getConf, "/index.html")))
  }

  def createManagedTableWithoutSchemaException(
      tableName: String, spark: SparkSession): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_MANAGED_TABLE_SYNTAX_NO_SCHEMA",
      messageParameters = Array(tableName, s"""${generateDocsLink(spark.sparkContext.getConf,
        "/index.html")}""".stripMargin)
    )
  }

  def readTableWithoutSchemaException(identifier: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_READ_TABLE_WITHOUT_COLUMNS",
      messageParameters = Array(identifier))
  }

  def createTableWithDifferentSchemaException(
      path: Path,
      specifiedSchema: StructType,
      existingSchema: StructType,
      diffs: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CREATE_TABLE_SCHEME_MISMATCH",
      messageParameters = Array(path.toString,
        specifiedSchema.treeString, existingSchema.treeString,
        diffs.map("\n".r.replaceAllIn(_, "\n  ")).mkString("- ", "\n- ", "")))
  }

  def createTableWithDifferentPartitioningException(
      path: Path,
      specifiedColumns: Seq[String],
      existingColumns: Seq[String]): Throwable = {
    new AnalysisException(
      s"""The specified partitioning does not match the existing partitioning at $path.
         |
         |== Specified ==
         |${specifiedColumns.mkString(", ")}
         |
         |== Existing ==
         |${existingColumns.mkString(", ")}
        """.stripMargin)
  }

  def createTableWithDifferentPropertiesException(
      path: Path,
      specifiedProperties: Map[String, String],
      existingProperties: Map[String, String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY",
      messageParameters = Array(path.toString,
        specifiedProperties.map { case (k, v) => s"$k=$v" }.mkString("\n"),
        existingProperties.map { case (k, v) => s"$k=$v" }.mkString("\n"))
    )
  }

  def aggsNotSupportedException(op: String, cond: Expression): Throwable = {
    val condStr = s"(condition = ${cond.sql})."
    new DeltaAnalysisException(
      errorClass = "DELTA_AGGREGATION_NOT_SUPPORTED",
      messageParameters = Array(op, condStr)
    )
  }

  def nonDeterministicNotSupportedException(op: String, cond: Expression): Throwable = {
    val condStr = s"(condition = ${cond.sql})."
    new DeltaAnalysisException(
      errorClass = "DELTA_NON_DETERMINISTIC_FUNCTION_NOT_SUPPORTED",
      messageParameters = Array(op, s"$condStr")
    )
  }

  def noHistoryFound(logPath: Path): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NO_COMMITS_FOUND",
      messageParameters = Array(logPath.toString))
  }

  def noReproducibleHistoryFound(logPath: Path): Throwable = {
    new AnalysisException(s"No reproducible commits found at $logPath")
  }

  def unsupportedAbsPathAddFile(str: String): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_UNSUPPORTED_ABS_PATH_ADD_FILE",
      messageParameters = Array(str)
    )
  }

  case class TimestampEarlierThanCommitRetentionException(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp,
      timestampString: String) extends AnalysisException(
    s"""The provided timestamp ($userTimestamp) is before the earliest version available to this
         |table ($commitTs). Please use a timestamp after $timestampString.
         """.stripMargin)

  def timestampGreaterThanLatestCommit(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp,
      timestampString: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_TIMESTAMP_GREATER_THAN_COMMIT",
      messageParameters = Array(s"$userTimestamp", s"$commitTs", timestampString)
    )
  }

  def timestampInvalid(expr: Expression): Throwable = {
    new AnalysisException(
      s"The provided timestamp (${expr.sql}) cannot be converted to a valid timestamp.")
  }

  case class TemporallyUnstableInputException(
      userTimestamp: java.sql.Timestamp,
      commitTs: java.sql.Timestamp,
      timestampString: String,
      commitVersion: Long) extends AnalysisException(
    s"""The provided timestamp: $userTimestamp is after the latest commit timestamp of
         |$commitTs. If you wish to query this version of the table, please either provide
         |the version with "VERSION AS OF $commitVersion" or use the exact timestamp
         |of the last commit: "TIMESTAMP AS OF '$timestampString'".
       """.stripMargin)

  def restoreVersionNotExistException(
      userVersion: Long,
      earliest: Long,
      latest: Long): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_RESTORE_TABLE_VERSION",
      messageParameters = Array(userVersion.toString, earliest.toString, latest.toString))
  }

  def restoreTimestampGreaterThanLatestException(
      userTimestamp: String,
      latestTimestamp: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_RESTORE_TIMESTAMP_GREATER",
      messageParameters = Array(userTimestamp, latestTimestamp)
    )
  }

  def restoreTimestampBeforeEarliestException(
      userTimestamp: String,
      earliestTimestamp: String): Throwable = {
    new AnalysisException(
      s"Cannot restore table to timestamp ($userTimestamp) as it is before the earliest version " +
        s"available. Please use a timestamp after ($earliestTimestamp)"
    )
  }

  def timeTravelNotSupportedException: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_TIME_TRAVEL_VIEWS",
      messageParameters = Array.empty
    )
  }

  def multipleTimeTravelSyntaxUsed: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_TIME_TRAVEL_MULTIPLE_FORMATS",
      messageParameters = Array.empty
    )
  }

  def nonExistentDeltaTable(table: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_TABLE_NOT_FOUND", messageParameters = Array(table))
  }

  def nonExistentColumnInSchema(column: String, schema: String): Throwable = {
    new DeltaAnalysisException("DELTA_COLUMN_NOT_FOUND_IN_SCHEMA",
      Array(column, schema))
  }

  def provideOneOfInTimeTravel: Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_ONEOF_IN_TIMETRAVEL", messageParameters = null)
  }

  def emptyCalendarInterval: Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_INVALID_CALENDAR_INTERVAL_EMPTY",
      messageParameters = Array.empty
    )
  }

  def invalidMergeClauseWhenNotMatched(clause: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MERGE_INVALID_WHEN_NOT_MATCHED_CLAUSE",
      messageParameters = Array(clause)
    )
  }

  def unexpectedPartialScan(path: Path): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNEXPECTED_PARTIAL_SCAN",
      messageParameters = Array(s"$path")
    )
  }

  def deltaLogAlreadyExistsException(path: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_LOG_ALREADY_EXISTS",
      messageParameters = Array(path)
    )
  }

  // should only be used by fast import
  def commitAlreadyExistsException(version: Long, logPath: Path): Throwable = {
    new IllegalStateException(
      s"Commit of version $version already exists in the log: ${logPath.toUri.toString}")
  }

  def missingProviderForConvertException(path: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MISSING_PROVIDER_FOR_CONVERT",
      messageParameters = Array(path))
  }

  def convertNonParquetTablesException(ident: TableIdentifier, sourceName: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CONVERT_NON_PARQUET_TABLE",
      messageParameters = Array(sourceName, ident.toString)
    )
  }

  def unexpectedPartitionColumnFromFileNameException(
      path: String, parsedCol: String, expectedCol: String): Throwable = {
    new AnalysisException(s"Expecting partition column ${formatColumn(expectedCol)}, but" +
      s" found partition column ${formatColumn(parsedCol)} from parsing the file name: $path")
  }

  def unexpectedNumPartitionColumnsFromFileNameException(
      path: String, parsedCols: Seq[String], expectedCols: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNEXPECTED_NUM_PARTITION_COLUMNS_FROM_FILE_NAME",
      messageParameters = Array(
        expectedCols.size.toString,
        formatColumnList(expectedCols),
        parsedCols.size.toString,
        formatColumnList(parsedCols),
        path)
    )
  }

  def castPartitionValueException(partitionValue: String, dataType: DataType): Throwable = {
    new DeltaRuntimeException(
      errorClass = "DELTA_FAILED_CAST_PARTITION_VALUE",
      messageParameters = Array(partitionValue, dataType.toString))
  }

  def emptyDirectoryException(directory: String): Throwable = {
    new DeltaFileNotFoundException(
      errorClass = "DELTA_EMPTY_DIRECTORY",
      messageParameters = Array(directory)
    )
  }

  def alterTableSetLocationSchemaMismatchException(
      original: StructType, destination: StructType): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_SET_LOCATION_SCHEMA_MISMATCH",
      messageParameters = Array(formatSchema(original), formatSchema(destination),
        DeltaSQLConf.DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK.key))
  }

  def sparkSessionNotSetException(): Throwable = {
    new DeltaIllegalStateException(errorClass = "DELTA_SPARK_SESSION_NOT_SET")
  }

  def setLocationNotSupportedOnPathIdentifiers(): Throwable = {
    new AnalysisException("Cannot change the location of a path based table.")
  }

  def useSetLocation(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_CHANGE_LOCATION",
      messageParameters = Array.empty
    )
  }

  def cannotSetLocationMultipleTimes(locations : Seq[String]) : Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_CANNOT_SET_LOCATION_MULTIPLE_TIMES",
      messageParameters = Array(s"${locations}")
    )
  }

  def cannotReplaceMissingTableException(itableIdentifier: Identifier): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_REPLACE_MISSING_TABLE",
      messageParameters = Array(itableIdentifier.toString))
  }

  def cannotCreateLogPathException(logPath: String): Throwable = {
    new DeltaIOException(
      errorClass = "DELTA_CANNOT_CREATE_LOG_PATH",
      messageParameters = Array(logPath))
  }

  def cannotChangeProvider(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_CHANGE_PROVIDER",
      messageParameters = Array.empty
    )
  }

  def describeViewHistory: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_DESCRIBE_VIEW_HISTORY",
      messageParameters = Array.empty
    )
  }

  def viewNotSupported(operationName: String): Throwable = {
    new AnalysisException(s"Operation $operationName can not be performed on a view")
  }

  def postCommitHookFailedException(
      failedHook: PostCommitHook,
      failedOnCommitVersion: Long,
      extraErrorMessage: String,
      error: Throwable): Throwable = {
    var errorMessage = ""
    if (extraErrorMessage != null && extraErrorMessage.nonEmpty) {
      errorMessage += s": $extraErrorMessage"
    }
    val ex = new DeltaRuntimeException(
      errorClass = "DELTA_POST_COMMIT_HOOK_FAILED",
      messageParameters = Array(s"$failedOnCommitVersion", failedHook.name, errorMessage)
    )
    ex.initCause(error)
    ex
  }

  def unsupportedGenerateModeException(modeName: String): Throwable = {
    import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
    val supportedModes = DeltaGenerateCommand.modeNameToGenerationFunc.keys.toSeq.mkString(", ")
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_MODE_NOT_SUPPORTED",
      messageParameters = Array(modeName, supportedModes))
  }

  def illegalUsageException(option: String, operation: String): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_ILLEGAL_USAGE",
      messageParameters = Array(option, operation))
  }

  def foundMapTypeColumnException(key: String, value: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_FOUND_MAP_TYPE_COLUMN",
      messageParameters = Array(key, value)
    )
  }
  def columnNotInSchemaException(column: String, schema: StructType): Throwable = {
    nonExistentColumnInSchema(column, schema.treeString)
  }

  def metadataAbsentException(): Throwable = {
    new DeltaIllegalStateException(errorClass = "DELTA_METADATA_ABSENT",
      messageParameters = Array(DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key))
  }

  def updateSchemaMismatchExpression(from: StructType, to: StructType): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UPDATE_SCHEMA_MISMATCH_EXPRESSION",
      messageParameters = Array(from.catalogString, to.catalogString)
    )
  }

  def extractReferencesFieldNotFound(field: String, exception: Throwable): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_EXTRACT_REFERENCES_FIELD_NOT_FOUND",
      messageParameters = Array(field),
      cause = exception)
  }

  def addFilePartitioningMismatchException(
    addFilePartitions: Seq[String],
    metadataPartitions: Seq[String]): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_INVALID_PARTITIONING_SCHEMA",
      messageParameters = Array(s"${DeltaErrors.formatColumnList(metadataPartitions)}",
        s"${DeltaErrors.formatColumnList(addFilePartitions)}",
        s"${DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key}")
    )
  }

  def concurrentModificationExceptionMsg(
      sparkConf: SparkConf,
      baseMessage: String,
      commit: Option[CommitInfo]): String = {
    baseMessage +
      commit.map(ci => s"\nConflicting commit: ${JsonUtils.toJson(ci)}").getOrElse("") +
      s"\nRefer to " +
      s"${DeltaErrors.generateDocsLink(sparkConf, "/concurrency-control.html")} " +
      "for more details."
  }

  def ignoreStreamingUpdatesAndDeletesWarning(spark: SparkSession): String = {
    val docPage = DeltaErrors.generateDocsLink(
      spark.sparkContext.getConf,
      "/delta-streaming.html#ignoring-updates-and-deletes")
    s"""WARNING: The 'ignoreFileDeletion' option is deprecated. Switch to using one of
       |'ignoreDeletes' or 'ignoreChanges'. Refer to $docPage for details.
         """.stripMargin
  }

  def configureSparkSessionWithExtensionAndCatalog(originalException: Throwable): Throwable = {
    val catalogImplConfig = SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key
    new DeltaAnalysisException(
      errorClass = "DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG",
      messageParameters = Array(classOf[DeltaSparkSessionExtension].getName,
        catalogImplConfig, classOf[DeltaCatalog].getName),
      cause = Some(originalException)
    )
  }

  def duplicateColumnsOnUpdateTable(originalException: Throwable): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_DUPLICATE_COLUMNS_ON_UPDATE_TABLE",
      messageParameters = Array(originalException.getMessage),
      cause = Some(originalException))
  }

  def maxCommitRetriesExceededException(
      attemptNumber: Int,
      attemptVersion: Long,
      initAttemptVersion: Long,
      numActions: Int,
      totalCommitAttemptTime: Long): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_MAX_COMMIT_RETRIES_EXCEEDED",
      messageParameters = Array(s"$attemptNumber", s"$initAttemptVersion", s"$attemptVersion",
        s"$numActions", s"$totalCommitAttemptTime"))
  }

  def generatedColumnsNonDeltaFormatError(): Throwable = {
    new AnalysisException("Generated columns are only supported by Delta")
  }

  def generatedColumnsReferToWrongColumns(e: AnalysisException): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_GENERATED_COLUMN_REFERENCES", Array.empty, cause = Some(e))
  }

  def generatedColumnsUpdateColumnType(current: StructField, update: StructField): Throwable = {
    new AnalysisException(
      s"Column ${current.name} is a generated column or a column used by a generated column. " +
        s"The data type is ${current.dataType.sql}. It doesn't accept data type " +
        s"${update.dataType.sql}")
  }

  def generatedColumnsUDF(expr: Expression): Throwable = {
    new AnalysisException(
      s"Found ${expr.sql}. A generated column cannot use a user-defined function")
  }

  def generatedColumnsNonDeterministicExpression(expr: Expression): Throwable = {
    new AnalysisException(
      s"Found ${expr.sql}. A generated column cannot use a non deterministic expression")
  }

  def generatedColumnsAggregateExpression(expr: Expression): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_AGGREGATE_IN_GENERATED_COLUMN",
      messageParameters = Array(expr.sql.toString)
    )
  }

  def generatedColumnsUnsupportedExpression(expr: Expression): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
      messageParameters = Array(s"${expr.sql}")
    )
  }

  def generatedColumnsTypeMismatch(
      column: String,
      columnType: DataType,
      exprType: DataType): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_GENERATED_COLUMNS_EXPR_TYPE_MISMATCH",
      messageParameters = Array(column, exprType.sql, columnType.sql)
    )
  }

  def expressionsNotFoundInGeneratedColumn(column: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_EXPRESSIONS_NOT_FOUND_IN_GENERATED_COLUMN",
      messageParameters = Array(column)
    )
  }

  def cannotChangeDataType(msg: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_CHANGE_DATA_TYPE",
      messageParameters = Array(msg)
    )
  }

  def unsupportedDataTypes(
      unsupportedDataType: UnsupportedDataTypeInfo,
      moreUnsupportedDataTypes: UnsupportedDataTypeInfo*): Throwable = {
    val prettyMessage = (unsupportedDataType +: moreUnsupportedDataTypes)
      .map(dt => s"${dt.column}: ${dt.dataType}")
      .mkString("[", ", ", "]")
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_DATA_TYPES",
      messageParameters = Array(prettyMessage, DeltaSQLConf.DELTA_SCHEMA_TYPE_CHECK.key)
    )
  }

  def tableAlreadyExists(table: CatalogTable): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_TABLE_ALREADY_EXISTS",
      messageParameters = Array(s"${table.identifier.quotedString}")
    )
  }

  def indexLargerThanStruct(pos: Int, column: StructField, len: Int): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INDEX_LARGER_THAN_STRUCT",
      messageParameters = Array(s"$pos", s"$column", s"$len")
    )
  }

  def indexLargerOrEqualThanStruct(pos: Int, len: Int): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INDEX_LARGER_OR_EQUAL_THAN_STRUCT",
      messageParameters = Array(s"$pos", s"$len")
    )
  }

  def invalidV1TableCall(callVersion: String, tableVersion: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_INVALID_V1_TABLE_CALL",
      messageParameters = Array(callVersion, tableVersion)
    )
  }

  def cannotGenerateUpdateExpressions(): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_CANNOT_GENERATE_UPDATE_EXPRESSIONS",
      messageParameters = Array.empty
    )
  }

  def unrecognizedInvariant(): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_UNRECOGNIZED_INVARIANT",
      messageParameters = Array.empty
    )
  }

  def unrecognizedColumnChange(otherClass: String) : Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_UNRECOGNIZED_COLUMN_CHANGE",
      messageParameters = Array(otherClass)
    )
  }

  def notNullColumnNotFoundInStruct(struct: String): Throwable = {
    new DeltaIndexOutOfBoundsException(
      errorClass = "DELTA_NOT_NULL_COLUMN_NOT_FOUND_IN_STRUCT",
      messageParameters = Array(struct)
    )
  }

  def unSupportedInvariantNonStructType: Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_UNSUPPORTED_INVARIANT_NON_STRUCT",
      messageParameters = Array.empty
    )
  }

  def cannotResolveColumn(fieldName: String, schema: StructType): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CANNOT_RESOLVE_COLUMN",
      messageParameters = Array(fieldName, schema.treeString)
    )
  }

  def unsupportedTruncateSampleTables: Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_TRUNCATE_SAMPLE_TABLES",
      messageParameters = Array.empty
    )
  }

  def unrecognizedFileAction(otherAction: String, otherClass: String) : Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_UNRECOGNIZED_FILE_ACTION",
      messageParameters = Array(otherAction, otherClass)
    )
  }

  def operationOnTempViewWithGenerateColsNotSupported(op: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_OPERATION_ON_TEMP_VIEW_WITH_GENERATED_COLS_NOT_SUPPORTED",
      messageParameters = Array(op, op))
  }

  def cannotModifyTableProperty(prop: String): Throwable =
    new UnsupportedOperationException(
      s"The Delta table configuration $prop cannot be specified by the user")

  /**
   * We have plans to support more column mapping modes, but they are not implemented yet,
   * so we error for now to be forward compatible with tables created in the future.
   */
  def unsupportedColumnMappingMode(mode: String): Throwable =
    new ColumnMappingUnsupportedException(s"The column mapping mode `$mode` is " +
      s"not supported for this Delta version. Please upgrade if you want to use this mode.")

  def missingColumnId(mode: DeltaColumnMappingMode, field: String): Throwable = {
    ColumnMappingException(s"Missing column ID in column mapping mode `${mode.name}`" +
      s" in the field: $field", mode)
  }

  def missingPhysicalName(mode: DeltaColumnMappingMode, field: String): Throwable =
    ColumnMappingException(s"Missing physical name in column mapping mode `${mode.name}`" +
      s" in the field: $field", mode)

  def duplicatedColumnId(
      mode: DeltaColumnMappingMode,
      id: Long,
      schema: StructType): Throwable = {
    ColumnMappingException(
      s"Found duplicated column id `$id` in column mapping mode `${mode.name}` \n" +
      s"schema: \n ${schema.prettyJson}", mode
    )
  }

  def duplicatedPhysicalName(
      mode: DeltaColumnMappingMode,
      physicalName: String,
      schema: StructType): Throwable = {
    ColumnMappingException(
      s"Found duplicated physical name `$physicalName` in column mapping mode `${mode.name}` \n\t" +
      s"schema: \n ${schema.prettyJson}", mode
    )
  }

  def changeColumnMappingModeNotSupported(oldMode: String, newMode: String): Throwable = {
    new DeltaColumnMappingUnsupportedException(
      errorClass = "DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE",
      messageParameters = Array(oldMode, newMode))
  }

  def generateManifestWithColumnMappingNotSupported: Throwable = {
    new DeltaColumnMappingUnsupportedException(
      errorClass = "DELTA_UNSUPPORTED_MANIFEST_GENERATION_WITH_COLUMN_MAPPING")
  }

  def convertToDeltaWithColumnMappingNotSupported(mode: DeltaColumnMappingMode): Throwable = {
    new DeltaColumnMappingUnsupportedException(
      errorClass = "DELTA_CONVERSION_UNSUPPORTED_COLUMN_MAPPING",
      messageParameters = Array(
        DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey,
        mode.name))
  }

  def changeColumnMappingModeOnOldProtocol(oldProtocol: Protocol): Throwable = {
    new DeltaColumnMappingUnsupportedException(
      errorClass = "DELTA_UNSUPPORTED_COLUMN_MAPPING_PROTOCOL",
      messageParameters = Array(
        s"${DeltaConfigs.COLUMN_MAPPING_MODE.key}",
        columnMappingAdviceMessage))
  }

  private def columnMappingAdviceMessage: String = {
    s"""
       |Please upgrade your Delta table to reader version 2 and writer version 5
       | and change the column mapping mode to 'name' mapping. You can use the following command:
       |
       | ALTER TABLE <table_name> SET TBLPROPERTIES (
       |   'delta.columnMapping.mode' = 'name',
       |   'delta.minReaderVersion' = '2',
       |   'delta.minWriterVersion' = '5')
       |
    """.stripMargin
  }

  def columnRenameNotSupported: Throwable = {
    val adviceMsg = columnMappingAdviceMessage
    new DeltaAnalysisException("DELTA_UNSUPPORTED_RENAME_COLUMN", Array(adviceMsg))
  }

  def dropColumnNotSupported(suggestUpgrade: Boolean): Throwable = {
    val adviceMsg = if (suggestUpgrade) columnMappingAdviceMessage else ""
    new DeltaAnalysisException("DELTA_UNSUPPORTED_DROP_COLUMN", Array(adviceMsg))
  }

  def dropNestedColumnsFromNonStructTypeException(struct : StructField) : Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_DROP_NESTED_COLUMN_FROM_NON_STRUCT_TYPE",
      messageParameters = Array(s"$struct")
    )
  }

  def dropPartitionColumnNotSupported(droppingPartCols: Seq[String]): Throwable = {
    new DeltaAnalysisException("DELTA_UNSUPPORTED_DROP_PARTITION_COLUMN",
      Array(droppingPartCols.mkString(",")))
  }

  def schemaChangeDuringMappingModeChangeNotSupported(
      oldSchema: StructType,
      newSchema: StructType): Throwable =
    new DeltaColumnMappingUnsupportedException(
      errorClass = "DELTA_UNSUPPORTED_COLUMN_MAPPING_SCHEMA_CHANGE",
      messageParameters = Array(
        formatSchema(oldSchema),
        formatSchema(newSchema)))

  def foundInvalidCharsInColumnNames(cause: Throwable): Throwable =
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES",
      messageParameters = Array(columnMappingAdviceMessage),
      cause = Some(cause))

  def foundViolatingConstraintsForColumnChange(
      operation: String,
      columnName: String,
      constraints: Map[String, String]): Throwable = {
    val plural = if (constraints.size > 1) "s" else ""
    new AnalysisException(
      s"""
        |Cannot $operation column $columnName because this column is referenced by the following
        | check constraint$plural:\n\t${constraints.mkString("\n\t")}
        |""".stripMargin)
  }

  def foundViolatingGeneratedColumnsForColumnChange(
      operation: String,
      columnName: String,
      fields: Seq[StructField]): Throwable = {
    val plural = if (fields.size > 1) "s" else ""
    new AnalysisException(
      s"""
         |Cannot $operation column $columnName because this column is referenced by the following
         | generated column$plural:\n\t${fields.map(_.name).mkString("\n\t")}
         |""".stripMargin)
  }

  def missingColumnsInInsertInto(column: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INSERT_COLUMN_MISMATCH",
      messageParameters = Array(column))
  }

  def schemaNotConsistentWithTarget(tableSchema: String, targetAttr: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_SCHEMA_NOT_CONSISTENT_WITH_TARGET",
      messageParameters = Array(tableSchema, targetAttr)
    )
  }

  def logStoreConfConflicts(schemeConf: Seq[(String, String)]): Throwable = {
    val schemeConfStr = schemeConf.map("spark.delta.logStore." + _._1).mkString(", ")
    new DeltaAnalysisException(
      errorClass = "DELTA_INVALID_LOGSTORE_CONF",
      messageParameters = Array(schemeConfStr)
    )
  }

  def ambiguousPathsInCreateTableException(identifier: String, location: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_AMBIGUOUS_PATHS_IN_CREATE_TABLE",
      messageParameters = Array(identifier, location,
        DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS.key))
  }

  def concurrentWriteException(
      conflictingCommit: Option[CommitInfo]): io.delta.exceptions.ConcurrentWriteException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      s"A concurrent transaction has written new data since the current transaction " +
        s"read the table. Please try the operation again.",
      conflictingCommit)
    new io.delta.exceptions.ConcurrentWriteException(message)
  }

  def metadataChangedException(
      conflictingCommit: Option[CommitInfo]): io.delta.exceptions.MetadataChangedException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "The metadata of the Delta table has been changed by a concurrent update. " +
        "Please try the operation again.",
      conflictingCommit)
    new io.delta.exceptions.MetadataChangedException(message)
  }

  def protocolPropNotIntException(key: String, value: String): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_PROTOCOL_PROPERTY_NOT_INT",
      Array(key, value))
  }

  def protocolChangedException(
      conflictingCommit: Option[CommitInfo]): io.delta.exceptions.ProtocolChangedException = {
    val additionalInfo = conflictingCommit.map { v =>
      if (v.version.getOrElse(-1) == 0) {
        "This happens when multiple writers are writing to an empty directory. " +
          "Creating the table ahead of time will avoid this conflict. "
      } else {
        ""
      }
    }.getOrElse("")
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "The protocol version of the Delta table has been changed by a concurrent update. " +
        additionalInfo + "Please try the operation again.",
      conflictingCommit)
    new io.delta.exceptions.ProtocolChangedException(message)
  }

  def concurrentAppendException(
      conflictingCommit: Option[CommitInfo],
      partition: String,
      customRetryMsg: Option[String] = None): io.delta.exceptions.ConcurrentAppendException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      s"Files were added to $partition by a concurrent update. " +
        customRetryMsg.getOrElse("Please try the operation again."),
      conflictingCommit)
    new io.delta.exceptions.ConcurrentAppendException(message)
  }

  def concurrentDeleteReadException(
      conflictingCommit: Option[CommitInfo],
      file: String): io.delta.exceptions.ConcurrentDeleteReadException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "This transaction attempted to read one or more files that were deleted" +
        s" (for example $file) by a concurrent update. Please try the operation again.",
      conflictingCommit)
    new io.delta.exceptions.ConcurrentDeleteReadException(message)
  }

  def concurrentDeleteDeleteException(
      conflictingCommit: Option[CommitInfo],
      file: String): io.delta.exceptions.ConcurrentDeleteDeleteException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "This transaction attempted to delete one or more files that were deleted " +
        s"(for example $file) by a concurrent update. Please try the operation again.",
      conflictingCommit)
    new io.delta.exceptions.ConcurrentDeleteDeleteException(message)
  }


  def concurrentTransactionException(
      conflictingCommit: Option[CommitInfo]): io.delta.exceptions.ConcurrentTransactionException = {
    val message = DeltaErrors.concurrentModificationExceptionMsg(
        SparkEnv.get.conf,
      s"This error occurs when multiple streaming queries are using the same checkpoint to write " +
        "into this table. Did you run multiple instances of the same streaming query" +
        " at the same time?",
      conflictingCommit)
    new io.delta.exceptions.ConcurrentTransactionException(message)
  }

  def restoreMissedDataFilesError(missedFiles: Array[String], version: Long): Throwable =
    new IllegalArgumentException(
      s"""Not all files from version $version are available in file system.
         | Missed files (top 100 files): ${missedFiles.mkString(",")}.
         | Please use more recent version or timestamp for restoring.
         | To disable check update option ${SQLConf.IGNORE_MISSING_FILES.key}"""
        .stripMargin
    )

  def unexpectedAlias(alias : String) : Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_UNEXPECTED_ALIAS",
      messageParameters = Array(alias)
    )
  }

  def unexpectedAttributeReference(ref: String): Throwable = {
    new DeltaIllegalStateException(errorClass = "DELTA_UNEXPECTED_ATTRIBUTE_REFERENCE",
      messageParameters = Array(ref))
  }

  def unsetNonExistentProperty(key: String, table: String): Throwable = {
    new DeltaAnalysisException(errorClass = "DELTA_UNSET_NON_EXISTENT_PROPERTY", Array(key, table))
  }

  def identityColumnNotSupported(): Throwable = {
    new AnalysisException("IDENTITY column is not supported")
  }

  def identityColumnInconsistentMetadata(
      colName: String,
      hasStart: Boolean,
      hasStep: Boolean,
      hasInsert: Boolean): Throwable = {
    new AnalysisException(s"Inconsistent IDENTITY metadata for column $colName " +
      s"detected: $hasStart, $hasStep, $hasInsert")
  }

  def activeSparkSessionNotFound(): Throwable = {
    new DeltaIllegalArgumentException(errorClass = "DELTA_ACTIVE_SPARK_SESSION_NOT_FOUND")
  }

  def sparkTaskThreadNotFound: Throwable = {
    new DeltaIllegalStateException(errorClass = "DELTA_SPARK_THREAD_NOT_FOUND")
  }

  def iteratorAlreadyClosed(): Throwable = {
    new DeltaIllegalStateException(errorClass = "DELTA_ITERATOR_ALREADY_CLOSED")
  }

  def activeTransactionAlreadySet(): Throwable = {
    new DeltaIllegalStateException(errorClass = "DELTA_ACTIVE_TRANSACTION_ALREADY_SET")
  }

  /** This is a method only used for testing Py4J exception handling. */
  def throwDeltaIllegalArgumentException(): Throwable = {
    new DeltaIllegalArgumentException(errorClass = "DELTA_UNRECOGNIZED_INVARIANT")
  }

  def invalidSourceVersion(version: JValue): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_INVALID_SOURCE_VERSION",
      messageParameters = Array(version.toString)
    )
  }

  def invalidCommittedVersion(attemptVersion: Long, currentVersion: Long): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_INVALID_COMMITTED_VERSION",
      messageParameters = Array(attemptVersion.toString, currentVersion.toString)
    )
  }

  def nonPartitionColumnReference(colName: String, partitionColumns: Seq[String]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_NON_PARTITION_COLUMN_REFERENCE",
      messageParameters = Array(colName, partitionColumns.mkString(", "))
    )
  }

  def missingColumn(attr: Attribute, targetAttrs: Seq[Attribute]): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MISSING_COLUMN",
      messageParameters = Array(attr.name, targetAttrs.map(_.name).mkString(", "))
    )
  }

  def missingPartitionColumn(col: String, schemaCatalog: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_MISSING_PARTITION_COLUMN",
      messageParameters = Array(col, schemaCatalog)
    )
  }

  def noNewAttributeId(oldAttr: AttributeReference): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_NO_NEW_ATTRIBUTE_ID",
      messageParameters = Array(oldAttr.qualifiedName)
    )
  }

  def nonGeneratedColumnMissingUpdateExpression(column: Attribute): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_NON_GENERATED_COLUMN_MISSING_UPDATE_EXPR",
      messageParameters = Array(column.toString)
    )
  }

  def failedInferSchema: Throwable = {
    new DeltaRuntimeException("DELTA_FAILED_INFER_SCHEMA")
  }

  def failedReadFileFooter(file: String, e: Throwable): Throwable = {
    new DeltaIOException(
      errorClass = "DELTA_FAILED_READ_FILE_FOOTER",
      messageParameters = Array(file),
      cause = e
    )
  }

  def failedScanWithHistoricalVersion(historicalVersion: Long): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_FAILED_SCAN_WITH_HISTORICAL_VERSION",
      messageParameters = Array(historicalVersion.toString)
    )
  }

  def failedRecognizePredicate(predicate: String, cause: Throwable): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_FAILED_RECOGNIZE_PREDICATE", messageParameters = Array(predicate),
      cause = Some(cause)
    )
  }

  def failedFindAttributeInOutputCollumns(newAttrName: String, targetCollNames: String): Throwable =
  {
    new DeltaAnalysisException(
      errorClass = "DELTA_FAILED_FIND_ATTRIBUTE_IN_OUTPUT_COLLUMNS",
      messageParameters = Array(newAttrName, targetCollNames)
    )
  }

  def deltaTableFoundInExecutor(): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_TABLE_FOUND_IN_EXECUTOR",
      messageParameters = Array.empty
    )
  }

  def unsupportSubqueryInPartitionPredicates(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_UNSUPPORTED_SUBQUERY_IN_PARTITION_PREDICATES",
      messageParameters = Array.empty
    )
  }

  def fileAlreadyExists(file: String): Throwable = {
    new DeltaFileAlreadyExistsException(
      errorClass = "DELTA_FILE_ALREADY_EXISTS",
      messageParameters = Array(file)
    )
  }

  def replaceWhereUsedWithDynamicPartitionOverwrite(): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_REPLACE_WHERE_WITH_DYNAMIC_PARTITION_OVERWRITE"
    )
  }

  def replaceWhereUsedInOverwrite(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_REPLACE_WHERE_IN_OVERWRITE", messageParameters = Array.empty
    )
  }

  def incorrectArrayAccessByName(rightName: String, wrongName: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_INCORRECT_ARRAY_ACCESS_BY_NAME",
      messageParameters = Array(rightName, wrongName)
    )
  }

  def showPartitionInNotPartitionedTable(tableName: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_SHOW_PARTITION_IN_NON_PARTITIONED_TABLE",
      messageParameters = Array(tableName)
    )
  }

  def duplicateColumnOnInsert(): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_DUPLICATE_COLUMNS_ON_INSERT",
      messageParameters = Array.empty
    )
  }

  def timeTravelInvalidBeginValue(timeTravelKey: String, cause: Throwable): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_TIME_TRAVEL_INVALID_BEGIN_VALUE",
      messageParameters = Array(timeTravelKey),
      cause = cause
    )
  }

  def removeFileCDCMissingExtendedMetadata(fileName: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_REMOVE_FILE_CDC_MISSING_EXTENDED_METADATA",
      messageParameters = Array(fileName)
    )
  }

  def failRelativizePath(pathName: String): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_FAIL_RELATIVIZE_PATH", messageParameters = Array(pathName, pathName)
    )
  }

  def invalidFormatFromSourceVersion(wrongVersion: Long, expectedVersion: Integer): Throwable = {
    new DeltaIllegalStateException(
      errorClass = "DELTA_INVALID_FORMAT_FROM_SOURCE_VERSION",
      messageParameters = Array(expectedVersion.toString, wrongVersion.toString)
    )
  }

  def createTableWithNonEmptyLocation(tableId: String, tableLocation: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_CREATE_TABLE_WITH_NON_EMPTY_LOCATION",
      messageParameters = Array(tableId, tableLocation)
    )
  }

  def maxArraySizeExceeded(): Throwable = {
    new DeltaIllegalArgumentException(
      errorClass = "DELTA_MAX_ARRAY_SIZE_EXCEEDED", messageParameters = Array.empty
    )
  }

  def replaceWhereWithFilterDataChangeUnset(dataFilters: String): Throwable = {
    new DeltaAnalysisException(
      errorClass = "DELTA_REPLACE_WHERE_WITH_FILTER_DATA_CHANGE_UNSET",
      messageParameters = Array(dataFilters)
    )
  }

  def blockColumnMappingAndCdcOperation(op: DeltaOperations.Operation): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_BLOCK_COLUMN_MAPPING_AND_CDC_OPERATION",
      messageParameters = Array(op.name)
    )
  }

  def missingDeltaStorageJar(e: NoClassDefFoundError): Throwable = {
    // scalastyle:off line.size.limit
    new NoClassDefFoundError(
      s"""${e.getMessage}
         |Please ensure that the delta-storage dependency is included.
         |
         |If using Python, please ensure you call `configure_spark_with_delta_pip` or use
         |`--packages io.delta:delta-core_<scala-version>:<delta-lake-version>`.
         |See https://docs.delta.io/latest/quick-start.html#python.
         |
         |More information about this dependency and how to include it can be found here:
         |https://docs.delta.io/latest/porting.html#delta-lake-1-1-or-below-to-delta-lake-1-2-or-above.
         |""".stripMargin)
    // scalastyle:on line.size.limit
  }

  def blockCdfAndColumnMappingReads(): Throwable = {
    new DeltaUnsupportedOperationException(
      errorClass = "DELTA_BLOCK_CDF_COLUMN_MAPPING_READS"
    )
  }
}

object DeltaErrors extends DeltaErrorsBase
/** The basic class for all Tahoe commit conflict exceptions. */
abstract class DeltaConcurrentModificationException(message: String)
  extends ConcurrentModificationException(message) {

  /**
   * Type of the commit conflict.
   */
  def conflictType: String = this.getClass.getSimpleName.stripSuffix("Exception")
}

/**
 * This class is kept for backward compatibility.
 * Use [[io.delta.exceptions.ConcurrentWriteException]] instead.
 */
class ConcurrentWriteException(message: String)
  extends io.delta.exceptions.DeltaConcurrentModificationException(message) {
  def this(conflictingCommit: Option[CommitInfo]) = this(
    DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      s"A concurrent transaction has written new data since the current transaction " +
        s"read the table. Please try the operation again.",
      conflictingCommit))
}

/**
 * Thrown when time travelling to a version that does not exist in the Delta Log.
 * @param userVersion - the version time travelling to
 * @param earliest - earliest version available in the Delta Log
 * @param latest - The latest version available in the Delta Log
 */
case class VersionNotFoundException(
    userVersion: Long,
    earliest: Long,
    latest: Long) extends AnalysisException(
      s"Cannot time travel Delta table to version $userVersion. " +
      s"Available versions: [$earliest, $latest]."
    )

/**
 * This class is kept for backward compatibility.
 * Use [[io.delta.exceptions.MetadataChangedException]] instead.
 */
class MetadataChangedException(message: String)
  extends io.delta.exceptions.DeltaConcurrentModificationException(message) {
  def this(conflictingCommit: Option[CommitInfo]) = this(
    DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "The metadata of the Delta table has been changed by a concurrent update. " +
        "Please try the operation again.",
      conflictingCommit))
}

/**
 * This class is kept for backward compatibility.
 * Use [[io.delta.exceptions.ProtocolChangedException]] instead.
 */
class ProtocolChangedException(message: String)
  extends io.delta.exceptions.DeltaConcurrentModificationException(message) {
  def this(conflictingCommit: Option[CommitInfo]) = this(
    DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "The protocol version of the Delta table has been changed by a concurrent update. " +
        "Please try the operation again.",
      conflictingCommit))
}

/**
 * This class is kept for backward compatibility.
 * Use [[io.delta.exceptions.ConcurrentAppendException]] instead.
 */
class ConcurrentAppendException(message: String)
  extends io.delta.exceptions.DeltaConcurrentModificationException(message) {
  def this(
      conflictingCommit: Option[CommitInfo],
      partition: String,
      customRetryMsg: Option[String] = None) = this(
    DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      s"Files were added to $partition by a concurrent update. " +
        customRetryMsg.getOrElse("Please try the operation again."),
      conflictingCommit))
}

/**
 * This class is kept for backward compatibility.
 * Use [[io.delta.exceptions.ConcurrentDeleteReadException]] instead.
 */
class ConcurrentDeleteReadException(message: String)
  extends io.delta.exceptions.DeltaConcurrentModificationException(message) {
  def this(conflictingCommit: Option[CommitInfo], file: String) = this(
    DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "This transaction attempted to read one or more files that were deleted" +
        s" (for example $file) by a concurrent update. Please try the operation again.",
      conflictingCommit))
}

/**
 * This class is kept for backward compatibility.
 * Use [[io.delta.exceptions.ConcurrentDeleteDeleteException]] instead.
 */
class ConcurrentDeleteDeleteException(message: String)
  extends io.delta.exceptions.DeltaConcurrentModificationException(message) {
  def this(conflictingCommit: Option[CommitInfo], file: String) = this(
    DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      "This transaction attempted to delete one or more files that were deleted " +
        s"(for example $file) by a concurrent update. Please try the operation again.",
      conflictingCommit))
}

/**
 * This class is kept for backward compatibility.
 * Use [[io.delta.exceptions.ConcurrentTransactionException]] instead.
 */
class ConcurrentTransactionException(message: String)
  extends io.delta.exceptions.DeltaConcurrentModificationException(message) {
  def this(conflictingCommit: Option[CommitInfo]) = this(
    DeltaErrors.concurrentModificationExceptionMsg(
      SparkEnv.get.conf,
      s"This error occurs when multiple streaming queries are using the same checkpoint to write " +
        "into this table. Did you run multiple instances of the same streaming query" +
        " at the same time?",
      conflictingCommit))
}

/** A helper class in building a helpful error message in case of metadata mismatches. */
class MetadataMismatchErrorBuilder {
  private var bits: Seq[String] = Nil

  def addSchemaMismatch(original: StructType, data: StructType, id: String): Unit = {
    bits ++=
      s"""A schema mismatch detected when writing to the Delta table (Table ID: $id).
         |To enable schema migration using DataFrameWriter or DataStreamWriter, please set:
         |'.option("${DeltaOptions.MERGE_SCHEMA_OPTION}", "true")'.
         |For other operations, set the session configuration
         |${DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key} to "true". See the documentation
         |specific to the operation for details.
         |
         |Table schema:
         |${DeltaErrors.formatSchema(original)}
         |
         |Data schema:
         |${DeltaErrors.formatSchema(data)}
         """.stripMargin :: Nil
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
  }

  def finalizeAndThrow(conf: SQLConf): Unit = {
    throw new AnalysisException(bits.mkString("\n"))
  }
}

class DeltaColumnMappingUnsupportedException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty)
  extends ColumnMappingUnsupportedException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

class DeltaFileNotFoundException(
  errorClass: String,
  messageParameters: Array[String] = Array.empty)
  extends FileNotFoundException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

class DeltaFileAlreadyExistsException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty)
  extends FileAlreadyExistsException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

class DeltaIOException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty,
    cause: Throwable = null)
  extends IOException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters), cause)
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

class DeltaIllegalStateException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty,
    cause: Throwable = null)
  extends IllegalStateException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters), cause)
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

class DeltaIndexOutOfBoundsException(
  errorClass: String,
  messageParameters: Array[String] = Array.empty)
  extends IndexOutOfBoundsException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

class DeltaRuntimeException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty)
  extends RuntimeException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters))
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

class DeltaSparkException(
    errorClass: String,
    messageParameters: Array[String] = Array.empty,
    cause: Throwable = null)
  extends SparkException(
    DeltaThrowableHelper.getMessage(errorClass, messageParameters), cause)
    with DeltaThrowable {
  override def getErrorClass: String = errorClass
}

/** Errors thrown around column mapping. */
class ColumnMappingUnsupportedException(msg: String)
  extends UnsupportedOperationException(msg)
case class ColumnMappingException(msg: String, mode: DeltaColumnMappingMode)
  extends AnalysisException(msg)
