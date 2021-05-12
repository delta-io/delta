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

package org.apache.spark.sql.delta

// scalastyle:off import.ordering.noEmptyLine
import java.io.{FileNotFoundException, IOException}
import java.util.ConcurrentModificationException

import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.constraints.Constraints
import org.apache.spark.sql.delta.hooks.PostCommitHook
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.{InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils
import io.delta.sql.DeltaSparkSessionExtension
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
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
      "part of DocsPath.errorsWithDocsLinks")
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
    "useDeltaOnOtherFormatPathException",
    "useOtherFormatOnDeltaPathException",
    "createExternalTableWithoutLogException",
    "createExternalTableWithoutSchemaException",
    "createManagedTableWithoutSchemaException",
    "multipleSourceRowMatchingTargetRowInMergeException",
    "faqRelativePath",
    "ignoreStreamingUpdatesAndDeletesWarning",
    "concurrentModificationExceptionMsg",
    "incorrectLogStoreImplementationException"
  )
}

/**
 * A holder object for Delta errors.
 *
 * IMPORTANT: Any time you add a test that references the docs, add to the Seq defined in
 * DeltaErrorsSuite so that the doc links that are generated can be verified to work in Azure,
 * docs.databricks.com and docs.delta.io
 */
object DeltaErrors
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
    new UnsupportedOperationException(
      s"Detected deleted data (for example $removedFile) from streaming source at " +
        s"version $version. This is currently not supported. If you'd like to ignore deletes, " +
        "set the option 'ignoreDeletes' to 'true'.")
  }

  def deltaSourceIgnoreChangesError(version: Long, removedFile: String): Throwable = {
    new UnsupportedOperationException(
      s"Detected a data update (for example $removedFile) in the source table at version " +
        s"$version. This is currently not supported. If you'd like to ignore updates, set the " +
        "option 'ignoreChanges' to 'true'. If you would like the data update to be reflected, " +
        "please restart this query with a fresh checkpoint directory."
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
    new InvariantViolationException(s"Column ${UnresolvedAttribute(constraint.column).name}" +
      s", which has a NOT NULL constraint, is missing from the data being " +
      s"written into the table.")
  }

  def nestedNotNullConstraint(
      parent: String, nested: DataType, nestType: String): AnalysisException = {
    new AnalysisException(s"The $nestType type of the field $parent contains a NOT NULL " +
      s"constraint. Delta does not support NOT NULL constraints nested within arrays or maps. " +
      s"To suppress this error and silently ignore the specified constraints, set " +
      s"${DeltaSQLConf.ALLOW_UNENFORCED_NOT_NULL_CONSTRAINTS.key} = true.\n" +
      s"Parsed $nestType type:\n${nested.prettyJson}")
  }

  def constraintAlreadyExists(name: String, oldExpr: String): AnalysisException = {
    new AnalysisException(
      s"Constraint '$name' already exists as a CHECK constraint. Please delete the old " +
        s"constraint first.\nOld constraint:\n${oldExpr}")
  }

  def checkConstraintNotBoolean(name: String, expr: String): AnalysisException = {
    new AnalysisException(s"CHECK constraint '$name' ($expr) should be a boolean expression.'")
  }

  def newCheckConstraintViolated(num: Long, tableName: String, expr: String): AnalysisException = {
    new AnalysisException(s"$num rows in $tableName violate the new CHECK constraint ($expr)")
  }

  def newNotNullViolated(
      num: Long, tableName: String, col: UnresolvedAttribute): AnalysisException = {
    new AnalysisException(
      s"$num rows in $tableName violate the new NOT NULL constraint on ${col.name}")
  }

  def useAddConstraints: AnalysisException = {
    new AnalysisException(s"Please use ALTER TABLE ADD CONSTRAINT to add CHECK constraints.")
  }

  def incorrectLogStoreImplementationException(
      sparkConf: SparkConf,
      cause: Throwable): Throwable = {
    new IOException(s"""The error typically occurs when the default LogStore implementation, that
      | is, HDFSLogStore, is used to write into a Delta table on a non-HDFS storage system.
      | In order to get the transactional ACID guarantees on table updates, you have to use the
      | correct implementation of LogStore that is appropriate for your storage system.
      | See ${generateDocsLink(sparkConf, "/delta-storage.html")} " for details.
      """.stripMargin, cause)
  }

  def failOnDataLossException(expectedVersion: Long, seenVersion: Long): Throwable = {
    new IllegalStateException(
      s"""The stream from your Delta table was expecting process data from version $expectedVersion,
         |but the earliest available version in the _delta_log directory is $seenVersion. The files
         |in the transaction log may have been deleted due to log cleanup. In order to avoid losing
         |data, we recommend that you restart your stream with a new checkpoint location and to
         |increase your delta.logRetentionDuration setting, if you have explicitly set it below 30
         |days.
         |If you would like to ignore the missed data and continue your stream from where it left
         |off, you can set the .option("${DeltaOptions.FAIL_ON_DATA_LOSS_OPTION}", "false") as part
         |of your readStream statement.
       """.stripMargin
    )
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
    new AnalysisException(
      s"""The schema of your Delta table has changed in an incompatible way since your DataFrame or
         |DeltaTable object was created. Please redefine your DataFrame or DeltaTable object.
         |Changes:\n${schemaDiff.mkString("\n")}$legacyFlagMessage""".stripMargin)
  }

  def invalidColumnName(name: String): Throwable = {
    new AnalysisException(
      s"""Attribute name "$name" contains invalid character(s) among " ,;{}()\\n\\t=".
         |Please use alias to rename it.
       """.stripMargin.split("\n").mkString(" ").trim)
  }

  def invalidPartitionColumn(e: AnalysisException): Throwable = {
    new AnalysisException(
      """Found partition columns having invalid character(s) among " ,;{}()\n\t=". Please """ +
        "change the name to your partition columns. This check can be turned off by setting " +
        """spark.conf.set("spark.databricks.delta.partitionColumnValidity.enabled", false) """ +
        "however this is not recommended as other features of Delta may not work properly.",
      cause = Option(e))
  }

  def missingTableIdentifierException(operationName: String): Throwable = {
    new AnalysisException(
      s"Please provide the path or table identifier for $operationName.")
  }

  def viewInDescribeDetailException(view: TableIdentifier): Throwable = {
    new AnalysisException(
      s"$view is a view. DESCRIBE DETAIL is only supported for tables.")
  }

  def alterTableChangeColumnException(oldColumns: String, newColumns: String): Throwable = {
    new AnalysisException(
      "ALTER TABLE CHANGE COLUMN is not supported for changing column " + oldColumns + " to "
      + newColumns)
  }

  def notEnoughColumnsInInsert(
      table: String,
      query: Int,
      target: Int,
      nestedField: Option[String] = None): Throwable = {
    val nestedFieldStr = nestedField.map(f => s"not enough nested fields in $f")
      .getOrElse("not enough data columns")
    new AnalysisException(s"Cannot write to '$table', $nestedFieldStr; " +
        s"target table has ${target} column(s) but the inserted data has " +
        s"${query} column(s)")
  }

  def cannotInsertIntoColumn(
      tableName: String,
      source: String,
      target: String,
      targetType: String): Throwable = {
    new AnalysisException(
      s"Struct column $source cannot be inserted into a $targetType field $target in $tableName.")
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

  def tableNotSupportedException(operation: String): Throwable = {
    new AnalysisException(s"Table is not supported in $operation. Please use a path instead.")
  }

  def vacuumBasePathMissingException(baseDeltaPath: Path): Throwable = {
    new AnalysisException(
      s"Please provide the base path ($baseDeltaPath) when Vacuuming Delta tables. " +
        "Vacuuming specific partitions is currently not supported.")
  }

  def unexpectedDataChangeException(op: String): Throwable = {
    new AnalysisException(s"Attempting to change metadata when 'dataChange' option is set" +
      s" to false during $op")
  }

  def unknownConfigurationKeyException(confKey: String): Throwable = {
    new AnalysisException(s"Unknown configuration was specified: $confKey")
  }

  def cdcNotAllowedInThisVersion(): Throwable = {
    new AnalysisException("Configuration delta.enableChangeDataFeed cannot be set. Change " +
      "data feed from Delta is not yet available.")
  }

  def cdcWriteNotAllowedInThisVersion(): Throwable = {
    new AnalysisException("Cannot write to table with delta.enableChangeDataFeed set. Change " +
      "data feed from Delta is not yet available.")
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
        |To learn more about Delta, see ${generateDocsLink(spark.sparkContext.getConf,
        "/index.html")}
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
        |To learn more about Delta, see ${generateDocsLink(spark.sparkContext.getConf,
        "/index.html")}
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

  def checkpointNonExistTable(path: Path): Throwable = {
    new IllegalStateException(s"Cannot checkpoint a non-exist table $path. Did you manually " +
      s"delete files in the _delta_log directory?")
  }

  def multipleLoadPathsException(paths: Seq[String]): Throwable = {
    new AnalysisException(
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
      s"""Data written out does not match replaceWhere '$replaceWhere'.
         |Invalid data would be written to partitions $badPartitions.""".stripMargin)
  }

  def illegalDeltaOptionException(name: String, input: String, explain: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid value '$input' for option '$name', $explain")
  }

  def startingVersionAndTimestampBothSetException(
      versionOptKey: String,
      timestampOptKey: String): Throwable = {
    new IllegalArgumentException(s"Please either provide '$versionOptKey' or '$timestampOptKey'")
  }

  def unrecognizedLogFile(path: Path): Throwable = {
    new UnsupportedOperationException(s"Unrecognized log file $path")
  }

  def modifyAppendOnlyTableException: Throwable = {
    new UnsupportedOperationException(
      "This table is configured to only allow appends. If you would like to permit " +
        s"updates or deletes, use 'ALTER TABLE <table_name> SET TBLPROPERTIES " +
        s"(${DeltaConfigs.IS_APPEND_ONLY.key}=false)'.")
  }

  def missingPartFilesException(version: Long, ae: Exception): Throwable = {
    new IllegalStateException(
      s"Couldn't find all part files of the checkpoint version: $version", ae)
  }

  def deltaVersionsNotContiguousException(
      spark: SparkSession, deltaVersions: Seq[Long]): Throwable = {
    new IllegalStateException(s"Versions ($deltaVersions) are not contiguous.")
  }

  def actionNotFoundException(action: String, version: Long): Throwable = {
    new IllegalStateException(
      s"""
         |The $action of your Delta table couldn't be recovered while Reconstructing
         |version: ${version.toString}. Did you manually delete files in the _delta_log directory?
         |Set ${DeltaSQLConf.DELTA_STATE_RECONSTRUCTION_VALIDATION_ENABLED.key}
         |to "false" to skip validation.
       """.stripMargin)
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

  def schemaNotProvidedException: Throwable = {
    new AnalysisException(
      "Table schema is not provided. Please provide the schema of the table when using " +
        "REPLACE table and an AS SELECT query is not provided.")
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

  def multipleSourceRowMatchingTargetRowInMergeException(spark: SparkSession): Throwable = {
    new UnsupportedOperationException(
      s"""Cannot perform Merge as multiple source rows matched and attempted to modify the same
         |target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
         |when multiple source rows match on the same target row, the result may be ambiguous
         |as it is unclear which source row should be used to update or delete the matching
         |target row. You can preprocess the source table to eliminate the possibility of
         |multiple matches. Please refer to
         |${generateDocsLink(spark.sparkContext.getConf,
        "/delta-update.html#upsert-into-a-table-using-merge")}""".stripMargin
    )
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
    new AnalysisException(
      s"""
         |You are trying to create an external table $tableName
         |from `$path` using Delta Lake, but there is no transaction log present at
         |`$path/_delta_log`. Check the upstream job to make sure that it is writing using
         |format("delta") and that the path is the root of the table.
         |
         |To learn more about Delta, see ${generateDocsLink(spark.sparkContext.getConf,
        "/index.html")}
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
         |To learn more about Delta, see ${generateDocsLink(spark.sparkContext.getConf,
        "/index.html")}
       """.stripMargin)
  }

  def createManagedTableWithoutSchemaException(
      tableName: String, spark: SparkSession): Throwable = {
    new AnalysisException(
      s"""
         |You are trying to create a managed table $tableName
         |using Delta Lake, but the schema is not specified.
         |
         |To learn more about Delta, see ${generateDocsLink(spark.sparkContext.getConf,
        "/index.html")}
       """.stripMargin)
  }

  def createTableWithDifferentSchemaException(
      path: Path,
      specifiedSchema: StructType,
      existingSchema: StructType,
      diffs: Seq[String]): Throwable = {
    new AnalysisException(
      s"""The specified schema does not match the existing schema at $path.
         |
         |== Specified ==
         |${specifiedSchema.treeString}
         |
         |== Existing ==
         |${existingSchema.treeString}
         |
         |== Differences==
         |${diffs.map("\n".r.replaceAllIn(_, "\n  ")).mkString("- ", "\n- ", "")}
         |
         |If your intention is to keep the existing schema, you can omit the
         |schema from the create table command. Otherwise please ensure that
         |the schema matches.
        """.stripMargin)
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
    new AnalysisException(
      s"""The specified properties do not match the existing properties at $path.
         |
         |== Specified ==
         |${specifiedProperties.map { case (k, v) => s"$k=$v" }.mkString("\n")}
         |
         |== Existing ==
         |${existingProperties.map { case (k, v) => s"$k=$v" }.mkString("\n")}
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
    new AnalysisException(
      s"""The provided timestamp ($userTimestamp) is after the latest version available to this
         |table ($commitTs). Please use a timestamp before or at $timestampString.
         """.stripMargin)
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

  def convertNonParquetTablesException(ident: TableIdentifier, sourceName: String): Throwable = {
    new AnalysisException("CONVERT TO DELTA only supports parquet tables, but you are trying to " +
      s"convert a $sourceName source: $ident")
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
    new FileNotFoundException(s"No file found in the directory: $directory.")
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

  def setLocationNotSupportedOnPathIdentifiers(): Throwable = {
    new AnalysisException("Cannot change the location of a path based table.")
  }

  def useSetLocation(): Throwable = {
    new AnalysisException(
      "Cannot change the 'location' of the Delta table using SET TBLPROPERTIES. Please use " +
      "ALTER TABLE SET LOCATION instead.")
  }

  def cannotChangeProvider(): Throwable = {
    new AnalysisException("'provider' is a reserved table property, and cannot be altered.")
  }

  def describeViewHistory: Throwable = {
    new AnalysisException("Cannot describe the history of a view.")
  }

  def copyIntoEncryptionOnlyS3(scheme: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid scheme $scheme. COPY INTO source encryption is only supported for S3 paths.")
  }

  def copyIntoEncryptionSseCRequired(): Throwable = {
    new IllegalArgumentException(
      s"Invalid encryption type. COPY INTO source encryption must specify 'type' = 'SSE-C'.")
  }

  def copyIntoEncryptionMasterKeyRequired(): Throwable = {
    new IllegalArgumentException(
      s"Invalid encryption arguments. COPY INTO source encryption must specify a masterKey.")
  }

  def copyIntoCredentialsOnlyS3(scheme: String): Throwable = {
    new IllegalArgumentException(
      s"Invalid scheme $scheme. COPY INTO source credentials are only supported for S3 paths.")
  }

  def copyIntoCredentialsAllRequired(cause: Throwable): Throwable = {
    new IllegalArgumentException(
      "COPY INTO credentials must include awsKeyId, awsSecretKey, and awsSessionToken.", cause)
  }

  def postCommitHookFailedException(
      failedHook: PostCommitHook,
      failedOnCommitVersion: Long,
      extraErrorMessage: String,
      error: Throwable): Throwable = {
    var errorMessage = s"Committing to the Delta table version $failedOnCommitVersion succeeded" +
      s" but error while executing post-commit hook ${failedHook.name}"
    if (extraErrorMessage != null && extraErrorMessage.nonEmpty) {
      errorMessage += s": $extraErrorMessage"
    }
    new RuntimeException(errorMessage, error)
  }

  def unsupportedGenerateModeException(modeName: String): Throwable = {
    import org.apache.spark.sql.delta.commands.DeltaGenerateCommand
    val supportedModes = DeltaGenerateCommand.modeNameToGenerationFunc.keys.toSeq.mkString(", ")
    new IllegalArgumentException(
      s"Specified mode '$modeName' is not supported. Supported modes are: $supportedModes")
  }

  def illegalUsageException(option: String, operation: String): Throwable = {
    new IllegalArgumentException(
      s"The usage of $option is not allowed when $operation a Delta table.")
  }

  def columnNotInSchemaException(column: String, schema: StructType): Throwable = {
    new AnalysisException(
      s"Couldn't find column $column in:\n${schema.treeString}")
  }

  def metadataAbsentException(): Throwable = {
    new IllegalStateException(
      s"""
         |Couldn't find Metadata while committing the first version of the Delta table. To disable
         |this check set ${DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key} to "false"
       """.stripMargin)
  }

  def updateSchemaMismatchExpression(from: StructType, to: StructType): Throwable = {
    new AnalysisException(s"Cannot cast ${from.catalogString} to ${to.catalogString}. All nested " +
      s"columns must match.")
  }

  def addFilePartitioningMismatchException(
      addFilePartitions: Seq[String],
      metadataPartitions: Seq[String]): Throwable = {
    new IllegalStateException(
      s"""
        |The AddFile contains partitioning schema different from the table's partitioning schema
        |expected: ${DeltaErrors.formatColumnList(metadataPartitions)}
        |actual: ${DeltaErrors.formatColumnList(addFilePartitions)}
        |To disable this check set ${DeltaSQLConf.DELTA_COMMIT_VALIDATION_ENABLED.key} to "false"
      """.stripMargin)
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
    new AnalysisException(
      s"""This Delta operation requires the SparkSession to be configured with the
         |DeltaSparkSessionExtension and the DeltaCatalog. Please set the necessary
         |configurations when creating the SparkSession as shown below.
         |
         |  SparkSession.builder()
         |    .option("spark.sql.extensions", "${classOf[DeltaSparkSessionExtension].getName}")
         |    .option("$catalogImplConfig", "${classOf[DeltaCatalog].getName}")
         |    ...
         |    .build()
      """.stripMargin,
      cause = Some(originalException))
  }

  def maxCommitRetriesExceededException(
      attemptNumber: Int,
      attemptVersion: Long,
      initAttemptVersion: Long,
      numActions: Int,
      totalCommitAttemptTime: Long): Throwable = {
    new IllegalStateException(
      s"""This commit has failed as it has been tried $attemptNumber times but did not succeed.
         |This can be caused by the Delta table being committed continuously by many concurrent
         |commits.
         |
         |Commit started at version: $initAttemptVersion
         |Commit failed at version: $attemptVersion
         |Number of actions attempted to commit: $numActions
         |Total time spent attempting this commit: $totalCommitAttemptTime ms
       """.stripMargin)
  }

  def generatedColumnsNonDeltaFormatError(): Throwable = {
    new AnalysisException("Generated columns are only supported by Delta")
  }

  def generatedColumnsReferToWrongColumns(e: AnalysisException): Throwable = {
    new AnalysisException(
      "A generated column cannot use a non-existent column or another generated column",
      cause = Some(e))
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
    new AnalysisException(
      s"Found ${expr.sql}. A generated column cannot use an aggregate expression")
  }

  def generatedColumnsUnsupportedExpression(expr: Expression): Throwable = {
    new AnalysisException(
      s"${expr.sql} cannot be used in a generated column")
  }

  def generatedColumnsTypeMismatch(
      column: String,
      columnType: DataType,
      exprType: DataType): Throwable = {
    new AnalysisException(
      s"The expression type of the generated column ${column} is ${exprType.sql}, " +
        s"but the column type is ${columnType.sql}")
  }

  def updateOnTempViewWithGenerateColsNotSupported: Throwable = {
    new AnalysisException(
      s"Updating a temp view referring to a Delta table that contains generated columns is not " +
        s"supported. Please run the update command on the Delta table directly")
  }


  def missingColumnsInInsertInto(column: String): Throwable = {
    new AnalysisException(s"Column $column is not specified in INSERT")
  }

  def logStoreConfConflicts(schemeConf: Seq[(String, String)]): Throwable = {
    val schemeConfStr = schemeConf.map("spark.delta.logStore." + _._1).mkString(", ")
    new AnalysisException(
      s"(`spark.delta.logStore.class`) and (`${schemeConfStr}`)" +
      " cannot be set at the same time. Please set only one group of them.")
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
}

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
