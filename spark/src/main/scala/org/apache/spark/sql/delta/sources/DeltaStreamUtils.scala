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

package org.apache.spark.sql.delta.sources

import java.sql.Timestamp

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.delta.DataFrameUtils
import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.Relocated._
import org.apache.spark.sql.delta.TypeWideningMode
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.util.{DateTimeUtils, TimestampFormatter}

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.classic.ClassicConversions._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.StructType

object DeltaStreamUtils {

  /**
   * Select `cols` from a micro batch DataFrame. Directly calling `select` won't work because it
   * will create a `QueryExecution` rather than inheriting `IncrementalExecution` from
   * the micro batch DataFrame. A streaming micro batch DataFrame to execute should use
   * `IncrementalExecution`.
   */
  def selectFromStreamingDataFrame(
      incrementalExecution: IncrementalExecution,
      df: DataFrame,
      cols: Column*): DataFrame = {
    val newMicroBatch = df.select(cols: _*)
    val newIncrementalExecution = createIncrementalExecution(
      newMicroBatch.sparkSession,
      newMicroBatch.queryExecution.logical,
      incrementalExecution.outputMode,
      incrementalExecution.checkpointLocation,
      incrementalExecution.queryId,
      incrementalExecution.runId,
      incrementalExecution.currentBatchId,
      incrementalExecution.prevOffsetSeqMetadata,
      incrementalExecution.offsetSeqMetadata,
      incrementalExecution.watermarkPropagator,
      incrementalExecution.isFirstBatch)
    newIncrementalExecution.executedPlan // Force the lazy generation of execution plan
    DataFrameUtils.ofRows(newIncrementalExecution)
  }

  /**
   * Configuration options for schema compatibility validation during Delta streaming reads.
   *
   * This class encapsulates various flags and settings that control how Delta streaming handles
   * schema changes and compatibility checks.
   *
   * TODO(#5319): Clean up the configs that were intended as escape-hatches for behavior changes
   * if they aren't needed anymore.
   *
   * @param allowUnsafeStreamingReadOnColumnMappingSchemaChanges
   *        Flag that allows user to force enable unsafe streaming read on Delta table with
   *        column mapping enabled AND drop/rename actions.
   * @param allowUnsafeStreamingReadOnPartitionColumnChanges
   *        Flag that allows user to force enable unsafe streaming read on Delta table with
   *        column mapping enabled AND partition column changes.
   * @param forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart
   *        Flag that allows user to disable the read-compatibility check during stream start which
   *        protects against a corner case in which verifyStreamHygiene could not detect.
   *        This is a bug fix but yet a potential behavior change, so we add a flag to fallback.
   * @param forceEnableUnsafeReadOnNullabilityChange
   *        Flag that allows user to fallback to the legacy behavior in which user can allow
   *        nullable=false schema to read nullable=true data, which is incorrect but a behavior
   *        change regardless.
   * @param isStreamingFromColumnMappingTable
   *        Whether we are streaming from a table with column mapping enabled.
   * @param typeWideningEnabled
   *        Whether we are streaming from a table that has the type widening table feature enabled.
   * @param enableSchemaTrackingForTypeWidening
   *        Whether we should track widening type changes to allow users to accept them and resume
   *        stream processing.
   */
  case class SchemaReadOptions(
      allowUnsafeStreamingReadOnColumnMappingSchemaChanges: Boolean,
      allowUnsafeStreamingReadOnPartitionColumnChanges: Boolean,
      forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart: Boolean,
      forceEnableUnsafeReadOnNullabilityChange: Boolean,
      isStreamingFromColumnMappingTable: Boolean,
      typeWideningEnabled: Boolean,
      enableSchemaTrackingForTypeWidening: Boolean
  )

  object SchemaReadOptions {
    /**
     * Creates a SchemaReadOptions instance from SparkSession configuration settings.
     *
     * @param spark The SparkSession from which to read configuration values.
     * @param isStreamingFromColumnMappingTable Whether the source table has column mapping enabled.
     * @param isTypeWideningSupportedInProtocol Whether the table's protocol version supports
     *        type widening.
     * @return A [[SchemaReadOptions]] instance containing all schema validation flags derived from
     *         the session configuration and provided table state.
     */
    def fromSparkSession(
        spark: SparkSession,
        isStreamingFromColumnMappingTable: Boolean,
        isTypeWideningSupportedInProtocol: Boolean): SchemaReadOptions = {
      val allowUnsafeStreamingReadOnColumnMappingSchemaChanges =
        spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_COLUMN_MAPPING_SCHEMA_CHANGES)
      val allowUnsafeStreamingReadOnPartitionColumnChanges =
        spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_STREAMING_UNSAFE_READ_ON_PARTITION_COLUMN_CHANGE)
      val forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart =
        spark.sessionState.conf.getConf(DeltaSQLConf.
            DELTA_STREAMING_UNSAFE_READ_ON_INCOMPATIBLE_SCHEMA_CHANGES_DURING_STREAM_START)
      val forceEnableUnsafeReadOnNullabilityChange =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STREAM_UNSAFE_READ_ON_NULLABILITY_CHANGE)
      val typeWideningEnabled =
        spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_ALLOW_TYPE_WIDENING_STREAMING_SOURCE) &&
            isTypeWideningSupportedInProtocol
      val enableSchemaTrackingForTypeWidening =
        spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_TYPE_WIDENING_ENABLE_STREAMING_SCHEMA_TRACKING)

      new DeltaStreamUtils.SchemaReadOptions(
        allowUnsafeStreamingReadOnColumnMappingSchemaChanges =
          allowUnsafeStreamingReadOnColumnMappingSchemaChanges,
        allowUnsafeStreamingReadOnPartitionColumnChanges =
          allowUnsafeStreamingReadOnPartitionColumnChanges,
        forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart =
          forceEnableStreamingReadOnReadIncompatibleSchemaChangesDuringStreamStart,
        forceEnableUnsafeReadOnNullabilityChange = forceEnableUnsafeReadOnNullabilityChange,
        isStreamingFromColumnMappingTable = isStreamingFromColumnMappingTable,
        typeWideningEnabled = typeWideningEnabled,
        enableSchemaTrackingForTypeWidening = enableSchemaTrackingForTypeWidening)
    }
  }

  sealed trait SchemaCompatibilityResult
  object SchemaCompatibilityResult {
    // Indicates that the schema change is compatible and can be applied safely
    case object Compatible extends SchemaCompatibilityResult
    // Indicates that the schema change is incompatible and would break the query,
    // but the change can be applied by recovering the query
    case object RetryableIncompatible extends SchemaCompatibilityResult
    // Indicates that the schema change is incompatible and would break the query,
    // but the change cannot be applied by recovering the query
    case object NonRetryableIncompatible extends SchemaCompatibilityResult

    // helper methods for java interop
    def isCompatible(result: SchemaCompatibilityResult): Boolean =
      result == Compatible
    def isRetryableIncompatible(result: SchemaCompatibilityResult): Boolean =
      result == RetryableIncompatible
  }

  /**
   * Validate schema compatibility between data schema and read schema. Checks for read
   * compatibility considering nullability, type widening, missing columns, and partition changes.
   *
   * @param dataSchema The actual schema of the data
   * @param readSchema The schema used by the reader to read data
   * @param newPartitionColumns The partition columns for new metadata
   * @param oldPartitionColumns The partition columns for old metadata
   * @param backfilling Whether the check is triggered during backfilling (processing old data)
   * @param readOptions Configuration options that control schema compatibility rules
   *
   * @return A [[SchemaCompatibilityResult]] on whether the data schema is compatible, and if not,
   *         whether restarting the stream will allow processing data across the schema change.
   */
  def checkSchemaChangesWhenNoSchemaTracking(
      dataSchema: StructType,
      readSchema: StructType,
      newPartitionColumns: Seq[String],
      oldPartitionColumns: Seq[String],
      backfilling: Boolean,
      readOptions: SchemaReadOptions): SchemaCompatibilityResult = {
    // We forbid the case when the data schema is nullable while the read schema is NOT
    // nullable, or in other words, `readSchema` should not tighten nullability from `dataSchema`,
    // because we don't ever want to read back any nulls when the read schema is non-nullable.
    val shouldForbidTightenNullability = !readOptions.forceEnableUnsafeReadOnNullabilityChange
    // If schema tracking is disabled for type widening, we allow widening type changes to go
    // through without requiring the user to set `allowSourceColumnTypeChange`. The schema change
    // will cause the stream to fail with a retryable exception, and the stream will restart using
    // the new schema.
    val allowWideningTypeChanges = readOptions.typeWideningEnabled &&
        !readOptions.enableSchemaTrackingForTypeWidening
    // If a user is streaming from a column mapping table and enable the unsafe flag to ignore
    // column mapping schema changes, we can allow the standard check to allow missing columns
    // from the read schema in the data schema, because the only case that happens is when
    // user rename/drops column but they don't care so they enabled the flag to unblock.
    // This is only allowed when we are "backfilling", i.e. the stream progress is older than
    // the analyzed table version. Any schema change past the analysis should still throw
    // exception, because additive schema changes MUST be taken into account.
    val shouldAllowMissingColumns = readOptions.isStreamingFromColumnMappingTable &&
        readOptions.allowUnsafeStreamingReadOnColumnMappingSchemaChanges && backfilling
    // When backfilling after a type change, allow processing the data using the new, wider
    // type.
    // typeWideningMode when using `readSchema` to read `dataSchema`
    val forwardTypeWideningMode = if (allowWideningTypeChanges && backfilling) {
      TypeWideningMode.AllTypeWidening
    } else {
      TypeWideningMode.NoTypeWidening
    }

    if (!SchemaUtils.isReadCompatible(
      existingSchema = dataSchema,
      readSchema = readSchema,
      forbidTightenNullability = shouldForbidTightenNullability,
      allowMissingColumns = shouldAllowMissingColumns,
      typeWideningMode = forwardTypeWideningMode,
      newPartitionColumns = newPartitionColumns,
      oldPartitionColumns = oldPartitionColumns
    )) {
      // Check for widening type changes that would succeed on retry when we backfill batches.
      // typeWideningMode when using `dataSchema` to read `readSchema`
      val backwardTypeWideningMode = if (allowWideningTypeChanges) {
        TypeWideningMode.AllTypeWidening
      } else {
        TypeWideningMode.NoTypeWidening
      }
      // Only schema change later than the current read snapshot/schema can be retried, in other
      // words, backfills could never be retryable, because we have no way to refresh
      // the latest schema to "catch up" when the schema change happens before than current read
      // schema version.
      // If not backfilling, we do another check to determine retryability, in which we assume
      // we will be reading using this later `dataSchema` back on the current outdated `readSchema`,
      // and if it works (including that `dataSchema` should not tighten the nullability
      // constraint from `readSchema`), it is a retryable exception.
      val retryable = !backfilling && SchemaUtils.isReadCompatible(
        existingSchema = readSchema,
        readSchema = dataSchema,
        forbidTightenNullability = shouldForbidTightenNullability,
        typeWideningMode = backwardTypeWideningMode
      )
      if (retryable) {
        SchemaCompatibilityResult.RetryableIncompatible
      } else {
        SchemaCompatibilityResult.NonRetryableIncompatible
      }
    } else {
      SchemaCompatibilityResult.Compatible
    }
  }

  /**
   * - If commit's timestamp exactly matches the provided timestamp, we return it.
   * - Otherwise, we return the earliest commit version
   *   with a timestamp greater than the provided one.
   * - If the provided timestamp is larger than the timestamp
   *   of any committed version, and canExceedLatest is disabled we throw an error.
   * - If the provided timestamp is larger than the timestamp
   *   of any committed version, and canExceedLatest is enabled we return a version that is greater
   *   than commitVersion by one
   *
   * @param timeZone - time zone for formatting error messages
   * @param commitTimestamp - timestamp of the commit
   * @param commitVersion - version of the commit
   * @param latestVersion - latest snapshot version
   * @param timestamp - user specified timestamp
   * @param canExceedLatest - if true, version can be greater than the latest snapshot commit
   * @return - corresponding version number for timestamp
   */
  def getStartingVersionFromCommitAtTimestamp(
      timeZone: String,
      commitTimestamp: Long,
      commitVersion: Long,
      latestVersion: Long,
      timestamp: Timestamp,
      canExceedLatest: Boolean = false): Long = {
    if (commitTimestamp >= timestamp.getTime) {
      // Find the commit at the `timestamp` or the earliest commit
      commitVersion
    } else {
      // commitTimestamp is not the same, so this commit is a commit before the timestamp and
      // the next version if exists should be the earliest commit after the timestamp.
      //
      // Note: In the use case of [[CDCReader]] timestamp passed in can exceed the latest commit
      // timestamp, caller doesn't expect exception, and can handle the non-existent version.
      val latestNotExceeded = commitVersion + 1 <= latestVersion
      if (latestNotExceeded || canExceedLatest) {
        commitVersion + 1
      } else {
        val commitTs = new Timestamp(commitTimestamp)
        val timestampFormatter = TimestampFormatter(DateTimeUtils.getTimeZone(timeZone))
        val tsString = DateTimeUtils.timestampToString(
          timestampFormatter, DateTimeUtils.fromJavaTimestamp(commitTs))
        throw DeltaErrors.timestampGreaterThanLatestCommit(timestamp, commitTs, tsString)
      }
    }
  }
}
