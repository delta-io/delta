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

// scalastyle:off import.ordering.noEmptyLine
import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8

import scala.io.{Source => IOSource}
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.actions.{FileAction, Metadata}
import org.apache.spark.sql.delta.storage.ClosableIterator._
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, MetadataVersionUtil}
import org.apache.spark.sql.types.{DataType, StructType}
// scalastyle:on import.ordering.noEmptyLine

/**
 * A PersistedSchema is an entry in Delta streaming source schema log, which can be used to read
 * data files during streaming.
 * @param tableId Delta table id
 * @param deltaCommitVersion Delta commit version in which this schema is captured. It does not
 *                           necessarily have to be the commit when there's a schema change.
 *                           But the invariant is: Delta table/snapshot must have it's schema as
 *                           `dataSchemaJson` at version `deltaCommitVersion`.
 *                           In streaming's context, when the stream restarts, the next batch it
 *                           should read will refer to Delta commit versions >= `deltaCommitVersion`
 *                           so schema should be readily compatible with that version.
 * @param dataSchemaJson Full schema json
 * @param partitionSchemaJson Partition schema json
 * @param sourceMetadataPath The checkpoint path that is unique to each source.
 * @param tableConfigurations The configurations of the table inside the metadata when the schema
 *                            change was detected. It is used to correctly create the right file
 *                            format when we use a particular schema to read.
 *                            Default to None for backward compatibility.
 */
case class PersistedSchema(
    tableId: String,
    deltaCommitVersion: Long,
    dataSchemaJson: String,
    partitionSchemaJson: String,
    sourceMetadataPath: String,
    tableConfigurations: Option[Map[String, String]] = None) {

  def toJson: String = JsonUtils.toJson(this)

  private def parseSchema(schemaJson: String): StructType = {
    try {
      DataType.fromJson(schemaJson).asInstanceOf[StructType]
    } catch {
      case NonFatal(_) =>
        throw DeltaErrors.failToParseSchemaLog
    }
  }

  @JsonIgnore
  lazy val dataSchema: StructType = parseSchema(dataSchemaJson)

  @JsonIgnore
  lazy val partitionSchema: StructType = parseSchema(partitionSchemaJson)

  def validateAgainstSnapshot(snapshot: Snapshot): Unit = {
    if (snapshot.deltaLog.tableId != tableId) {
      throw DeltaErrors.incompatibleSchemaLogDeltaTable(tableId, snapshot.deltaLog.tableId)
    }
  }

}

object PersistedSchema {
  val VERSION = 1
  val EMPTY_JSON = "{}"

  implicit val format: Formats = Serialization.formats(NoTypeHints)

  def fromJson(json: String): PersistedSchema = JsonUtils.fromJson[PersistedSchema](json)

  def apply(
      tableId: String,
      deltaCommitVersion: Long,
      metadata: Metadata,
      sourceMetadataPath: String): PersistedSchema = {
    PersistedSchema(tableId, deltaCommitVersion,
      metadata.schema.json, metadata.partitionSchema.json,
      // The schema is bound to the specific source
      sourceMetadataPath,
      // Table configurations come from the Metadata action
      Some(metadata.configuration)
    )
  }
}

/**
 * Tracks the schema changes for a particular Delta streaming source in a particular stream,
 * it is utilized to save and lookup the correct schema during streaming from a Delta table.
 * This schema log is NOT meant to be shared across different Delta streaming source instances.
 *
 * @param rootSchemaLocation Schema log location
 * @param sourceSnapshot Delta source snapshot for the Delta streaming source
 * @param sourceMetadataPathOpt The source metadata path that is used during streaming execution.
 * @param initSchemaLogEagerly If true, initialize schema log as early as possible, otherwise,
 *                             initialize only when detecting non-additive schema change.
 */
class DeltaSourceSchemaTrackingLog private(
    sparkSession: SparkSession,
    rootSchemaLocation: String,
    sourceSnapshot: Snapshot,
    sourceMetadataPathOpt: Option[String] = None,
    val initSchemaLogEagerly: Boolean = true)
  extends HDFSMetadataLog[PersistedSchema](sparkSession, rootSchemaLocation) {
  import PersistedSchema._

  protected val schemaAtLogInit: Option[PersistedSchema] = getLatestSchema

  protected val schemaSeqNumAtLogInit: Option[Long] = getLatest().map(_._1)

  // Next schema version to write, this should be updated after each schema evolution.
  // This allow HDFSMetadataLog to best detect concurrent schema log updates.
  protected var nextSeqNumToWrite: Long = schemaSeqNumAtLogInit.map(_ + 1).getOrElse(0L)
  protected var currentSeqNum: Long = schemaSeqNumAtLogInit.getOrElse(-1)

  // Current tracked schema log entry
  protected var currentTrackedSchema: Option[PersistedSchema] = schemaAtLogInit

  // Previous tracked schema log entry
  protected var previousTrackedSchema: Option[PersistedSchema] =
    schemaSeqNumAtLogInit.map(_ - 1L).flatMap(get)

  override protected def deserialize(in: InputStream): PersistedSchema = {
    // Called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw DeltaErrors.failToDeserializeSchemaLog(rootSchemaLocation)
    }

    MetadataVersionUtil.validateVersion(lines.next(), VERSION)
    val schemaJson = if (lines.hasNext) lines.next else EMPTY_JSON
    PersistedSchema.fromJson(schemaJson)
  }

  override protected def serialize(metadata: PersistedSchema, out: OutputStream): Unit = {
    // Called inside a try-finally where the underlying stream is closed in the caller
    out.write(s"v${VERSION}".getBytes(UTF_8))
    out.write('\n')

    // Write metadata
    out.write(metadata.toJson.getBytes(UTF_8))
  }

  /**
   * Globally latest schema log entry.
   */
  protected[delta] def getLatestSchema: Option[PersistedSchema] = getLatest().map(_._2)

  /**
   * The current schema being tracked since this schema log is created. This is typically used as
   * the read schema for Delta streaming source.
   */
  def getCurrentTrackedSchema: Option[PersistedSchema] = currentTrackedSchema

  /**
   * The previous schema being tracked since this schema log is created.
   */
  def getPreviousTrackedSchema: Option[PersistedSchema] = previousTrackedSchema

  /**
   * Write a new entry to the schema log.
   */
  def writeNewSchema(newSchema: PersistedSchema): Unit = synchronized {
    // Write to schema log
    logInfo(s"Writing a new metadata version $nextSeqNumToWrite in the metadata log")
    // Similar to how MicrobatchExecution detects concurrent checkpoint updates
    if (!add(nextSeqNumToWrite, newSchema)) {
      throw DeltaErrors.sourcesWithConflictingSchemaTrackingLocation(
        rootSchemaLocation, sourceSnapshot.deltaLog.dataPath.toString)
    }
    previousTrackedSchema = currentTrackedSchema
    currentTrackedSchema = Some(newSchema)
    currentSeqNum = nextSeqNumToWrite
    nextSeqNumToWrite += 1
  }

  /**
   * Replace the current (latest) entry in the schema log.
   */
  def replaceCurrentSchema(newSchema: PersistedSchema): Unit = synchronized {
    logInfo(s"Replacing the latest metadata version $currentSeqNum in the metadata log")
    // Ensure the current batch id is also the global latest.
    assert(getLatestBatchId().forall(_ == currentSeqNum))
    // Delete the current schema and then replace it with the new schema
    purgeAfter(currentSeqNum - 1)
    // Similar to how MicrobatchExecution detects concurrent checkpoint updates
    if (!add(currentSeqNum, newSchema)) {
      throw DeltaErrors.sourcesWithConflictingSchemaTrackingLocation(
        rootSchemaLocation, sourceSnapshot.deltaLog.dataPath.toString)
    }
    currentTrackedSchema = Some(newSchema)
  }

}

object DeltaSourceSchemaTrackingLog extends Logging {

  def fullSchemaTrackingLocation(
      rootSchemaTrackingLocation: String,
      tableId: String,
      sourceTrackingId: Option[String] = None): String = {
    val subdir = s"_schema_log_$tableId" + sourceTrackingId.map(n => s"_$n").getOrElse("")
    new Path(rootSchemaTrackingLocation, subdir).toString
  }

  /**
   * Create a schema log instance for a schema location.
   * The schema location is constructed as `$rootSchemaLocation/_schema_log_$tableId`
   * a suffix of `_$sourceTrackingId` is appended if provided to further differentiate the sources.
   */
  def create(
      sparkSession: SparkSession,
      rootSchemaLocation: String,
      sourceSnapshot: Snapshot,
      sourceTrackingId: Option[String] = None,
      sourceMetadataPathOpt: Option[String] = None,
      mergeConsecutiveSchemaChanges: Boolean = false,
      initSchemaLogEagerly: Boolean = true): DeltaSourceSchemaTrackingLog = {
    val schemaTrackingLocation = fullSchemaTrackingLocation(
      rootSchemaLocation, sourceSnapshot.deltaLog.tableId, sourceTrackingId)
    val log = new DeltaSourceSchemaTrackingLog(
      sparkSession,
      schemaTrackingLocation,
      sourceSnapshot,
      sourceMetadataPathOpt,
      initSchemaLogEagerly
    )

    // During initialize schema log, validate against:
    // 1. table snapshot to check for partition and tahoe id mismatch
    // 2. source metadata path to ensure we are not using the wrong schema log for the source
    log.getCurrentTrackedSchema.foreach { schema =>
      schema.validateAgainstSnapshot(sourceSnapshot)
      if (sparkSession.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_STREAMING_SCHEMA_TRACKING_METADATA_PATH_CHECK_ENABLED)) {
        sourceMetadataPathOpt.foreach { metadataPath =>
          require(metadataPath == schema.sourceMetadataPath,
            s"The Delta source metadata path used for execution '${metadataPath}' is different " +
              s"from the one persisted for previous processing '${schema.sourceMetadataPath}'. " +
              s"Please check if the schema location has been reused across different streaming " +
              s"sources. Pick a new `${DeltaOptions.SCHEMA_TRACKING_LOCATION}` or use " +
              s"`${DeltaOptions.STREAMING_SOURCE_TRACKING_ID}` to " +
              s"distinguish between streaming sources.")
        }
      }
    }

    // The consecutive schema merging logic is run in the analysis phase, when we figure the final
    // schema to read for the streaming dataframe.
    if (mergeConsecutiveSchemaChanges && log.getCurrentTrackedSchema.isDefined) {
      // If enable schema merging, skim ahead on consecutive schema changes and use the latest one
      // to update the log again if possible.
      // We use `replaceCurrentSchema` so that SQL conf validation logic can reliably tell the
      // previous read schema and the latest schema simply based on batch id / seq num in the log,
      // and then be able to determine if it's OK for the stream to proceed.
      getMergedConsecutiveSchemaChange(
        sparkSession, sourceSnapshot.deltaLog, log.getCurrentTrackedSchema.get
      ).foreach(log.replaceCurrentSchema)
    }

    // The validation is ran in execution phase where the metadata path becomes available.
    // While loading the current persisted schema, validate against previous persisted schema
    // to check if the stream can move ahead with the custom SQL conf.
    (log.getPreviousTrackedSchema, log.getCurrentTrackedSchema, sourceMetadataPathOpt) match {
      case (Some(prev), Some(curr), Some(metadataPath)) =>
        DeltaSourceSchemaEvolutionSupport
          .validateIfSchemaChangeCanBeUnblockedWithSQLConf(sparkSession, metadataPath, curr, prev)
      case _ =>
    }

    log
  }

  /**
   * Speculate ahead and find the next merged consecutive schema change if possible.
   */
  private def getMergedConsecutiveSchemaChange(
      spark: SparkSession,
      deltaLog: DeltaLog,
      currentSchema: PersistedSchema): Option[PersistedSchema] = {
    val currentSchemaVersion = currentSchema.deltaCommitVersion
    // We start from the currentSchemaVersion so that we can stop early in case the current
    // version still has file actions that potentially needs to be processed.
    val untilSchemaChange =
      deltaLog.getChangeLogFiles(currentSchemaVersion).map { case (version, fileStatus) =>
        val schemaChange =
          DeltaSource.createRewindableActionIterator(spark, deltaLog, fileStatus)
            .processAndClose { actionsIter =>
              var metadataAction: Option[Metadata] = None
              var hasFileAction = false
              actionsIter.foreach {
                case m: Metadata => metadataAction = Some(m)
                case _: FileAction => hasFileAction = true
                case _ =>
              }
              if (hasFileAction) None else metadataAction
            }
        schemaChange.map(m => (version, m))
      }.takeWhile(_.isDefined)
    DeltaSource.iteratorLast(untilSchemaChange.toClosable).flatten
      .flatMap { case (version, metadata) =>
      if (version == currentSchemaVersion) {
        None
      } else {
        log.info(s"Looked ahead from version $currentSchemaVersion and " +
          s"will use schema at version $version to read Delta stream.")
        Some(
          PersistedSchema(
            deltaLog.tableId, version, metadata,
            // Keep the same metadata path
            sourceMetadataPath = currentSchema.sourceMetadataPath
          )
        )
      }
    }
  }
}
