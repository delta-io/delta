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

import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions, NoMapping, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, FileAction, Metadata, Protocol}
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
 * @param deltaCommitVersion Delta commit version in which this change is captured. It does not
 *                           necessarily have to be the commit when there's an actual change, e.g.
 *                           during initialization.
 *                           The invariant is that the metadata must be read-compatible with the
 *                           table snapshot at this version.
 * @param dataSchemaJson Full schema json
 * @param partitionSchemaJson Partition schema json
 * @param sourceMetadataPath The checkpoint path that is unique to each source.
 * @param tableConfigurations The configurations of the table inside the metadata when the schema
 *                            change was detected. It is used to correctly create the right file
 *                            format when we use a particular schema to read.
 *                            Default to None for backward compatibility.
 * @param protocolJson JSON of the protocol change if any.
 *                     Default to None for backward compatibility.
 */
case class PersistedMetadata(
    tableId: String,
    deltaCommitVersion: Long,
    dataSchemaJson: String,
    partitionSchemaJson: String,
    sourceMetadataPath: String,
    tableConfigurations: Option[Map[String, String]] = None,
    protocolJson: Option[String] = None) {

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

  @JsonIgnore
  lazy val protocol: Option[Protocol] =
    protocolJson.map(Action.fromJson).map(_.asInstanceOf[Protocol])

  def validateAgainstSnapshot(snapshot: Snapshot): Unit = {
    if (snapshot.deltaLog.tableId != tableId) {
      throw DeltaErrors.incompatibleSchemaLogDeltaTable(tableId, snapshot.deltaLog.tableId)
    }
  }

}

object PersistedMetadata {
  val VERSION = 1
  val EMPTY_JSON = "{}"

  implicit val format: Formats = Serialization.formats(NoTypeHints)

  def fromJson(json: String): PersistedMetadata = JsonUtils.fromJson[PersistedMetadata](json)

  def apply(
      tableId: String,
      deltaCommitVersion: Long,
      metadata: Metadata,
      protocol: Protocol,
      sourceMetadataPath: String): PersistedMetadata = {
    PersistedMetadata(tableId, deltaCommitVersion,
      metadata.schema.json, metadata.partitionSchema.json,
      // The schema is bound to the specific source
      sourceMetadataPath,
      // Table configurations come from the Metadata action
      Some(metadata.configuration),
      Some(protocol.json)
    )
  }
}

/**
 * Tracks the metadata changes for a particular Delta streaming source in a particular stream,
 * it is utilized to save and lookup the correct metadata during streaming from a Delta table.
 * This schema log is NOT meant to be shared across different Delta streaming source instances.
 *
 * @param rootMetadataLocation Metadata log location
 * @param sourceSnapshot Delta source snapshot for the Delta streaming source
 * @param sourceMetadataPathOpt The source metadata path that is used during streaming execution.
 * @param initMetadataLogEagerly If true, initialize metadata log as early as possible, otherwise,
 *                             initialize only when detecting non-additive schema change.
 */
class DeltaSourceMetadataTrackingLog private(
    sparkSession: SparkSession,
    rootMetadataLocation: String,
    sourceSnapshot: Snapshot,
    sourceMetadataPathOpt: Option[String] = None,
    val initMetadataLogEagerly: Boolean = true)
  extends HDFSMetadataLog[PersistedMetadata](sparkSession, rootMetadataLocation) {
  import PersistedMetadata._

  protected val metadataAtLogInit: Option[PersistedMetadata] = getLatestMetadata

  protected val metadataSeqNumAtLogInit: Option[Long] = getLatest().map(_._1)

  // Next schema version to write, this should be updated after each schema evolution.
  // This allow HDFSMetadataLog to best detect concurrent schema log updates.
  protected var nextSeqNumToWrite: Long = metadataSeqNumAtLogInit.map(_ + 1).getOrElse(0L)
  protected var currentSeqNum: Long = metadataSeqNumAtLogInit.getOrElse(-1)

  // Current tracked schema log entry
  protected var currentTrackedMetadata: Option[PersistedMetadata] = metadataAtLogInit

  // Previous tracked schema log entry
  protected var previousTrackedMetadata: Option[PersistedMetadata] =
    metadataSeqNumAtLogInit.map(_ - 1L).flatMap(get)

  override protected def deserialize(in: InputStream): PersistedMetadata = {
    // Called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()
    if (!lines.hasNext) {
      throw DeltaErrors.failToDeserializeSchemaLog(rootMetadataLocation)
    }

    MetadataVersionUtil.validateVersion(lines.next(), VERSION)
    val schemaJson = if (lines.hasNext) lines.next else EMPTY_JSON
    PersistedMetadata.fromJson(schemaJson)
  }

  override protected def serialize(metadata: PersistedMetadata, out: OutputStream): Unit = {
    // Called inside a try-finally where the underlying stream is closed in the caller
    out.write(s"v${VERSION}".getBytes(UTF_8))
    out.write('\n')

    // Write metadata
    out.write(metadata.toJson.getBytes(UTF_8))
  }

  /**
   * Globally latest schema log entry.
   */
  protected[delta] def getLatestMetadata: Option[PersistedMetadata] = getLatest().map(_._2)

  /**
   * The current schema being tracked since this schema log is created. This is typically used as
   * the read schema for Delta streaming source.
   */
  def getCurrentTrackedMetadata: Option[PersistedMetadata] = currentTrackedMetadata

  /**
   * The previous schema being tracked since this schema log is created.
   */
  def getPreviousTrackedMetadata: Option[PersistedMetadata] = previousTrackedMetadata

  /**
   * Write a new entry to the schema log.
   */
  def writeNewMetadata(newSchema: PersistedMetadata): Unit = synchronized {
    // Write to schema log
    logInfo(s"Writing a new metadata version $nextSeqNumToWrite in the metadata log")
    // Similar to how MicrobatchExecution detects concurrent checkpoint updates
    if (!add(nextSeqNumToWrite, newSchema)) {
      throw DeltaErrors.sourcesWithConflictingSchemaTrackingLocation(
        rootMetadataLocation, sourceSnapshot.deltaLog.dataPath.toString)
    }
    previousTrackedMetadata = currentTrackedMetadata
    currentTrackedMetadata = Some(newSchema)
    currentSeqNum = nextSeqNumToWrite
    nextSeqNumToWrite += 1
  }

  /**
   * Replace the current (latest) entry in the schema log.
   */
  def replaceCurrentSchema(newSchema: PersistedMetadata): Unit = synchronized {
    logInfo(s"Replacing the latest metadata version $currentSeqNum in the metadata log")
    // Ensure the current batch id is also the global latest.
    assert(getLatestBatchId().forall(_ == currentSeqNum))
    // Delete the current schema and then replace it with the new schema
    purgeAfter(currentSeqNum - 1)
    // Similar to how MicrobatchExecution detects concurrent checkpoint updates
    if (!add(currentSeqNum, newSchema)) {
      throw DeltaErrors.sourcesWithConflictingSchemaTrackingLocation(
        rootMetadataLocation, sourceSnapshot.deltaLog.dataPath.toString)
    }
    currentTrackedMetadata = Some(newSchema)
  }

}

object DeltaSourceMetadataTrackingLog extends Logging {

  def fullMetadataTrackingLocation(
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
      rootMetadataLocation: String,
      sourceSnapshot: Snapshot,
      sourceTrackingId: Option[String] = None,
      sourceMetadataPathOpt: Option[String] = None,
      mergeConsecutiveSchemaChanges: Boolean = false,
      initMetadataLogEagerly: Boolean = true): DeltaSourceMetadataTrackingLog = {
    val metadataTrackingLocation = fullMetadataTrackingLocation(
      rootMetadataLocation, sourceSnapshot.deltaLog.tableId, sourceTrackingId)
    val log = new DeltaSourceMetadataTrackingLog(
      sparkSession,
      metadataTrackingLocation,
      sourceSnapshot,
      sourceMetadataPathOpt,
      initMetadataLogEagerly
    )

    // During initialize schema log, validate against:
    // 1. table snapshot to check for partition and tahoe id mismatch
    // 2. source metadata path to ensure we are not using the wrong schema log for the source
    log.getCurrentTrackedMetadata.foreach { schema =>
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
    if (mergeConsecutiveSchemaChanges && log.getCurrentTrackedMetadata.isDefined) {
      // If enable schema merging, skim ahead on consecutive schema changes and use the latest one
      // to update the log again if possible.
      // We use `replaceCurrentSchema` so that SQL conf validation logic can reliably tell the
      // previous read schema and the latest schema simply based on batch id / seq num in the log,
      // and then be able to determine if it's OK for the stream to proceed.
      getMergedConsecutiveMetadataChanges(
        sparkSession, sourceSnapshot.deltaLog, log.getCurrentTrackedMetadata.get
      ).foreach(log.replaceCurrentSchema)
    }

    // The validation is ran in execution phase where the metadata path becomes available.
    // While loading the current persisted schema, validate against previous persisted schema
    // to check if the stream can move ahead with the custom SQL conf.
    (log.getPreviousTrackedMetadata, log.getCurrentTrackedMetadata, sourceMetadataPathOpt) match {
      case (Some(prev), Some(curr), Some(metadataPath)) =>
        DeltaSourceMetadataEvolutionSupport
          .validateIfSchemaChangeCanBeUnblockedWithSQLConf(sparkSession, metadataPath, curr, prev)
      case _ =>
    }

    log
  }

  /**
   * Speculate ahead and find the next merged consecutive metadata change if possible.
   * A metadata change is either:
   * 1. A [[Metadata]] action change. OR
   * 2. A [[Protocol]] change.
   */
  private def getMergedConsecutiveMetadataChanges(
      spark: SparkSession,
      deltaLog: DeltaLog,
      currentMetadata: PersistedMetadata): Option[PersistedMetadata] = {
    val currentMetadataVersion = currentMetadata.deltaCommitVersion
    // We start from the currentSchemaVersion so that we can stop early in case the current
    // version still has file actions that potentially needs to be processed.
    val untilMetadataChange =
      deltaLog.getChangeLogFiles(currentMetadataVersion).map { case (version, fileStatus) =>
        var metadataAction: Option[Metadata] = None
        var protocolAction: Option[Protocol] = None
        var hasFileAction = false
        DeltaSource.createRewindableActionIterator(spark, deltaLog, fileStatus)
          .processAndClose { actionsIter =>
            actionsIter.foreach {
              case m: Metadata => metadataAction = Some(m)
              case p: Protocol => protocolAction = Some(p)
              case _: FileAction => hasFileAction = true
              case _ =>
            }
          }
        (!hasFileAction && (metadataAction.isDefined || protocolAction.isDefined),
          version, metadataAction, protocolAction)
      }.takeWhile(_._1)
    DeltaSource.iteratorLast(untilMetadataChange.toClosable)
      .flatMap { case (_, version, metadataOpt, protocolOpt) =>
      if (version == currentMetadataVersion) {
        None
      } else {
        log.info(s"Looked ahead from version $currentMetadataVersion and " +
          s"will use metadata at version $version to read Delta stream.")
        Some(
          currentMetadata.copy(
            deltaCommitVersion = version,
            dataSchemaJson =
              metadataOpt.map(_.schema.json).getOrElse(currentMetadata.dataSchemaJson),
            partitionSchemaJson =
              metadataOpt.map(_.partitionSchema.json)
                .getOrElse(currentMetadata.partitionSchemaJson),
            tableConfigurations = metadataOpt.map(_.configuration)
              .orElse(currentMetadata.tableConfigurations),
            protocolJson = protocolOpt.map(_.json).orElse(currentMetadata.protocolJson)
          )
        )
      }
    }
  }
}
