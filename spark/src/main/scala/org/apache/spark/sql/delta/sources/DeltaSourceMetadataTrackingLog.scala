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
import java.io.InputStream

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.streaming.{JsonSchemaSerializer, PartitionAndDataSchema, SchemaTrackingLog}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOptions, Snapshot}
import org.apache.spark.sql.delta.actions.{Action, FileAction, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.ClosableIterator._
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
// scalastyle:on import.ordering.noEmptyLine

/**
 * A [[PersistedMetadata]] is an entry in Delta streaming source schema log, which can be used to
 * read data files during streaming.
 *
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
 * @param previousMetadataSeqNum When defined, it points to the batch ID / seq num for the previous
 *                           metadata in the log sequence. It is used when we could not reliably
 *                           tell if the currentBatchId - 1 is indeed the previous schema evolution,
 *                           e.g. when we are merging consecutive schema changes during the analysis
 *                           phase and we are appending an extra schema after the merge to the log.
 *                           Default to None for backward compatibility.
 */
case class PersistedMetadata(
    tableId: String,
    deltaCommitVersion: Long,
    dataSchemaJson: String,
    partitionSchemaJson: String,
    sourceMetadataPath: String,
    tableConfigurations: Option[Map[String, String]] = None,
    protocolJson: Option[String] = None,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    previousMetadataSeqNum: Option[Long] = None) extends PartitionAndDataSchema {

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
    val initMetadataLogEagerly: Boolean = true) {

  import org.apache.spark.sql.delta.streaming.SchemaTrackingExceptions._

  protected val schemaSerializer =
    new JsonSchemaSerializer[PersistedMetadata](PersistedMetadata.VERSION) {
      override def deserialize(in: InputStream): PersistedMetadata =
        try super.deserialize(in) catch {
          case FailedToDeserializeException =>
            throw DeltaErrors.failToDeserializeSchemaLog(rootMetadataLocation)
        }
    }

  protected val trackingLog =
    new SchemaTrackingLog[PersistedMetadata](
      sparkSession, rootMetadataLocation, schemaSerializer)

  // Validate schema at log init
  trackingLog.getCurrentTrackedSchema.foreach(_.validateAgainstSnapshot(sourceSnapshot))

  /**
   * Get the global latest metadata for this metadata location.
   * Visible for testing
   */
  private[delta] def getLatestMetadata: Option[PersistedMetadata] =
    trackingLog.getLatest().map(_._2)

  /**
   * Get the current schema that is being tracked by this schema log. This is typically the latest
   * schema log entry to the best of this schema log's knowledge.
   */
  def getCurrentTrackedMetadata: Option[PersistedMetadata] =
    trackingLog.getCurrentTrackedSchema

  /**
   * Get the logically-previous tracked seq num by this schema log.
   * Considering the prev pointer from the latest entry if defined.
   */
  private def getPreviousTrackedSeqNum: Long = {
    getCurrentTrackedMetadata.flatMap(_.previousMetadataSeqNum) match {
      case Some(previousSeqNum) => previousSeqNum
      case None => trackingLog.getCurrentTrackedSeqNum - 1
    }
  }

  /**
   * Get the logically-previous tracked schema entry by this schema log.
   * DeltaSource requires it to compare the previous schema with the latest schema to determine if
   * an automatic stream restart is allowed.
   */
  def getPreviousTrackedMetadata: Option[PersistedMetadata] =
    trackingLog.getTrackedSchemaAtSeqNum(getPreviousTrackedSeqNum)

  /**
   * Track a new schema to the log.
   *
   * @param newMetadata The incoming new metadata with schema.
   * @param replaceCurrent If true, we will set a previous seq num pointer on the incoming metadata
   *                       change pointing to the previous seq num of the current latest metadata.
   *                       So that once the new metadata is written, getPreviousTrackedMetadata()
   *                       will return the updated reference.
   *                       If a previous metadata does not exist, this is noop.
   */
  def writeNewMetadata(
      newMetadata: PersistedMetadata,
      replaceCurrent: Boolean = false): PersistedMetadata = {
    try {
      trackingLog.addSchemaToLog(
        if (replaceCurrent && getCurrentTrackedMetadata.isDefined) {
          newMetadata.copy(previousMetadataSeqNum = Some(getPreviousTrackedSeqNum))
        } else newMetadata
      )
    } catch {
      case FailedToEvolveSchema =>
        throw DeltaErrors.sourcesWithConflictingSchemaTrackingLocation(
          rootMetadataLocation, sourceSnapshot.deltaLog.dataPath.toString)
    }
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
   * The schema location is constructed as `$rootMetadataLocation/_schema_log_$tableId`
   * a suffix of `_$sourceTrackingId` is appended if provided to further differentiate the sources.
   *
   * @param mergeConsecutiveSchemaChanges Defined during analysis phase.
   * @param sourceMetadataPathOpt Defined during execution phase.
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

    // The consecutive schema merging logic is run in the *analysis* phase, when we figure the final
    // schema to read for the streaming dataframe.
    if (mergeConsecutiveSchemaChanges && log.getCurrentTrackedMetadata.isDefined) {
      // If enable schema merging, skim ahead on consecutive schema changes and use the latest one
      // to update the log again if possible.
      // We add the prev pointer to the merged schema so that SQL conf validation logic later can
      // reliably fetch the previous read schema and the latest schema and then be able to determine
      // if it's OK for the stream to proceed.
      getMergedConsecutiveMetadataChanges(
        sparkSession,
        sourceSnapshot.deltaLog,
        log.getCurrentTrackedMetadata.get
      ).foreach { mergedSchema =>
        log.writeNewMetadata(mergedSchema, replaceCurrent = true)
      }
    }

    // The validation is ran in *execution* phase where the metadata path becomes available.
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
