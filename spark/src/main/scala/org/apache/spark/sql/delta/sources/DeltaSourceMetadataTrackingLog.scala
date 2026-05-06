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

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.streaming.{JsonSchemaSerializer, PartitionAndDataSchema, SchemaTrackingLog}
import org.apache.spark.sql.delta.{DeltaErrors, DeltaOptions}
import org.apache.spark.sql.delta.actions.{Action, Protocol}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.delta.v2.interop.{AbstractMetadata, AbstractProtocol}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
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

  def validateAgainstSourceTableId(sourceTableId: String): Unit = {
    if (sourceTableId != tableId) {
      throw DeltaErrors.incompatibleSchemaLogDeltaTable(
        tableId, sourceTableId)
    }
  }
}

object PersistedMetadata {
  val VERSION = 1
  val EMPTY_JSON = "{}"

  def fromJson(json: String): PersistedMetadata = JsonUtils.fromJson[PersistedMetadata](json)

  /**
   * Builds a [[PersistedMetadata]] from V2 interop abstractions.
   *
   * Contract on [[AbstractProtocol]]: `readerFeatures` / `writerFeatures` must be consistent
   * with the min protocol versions: `readerFeatures` may only be defined when
   * `minReaderVersion >= TABLE_FEATURES_MIN_READER_VERSION`, and `writerFeatures` may only be
   * defined when `minWriterVersion >= TABLE_FEATURES_MIN_WRITER_VERSION`. The conversion below
   * relies on this invariant; [[Protocol]] will throw a `require` failure if an implementation
   * gets it wrong.
   */
  def apply(
      tableId: String,
      deltaCommitVersion: Long,
      abstractMetadata: AbstractMetadata,
      abstractProtocol: AbstractProtocol,
      sourceMetadataPath: String): PersistedMetadata = {
    val protocol = Protocol(
      abstractProtocol.minReaderVersion,
      abstractProtocol.minWriterVersion
    ).copy(
      readerFeatures = abstractProtocol.readerFeatures,
      writerFeatures = abstractProtocol.writerFeatures
    )
    PersistedMetadata(tableId, deltaCommitVersion,
      abstractMetadata.schema.json, abstractMetadata.partitionSchema.json,
      // The schema is bound to the specific source
      sourceMetadataPath,
      // Table configurations come from the Metadata action
      Some(abstractMetadata.configuration),
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
 * @param sourceTableId Delta source table ID for the Delta streaming source
 * @param sourceDataPath Delta source table data path for the Delta streaming source
 * @param sourceMetadataPathOpt The source metadata path that is used during streaming execution.
 * @param initMetadataLogEagerly If true, initialize metadata log as early as possible, otherwise,
 *                             initialize only when detecting non-additive schema change.
 */
class DeltaSourceMetadataTrackingLog private(
    sparkSession: SparkSession,
    rootMetadataLocation: String,
    sourceTableId: String,
    sourceDataPath: String,
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
  trackingLog.getCurrentTrackedSchema.foreach(_.validateAgainstSourceTableId(sourceTableId))

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
   * Get the current tracked seq num by this schema log or -1 if no schema has been tracked yet.
   */
  def getCurrentTrackedSeqNum: Long = trackingLog.getCurrentTrackedSeqNum

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
          rootMetadataLocation, sourceDataPath)
    }
  }
}

object DeltaSourceMetadataTrackingLog {

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
   * @param consecutiveSchemaChangesMerger A connector-specific function that, given the current
   *                                       tracked metadata, looks ahead through consecutive
   *                                       metadata-only commits and returns a merged
   *                                       [[PersistedMetadata]] if possible. V1 uses DeltaLog to
   *                                       iterate commits; V2 would use Delta Kernel.
   * @param sourceMetadataPathOpt Defined during execution phase.
   */
  def create(
      sparkSession: SparkSession,
      rootMetadataLocation: String,
      sourceTableId: String,
      sourceDataPath: String,
      parameters: Map[String, String],
      sourceMetadataPathOpt: Option[String] = None,
      mergeConsecutiveSchemaChanges: Boolean = false,
      consecutiveSchemaChangesMerger: Option[PersistedMetadata => Option[PersistedMetadata]] =
        None,
      initMetadataLogEagerly: Boolean = true): DeltaSourceMetadataTrackingLog = {
    require(
      !mergeConsecutiveSchemaChanges || consecutiveSchemaChangesMerger.isDefined,
      "consecutiveSchemaChangesMerger must be provided when mergeConsecutiveSchemaChanges is true")
    val options = new CaseInsensitiveStringMap(parameters.asJava)
    val sourceTrackingId = Option(options.get(DeltaOptions.STREAMING_SOURCE_TRACKING_ID))
    val metadataTrackingLocation = fullMetadataTrackingLocation(
      rootMetadataLocation, sourceTableId, sourceTrackingId)
    val log = new DeltaSourceMetadataTrackingLog(
      sparkSession,
      metadataTrackingLocation,
      sourceTableId,
      sourceDataPath,
      sourceMetadataPathOpt,
      initMetadataLogEagerly
    )

    // During initialize schema log, validate against:
    // 1. table id mismatch
    // 2. source metadata path to ensure we are not using the wrong schema log for the source
    log.getCurrentTrackedMetadata.foreach { schema =>
      schema.validateAgainstSourceTableId(sourceTableId)
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
      consecutiveSchemaChangesMerger.get
        .apply(log.getCurrentTrackedMetadata.get)
        .foreach { mergedSchema =>
          log.writeNewMetadata(mergedSchema, replaceCurrent = true)
        }
    }

    // The validation is ran in *execution* phase where the metadata path becomes available.
    // While loading the current persisted schema, validate against previous persisted schema
    // to check if the stream can move ahead with the custom SQL conf.
    (log.getPreviousTrackedMetadata, log.getCurrentTrackedMetadata, sourceMetadataPathOpt) match {
      case (Some(prev), Some(curr), Some(metadataPath)) =>
        DeltaSourceMetadataEvolutionSupport
          .validateIfSchemaChangeCanBeUnblocked(
            sparkSession, parameters, metadataPath, curr, prev)
      case _ =>
    }

    log
  }
}
