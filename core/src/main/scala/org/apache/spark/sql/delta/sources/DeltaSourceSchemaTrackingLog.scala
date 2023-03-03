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

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Locale

import scala.io.{Source => IOSource}
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{DeltaColumnMapping, DeltaErrors, DeltaLog, Snapshot}
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.fs.Path
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, MetadataVersionUtil}
import org.apache.spark.sql.types.{DataType, StructType}

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
 */
case class PersistedSchema(
    tableId: String,
    deltaCommitVersion: Long,
    dataSchemaJson: String,
    partitionSchemaJson: String) {

  def toJson: String = JsonUtils.toJson(this)

  private def parseSchema(schemaJson: String): StructType = {
    try {
      DataType.fromJson(schemaJson).asInstanceOf[StructType]
    } catch {
      case NonFatal(_) =>
        throw DeltaErrors.failToParseSchemaLog
    }
  }

  def dataSchema: StructType = parseSchema(dataSchemaJson)

  def partitionSchema: StructType = parseSchema(partitionSchemaJson)

  def validateAgainstSnapshot(snapshot: Snapshot): Unit = {
    if (snapshot.deltaLog.tableId != tableId) {
      throw DeltaErrors.incompatibleSchemaLogDeltaTable(tableId, snapshot.deltaLog.tableId)
    }
    if (snapshot.metadata.partitionSchema != partitionSchema) {
      throw DeltaErrors.incompatibleSchemaLogPartitionSchema(
        partitionSchema, snapshot.metadata.partitionSchema)
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
      deltaVersion: Long,
      dataSchema: StructType,
      partitionSchema: StructType): PersistedSchema =
    PersistedSchema(tableId, deltaVersion, dataSchema.json, partitionSchema.json)
}

/**
 * Tracks the schema changes for a particular Delta streaming source in a particular stream,
 * it is utilized to save and lookup the correct schema during streaming from a Delta table.
 * This schema log is NOT meant to be shared across different Delta streaming source instances.
 *
 * @param rootSchemaLocation Schema log location
 * @param sourceSnapshot Delta source snapshot for the Delta streaming source
 */
class DeltaSourceSchemaTrackingLog private(
    sparkSession: SparkSession,
    rootSchemaLocation: String,
    sourceSnapshot: Snapshot)
  extends HDFSMetadataLog[PersistedSchema](sparkSession, rootSchemaLocation) {
  import PersistedSchema._

  protected val schemaAtLogInit: Option[PersistedSchema] = {
    val currentLatestSchemaOpt = getLatestSchema
    currentLatestSchemaOpt.foreach(_.validateAgainstSnapshot(sourceSnapshot))
    currentLatestSchemaOpt
  }

  protected val schemaBatchIdAtLogInit: Option[Long] = getLatest().map(_._1)

  // Next schema version to write, this should be updated after each schema evolution.
  // This allow HDFSMetadataLog to best detect concurrent schema log updates.
  protected var nextVersionToWrite: Long = schemaBatchIdAtLogInit.map(_ + 1).getOrElse(0L)

  // Current tracked schema log entry
  protected var currentTrackedSchema: Option[PersistedSchema] = schemaAtLogInit

  // Previous tracked schema log entry
  protected var previousTrackedSchema: Option[PersistedSchema] =
    schemaBatchIdAtLogInit.map(_ - 1L).flatMap(get)

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

  def evolveSchema(newSchema: PersistedSchema): Unit = {
    // Write to schema log
    logInfo(s"Writing a new metadata version $nextVersionToWrite in the metadata log")
    // Similar to how MicrobatchExecution detects concurrent checkpoint updates
    if (!add(nextVersionToWrite, newSchema)) {
      throw DeltaErrors.sourcesWithConflictingSchemaTrackingLocation(
        rootSchemaLocation, sourceSnapshot.deltaLog.dataPath.toString)
    }
    previousTrackedSchema = currentTrackedSchema
    currentTrackedSchema = Some(newSchema)
    nextVersionToWrite += 1
  }

}

object DeltaSourceSchemaTrackingLog {

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
      sourceTrackingId: Option[String] = None): DeltaSourceSchemaTrackingLog = {
    val schemaTrackingLocation = fullSchemaTrackingLocation(
      rootSchemaLocation, sourceSnapshot.deltaLog.tableId, sourceTrackingId)
    new DeltaSourceSchemaTrackingLog(sparkSession, schemaTrackingLocation, sourceSnapshot)
  }
}
