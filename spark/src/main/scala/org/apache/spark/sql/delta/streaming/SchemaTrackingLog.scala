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

package org.apache.spark.sql.delta.streaming

import java.io.{InputStream, OutputStream}
import java.nio.charset.StandardCharsets._

import scala.io.{Source => IOSource}
import scala.reflect.ClassTag

import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.annotation.JsonIgnore

import org.apache.spark.internal.{LoggingShims, MDC}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.{HDFSMetadataLog, MetadataVersionUtil}
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A serializable schema with a partition schema and a data schema.
 */
trait PartitionAndDataSchema {

  @JsonIgnore
  def dataSchema: DataType

  @JsonIgnore
  def partitionSchema: StructType
}

/**
 * A schema serializer handles the SerDe of a [[PartitionAndDataSchema]]
 */
sealed trait SchemaSerializer[T <: PartitionAndDataSchema] {
  def serdeVersion: Int

  def serialize(schema: T, outputStream: OutputStream): Unit

  def deserialize(in: InputStream): T
}

/**
 *A schema serializer that reads/writes schema using the following format:
 * {SERDE_VERSION}
 * {JSON of the serializable schema}
 */
class JsonSchemaSerializer[T <: PartitionAndDataSchema: ClassTag: Manifest]
  (override val serdeVersion: Int) extends SchemaSerializer[T] {

  import SchemaTrackingExceptions._

  val EMPTY_JSON = "{}"

  /**
   * Deserializes the log entry from input stream.
   * @throws FailedToDeserializeException
   */
  override def deserialize(in: InputStream): T = {
    // Called inside a try-finally where the underlying stream is closed in the caller
    val lines = IOSource.fromInputStream(in, UTF_8.name()).getLines()

    if (!lines.hasNext) {
      throw FailedToDeserializeException
    }

    MetadataVersionUtil.validateVersion(lines.next(), serdeVersion)
    val schemaJson = if (lines.hasNext) lines.next() else EMPTY_JSON
    JsonUtils.fromJson(schemaJson)
  }

  override def serialize(metadata: T, out: OutputStream): Unit = {
    // Called inside a try-finally where the underlying stream is closed in the caller
    out.write(s"v${serdeVersion}".getBytes(UTF_8))
    out.write('\n')

    // Write metadata
    out.write(JsonUtils.toJson(metadata).getBytes(UTF_8))
  }
}

/**
 * The underlying class for a streaming log that keeps track of a sequence of schema changes.
 *
 * It keeps tracks of the sequence of schema changes that this log is aware of, and it detects any
 * concurrent modifications to the schema log to prevent accidents on a best effort basis.
 */
class SchemaTrackingLog[T <: PartitionAndDataSchema: ClassTag: Manifest](
    sparkSession: SparkSession,
    path: String,
    schemaSerializer: SchemaSerializer[T])
  extends HDFSMetadataLog[T](sparkSession, path) with LoggingShims {

  import SchemaTrackingExceptions._

  // The schema and version detected when this log is initialized
  private val schemaAndSeqNumAtLogInit: Option[(Long, T)] = getLatest()

  // Next schema version to write, this should be updated after each schema evolution.
  // This allow HDFSMetadataLog to best detect concurrent schema log updates.
  private var currentSeqNum: Long = schemaAndSeqNumAtLogInit.map(_._1).getOrElse(-1L)
  private var nextSeqNumToWrite: Long = currentSeqNum + 1

  // The current persisted schema this log has been tracking. Note that this does NOT necessarily
  // always equal to the globally latest schema. Attempting to commit to a schema version that
  // already exists is illegal.
  // Subclass can leverage this to compare the differences.
  private var currentTrackedSchema: Option[T] = schemaAndSeqNumAtLogInit.map(_._2)


  /**
   * Get the latest tracked schema entry by this schema log
   */
  def getCurrentTrackedSchema: Option[T] = currentTrackedSchema

  /**
   * Get the latest tracked schema batch ID / seq num by this log
   */
  def getCurrentTrackedSeqNum: Long = currentSeqNum

  /**
   * Get the tracked schema at specified seq num.
   */
  def getTrackedSchemaAtSeqNum(seqNum: Long): Option[T] = get(seqNum)

  /**
   * Deserializes the log entry from input stream.
   * @throws FailedToDeserializeException
   */
  override protected def deserialize(in: InputStream): T =
    schemaSerializer.deserialize(in).asInstanceOf[T]

  override protected def serialize(metadata: T, out: OutputStream): Unit =
    schemaSerializer.serialize(metadata, out)

  /**
   * Main API to actually write the log entry to the schema log. Clients can leverage this
   * to save their new schema to the log.
   * @throws FailedToEvolveSchema
   * @param newSchema New persisted schema
   */
  def addSchemaToLog(newSchema: T): T = {
    // Write to schema log
    logInfo(log"Writing a new metadata version " +
      log"${MDC(DeltaLogKeys.VERSION, nextSeqNumToWrite)} in the metadata log")
    if (currentTrackedSchema.contains(newSchema)) {
      // Record a warning if schema has not changed
      logWarning(log"Schema didn't change after schema evolution. " +
        log"currentSchema = ${MDC(DeltaLogKeys.SCHEMA, currentTrackedSchema)}.")
      return newSchema
    }
    // Similar to how MicrobatchExecution detects concurrent checkpoint updates
    if (!add(nextSeqNumToWrite, newSchema)) {
      throw FailedToEvolveSchema
    }

    currentTrackedSchema = Some(newSchema)
    currentSeqNum = nextSeqNumToWrite
    nextSeqNumToWrite += 1
    newSchema
  }
}

object SchemaTrackingExceptions {
  // Designated exceptions
  val FailedToDeserializeException =
    new RuntimeException("Failed to deserialize schema log")
  val FailedToEvolveSchema =
    new RuntimeException("Failed to add schema entry to log. Concurrent operations detected.")
}
