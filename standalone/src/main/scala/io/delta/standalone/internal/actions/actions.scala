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

package io.delta.standalone.internal.actions

import java.net.URI

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonRawValue}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize

import io.delta.standalone.types.StructType
import io.delta.standalone.internal.util.{DataTypeParser, JsonUtils}

private[internal] object Action {
  /** The maximum version of the protocol that this version of Delta Standalone understands. */
  val readerVersion = 1
  val writerVersion = 2
  val protocolVersion: Protocol = Protocol(readerVersion, writerVersion)

  def fromJson(json: String): Action = {
    JsonUtils.mapper.readValue[SingleAction](json).unwrap
  }
}

/**
 * Represents a single change to the state of a Delta table. An order sequence
 * of actions can be replayed using [[InMemoryLogReplay]] to derive the state
 * of the table at a given point in time.
 */
private[internal] sealed trait Action {
  def wrap: SingleAction

  def json: String = JsonUtils.toJson(wrap)
}

/**
 * Used to block older clients from reading or writing the log when backwards
 * incompatible changes are made to the protocol. Readers and writers are
 * responsible for checking that they meet the minimum versions before performing
 * any other operations.
 *
 * Since this action allows us to explicitly block older clients in the case of a
 * breaking change to the protocol, clients should be tolerant of messages and
 * fields that they do not understand.
 */
private[internal] case class Protocol(
    minReaderVersion: Int = Action.readerVersion,
    minWriterVersion: Int = Action.writerVersion) extends Action {
  override def wrap: SingleAction = SingleAction(protocol = this)

  @JsonIgnore
  def simpleString: String = s"($minReaderVersion,$minWriterVersion)"
}

/** Actions pertaining to the addition and removal of files. */
private[internal] sealed trait FileAction extends Action {
  val path: String
  val dataChange: Boolean
  @JsonIgnore
  lazy val pathAsUri: URI = new URI(path)
}

/**
 * Adds a new file to the table. When multiple [[AddFile]] file actions
 * are seen with the same `path` only the metadata from the last one is
 * kept.
 */
private[internal] case class AddFile(
    path: String,
    @JsonInclude(JsonInclude.Include.ALWAYS)
    partitionValues: Map[String, String],
    size: Long,
    modificationTime: Long,
    dataChange: Boolean,
    @JsonRawValue
    stats: String = null,
    tags: Map[String, String] = null) extends FileAction {
  require(path.nonEmpty)

  override def wrap: SingleAction = SingleAction(add = this)

  def remove: RemoveFile = removeWithTimestamp()

  def removeWithTimestamp(
    timestamp: Long = System.currentTimeMillis(),
    dataChange: Boolean = true): RemoveFile = {
    // scalastyle:off
    RemoveFile(path, Some(timestamp), dataChange)
    // scalastyle:on
  }
}

/**
 * Logical removal of a given file from the reservoir. Acts as a tombstone before a file is
 * deleted permanently.
 */
private[internal] case class RemoveFile(
    path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    dataChange: Boolean = true) extends FileAction {
  override def wrap: SingleAction = SingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)
}

private[internal] case class Format(
    provider: String = "parquet",
    options: Map[String, String] = Map.empty)

/**
 * Updates the metadata of the table. Only the last update to the [[Metadata]]
 * of a table is kept. It is the responsibility of the writer to ensure that
 * any data already present in the table is still valid after any change.
 */
private[internal] case class Metadata(
    id: String = java.util.UUID.randomUUID().toString,
    name: String = null,
    description: String = null,
    format: Format = Format(),
    schemaString: String = null,
    partitionColumns: Seq[String] = Nil,
    configuration: Map[String, String] = Map.empty,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    createdTime: Option[Long] = Some(System.currentTimeMillis())) extends Action {

  /** Returns the schema as a [[StructType]] */
  @JsonIgnore
  lazy val schema: StructType =
    Option(schemaString).map { s =>
      DataTypeParser.fromJson(s).asInstanceOf[StructType]
    }.getOrElse(new StructType(Array.empty))

  override def wrap: SingleAction = SingleAction(metaData = this)
}

/**
 * Interface for objects that represents the information for a commit. Commits can be referred to
 * using a version and timestamp. The timestamp of a commit comes from the remote storage
 * `lastModifiedTime`, and can be adjusted for clock skew. Hence we have the method `withTimestamp`.
 */
private[internal] trait CommitMarker {
  /** Get the timestamp of the commit as millis after the epoch. */
  def getTimestamp: Long
  /** Return a copy object of this object with the given timestamp. */
  def withTimestamp(timestamp: Long): CommitMarker
  /** Get the version of the commit. */
  def getVersion: Long
}

/** A serialization helper to create a common action envelope. */
private[internal] case class SingleAction(
    add: AddFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (protocol != null) {
      protocol
    } else {
      null
    }
  }
}