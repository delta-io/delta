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
import java.sql.Timestamp

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonInclude, JsonRawValue}
import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.databind.{JsonSerializer, SerializerProvider}
import com.fasterxml.jackson.databind.annotation.{JsonDeserialize, JsonSerialize}
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

private[internal] object Protocol {
  val MIN_READER_VERSION_PROP = "delta.minReaderVersion"
  val MIN_WRITER_VERSION_PROP = "delta.minWriterVersion"

  def checkMetadataProtocolProperties(metadata: Metadata, protocol: Protocol): Unit = {
    assert(!metadata.configuration.contains(MIN_READER_VERSION_PROP), s"Should not have the " +
      s"protocol version ($MIN_READER_VERSION_PROP) as part of table properties")
    assert(!metadata.configuration.contains(MIN_WRITER_VERSION_PROP), s"Should not have the " +
      s"protocol version ($MIN_WRITER_VERSION_PROP) as part of table properties")
  }
}

/**
* Sets the committed version for a given application. Used to make operations
* like streaming append idempotent.
*/
private[internal] case class SetTransaction(
    appId: String,
    version: Long,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    lastUpdated: Option[Long]) extends Action {
  override def wrap: SingleAction = SingleAction(txn = this)
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
 *
 * Note that for protocol compatibility reasons, the fields `partitionValues`, `size`, and `tags`
 * are only present when the extendedFileMetadata flag is true. New writers should generally be
 * setting this flag, but old writers (and FSCK) won't, so readers must check this flag before
 * attempting to consume those values.
 */
private[internal] case class RemoveFile(
    path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    dataChange: Boolean = true,
    extendedFileMetadata: Boolean = false,
    partitionValues: Map[String, String] = null,
    size: Long = 0,
    tags: Map[String, String] = null) extends FileAction {
  override def wrap: SingleAction = SingleAction(remove = this)

  @JsonIgnore
  val delTimestamp: Long = deletionTimestamp.getOrElse(0L)
}

/**
 * A change file containing CDC data for the Delta version it's within. Non-CDC readers should
 * ignore this, CDC readers should scan all ChangeFiles in a version rather than computing
 * changes from AddFile and RemoveFile actions.
 */
private[internal] case class AddCDCFile(
    path: String,
    partitionValues: Map[String, String],
    size: Long,
    tags: Map[String, String] = null) extends FileAction {
  override val dataChange = false

  override def wrap: SingleAction = SingleAction(cdc = this)
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

  /** Columns written out to files. */
  @JsonIgnore
  lazy val dataSchema: StructType = {
    val partitions = partitionColumns.toSet
    new StructType(schema.getFields.filterNot(f => partitions.contains(f.getName)))
  }

  /** Returns the partitionSchema as a [[StructType]] */
  @JsonIgnore
  lazy val partitionSchema: StructType =
    new StructType(partitionColumns.map(c => schema.get(c)).toArray)

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

/**
 * Holds provenance information about changes to the table. This [[Action]]
 * is not stored in the checkpoint and has reduced compatibility guarantees.
 * Information stored in it is best effort (i.e. can be falsified by the writer).
 */
private[internal] case class CommitInfo(
    // The commit version should be left unfilled during commit(). When reading a delta file, we can
    // infer the commit version from the file name and fill in this field then.
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    version: Option[Long],
    timestamp: Timestamp,
    userId: Option[String],
    userName: Option[String],
    operation: String,
    @JsonSerialize(using = classOf[JsonMapSerializer])
    operationParameters: Map[String, String],
    job: Option[JobInfo],
    notebook: Option[NotebookInfo],
    clusterId: Option[String],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    readVersion: Option[Long],
    isolationLevel: Option[String],
    /** Whether this commit has blindly appended without caring about existing files */
    isBlindAppend: Option[Boolean],
    operationMetrics: Option[Map[String, String]],
    userMetadata: Option[String],
    engineInfo: Option[String]) extends Action with CommitMarker {
  override def wrap: SingleAction = SingleAction(commitInfo = this)

  override def withTimestamp(timestamp: Long): CommitInfo = {
    this.copy(timestamp = new Timestamp(timestamp))
  }

  override def getTimestamp: Long = timestamp.getTime
  @JsonIgnore
  override def getVersion: Long = version.get
}

private[internal] object CommitInfo {
  def empty(version: Option[Long] = None): CommitInfo = {
    CommitInfo(version, null, None, None, null, null, None, None,
      None, None, None, None, None, None, None)
  }

  def apply(
      time: Long,
      operation: String,
      operationParameters: Map[String, String],
      commandContext: Map[String, String],
      readVersion: Option[Long],
      isolationLevel: Option[String],
      isBlindAppend: Option[Boolean],
      operationMetrics: Option[Map[String, String]],
      userMetadata: Option[String],
      engineInfo: Option[String]): CommitInfo = {
    val getUserName = commandContext.get("user").flatMap {
      case "unknown" => None
      case other => Option(other)
    }

    CommitInfo(
      None,
      new Timestamp(time),
      commandContext.get("userId"),
      getUserName,
      operation,
      operationParameters,
      JobInfo.fromContext(commandContext),
      NotebookInfo.fromContext(commandContext),
      commandContext.get("clusterId"),
      readVersion,
      isolationLevel,
      isBlindAppend,
      operationMetrics,
      userMetadata,
      engineInfo
    )
  }
}

private[internal] case class JobInfo(
    jobId: String,
    jobName: String,
    runId: String,
    jobOwnerId: String,
    triggerType: String)

private[internal] object JobInfo {
  def fromContext(context: Map[String, String]): Option[JobInfo] = {
    context.get("jobId").map { jobId =>
      JobInfo(
        jobId,
        context.get("jobName").orNull,
        context.get("runId").orNull,
        context.get("jobOwnerId").orNull,
        context.get("jobTriggerType").orNull)
    }
  }
}

private[internal] case class NotebookInfo(notebookId: String)

private[internal] object NotebookInfo {
  def fromContext(context: Map[String, String]): Option[NotebookInfo] = {
    context.get("notebookId").map { nbId => NotebookInfo(nbId) }
  }
}

/** A serialization helper to create a common action envelope. */
private[internal] case class SingleAction(
    txn: SetTransaction = null,
    add: AddFile = null,
    remove: RemoveFile = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    cdc: AddCDCFile = null,
    commitInfo: CommitInfo = null) {

  def unwrap: Action = {
    if (add != null) {
      add
    } else if (remove != null) {
      remove
    } else if (metaData != null) {
      metaData
    } else if (txn != null) {
      txn
    } else if (protocol != null) {
      protocol
    } else if (cdc != null) {
      cdc
    } else if (commitInfo != null) {
      commitInfo
    } else {
      null
    }
  }
}

/** Serializes Maps containing JSON strings without extra escaping. */
private[internal] class JsonMapSerializer extends JsonSerializer[Map[String, String]] {
  def serialize(
      parameters: Map[String, String],
      jgen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    jgen.writeStartObject()
    parameters.foreach { case (key, value) =>
      if (value == null) {
        jgen.writeNullField(key)
      } else {
        jgen.writeFieldName(key)
        // Write value as raw data, since it's already JSON text
        jgen.writeRawValue(value)
      }
    }
    jgen.writeEndObject()
  }
}

/**
 * Parquet4s Wrapper Classes
 *
 * With the inclusion of RemoveFile as an exposed Java API, and since it was upgraded to match the
 * latest Delta OSS release, we now had a case class inside of [[SingleAction]] that had "primitive"
 * default parameters. They are primitive in the sense that Parquet4s would try to decode them using
 * the [[PrimitiveValueCodecs]] trait. But since these parameters have default values, there is no
 * guarantee that they will exist in the underlying parquet checkpoint files. Thus (without these
 * classes), parquet4s would throw errors like this:
 *
 * Cause: java.lang.IllegalArgumentException: NullValue cannot be decoded to required type
 *   at com.github.mjakubowski84.parquet4s.RequiredValueCodec.decode(ValueCodec.scala:61)
 *   at com.github.mjakubowski84.parquet4s.RequiredValueCodec.decode$(ValueCodec.scala:58)
 *   at com.github.mjakubowski84.parquet4s.PrimitiveValueCodecs$$anon$5.decode(ValueCodec.scala:137)
 *
 * Note this only happens with "primitive" parameters with default arguments, and not with "complex"
 * or optional constructor parameters.
 *
 * We solve this issue by creating wrapper classes that wrap these primitive constructor parameters
 * in [[Option]]s, and then un-wrapping them as needed, performing the appropriate Option[T] => T
 * parameter conversions.
 */

private[internal] trait Parquet4sWrapper[T] {
  def unwrap: T
}

private[internal] case class Parquet4sRemoveFileWrapper(
    path: String,
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    deletionTimestamp: Option[Long],
    dataChangeOpt: Option[Boolean] = Some(true),
    extendedFileMetadataOpt: Option[Boolean] = Some(false),
    partitionValues: Map[String, String] = null,
    size: Option[Long] = Some(0),
    tags: Map[String, String] = null) extends Parquet4sWrapper[RemoveFile] {

  override def unwrap: RemoveFile = RemoveFile(
    path,
    deletionTimestamp,
    dataChangeOpt.contains(true),
    extendedFileMetadataOpt.contains(true),
    partitionValues,
    size match {
      case Some(x) => x;
      case _ => 0
    },
    tags
  )
}

private[internal] case class Parquet4sSingleActionWrapper(
    txn: SetTransaction = null,
    add: AddFile = null,
    remove: Parquet4sRemoveFileWrapper = null,
    metaData: Metadata = null,
    protocol: Protocol = null,
    cdc: AddCDCFile = null,
    commitInfo: CommitInfo = null) extends Parquet4sWrapper[SingleAction] {

  override def unwrap: SingleAction = SingleAction(
    txn,
    add,
    remove match {
      case x: Parquet4sRemoveFileWrapper if x != null => x.unwrap
      case _ => null
    },
    metaData,
    protocol,
    cdc,
    commitInfo
  )
}
