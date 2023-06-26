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

package org.apache.spark.sql.delta

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.delta.actions.{CheckpointMetadata, SidecarFile, SingleAction}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames.{checkpointVersion, numCheckpointParts}
import org.apache.spark.sql.delta.util.JsonUtils
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonPropertyOrder}
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonNode}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.FileStatus

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
 * Information about the V2 Checkpoint in the LAST_CHECKPOINT file
 * @param path             file name corresponding to the uuid-named v2 checkpoint
 * @param sizeInBytes      size in bytes for the uuid-named v2 checkpoint
 * @param modificationTime modification time for the uuid-named v2 checkpoint
 * @param nonFileActions   all non file actions for the v2 checkpoint. This info may or may not be
 *                         available. A None value means that info is missing.
 *                         If it is not None, then it should have all the non-FileAction
 *                         corresponding to the checkpoint.
 * @param sidecarFiles     sidecar files corresponding to the v2 checkpoint. This info may or may
 *                         not be available. A None value means that this info is missing.
 *                         An empty list denotes that the v2 checkpoint has no sidecars.
 */
case class LastCheckpointV2(
    path: String,
    sizeInBytes: Long,
    modificationTime: Long,
    nonFileActions: Option[Seq[SingleAction]],
    sidecarFiles: Option[Seq[SidecarFile]]) {

  @JsonIgnore
  lazy val checkpointMetadataOpt: Option[CheckpointMetadata] =
    nonFileActions.flatMap(_.map(_.unwrap).collectFirst { case cm: CheckpointMetadata => cm })

}

object LastCheckpointV2 {
  def apply(
      fileStatus: FileStatus,
      nonFileActions: Option[Seq[SingleAction]] = None,
      sidecarFiles: Option[Seq[SidecarFile]] = None): LastCheckpointV2 = {
    LastCheckpointV2(
      path = fileStatus.getPath.getName,
      sizeInBytes = fileStatus.getLen,
      modificationTime = fileStatus.getModificationTime,
      nonFileActions = nonFileActions,
      sidecarFiles = sidecarFiles)
  }
}

/**
 * Records information about a checkpoint.
 *
 * This class provides the checksum validation logic, needed to ensure that content of
 * LAST_CHECKPOINT file points to a valid json. The readers might read some part from old file and
 * some part from the new file (if the file is read across multiple requests). In some rare
 * scenarios, the split read might produce a valid json and readers will be able to parse it and
 * convert it into a [[LastCheckpointInfo]] object that contains invalid data. In order to prevent
 * using it, we do a checksum match on the read json to validate that it is consistent.
 *
 * For old Delta versions, which do not have checksum logic, we want to make sure that the old
 * fields (i.e. version, size, parts) are together in the beginning of last_checkpoint json. All
 * these fields together are less than 50 bytes, so even in split read scenario, we want to make
 * sure that old delta readers which do not do have checksum validation logic, gets all 3 fields
 * from one read request. For this reason, we use `JsonPropertyOrder` to force them in the beginning
 * together.
 *
 * @param version the version of this checkpoint
 * @param size the number of actions in the checkpoint, -1 if the information is unavailable.
 * @param parts the number of parts when the checkpoint has multiple parts. None if this is a
 *              singular checkpoint
 * @param sizeInBytes the number of bytes of the checkpoint
 * @param numOfAddFiles the number of AddFile actions in the checkpoint
 * @param checkpointSchema the schema of the underlying checkpoint files
 * @param checksum the checksum of the [[LastCheckpointInfo]].
 */
@JsonPropertyOrder(Array("version", "size", "parts"))
case class LastCheckpointInfo(
    version: Long,
    size: Long,
    parts: Option[Int],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    sizeInBytes: Option[Long],
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    numOfAddFiles: Option[Long],
    checkpointSchema: Option[StructType],
    v2Checkpoint: Option[LastCheckpointV2] = None,
    checksum: Option[String] = None) {

  @JsonIgnore
  def getFormatEnum(): CheckpointInstance.Format = parts match {
    case _ if v2Checkpoint.nonEmpty => CheckpointInstance.Format.V2
    case Some(_) => CheckpointInstance.Format.WITH_PARTS
    case None => CheckpointInstance.Format.SINGLE
  }

  /** Whether two [[LastCheckpointInfo]] represents the same checkpoint */
  def semanticEquals(other: LastCheckpointInfo): Boolean = {
    CheckpointInstance(this) == CheckpointInstance(other)
  }
}

object LastCheckpointInfo {

  val STORED_CHECKSUM_KEY = "checksum"

  /** Whether to store checksum OR do checksum validations around [[LastCheckpointInfo]]  */
  def checksumEnabled(spark: SparkSession): Boolean =
    spark.sessionState.conf.getConf(DeltaSQLConf.LAST_CHECKPOINT_CHECKSUM_ENABLED)

  /**
   * Returns the json representation of this [[LastCheckpointInfo]] object.
   * Also adds the checksum to the returned json if `addChecksum` is set. The checksum can be
   * used by readers to validate consistency of the [[LastCheckpointInfo]].
   * It is calculated using rules mentioned in "JSON checksum" section in PROTOCOL.md.
   */
  def serializeToJson(
      lastCheckpointInfo: LastCheckpointInfo,
      addChecksum: Boolean,
      suppressOptionalFields: Boolean = false): String = {
    if (suppressOptionalFields) {
      return JsonUtils.toJson(
        LastCheckpointInfo(
          lastCheckpointInfo.version,
          lastCheckpointInfo.size,
          lastCheckpointInfo.parts,
          sizeInBytes = None,
          numOfAddFiles = None,
          v2Checkpoint = None,
          checkpointSchema = None))
    }

    val jsonStr: String = JsonUtils.toJson(lastCheckpointInfo.copy(checksum = None))
    if (!addChecksum) return jsonStr
    val rootNode = JsonUtils.mapper.readValue(jsonStr, classOf[ObjectNode])
    val checksum = treeNodeToChecksum(rootNode)
    rootNode.put(STORED_CHECKSUM_KEY, checksum).toString
  }

  /**
   * Converts the given `jsonStr` into a [[LastCheckpointInfo]] object.
   * if `validate` is set, then it also validates the consistency of the json:
   *  - calculating the checksum and comparing it with the `storedChecksum`.
   *  - json should not have any duplicates.
   */
  def deserializeFromJson(jsonStr: String, validate: Boolean): LastCheckpointInfo = {
    if (validate) {
      val (storedChecksumOpt, actualChecksum) = LastCheckpointInfo.getChecksums(jsonStr)
      storedChecksumOpt.filter(_ != actualChecksum).foreach { storedChecksum =>
        throw new IllegalStateException(s"Checksum validation failed for json: $jsonStr,\n" +
          s"storedChecksum:$storedChecksum, actualChecksum:$actualChecksum")
      }
    }

    // This means:
    // 1) EITHER: Checksum validation is config-disabled
    // 2) OR: The json lacked a checksum (e.g. written by old client). Nothing to validate.
    // 3) OR: The Stored checksum matches the calculated one. Validation succeeded.
    JsonUtils.fromJson[LastCheckpointInfo](jsonStr)
  }

  /**
   * Analyzes the json representation of [[LastCheckpointInfo]] and returns checksum tuple where
   * - first element refers to the stored checksum in the json representation of
   *   [[LastCheckpointInfo]], None if the checksum is not present.
   * - second element refers to the checksum computed from the canonicalized json representation of
   *   the [[LastCheckpointInfo]].
   */
  def getChecksums(jsonStr: String): (Option[String], String) = {
    val reader =
      JsonUtils.mapper.reader().withFeatures(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
    val rootNode = reader.readTree(jsonStr)
    val storedChecksum = if (rootNode.has(STORED_CHECKSUM_KEY)) {
      Some(rootNode.get(STORED_CHECKSUM_KEY).asText())
    } else {
      None
    }
    val actualChecksum = treeNodeToChecksum(rootNode)
    storedChecksum -> actualChecksum
  }

  /**
   * Canonicalizes the given `treeNode` json and returns its md5 checksum.
   * Refer to "JSON checksum" section in PROTOCOL.md for canonicalization steps.
   */
  def treeNodeToChecksum(treeNode: JsonNode): String = {
    val jsonEntriesBuffer = ArrayBuffer.empty[(String, String)]

    import scala.collection.JavaConverters._
    def traverseJsonNode(currentNode: JsonNode, prefix: ArrayBuffer[String]): Unit = {
      if (currentNode.isObject) {
        currentNode.fields().asScala.foreach { entry =>
          prefix.append(encodeString(entry.getKey))
          traverseJsonNode(entry.getValue, prefix)
          prefix.trimEnd(1)
        }
      } else if (currentNode.isArray) {
        currentNode.asScala.zipWithIndex.foreach { case (jsonNode, index) =>
          prefix.append(index.toString)
          traverseJsonNode(jsonNode, prefix)
          prefix.trimEnd(1)
        }
      } else {
        var nodeValue = currentNode.asText()
        if (currentNode.isTextual) nodeValue = encodeString(nodeValue)
        jsonEntriesBuffer.append(prefix.mkString("+") -> nodeValue)
      }
    }
    traverseJsonNode(treeNode, prefix = ArrayBuffer.empty)
    import Ordering.Implicits._
    val normalizedJsonKeyValues = jsonEntriesBuffer
      .filter { case (k, _) => k != s""""$STORED_CHECKSUM_KEY"""" }
      .map { case (k, v) => s"$k=$v" }
      .sortBy(_.toSeq: Seq[Char])
      .mkString(",")
    DigestUtils.md5Hex(normalizedJsonKeyValues)
  }

  private val isUnreservedOctet =
    (Set.empty ++ ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ "-._~").map(_.toByte)

  /**
   * URL encodes a String based on the following rules:
   * 1. Use uppercase hexadecimals for all percent encodings
   * 2. percent-encode everything other than unreserved characters
   * 3. unreserved characters are = a-z / A-Z / 0-9 / "-" / "." / "_" / "~"
   */
  private def encodeString(str: String): String = {
    val result = str.getBytes(java.nio.charset.StandardCharsets.UTF_8).map {
      case b if isUnreservedOctet(b) => b.toChar.toString
      case b =>
        // convert to char equivalent of unsigned byte
        val c = (b & 0xff)
        f"%%$c%02X"
    }.mkString
    s""""$result""""
  }

  def fromFiles(files: Seq[FileStatus]): LastCheckpointInfo = {
    assert(files.nonEmpty, "files should be non empty to construct LastCheckpointInfo")
    LastCheckpointInfo(
      version = checkpointVersion(files.head),
      size = -1L,
      parts = numCheckpointParts(files.head.getPath),
      sizeInBytes = Some(files.map(_.getLen).sum),
      numOfAddFiles = None,
      checkpointSchema = None
    )
  }
}
