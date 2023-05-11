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

package org.apache.spark.sql.delta.actions

import java.net.URI
import java.util.UUID

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.util.{Codec, DeltaEncoder, JsonUtils}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{Column, Encoder}
import org.apache.spark.sql.functions.{concat, lit, when}
import org.apache.spark.sql.types._

/** Information about a deletion vector attached to a file action. */
case class DeletionVectorDescriptor(
    /**
     * Indicates how the DV is stored.
     * Should be a single letter (see [[pathOrInlineDv]] below.)
     */
    storageType: String,

    /**
     * Contains the actual data that allows accessing the DV.
     *
     * Three options are currently supported:
     * - `storageType="u"` format: `<random prefix - optional><base85 encoded uuid>`
     *                            The deletion vector is stored in a file with a path relative to
     *                            the data directory of this Delta Table, and the file name can be
     *                            reconstructed from the UUID.
     *                            The encoded UUID is always exactly 20 characters, so the random
     *                            prefix length can be determined any characters exceeding 20.
     * - `storageType="i"` format: `<base85 encoded bytes>`
     *                            The deletion vector is stored inline in the log.
     * - `storageType="p"` format: `<absolute path>`
     *                             The DV is stored in a file with an absolute path given by this
     *                             url.
     */
    pathOrInlineDv: String,
    /**
     * Start of the data for this DV in number of bytes from the beginning of the file it is stored
     * in.
     *
     * Always None when storageType = "i".
     */
    @JsonDeserialize(contentAs = classOf[java.lang.Integer])
    offset: Option[Int] = None,
    /** Size of the serialized DV in bytes (raw data size, i.e. before base85 encoding). */
    sizeInBytes: Int,
    /** Number of rows the DV logically removes from the file. */
    cardinality: Long,
    /**
     * Transient property that is used to validate DV correctness.
     * It is not stored in the log.
     */
    @JsonDeserialize(contentAs = classOf[java.lang.Long])
    maxRowIndex: Option[Long] = None) {

  import DeletionVectorDescriptor._

  @JsonIgnore
  @transient
  lazy val uniqueId: String = {
    offset match {
      case Some(offset) => s"$uniqueFileId@$offset"
      case None => uniqueFileId
    }
  }

  @JsonIgnore
  @transient
  lazy val uniqueFileId: String = s"$storageType$pathOrInlineDv"

  @JsonIgnore
  protected[delta] def isOnDisk: Boolean = !isInline

  @JsonIgnore
  protected[delta] def isInline: Boolean = storageType == INLINE_DV_MARKER

  @JsonIgnore
  protected[delta] def isRelative: Boolean = storageType == UUID_DV_MARKER

  @JsonIgnore
  protected[delta] def isAbsolute: Boolean = storageType == PATH_DV_MARKER

  @JsonIgnore
  protected[delta] def isEmpty: Boolean = cardinality == 0

  def absolutePath(tableLocation: Path): Path = {
    require(isOnDisk, "Can't get a path for an inline deletion vector")
    storageType match {
      case UUID_DV_MARKER =>
        // If the file was written with a random prefix, we have to extract that,
        // before decoding the UUID.
        val randomPrefixLength = pathOrInlineDv.length - Codec.Base85Codec.ENCODED_UUID_LENGTH
        val (randomPrefix, encodedUuid) = pathOrInlineDv.splitAt(randomPrefixLength)
        val uuid = Codec.Base85Codec.decodeUUID(encodedUuid)
        assembleDeletionVectorPath(tableLocation, uuid, randomPrefix)
      case PATH_DV_MARKER =>
        // Since there is no need for legacy support for relative paths for DVs,
        // relative DVs should *always* use the UUID variant.
        val parsedUri = new URI(pathOrInlineDv)
        assert(parsedUri.isAbsolute, "Relative URIs are not supported for DVs")
        new Path(parsedUri)
      case _ => throw DeltaErrors.cannotReconstructPathFromURI(pathOrInlineDv)
    }
  }

  /**
   * Produce a copy of this DV, but using an absolute path.
   *
   * If the DV already has an absolute path or is inline, then this is just a normal copy.
   */
  def copyWithAbsolutePath(tableLocation: Path): DeletionVectorDescriptor = {
    storageType match {
      case UUID_DV_MARKER =>
        val absolutePath = this.absolutePath(tableLocation)
        this.copy(storageType = PATH_DV_MARKER, pathOrInlineDv = absolutePath.toString)
      case PATH_DV_MARKER | INLINE_DV_MARKER => this.copy()
    }
  }

  /**
   * Produce a copy of this DV, with `pathOrInlineDv` replaced by a relative path based on `id`
   * and `randomPrefix`.
   *
   * If the DV already has a relative path or is inline, then this is just a normal copy.
   */
  def copyWithNewRelativePath(id: UUID, randomPrefix: String): DeletionVectorDescriptor = {
    storageType match {
      case PATH_DV_MARKER =>
        this.copy(storageType = UUID_DV_MARKER, pathOrInlineDv = encodeUUID(id, randomPrefix))
      case UUID_DV_MARKER | INLINE_DV_MARKER => this.copy()
    }
  }

  @JsonIgnore
  def inlineData: Array[Byte] = {
    require(isInline, "Can't get data for an on-disk DV from the log.")
    // The sizeInBytes is used to remove any padding that might have been added during encoding.
    Codec.Base85Codec.decodeBytes(pathOrInlineDv, sizeInBytes)
  }

  /** Returns the estimated number of bytes required to serialize this object. */
  @JsonIgnore
  protected[delta] lazy val estimatedSerializedSize: Int = {
    // (cardinality(8) + sizeInBytes(4)) + storageType + pathOrInlineDv + option[offset(4)]
    12 + storageType.length + pathOrInlineDv.length + (if (offset.isDefined) 4 else 0)
  }
}

object DeletionVectorDescriptor {

  /** String that is used in all file names generated by deletion vector store */
  val DELETION_VECTOR_FILE_NAME_CORE = "deletion_vector"

  // Markers to separate different kinds of DV storage.
  final val PATH_DV_MARKER: String = "p"
  final val INLINE_DV_MARKER: String = "i"
  final val UUID_DV_MARKER: String = "u"

  final val STRUCT_TYPE: StructType = new StructType()
    .add("storageType", StringType)
    .add("pathOrInlineDv", StringType)
    .add("offset", IntegerType)
    .add("sizeInBytes", IntegerType, nullable = false)
    .add("cardinality", LongType, nullable = false)

  private lazy val _encoder = new DeltaEncoder[DeletionVectorDescriptor]
  implicit def encoder: Encoder[DeletionVectorDescriptor] = _encoder.get

  /** Utility method to create an on-disk [[DeletionVectorDescriptor]] */
  def onDiskWithRelativePath(
      id: UUID,
      randomPrefix: String = "",
      sizeInBytes: Int,
      cardinality: Long,
      offset: Option[Int] = None,
      maxRowIndex: Option[Long] = None): DeletionVectorDescriptor =
    DeletionVectorDescriptor(
      storageType = UUID_DV_MARKER,
      pathOrInlineDv = encodeUUID(id, randomPrefix),
      offset = offset,
      sizeInBytes = sizeInBytes,
      cardinality = cardinality,
      maxRowIndex = maxRowIndex)

  /** Utility method to create an on-disk [[DeletionVectorDescriptor]] */
  def onDiskWithAbsolutePath(
      path: String,
      sizeInBytes: Int,
      cardinality: Long,
      offset: Option[Int] = None,
      maxRowIndex: Option[Long] = None): DeletionVectorDescriptor =
    DeletionVectorDescriptor(
      storageType = PATH_DV_MARKER,
      pathOrInlineDv = path,
      offset = offset,
      sizeInBytes = sizeInBytes,
      cardinality = cardinality,
      maxRowIndex = maxRowIndex)

  /** Utility method to create an inline [[DeletionVectorDescriptor]] */
  def inlineInLog(
      data: Array[Byte],
      cardinality: Long): DeletionVectorDescriptor =
    DeletionVectorDescriptor(
      storageType = INLINE_DV_MARKER,
      pathOrInlineDv = encodeData(data),
      sizeInBytes = data.length,
      cardinality = cardinality)

  /**
   * This produces the same output as [[DeletionVectorDescriptor.uniqueId]] but as a column
   * expression, so it can be used directly in a Spark query.
   */
  def uniqueIdExpression(deletionVectorCol: Column): Column = {
    when(deletionVectorCol("offset").isNotNull,
        concat(
          deletionVectorCol("storageType"),
          deletionVectorCol("pathOrInlineDv"),
          lit('@'),
          deletionVectorCol("offset")))
      .otherwise(concat(
        deletionVectorCol("storageType"),
        deletionVectorCol("pathOrInlineDv")))
  }

  /**
   * Return the unique path under `parentPath` that is based on `id`.
   *
   * Optionally, prepend a `prefix` to the name.
   */
  def assembleDeletionVectorPath(targetParentPath: Path, id: UUID, prefix: String = ""): Path = {
    val fileName = s"${DELETION_VECTOR_FILE_NAME_CORE}_${id}.bin"
    if (prefix.nonEmpty) {
      new Path(new Path(targetParentPath, prefix), fileName)
    } else {
      new Path(targetParentPath, fileName)
    }
  }

  /** Descriptor for an empty stored bitmap. */
  val EMPTY: DeletionVectorDescriptor = DeletionVectorDescriptor(
    storageType = INLINE_DV_MARKER,
    pathOrInlineDv = "",
    sizeInBytes = 0,
    cardinality = 0)

  private[delta] def fromJson(jsonString: String): DeletionVectorDescriptor = {
    JsonUtils.fromJson[DeletionVectorDescriptor](jsonString)
  }

  private[delta] def encodeUUID(id: UUID, randomPrefix: String): String = {
    val uuidData = Codec.Base85Codec.encodeUUID(id)
    // This should always be true and we are relying on it for separating out the
    // prefix again later without having to spend an extra character as a separator.
    assert(uuidData.length == 20)
    s"$randomPrefix$uuidData"
  }

  def encodeData(bytes: Array[Byte]): String = Codec.Base85Codec.encodeBytes(bytes)
}
