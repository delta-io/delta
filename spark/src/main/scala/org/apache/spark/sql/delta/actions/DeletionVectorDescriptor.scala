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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.URI
import java.util.{Base64, UUID}

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.DeltaUDF
import org.apache.spark.sql.delta.amt.AMTUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.{Codec, DeltaEncoder}
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.hadoop.fs.Path

import org.apache.spark.paths.SparkPath
import org.apache.spark.sql.{Column, Encoder}
import org.apache.spark.sql.functions.{concat, lit, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

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
     * Four options are currently supported:
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
     *                             url. Special characters in this path must be escaped.
     * - `storageType="r"` format: `<relative path>`
     *                             The DV is stored in a file at this path, relative to the root of
     *                             this Delta Table. Unlike `u`, the file name is arbitrary rather
     *                             than derived from a UUID; unlike `p`, the path is neither
     *                             absolute nor URL-encoded. Only permitted when the
     *                             `adaptiveMetadata` table feature is enabled.
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

  /**
   * The legacy unique id of the DV.
   * Prefer [[uniqueId]] if possible. See more details in the [[uniqueId]] documentation.
   */
  @JsonIgnore
  @transient
  lazy val legacyUniqueId: String = {
    offset match {
      case Some(offset) => s"$uniqueFileId@$offset"
      case None => uniqueFileId
    }
  }

  @JsonIgnore
  @transient
  lazy val uniqueFileId: String = s"$storageType$pathOrInlineDv"

  /**
   * Unique identifier for this DV.
   *
   * When `useObjectIdentity` is false, this returns the legacy descriptor identity:
   * `storageType + pathOrInlineDv + optional offset`. That identity is stable for one serialized
   * descriptor, but it treats equivalent `u`, `r`, and in-table `p` descriptors as different.
   *
   * When `useObjectIdentity` is true, this returns a normalized object identity. That identity
   * represents the physical DV object relative to the table root when possible, plus the offset, so
   * equivalent `u`, `r`, and in-table `p` descriptors compare equal.
   */
  def uniqueId(tableRoot: Path, useObjectIdentity: Boolean): String = {
    if (useObjectIdentity) normalizedTableRelativeObjectId(tableRoot) else legacyUniqueId
  }

  @JsonIgnore
  protected[delta] def isOnDisk: Boolean = !isInline

  @JsonIgnore
  protected[delta] def isInline: Boolean = storageType == INLINE_DV_MARKER

  @JsonIgnore
  protected[delta] def isUuidRelative: Boolean = storageType == UUID_DV_MARKER

  @JsonIgnore
  protected[delta] def isUnencodedRelative: Boolean = storageType == RELATIVE_DV_MARKER

  @JsonIgnore
  protected[delta] def isAbsolute: Boolean = storageType == PATH_DV_MARKER

  @JsonIgnore
  protected[delta] def isEmpty: Boolean = cardinality == 0

  def absolutePath(tableLocation: Path): Path = {
    require(isOnDisk, "Can't get a path for an inline deletion vector")
    storageType match {
      case UUID_DV_MARKER =>
        val (randomPrefix, uuid) = getRandomPrefixAndUuid.get
        assembleDeletionVectorPath(tableLocation, uuid, randomPrefix)
      case PATH_DV_MARKER =>
        val parsedUri = new URI(pathOrInlineDv)
        assert(parsedUri.isAbsolute, "Relative URIs are not supported for DVs")
        new Path(parsedUri)
      case RELATIVE_DV_MARKER =>
        val path = new Path(pathOrInlineDv)
        new Path(tableLocation, path)
      case _ => throw DeltaErrors.cannotReconstructPathFromURI(pathOrInlineDv)
    }
  }

  /** Returns the url encoded absolute path of the deletion vector. */
  def urlEncodedPath(tablePath: Path): String =
    SparkPath.fromPath(absolutePath(tablePath)).urlEncoded

  /**
   * Returns the url encoded relative path of the deletion vector if possible.
   * If the DV path is outside the table directory, returns None.
   */
  def urlEncodedRelativePathIfExists(tablePath: Path): Option[String] = {
    if (isUuidRelative) {
      return Some(SparkPath.fromPath(absolutePath(new Path("."))).urlEncoded)
    }

    // DV path is not relative. Attempt to relativize it.
    val basePathUri = tablePath.toUri
    val absolutePathUri = absolutePath(tablePath).toUri
    val relativePath = basePathUri.relativize(absolutePathUri)
    if (!relativePath.isAbsolute) {
      Some(SparkPath.fromUri(relativePath).urlEncoded)
    } else {
      None
    }
  }

  /**
   * Parse the prefix and UUID of a u DV. Returns None if the DV is not of type u.
   */
  @JsonIgnore
  def getRandomPrefixAndUuid: Option[(String, UUID)] = storageType match {
    case UUID_DV_MARKER =>
      // If the file was written with a random prefix, we have to extract that,
      // before decoding the UUID.
      val randomPrefixLength = pathOrInlineDv.length - Codec.Base85Codec.ENCODED_UUID_LENGTH
      val (randomPrefix, encodedUuid) = pathOrInlineDv.splitAt(randomPrefixLength)
      Some((randomPrefix, Codec.Base85Codec.decodeUUID(encodedUuid)))
    case _ =>
      None
  }

  /**
   * Computes a normalized object identity for this descriptor. Use this when the caller wants
   * to identify the underlying DV object rather than this descriptor's storage encoding.
   */
  def normalizedTableRelativeObjectId(tableRoot: Path): String = {
    storageType match {
      case INLINE_DV_MARKER =>
        formatIdentity(INLINE_DV_MARKER, pathOrInlineDv, offset)
      case UUID_DV_MARKER =>
        val (randomPrefix, uuid) = getRandomPrefixAndUuid.get
        val fileName = assembleDeletionVectorFileName(uuid)
        val relativePath = if (randomPrefix.isEmpty) fileName else s"$randomPrefix/$fileName"
        formatIdentity(RELATIVE_DV_MARKER, relativePath, offset)
      case RELATIVE_DV_MARKER =>
        formatIdentity(RELATIVE_DV_MARKER, pathOrInlineDv, offset)
      case PATH_DV_MARKER =>
        val path = SparkPath.fromUrlString(pathOrInlineDv).toPath.toString
        val relativePath = AMTUtils.relativizeLocation(tableRoot.toString, path)
        if (AMTUtils.isAbsoluteLocation(relativePath)) {
          formatIdentity(PATH_DV_MARKER, pathOrInlineDv, offset)
        } else {
          formatIdentity(RELATIVE_DV_MARKER, relativePath, offset)
        }
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported deletion vector storage type: $storageType")
    }
  }

  /**
   * Produce a copy of this DV, but using an absolute path.
   *
   * If the DV already has an absolute path or is inline, then this is just a normal copy.
   */
  def copyWithAbsolutePath(tableLocation: Path): DeletionVectorDescriptor = {
    storageType match {
      case UUID_DV_MARKER | RELATIVE_DV_MARKER =>
        this.copy(
          storageType = PATH_DV_MARKER,
          pathOrInlineDv = urlEncodedPath(tableLocation))
      case PATH_DV_MARKER | INLINE_DV_MARKER => this.copy()
    }
  }

  /**
   * Produce a copy of this DV, with `pathOrInlineDv` replaced by a relative path based on `id`
   * and `randomPrefix`.
   *
   * If the DV already has a relative path or is inline, then this is just a normal copy.
   */
  def copyWithNewUuidRelativePath(id: UUID, randomPrefix: String): DeletionVectorDescriptor = {
    storageType match {
      case PATH_DV_MARKER =>
        this.copy(storageType = UUID_DV_MARKER, pathOrInlineDv = encodeUUID(id, randomPrefix))
      case UUID_DV_MARKER | RELATIVE_DV_MARKER | INLINE_DV_MARKER => this.copy()
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

  /*
   * Serialize the DV descriptor to a base64 encoded string.
   */
  def serializeToBase64(): String = {
    val bs = new ByteArrayOutputStream()
    val ds = new DataOutputStream(bs)
    try {
      ds.writeLong(cardinality)
      ds.writeInt(sizeInBytes)

      val storageTypeBytes = storageType.getBytes()
      assert(storageTypeBytes.length == 1, s"Storage type must be 1byte value: $storageType")
      assert(storageTypeBytes.head.toChar.isLower,
        s"Storage type must be lowercase: $storageType")

      if (storageType != INLINE_DV_MARKER) {
        offset match {
          case Some(o) =>
            // Lowercase marker indicates offset follows.
            ds.writeByte(storageTypeBytes.head)
            ds.writeInt(o)
          case None =>
            // Uppercase marker indicates no offset bytes follow.
            ds.writeByte(storageTypeBytes.head.toChar.toUpper.toByte)
        }
      } else {
        assert(offset.isEmpty)
        ds.writeByte(storageTypeBytes.head)
      }

      ds.writeUTF(pathOrInlineDv)
      Base64.getEncoder.encodeToString(bs.toByteArray)
    } finally {
      ds.close()
    }
  }
}

object DeletionVectorDescriptor {

  /** Prefix that is used in all file names generated by deletion vector store. */
  val DELETION_VECTOR_FILE_NAME_PREFIX = SQLConf.get.getConf(DeltaSQLConf.TEST_DV_NAME_PREFIX)

  /** String that is used in all file names generated by deletion vector store */
  val DELETION_VECTOR_FILE_NAME_CORE = DELETION_VECTOR_FILE_NAME_PREFIX + "deletion_vector"


  // Markers to separate different kinds of DV storage.
  final val PATH_DV_MARKER: String = "p"
  final val INLINE_DV_MARKER: String = "i"
  final val UUID_DV_MARKER: String = "u"
  final val RELATIVE_DV_MARKER: String = "r"

  private def formatIdentity(
      storageType: String,
      pathOrInlineDv: String,
      offset: Option[Int]): String = {
    offset match {
      case Some(offsetValue) => s"$storageType$pathOrInlineDv@$offsetValue"
      case None => s"$storageType$pathOrInlineDv"
    }
  }

  private final val deletionVectorFileNameRegex =
    raw"${new Path(DELETION_VECTOR_FILE_NAME_CORE).toUri}_([^.]+)\.bin".r
  private final val deletionVectorFileNamePattern = deletionVectorFileNameRegex.pattern

  final lazy val STRUCT_TYPE: StructType =
    Action.addFileSchema("deletionVector").dataType.asInstanceOf[StructType]

  private lazy val _encoder = new DeltaEncoder[DeletionVectorDescriptor]
  implicit def encoder: Encoder[DeletionVectorDescriptor] = _encoder.get

  /** Utility method to create an on-disk [[DeletionVectorDescriptor]] */
  def onDiskWithUuidRelativePath(
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

  /**
   * Utility method to create a [[DeletionVectorDescriptor]] for an unencoded path relative to the
   * table root. Callers are responsible for relativizing the path before invoking this method.
   */
  def createRelativePathDVDescriptor(
      relativePath: String,
      sizeInBytes: Int,
      cardinality: Long,
      offset: Option[Int] = None,
      maxRowIndex: Option[Long] = None): DeletionVectorDescriptor = {
    if (Utils.isTesting) {
      require(!AMTUtils.isAbsoluteLocation(relativePath),
        s"A '$RELATIVE_DV_MARKER' deletion vector must have a relative path, " +
          s"but got: $relativePath")
    }
    DeletionVectorDescriptor(
      storageType = RELATIVE_DV_MARKER,
      pathOrInlineDv = relativePath,
      offset = offset,
      sizeInBytes = sizeInBytes,
      cardinality = cardinality,
      maxRowIndex = maxRowIndex)
  }

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
   * Returns whether the path points to a deletion vector file.
   * Note, external writers are no enforced to create DV files with the same naming convertions.
   * This function is intended for testing. */
  private[delta] def isDeletionVectorPath(path: Path): Boolean =
    deletionVectorFileNamePattern.matcher(path.getName).matches()

  /** Only for testing. */
  private[delta] def isDeletionVectorPath(path: String): Boolean =
    isDeletionVectorPath(new Path(path))

  /** Same as above but as a column expression. Only for testing. */
  private[delta] def isDeletionVectorPath(pathCol: Column): Column =
    DeltaUDF.booleanFromString(isDeletionVectorPath)(pathCol)

  /** Returns a boolean column that corresponds to whether each deletion vector is inline. */
  def isInline(dv: Column): Column =
    DeltaUDF.booleanFromDeletionVectorDescriptor(_.isInline)(dv)

  /**
   * Returns a column with the url encoded deletion vector paths.
   *
   * WARNING: It throws an exception if it encounters any inline DVs. The caller is responsible
   * for handling these separately.
   */
  def urlEncodedPath(deletionVectorCol: Column, tablePath: Path): Column =
    DeltaUDF.stringFromDeletionVectorDescriptor(_.urlEncodedPath(tablePath))(deletionVectorCol)

  /**
   * Returns a column with the url encoded deletion vector relative paths. For paths that cannot
   * be relativized, it returns None.
   *
   * WARNING: It throws an exception if it encounters any inline DVs. The caller is responsible
   * for handling these separately.
   */
  def urlEncodedRelativePathIfExists(deletionVectorCol: Column, tablePath: Path): Column =
    DeltaUDF.stringOptionFromDeletionVectorDescriptor(
      _.urlEncodedRelativePathIfExists(tablePath))(deletionVectorCol)

  /**
   * This produces the same output as [[DeletionVectorDescriptor.legacyUniqueId]] but as a
   * column expression, so it can be used directly in a Spark query.
   */
  def legacyUniqueIdExpression(deletionVectorCol: Column): Column = {
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
   * Produces the same output as [[DeletionVectorDescriptor.uniqueId]] with object identity, but as
   * a column expression, so it can be used directly in a Spark query.
   */
  def objectUniqueIdExpression(deletionVectorCol: Column, tableRoot: Path): Column = {
    val objectUniqueId = DeltaUDF.stringFromDeletionVectorDescriptor(
      _.uniqueId(tableRoot, useObjectIdentity = true))
    when(deletionVectorCol.isNotNull, objectUniqueId(deletionVectorCol))
  }

  /**
   * Produces the same output as [[DeletionVectorDescriptor.uniqueId]] but as a column expression,
   * so it can be used directly in a Spark query.
   */
  def uniqueIdExpression(
      deletionVectorCol: Column,
      tableRoot: Path,
      useObjectIdentity: Boolean): Column = {
    if (useObjectIdentity) {
      objectUniqueIdExpression(deletionVectorCol, tableRoot)
    } else {
      legacyUniqueIdExpression(deletionVectorCol)
    }
  }

  /**
   * Return the unique path under `parentPath` that is based on `id`.
   *
   * Optionally, prepend a `prefix` to the name.
   */
  def assembleDeletionVectorPath(targetParentPath: Path, id: UUID, prefix: String = ""): Path = {
    val fileName = assembleDeletionVectorFileName(id)
    if (prefix.nonEmpty) {
      new Path(new Path(targetParentPath, prefix), fileName)
    } else {
      new Path(targetParentPath, fileName)
    }
  }

  /**
   * Return the unique file name for a deletion vector based on `id`.
   */
  def assembleDeletionVectorFileName(id: UUID): String =
    s"${DELETION_VECTOR_FILE_NAME_CORE}_${id}.bin"

  /**
   * Parse the UUID from a deletion vector file name. This throws an IllegalArgumentException
   * if the file name does not contain a valid UUID.
   */
  def getUUIDFromDeletionVectorFileName(fileName: String): UUID = {
    val uuidString = fileName.stripPrefix(s"${DELETION_VECTOR_FILE_NAME_CORE}_").stripSuffix(".bin")
    UUID.fromString(uuidString)
  }

  /** Descriptor for an empty stored bitmap. */
  val EMPTY: DeletionVectorDescriptor = DeletionVectorDescriptor(
    storageType = INLINE_DV_MARKER,
    pathOrInlineDv = "",
    sizeInBytes = 0,
    cardinality = 0)

  private[delta] def encodeUUID(id: UUID, randomPrefix: String): String = {
    val uuidData = Codec.Base85Codec.encodeUUID(id)
    // This should always be true and we are relying on it for separating out the
    // prefix again later without having to spend an extra character as a separator.
    assert(uuidData.length == 20)
    s"$randomPrefix$uuidData"
  }

  def encodeData(bytes: Array[Byte]): String = Codec.Base85Codec.encodeBytes(bytes)

  /*
   * Deserialize the base64 encoded string to a DV descriptor.
   *
   * The format must be in sync with [[DeletionVectorDescriptor.serializeToBase64]].
   */
  def deserializeFromBase64(encoded: String): DeletionVectorDescriptor = {
    val buffer = Base64.getDecoder.decode(encoded)
    val ds = new DataInputStream(new ByteArrayInputStream(buffer))
    try {
      val cardinality = ds.readLong()
      val sizeInBytes = ds.readInt()
      val serializedStorageType = ds.readByte().toChar
      // Lowercase on-disk markers (u, p) are followed by an offset int.
      // Uppercase on-disk markers (U, P) indicate no offset is present.
      // Inline marker (i) never has an offset.
      val (storageType, offset) = serializedStorageType match {
        case c if c == INLINE_DV_MARKER.head => (INLINE_DV_MARKER, None)
        case c if c.isUpper =>
          (c.toLower.toString, None)
        case c =>
          (c.toString, Some(ds.readInt()))
      }
      val pathOrInlineDv = ds.readUTF()
      DeletionVectorDescriptor(storageType, pathOrInlineDv, offset, sizeInBytes, cardinality)
    } finally {
      ds.close()
    }
  }
}
