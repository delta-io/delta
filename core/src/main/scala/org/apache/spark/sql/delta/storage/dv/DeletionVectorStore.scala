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

package org.apache.spark.sql.delta.storage.dv

import java.io.{Closeable, DataInputStream}
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import java.util.zip.CRC32

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.spark.sql.delta.actions.DeletionVectorDescriptor
import org.apache.spark.sql.delta.deletionvectors.{RoaringBitmapArray, StoredBitmap}
import org.apache.spark.sql.delta.util.PathWithFileSystem
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FSDataOutputStream, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

trait DeletionVectorStore extends Logging {
  /**
   * Read a Deletion Vector and parse it as [[RoaringBitmapArray]].
   */
  def read(
      dvDescriptor: DeletionVectorDescriptor,
      tablePath: Path): RoaringBitmapArray =
    StoredBitmap.create(dvDescriptor, tablePath).load(this)

  /**
   * Read Deletion Vector and parse it as [[RoaringBitmapArray]].
   */
  def read(path: Path, offset: Int, size: Int): RoaringBitmapArray

  /**
   * Returns a writer that can be used to write multiple deletion vectors to the file at `path`.
   */
  def createWriter(path: PathWithFileSystem): DeletionVectorStore.Writer

  /**
   * Returns full path for a DV with `filedId` UUID under `targetPath`.
   *
   * Optionally, prepend a `prefix` to the name.
   */
  def generateFileNameInTable(
      targetPath: PathWithFileSystem,
      fileId: UUID,
      prefix: String = ""): PathWithFileSystem = {
    DeletionVectorStore.assembleDeletionVectorPathWithFileSystem(targetPath, fileId, prefix)
  }

  /**
   * Return a new unique path under `targetPath`.
   *
   * Optionally, prepend a `prefix` to the name.
   */
  def generateUniqueNameInTable(
      targetPath: PathWithFileSystem,
      prefix: String = ""): PathWithFileSystem =
    generateFileNameInTable(targetPath, UUID.randomUUID(), prefix)

  /**
   * Creates a [[PathWithFileSystem]] instance
   * by using the configuration of this `DeletionVectorStore` instance
   */
  def pathWithFileSystem(path: Path): PathWithFileSystem
}

/**
 * Trait containing the utility and constants needed for [[DeletionVectorStore]]
 */
trait DeletionVectorStoreUtils {
  final val DV_FILE_FORMAT_VERSION_ID_V1: Byte = 1

  /** The length of a DV checksum. See [[calculateChecksum()]]. */
  final val CHECKSUM_LEN = 4
  /** The size of the stored length of a DV. */
  final val DATA_SIZE_LEN = 4

  // scalastyle:off pathfromuri
  /** Convert the given String path to a Hadoop Path, handing special characters properly. */
  def stringToPath(path: String): Path = new Path(new URI(path))
  // scalastyle:on pathfromuri

  /** Convert the given Hadoop path to a String Path, handing special characters properly. */
  def pathToString(path: Path): String = path.toUri.toString

  /**
   * Calculate checksum of a serialized deletion vector. We are using CRC32 which has 4bytes size,
   * but CRC32 implementation conforms to Java Checksum interface which requires a long. However,
   * the high-order bytes are zero, so here is safe to cast to Int. This will result in negative
   * checksums, but this is not a problem because we only care about equality.
   */
  def calculateChecksum(data: Array[Byte]): Int = {
    val crc = new CRC32()
    crc.update(data)
    crc.getValue.toInt
  }

  /**
   * Read a serialized deletion vector from a data stream.
   */
  def readRangeFromStream(reader: DataInputStream, size: Int): Array[Byte] = {
    val sizeAccordingToFile = reader.readInt()
    if (size != sizeAccordingToFile) {
      throw DeltaErrors.deletionVectorSizeMismatch()
    }

    val buffer = new Array[Byte](size)
    reader.readFully(buffer)

    val expectedChecksum = reader.readInt()
    val actualChecksum = calculateChecksum(buffer)
    if (expectedChecksum != actualChecksum) {
      throw DeltaErrors.deletionVectorChecksumMismatch()
    }

    buffer
  }

  /**
   * Same as `assembleDeletionVectorPath`, but keeps the new path bundled with the fs.
   */
  def assembleDeletionVectorPathWithFileSystem(
      targetParentPathWithFileSystem: PathWithFileSystem,
      id: UUID,
      prefix: String = ""): PathWithFileSystem = {
    targetParentPathWithFileSystem.copy(path =
      DeletionVectorDescriptor.assembleDeletionVectorPath(
        targetParentPathWithFileSystem.path, id, prefix))
  }

  /** Descriptor for a serialized Deletion Vector in a file. */
  case class DVRangeDescriptor(offset: Int, length: Int, checksum: Int)

  trait Writer extends Closeable {
    /**
     * Appends the serialized deletion vector in `data` to the file, and returns the offset in the
     * file that the deletion vector was written to and its checksum.
     */
    def write(data: Array[Byte]): DVRangeDescriptor

    /**
     * Returns UTF-8 encoded path of the file that is being written by this writer.
     */
    def serializedPath: Array[Byte]

    /**
     * Closes this writer. After calling this method it is no longer valid to call write (or close).
     * This method must always be called when the owner of this writer is done writing deletion
     * vectors.
     */
    def close(): Unit
  }
}

object DeletionVectorStore extends DeletionVectorStoreUtils {
  /** Create a new instance of [[DeletionVectorStore]] from the given Hadoop configuration. */
  private[delta] def createInstance(
      hadoopConf: Configuration): DeletionVectorStore =
    new HadoopFileSystemDVStore(hadoopConf)
}

/**
 * Default [[DeletionVectorStore]] implementation for Hadoop [[FileSystem]] implementations.
 *
 * Note: This class must be thread-safe,
 * because we sometimes write multiple deletion vectors in parallel through the same store.
 */
class HadoopFileSystemDVStore(hadoopConf: Configuration)
    extends DeletionVectorStore {

  override def read(path: Path, offset: Int, size: Int): RoaringBitmapArray = {
    val fs = path.getFileSystem(hadoopConf)
    val buffer = Utils.tryWithResource(fs.open(path)) { reader =>
      reader.seek(offset)
      DeletionVectorStore.readRangeFromStream(reader, size)
    }
    RoaringBitmapArray.readFrom(buffer)
  }

  override def createWriter(path: PathWithFileSystem): DeletionVectorStore.Writer = {
    new DeletionVectorStore.Writer {
      // Lazily create the writer for the deletion vectors, so that we don't write an empty file
      // in case all deletion vectors are empty.
      private var outputStream: FSDataOutputStream = _

      override def write(data: Array[Byte]): DeletionVectorStore.DVRangeDescriptor = {
        if (outputStream == null) {
          val overwrite = false // `create` Java API does not support named parameters
          outputStream = path.fs.create(path.path, overwrite)
          outputStream.writeByte(DeletionVectorStore.DV_FILE_FORMAT_VERSION_ID_V1)
        }
        val dvRange = DeletionVectorStore.DVRangeDescriptor(
          offset = outputStream.size(),
          length = data.length,
          checksum = DeletionVectorStore.calculateChecksum(data)
          )
        log.debug(s"Writing DV range to file: Path=${path.path}, Range=${dvRange}")
        outputStream.writeInt(data.length)
        outputStream.write(data)
        outputStream.writeInt(dvRange.checksum)
        dvRange
      }

      override val serializedPath: Array[Byte] =
        DeletionVectorStore.pathToString(path.path).getBytes(UTF_8)

      override def close(): Unit = {
        if (outputStream != null) {
          outputStream.close()
        }
      }
    }
  }

  override def pathWithFileSystem(path: Path): PathWithFileSystem =
    PathWithFileSystem.withConf(path, hadoopConf)
}
