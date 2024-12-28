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

package org.apache.spark.sql.delta.commands.convert

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.{Integer => JInt, Long => JLong}
import java.nio.ByteBuffer
import java.util.{List => JList, Map => JMap}
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import org.apache.iceberg.{DataFile, FileContent, FileFormat, ManifestContent, ManifestFile, PartitionData, StructLike}
import org.apache.iceberg.ManifestFile.PartitionFieldSummary

/**
 * The classes in this file are wrappers of Iceberg classes
 * in the format of case classes so they can be serialized by
 * Spark automatically.
 */

case class ManifestFileWrapper(
    path: String,
    length: Long,
    partitionSpecId: Int,
    sequenceNumber: Long,
    minSequenceNumber: Long,
    snapshotId: JLong,
    addedFilesCount: JInt,
    addedRowsCount: JLong,
    existingFilesCount: JInt,
    existingRowsCount: JLong,
    deletedFilesCount: JInt,
    deletedRowsCount: JLong,
    _partitions: Seq[PartitionFieldSummaryWrapper])
  extends ManifestFile {

  def this(manifest: ManifestFile) =
    this(
      manifest.path,
      manifest.length,
      manifest.partitionSpecId,
      manifest.sequenceNumber,
      manifest.minSequenceNumber,
      manifest.snapshotId,
      manifest.addedFilesCount,
      manifest.addedRowsCount,
      manifest.existingFilesCount,
      manifest.existingRowsCount,
      manifest.deletedFilesCount,
      manifest.deletedRowsCount,
      manifest.partitions.asScala.map(new PartitionFieldSummaryWrapper(_)).toSeq
    )
  override def content: ManifestContent = ManifestContent.DATA
  override def partitions: JList[PartitionFieldSummary] =
    _partitions.asJava.asInstanceOf[JList[PartitionFieldSummary]]
  override def copy: ManifestFile = this.copy
}

case class PartitionFieldSummaryWrapper(
    containsNull: Boolean,
    _lowerBound: ByteBufferWrapper,
    _upperBound: ByteBufferWrapper) extends PartitionFieldSummary {

  def this(src: PartitionFieldSummary) =
    this(
      src.containsNull,
      IcebergSparkWrappers.serializeByteBuffer(src.lowerBound),
      IcebergSparkWrappers.serializeByteBuffer(src.upperBound)
    )
  override def lowerBound: ByteBuffer = IcebergSparkWrappers.deserializeByteBuffer(_lowerBound)
  override def upperBound: ByteBuffer = IcebergSparkWrappers.deserializeByteBuffer(_upperBound)
  override def copy: PartitionFieldSummary = this.copy
}

case class DataFileWrapper(
    pos: JLong,
    specId: Int,
    path: String,
    recordCount: Long,
    fileSizeInBytes: Long,
    _partition: Array[Byte],
    _columnSizes: Map[JInt, JLong],
    _valueCounts: Map[JInt, JLong],
    _nullValueCounts: Map[JInt, JLong],
    _nanValueCounts: Map[JInt, JLong],
    _lowerBounds: Map[JInt, ByteBufferWrapper],
    _upperBounds: Map[JInt, ByteBufferWrapper],
    _keyMetadata: ByteBufferWrapper,
    _splitOffsets: Seq[JLong]) extends DataFile {

  def this(df: DataFile) = {
    this(
      df.pos,
      df.specId,
      df.path.toString,
      df.recordCount,
      df.fileSizeInBytes,
      IcebergSparkWrappers.serialize(df.partition.asInstanceOf[java.io.Serializable]),
      df.columnSizes.asScala.toMap,
      df.valueCounts.asScala.toMap,
      df.nullValueCounts.asScala.toMap,
      df.nanValueCounts.asScala.toMap,
      IcebergSparkWrappers.serializeMap(df.lowerBounds),
      IcebergSparkWrappers.serializeMap(df.upperBounds),
      IcebergSparkWrappers.serializeByteBuffer(df.keyMetadata),
      df.splitOffsets.asScala.toSeq)
    require(df.content == FileContent.DATA)
    require(df.format == FileFormat.PARQUET)
  }

  override def content: FileContent = FileContent.DATA
  override def format: FileFormat = FileFormat.PARQUET
  override def partition: StructLike =
    IcebergSparkWrappers.deserialize[PartitionData](this._partition)
  override def columnSizes: JMap[JInt, JLong] = _columnSizes.asJava
  override def valueCounts: JMap[JInt, JLong] = _valueCounts.asJava
  override def nullValueCounts: JMap[JInt, JLong] = _nullValueCounts.asJava
  override def nanValueCounts: JMap[JInt, JLong] = _nanValueCounts.asJava
  override def lowerBounds: JMap[JInt, ByteBuffer] =
    IcebergSparkWrappers.deserializeMap(_lowerBounds)
  override def upperBounds: JMap[JInt, ByteBuffer] =
    IcebergSparkWrappers.deserializeMap(_upperBounds)
  override def keyMetadata: ByteBuffer = IcebergSparkWrappers.deserializeByteBuffer(_keyMetadata)
  override def splitOffsets: JList[JLong] = _splitOffsets.asJava
  override def copy: DataFile = this.copy
  override def copyWithoutStats: DataFile = this.copy
}

case class ByteBufferWrapper(bytes: Array[Byte], isNull: Boolean)
object IcebergSparkWrappers {
  def serialize(obj: java.io.Serializable): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(baos)
    try {
      oos.writeObject(obj)
      oos.flush()
      baos.toByteArray
    } finally {
      oos.close()
      baos.close()
    }
  }
  def deserialize[T](bytes: Array[Byte]): T = {
    val bais = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bais)
    try {
      ois.readObject().asInstanceOf[T] // Cast to the expected type
    } finally {
      ois.close()
      bais.close()
    }
  }
  def serializeByteBuffer(byteBuffer: ByteBuffer): ByteBufferWrapper = {
    if (byteBuffer == null) {
      ByteBufferWrapper(Array.emptyByteArray, isNull = true)
    } else {
      ByteBufferWrapper(byteBuffer.array(), isNull = false)
    }
  }
  def deserializeByteBuffer(byteBufferWrapper: ByteBufferWrapper): ByteBuffer = {
    if (byteBufferWrapper.isNull) {
      null
    } else {
      ByteBuffer.wrap(byteBufferWrapper.bytes)
    }
  }
  def serializeMap[T](map: JMap[T, ByteBuffer]): Map[T, ByteBufferWrapper] =
    map.asScala.mapValues(serializeByteBuffer).toMap
  def deserializeMap[T](map: Map[T, ByteBufferWrapper]): JMap[T, ByteBuffer] =
    map.mapValues(deserializeByteBuffer).toMap.asJava
}
