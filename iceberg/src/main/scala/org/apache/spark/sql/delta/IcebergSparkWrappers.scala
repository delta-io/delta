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
    _lowerBound: Option[Array[Byte]],
    _upperBound: Option[Array[Byte]]) extends PartitionFieldSummary {

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
    _columnSizes: Option[Map[JInt, JLong]],
    _valueCounts: Option[Map[JInt, JLong]],
    _nullValueCounts: Option[Map[JInt, JLong]],
    _nanValueCounts: Option[Map[JInt, JLong]],
    _lowerBounds: Option[Map[JInt, Option[Array[Byte]]]],
    _upperBounds: Option[Map[JInt, Option[Array[Byte]]]],
    _keyMetadata: Option[Array[Byte]],
    _splitOffsets: Option[Seq[JLong]]) extends DataFile {

  def this(df: DataFile) = {
    this(
      df.pos,
      df.specId,
      df.path.toString,
      df.recordCount,
      df.fileSizeInBytes,
      IcebergSparkWrappers.serialize(df.partition.asInstanceOf[java.io.Serializable]),
      Option(df.columnSizes).map(_.asScala.toMap),
      Option(df.valueCounts).map(_.asScala.toMap),
      Option(df.nullValueCounts).map(_.asScala.toMap),
      Option(df.nanValueCounts).map(_.asScala.toMap),
      IcebergSparkWrappers.serializeMap(df.lowerBounds),
      IcebergSparkWrappers.serializeMap(df.upperBounds),
      IcebergSparkWrappers.serializeByteBuffer(df.keyMetadata),
      Option(df.splitOffsets).map(_.asScala.toSeq))
    require(df.content == FileContent.DATA)
    require(df.format == FileFormat.PARQUET)
  }

  override def content: FileContent = FileContent.DATA
  override def format: FileFormat = FileFormat.PARQUET
  override def partition: StructLike =
    IcebergSparkWrappers.deserialize[PartitionData](this._partition)
  override def columnSizes: JMap[JInt, JLong] = _columnSizes.map(_.asJava).orNull
  override def valueCounts: JMap[JInt, JLong] = _valueCounts.map(_.asJava).orNull
  override def nullValueCounts: JMap[JInt, JLong] = _nullValueCounts.map(_.asJava).orNull
  override def nanValueCounts: JMap[JInt, JLong] = _nanValueCounts.map(_.asJava).orNull
  override def lowerBounds: JMap[JInt, ByteBuffer] =
    IcebergSparkWrappers.deserializeMap(_lowerBounds)
  override def upperBounds: JMap[JInt, ByteBuffer] =
    IcebergSparkWrappers.deserializeMap(_upperBounds)
  override def keyMetadata: ByteBuffer = IcebergSparkWrappers.deserializeByteBuffer(_keyMetadata)
  override def splitOffsets: JList[JLong] = _splitOffsets.map(_.asJava).orNull
  override def copy: DataFile = this.copy
  override def copyWithoutStats: DataFile = this.copy
}

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
  def serializeByteBuffer(byteBuffer: ByteBuffer): Option[Array[Byte]] = {
    Option(byteBuffer).map(_.array())
  }
  def deserializeByteBuffer(byteBufferWrapper: Option[Array[Byte]]): ByteBuffer = {
    byteBufferWrapper.map(ByteBuffer.wrap).orNull
  }
  def serializeMap[T](map: JMap[T, ByteBuffer]): Option[Map[T, Option[Array[Byte]]]] =
    Option(map).map(_.asScala.mapValues(serializeByteBuffer).toMap)
  def deserializeMap[T](map: Option[Map[T, Option[Array[Byte]]]]): JMap[T, ByteBuffer] =
    map.map(_.mapValues(deserializeByteBuffer).toMap.asJava).orNull
}
