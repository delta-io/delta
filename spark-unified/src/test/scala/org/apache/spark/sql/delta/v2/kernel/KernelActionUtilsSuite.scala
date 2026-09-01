/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.kernel

import java.util.Optional

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.actions.AddFile
import io.delta.kernel.{CommitActions => KernelCommitActions}
import io.delta.kernel.data.{ColumnarBatch => KernelColumnarBatch}
import io.delta.kernel.data.{ColumnVector => KernelColumnVector}
import io.delta.kernel.data.{Row => KernelRow}
import io.delta.kernel.data.MapValue
import io.delta.kernel.internal.DeltaLogActionUtils.{DeltaAction => KernelDeltaAction}
import io.delta.kernel.internal.actions.{AddFile => KernelAddFile}
import io.delta.kernel.internal.actions.{CommitInfo => KernelCommitInfo}
import io.delta.kernel.internal.actions.{
  DeletionVectorDescriptor => KernelDeletionVectorDescriptor
}
import io.delta.kernel.internal.actions.{Format => KernelFormat}
import io.delta.kernel.internal.actions.{Metadata => KernelMetadata}
import io.delta.kernel.internal.actions.{Protocol => KernelProtocol}
import io.delta.kernel.internal.actions.{RemoveFile => KernelRemoveFile}
import io.delta.kernel.internal.data.GenericColumnVector
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.util.{Utils => KernelUtils}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.types.{StringType, StructType}
import io.delta.kernel.utils.{CloseableIterator => KernelCloseableIterator}

import org.apache.spark.SparkFunSuite

class KernelActionUtilsSuite extends SparkFunSuite {

  private val emptySchemaString = """{"type":"struct","fields":[]}"""

  test("metadataFromKernel preserves all populated fields") {
    val kernelMetadata = new KernelMetadata(
      "table-id",
      Optional.of("table-name"),
      Optional.of("table-description"),
      new KernelFormat("parquet", Map("compression" -> "snappy").asJava),
      emptySchemaString,
      new StructType(),
      VectorUtils.buildArrayValue(Seq("Part2", "part1").asJava, StringType.STRING),
      Optional.of(java.lang.Long.valueOf(12345L)),
      VectorUtils.stringStringMapValue(Map("delta.appendOnly" -> "true").asJava))

    val metadata = KernelActionUtils.metadataFromKernel(kernelMetadata)

    assert(metadata.id === "table-id")
    assert(metadata.name === "table-name")
    assert(metadata.description === "table-description")
    assert(metadata.format.provider === "parquet")
    assert(metadata.format.options === Map("compression" -> "snappy"))
    assert(metadata.schemaString === emptySchemaString)
    // Order and case of partition columns must round-trip unchanged.
    assert(metadata.partitionColumns === Seq("Part2", "part1"))
    assert(metadata.configuration === Map("delta.appendOnly" -> "true"))
    assert(metadata.createdTime === Some(12345L))
  }

  test("metadataFromKernel maps absent optionals and empty collections") {
    val kernelMetadata = new KernelMetadata(
      "table-id",
      Optional.empty[String](),
      Optional.empty[String](),
      new KernelFormat("parquet", Map.empty[String, String].asJava),
      emptySchemaString,
      new StructType(),
      VectorUtils.buildArrayValue(Seq.empty[String].asJava, StringType.STRING),
      Optional.empty[java.lang.Long](),
      VectorUtils.stringStringMapValue(Map.empty[String, String].asJava))

    val metadata = KernelActionUtils.metadataFromKernel(kernelMetadata)

    assert(metadata.name === null)
    assert(metadata.description === null)
    assert(metadata.format.provider === "parquet")
    assert(metadata.format.options.isEmpty)
    assert(metadata.partitionColumns.isEmpty)
    assert(metadata.configuration.isEmpty)
    assert(metadata.createdTime === None)
  }

  test("protocolFromKernel converts a table-features protocol with reader/writer features") {
    val kernelProtocol = new KernelProtocol(
      3,
      7,
      Set("v2Checkpoint").asJava,
      Set("appendOnly", "invariants").asJava)

    val protocol = KernelActionUtils.protocolFromKernel(kernelProtocol)

    assert(protocol.minReaderVersion === 3)
    assert(protocol.minWriterVersion === 7)
    assert(protocol.readerFeatures === Some(Set("v2Checkpoint")))
    assert(protocol.writerFeatures === Some(Set("appendOnly", "invariants")))
  }

  test("protocolFromKernel maps a legacy (1, 2) protocol with no table features") {
    // A legacy protocol does not support table features (reader < 3, writer < 7), so the
    // converted V1 Protocol must carry no feature sets at all.
    val kernelProtocol = new KernelProtocol(1, 2)

    val protocol = KernelActionUtils.protocolFromKernel(kernelProtocol)

    assert(protocol.minReaderVersion === 1)
    assert(protocol.minWriterVersion === 2)
    assert(protocol.readerFeatures === None)
    assert(protocol.writerFeatures === None)
  }

  test("protocolFromKernel maps a writer-only (1, 7) table-features protocol") {
    // Writer version 7 supports writer features while the legacy reader version 1 does not, so
    // only writerFeatures is populated and readerFeatures stays None.
    val kernelProtocol = new KernelProtocol(
      1,
      7,
      Set.empty[String].asJava,
      Set("appendOnly", "invariants").asJava)

    val protocol = KernelActionUtils.protocolFromKernel(kernelProtocol)

    assert(protocol.minReaderVersion === 1)
    assert(protocol.minWriterVersion === 7)
    assert(protocol.readerFeatures === None)
    assert(protocol.writerFeatures === Some(Set("appendOnly", "invariants")))
  }

  test("commitInfoFromKernel preserves retry-critical fields") {
    val commitInfo = KernelActionUtils.commitInfoFromKernel(
      new KernelCommitInfo(
        Optional.of(java.lang.Long.valueOf(12345L)),
        23456L,
        Optional.of("kernel-engine"),
        Optional.of("WRITE"),
        Map("mode" -> "Append").asJava,
        Optional.of(java.lang.Boolean.TRUE),
        Optional.of("txn-123"),
        Map("numFiles" -> "1").asJava))

    assert(commitInfo.inCommitTimestamp === Some(12345L))
    assert(commitInfo.timestamp.getTime === 23456L)
    assert(commitInfo.operation === "WRITE")
    assert(commitInfo.operationParameters === Map("mode" -> "Append"))
    assert(commitInfo.isBlindAppend === Some(true))
    assert(commitInfo.operationMetrics === Some(Map("numFiles" -> "1")))
    assert(commitInfo.engineInfo === Some("kernel-engine"))
    assert(commitInfo.txnId === Some("txn-123"))
  }

  private def optionalMapValue(values: Map[String, String]): Optional[MapValue] = {
    if (values.isEmpty) Optional.empty[MapValue]()
    else Optional.of(VectorUtils.stringStringMapValue(values.asJava))
  }

  private def addFileRow(
      path: String,
      partitionValues: Map[String, String],
      size: Long,
      modificationTime: Long,
      dataChange: Boolean,
      tags: Map[String, String] = Map.empty,
      deletionVector: Optional[KernelDeletionVectorDescriptor] = Optional.empty(),
      baseRowId: Optional[java.lang.Long] = Optional.empty(),
      defaultRowCommitVersion: Optional[java.lang.Long] = Optional.empty()): KernelRow =
    KernelAddFile.createAddFileRow(
      KernelAddFile.SCHEMA_WITHOUT_STATS,
      path,
      VectorUtils.stringStringMapValue(partitionValues.asJava),
      size,
      modificationTime,
      dataChange,
      deletionVector,
      optionalMapValue(tags),
      baseRowId,
      defaultRowCommitVersion,
      Optional.empty[DataFileStatistics]())

  private def kernelAddFile(
      path: String,
      partitionValues: Map[String, String],
      size: Long,
      modificationTime: Long,
      dataChange: Boolean,
      tags: Map[String, String] = Map.empty,
      deletionVector: Optional[KernelDeletionVectorDescriptor] = Optional.empty(),
      baseRowId: Optional[java.lang.Long] = Optional.empty(),
      defaultRowCommitVersion: Optional[java.lang.Long] = Optional.empty()): KernelAddFile =
    new KernelAddFile(addFileRow(
      path,
      partitionValues,
      size,
      modificationTime,
      dataChange,
      tags,
      deletionVector,
      baseRowId,
      defaultRowCommitVersion))

  private def buildKernelRemove(
      path: String,
      deletionTimestamp: Long,
      dataChange: Boolean,
      partitionValues: Map[String, String],
      size: Long,
      tags: Map[String, String] = Map.empty,
      stats: String = null,
      deletionVector: Optional[KernelDeletionVectorDescriptor] = Optional.empty(),
      baseRowId: Optional[java.lang.Long] = Optional.empty(),
      defaultRowCommitVersion: Optional[java.lang.Long] = Optional.empty()): KernelRemoveFile = {
    // scalastyle:off removeFile // KernelRemoveFile is the Kernel wrapper, not a V1 RemoveFile
    val fields = new java.util.HashMap[Integer, Object]()
    def put(fieldName: String, value: Object): Unit =
      fields.put(Int.box(KernelRemoveFile.FULL_SCHEMA.indexOf(fieldName)), value)

    put("path", path)
    put("deletionTimestamp", java.lang.Long.valueOf(deletionTimestamp))
    put("dataChange", java.lang.Boolean.valueOf(dataChange))
    put("extendedFileMetadata", java.lang.Boolean.TRUE)
    put("partitionValues", VectorUtils.stringStringMapValue(partitionValues.asJava))
    put("size", java.lang.Long.valueOf(size))
    if (stats != null) put("stats", stats)
    optionalMapValue(tags).ifPresent(tags => put("tags", tags))
    deletionVector.ifPresent(dv => put("deletionVector", dv.toRow))
    baseRowId.ifPresent(rowId => put("baseRowId", rowId))
    defaultRowCommitVersion.ifPresent(version => put("defaultRowCommitVersion", version))

    new KernelRemoveFile(new GenericRow(KernelRemoveFile.FULL_SCHEMA, fields))
  }
  // scalastyle:on removeFile

  test("addFileFromKernel preserves all populated fields") {
    val kernelDv = new KernelDeletionVectorDescriptor(
      "u", "storage-path", Optional.of(java.lang.Integer.valueOf(10)), 128, 5L)
    val addFile = KernelActionUtils.addFileFromKernel(
      kernelAddFile(
        path = "part-00000.parquet",
        partitionValues = Map("p" -> "1", "q" -> "x"),
        size = 4096L,
        modificationTime = 111L,
        dataChange = true,
        tags = Map("tag-a" -> "value-a"),
        deletionVector = Optional.of(kernelDv),
        baseRowId = Optional.of(java.lang.Long.valueOf(42L)),
        defaultRowCommitVersion = Optional.of(java.lang.Long.valueOf(7L))))

    assert(addFile.path === "part-00000.parquet")
    assert(addFile.partitionValues === Map("p" -> "1", "q" -> "x"))
    assert(addFile.size === 4096L)
    assert(addFile.modificationTime === 111L)
    // dataChange must round-trip from the log (unlike the snapshot-scan path, which forces false).
    assert(addFile.dataChange)
    assert(addFile.tagsOrEmpty === Map("tag-a" -> "value-a"))
    assert(addFile.baseRowId === Some(42L))
    assert(addFile.defaultRowCommitVersion === Some(7L))
    assert(addFile.deletionVector != null)
    assert(addFile.deletionVector.storageType === "u")
    assert(addFile.deletionVector.pathOrInlineDv === "storage-path")
    assert(addFile.deletionVector.offset === Some(10))
    assert(addFile.deletionVector.sizeInBytes === 128)
    assert(addFile.deletionVector.cardinality === 5L)
  }

  test("addFileFromKernel maps absent optionals and empty partition values") {
    val addFile = KernelActionUtils.addFileFromKernel(
      kernelAddFile("f.parquet", Map.empty, size = 1L, modificationTime = 1L, dataChange = false))

    assert(addFile.partitionValues.isEmpty)
    assert(!addFile.dataChange)
    assert(addFile.deletionVector === null)
    assert(addFile.baseRowId === None)
    assert(addFile.defaultRowCommitVersion === None)
    // No stats were provided (SCHEMA_WITHOUT_STATS), so the V1 stats string is null.
    assert(addFile.stats === null)
  }

  test("addFileFromKernel preserves a null partition value") {
    val addFile = KernelActionUtils.addFileFromKernel(
      kernelAddFile("f.parquet", Map("p" -> null), size = 1L, modificationTime = 1L,
        dataChange = true))

    assert(addFile.partitionValues === Map("p" -> null))
  }

  test("removeFileFromKernel preserves all populated fields") {
    val kernelDv = new KernelDeletionVectorDescriptor(
      "u", "storage-path", Optional.of(java.lang.Integer.valueOf(10)), 128, 5L)
    val removeFile = KernelActionUtils.removeFileFromKernel(
      buildKernelRemove(
        path = "part-00000.parquet",
        deletionTimestamp = 222L,
        dataChange = true,
        partitionValues = Map("p" -> "1"),
        size = 4096L,
        tags = Map("remove-tag" -> "remove-value"),
        stats = """{"numRecords":9}""",
        deletionVector = Optional.of(kernelDv),
        baseRowId = Optional.of(java.lang.Long.valueOf(84L)),
        defaultRowCommitVersion = Optional.of(java.lang.Long.valueOf(8L))))

    assert(removeFile.path === "part-00000.parquet")
    assert(removeFile.deletionTimestamp === Some(222L))
    assert(removeFile.dataChange)
    assert(removeFile.extendedFileMetadata === Some(true))
    assert(removeFile.partitionValues === Map("p" -> "1"))
    assert(removeFile.size === Some(4096L))
    assert(removeFile.tagsOrEmpty === Map("remove-tag" -> "remove-value"))
    assert(removeFile.stats === """{"numRecords":9}""")
    assert(removeFile.baseRowId === Some(84L))
    assert(removeFile.defaultRowCommitVersion === Some(8L))
    assert(removeFile.deletionVector != null)
    assert(removeFile.deletionVector.storageType === "u")
    assert(removeFile.deletionVector.cardinality === 5L)
  }

  test("removeFileFromKernel maps an absent deletion vector") {
    val removeFile = KernelActionUtils.removeFileFromKernel(
      buildKernelRemove(
        path = "f.parquet",
        deletionTimestamp = 1L,
        dataChange = false,
        partitionValues = Map.empty,
        size = 1L))

    assert(removeFile.dataChange === false)
    assert(removeFile.partitionValues.isEmpty)
    assert(removeFile.deletionVector === null)
  }

  /** Wraps a single action column into a one-batch [[KernelCommitActions]] for direct decoding. */
  private def singleColumnCommitActions(
      colName: String, column: KernelColumnVector): KernelCommitActions = {
    val batch = new KernelColumnarBatch {
      override def getSchema: StructType = new StructType().add(colName, column.getDataType)
      override def getColumnVector(ordinal: Int): KernelColumnVector = column
      override def getSize: Int = column.getSize
    }
    new KernelCommitActions {
      override def getVersion: Long = 1L
      override def getTimestamp: Long = 0L
      override def getActions(): KernelCloseableIterator[KernelColumnarBatch] =
        KernelUtils.singletonCloseableIterator(batch)
      override def close(): Unit = {}
    }
  }

  test("readActions decodes an AddFile action from a hand-built commit batch") {
    val addColumn = new GenericColumnVector(
      java.util.Collections.singletonList(addFileRow(
        path = "part-00000.parquet",
        partitionValues = Map("p" -> "1"),
        size = 4096L,
        modificationTime = 111L,
        dataChange = true,
        tags = Map("tag-a" -> "value-a"),
        baseRowId = Optional.of(java.lang.Long.valueOf(42L)),
        defaultRowCommitVersion = Optional.of(java.lang.Long.valueOf(7L)))),
      KernelAddFile.SCHEMA_WITHOUT_STATS)

    val actions = KernelActionUtils.readActions(singleColumnCommitActions("add", addColumn))

    val adds = actions.collect { case a: AddFile => a }
    assert(adds.size === 1)
    assert(adds.head.path === "part-00000.parquet")
    assert(adds.head.partitionValues === Map("p" -> "1"))
    assert(adds.head.size === 4096L)
    assert(adds.head.dataChange)
    assert(adds.head.baseRowId === Some(42L))
    assert(adds.head.defaultRowCommitVersion === Some(7L))
  }

  test("readActions fails loud on an action type with no decoder (hand-built commit batch)") {
    val presentColumn = new GenericColumnVector(
      java.util.Collections.singletonList("present"), StringType.STRING)
    Seq(KernelDeltaAction.TXN, KernelDeltaAction.DOMAINMETADATA, KernelDeltaAction.CDC)
      .foreach { action =>
        val e = intercept[UnsupportedOperationException] {
          KernelActionUtils.readActions(singleColumnCommitActions(action.colName, presentColumn))
        }
        assert(e.getMessage.contains("No V1 action from Kernel decoder"))
        assert(e.getMessage.contains(action.colName))
      }
  }

}
