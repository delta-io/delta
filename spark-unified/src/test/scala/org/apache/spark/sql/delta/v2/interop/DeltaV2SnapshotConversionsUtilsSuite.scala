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

package org.apache.spark.sql.delta.v2.interop

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.internal.actions.{Format => KernelFormat}
import io.delta.kernel.internal.actions.{Metadata => KernelMetadata}
import io.delta.kernel.internal.actions.{Protocol => KernelProtocol}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.{StringType, StructType}

import org.apache.spark.SparkFunSuite

/**
 * Unit tests for [[DeltaV2SnapshotConversionsUtils]], which bridges Kernel's actions to V1 Delta
 * actions. The tests build real Kernel [[KernelMetadata]] / [[KernelProtocol]] instances and assert
 * the converted V1 Metadata / Protocol preserves every field.
 */
class DeltaV2SnapshotConversionsUtilsSuite extends SparkFunSuite {

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

    val metadata = DeltaV2SnapshotConversionsUtils.metadataFromKernel(kernelMetadata)

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

    val metadata = DeltaV2SnapshotConversionsUtils.metadataFromKernel(kernelMetadata)

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

    val protocol = DeltaV2SnapshotConversionsUtils.protocolFromKernel(kernelProtocol)

    assert(protocol.minReaderVersion === 3)
    assert(protocol.minWriterVersion === 7)
    assert(protocol.readerFeatures === Some(Set("v2Checkpoint")))
    assert(protocol.writerFeatures === Some(Set("appendOnly", "invariants")))
  }

  test("protocolFromKernel maps a legacy (1, 2) protocol with no table features") {
    // A legacy protocol does not support table features (reader < 3, writer < 7), so the
    // converted V1 Protocol must carry no feature sets at all.
    val kernelProtocol = new KernelProtocol(1, 2)

    val protocol = DeltaV2SnapshotConversionsUtils.protocolFromKernel(kernelProtocol)

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

    val protocol = DeltaV2SnapshotConversionsUtils.protocolFromKernel(kernelProtocol)

    assert(protocol.minReaderVersion === 1)
    assert(protocol.minWriterVersion === 7)
    assert(protocol.readerFeatures === None)
    assert(protocol.writerFeatures === Some(Set("appendOnly", "invariants")))
  }
}
