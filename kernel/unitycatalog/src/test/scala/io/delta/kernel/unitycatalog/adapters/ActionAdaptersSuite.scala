/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.unitycatalog.adapters

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector}
import io.delta.kernel.internal.actions.{Format, Metadata => KernelMetadata, Protocol => KernelProtocol}
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ActionAdaptersSuite extends AnyFunSuite {

  test("ProtocolAdapter") {
    // ===== GIVEN =====
    val readerFeatures = Set("v2Checkpoint").asJava
    val writerFeatures = Set("v2Checkpoint", "rowTracking").asJava
    val kernelProtocol = new KernelProtocol(3, 7, readerFeatures, writerFeatures)

    // ===== WHEN =====
    val adapterProtocol = new ProtocolAdapter(kernelProtocol)

    // ===== THEN =====
    assert(adapterProtocol.getMinReaderVersion === 3)
    assert(adapterProtocol.getMinWriterVersion === 7)
    assert(adapterProtocol.getReaderFeatures.asScala == Set("v2Checkpoint"))
    assert(adapterProtocol.getWriterFeatures.asScala == Set("v2Checkpoint", "rowTracking"))
  }

  test("MetadataAdapter") {
    // ===== GIVEN =====
    val partCols = new ArrayValue() {
      override def getSize = 1
      override def getElements: ColumnVector = singletonStringColumnVector("part1")
    }
    val formatOptions = Map("foo" -> "bar").asJava
    val format = new Format("parquet", formatOptions)
    val configuration = Map("zip" -> "zap").asJava

    val kernelMetadata = new KernelMetadata(
      "id",
      java.util.Optional.of("name"),
      java.util.Optional.of("description"),
      format,
      "schemaStringJson",
      new StructType().add("part1", IntegerType.INTEGER).add("col1", IntegerType.INTEGER),
      partCols,
      java.util.Optional.of(42L), // createdTime
      VectorUtils.stringStringMapValue(configuration))

    // ===== WHEN =====
    val adapter = new MetadataAdapter(kernelMetadata)

    // ===== THEN =====
    assert(adapter.getId === "id")
    assert(adapter.getName === "name")
    assert(adapter.getDescription === "description")
    assert(adapter.getProvider === "parquet")
    assert(adapter.getFormatOptions.asScala == Map("foo" -> "bar"))
    assert(adapter.getSchemaString === "schemaStringJson")
    assert(adapter.getPartitionColumns.asScala == Seq("part1"))
    assert(adapter.getConfiguration.asScala == Map("zip" -> "zap"))
    assert(adapter.getCreatedTime === 42L)
  }
}
