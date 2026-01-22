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
package org.apache.spark.sql.delta

import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.util.VectorUtils
import io.delta.spark.internal.v2.read.ProtocolMetadataAdapterV2
import io.delta.spark.internal.v2.utils.SchemaUtils
import io.delta.kernel.types.{ArrayType, StringType => KernelStringType}

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.scalactic.source.Position
import org.scalatest.Tag

import java.util.Optional
import scala.jdk.CollectionConverters._

/**
 * Unit tests for ProtocolMetadataAdapterV2.
 *
 * This suite tests the V2 wrapper implementation that adapts kernel's Protocol and Metadata
 * to the ProtocolMetadataAdapter interface.
 */
class ProtocolMetadataAdapterV2Suite extends ProtocolMetadataAdapterSuiteBase {

  /**
   * Tests that are not applicable to V2 (kernel-based) implementation.
   * These tests can be ignored because V2 has different behavior or limitations.
   */
  protected def ignoredTests: Set[String] = Set(
    // TODO(delta-io/delta#5649): add type widening validation
    "assertTableReadable with table with unsupported type widening",
    // V1 IcebergCompat is not supported in Kernel (only V2/V3)
    "isIcebergCompatAnyEnabled when v1 enabled",
    "isIcebergCompatGeqEnabled when v1 enabled"
  )

  override protected def test(
      testName: String,
      testTags: Tag*)(testFun: => Any)(implicit pos: Position): Unit = {
    if (ignoredTests.contains(testName)) {
      super.ignore(s"$testName - not applicable to V2 implementation")(testFun)
    } else {
      super.test(testName, testTags: _*)(testFun)
    }
  }

  override protected def createWrapper(
      minReaderVersion: Int = 1,
      minWriterVersion: Int = 2,
      readerFeatures: Option[Set[String]] = None,
      writerFeatures: Option[Set[String]] = None,
      schema: StructType = new StructType().add("id", IntegerType),
      configuration: Map[String, String] = Map.empty): ProtocolMetadataAdapter = {

    // Create kernel Protocol
    val protocol = new Protocol(
      minReaderVersion,
      minWriterVersion,
      readerFeatures.map(_.asJava).getOrElse(java.util.Collections.emptySet()),
      writerFeatures.map(_.asJava).getOrElse(java.util.Collections.emptySet())
    )

    // Convert Spark schema to Kernel schema
    val kernelSchema = SchemaUtils.convertSparkSchemaToKernelSchema(schema)
    val schemaString = kernelSchema.toJson

    // Create kernel Metadata
    val metadata = new Metadata(
      "test-id",
      Optional.of("test-table"),
      Optional.of("test description"),
      new Format("parquet", java.util.Collections.emptyMap()),
      schemaString,
      kernelSchema,
      VectorUtils.buildArrayValue(
        java.util.Collections.emptyList(),
        new ArrayType(KernelStringType.STRING, true)),
      Optional.of(System.currentTimeMillis()),
      VectorUtils.stringStringMapValue(configuration.asJava)
    )

    // Create and return the V2 adapter
    new ProtocolMetadataAdapterV2(protocol, metadata)
  }
}
