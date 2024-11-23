/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.internal

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableFeatures.validateWriteSupportedTable
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util.{Collections, Optional}
import scala.collection.JavaConverters._

/**
 * Suite that tests Kernel throws error when it receives a unsupported protocol and metadata
 */
class TableFeaturesSuite extends AnyFunSuite {
  test("throws when table does not support reader features but readerFeatures is non-empty") {
    val exMsg = intercept[KernelException] {
      new Protocol(1, 7, Set("columnMapping").asJava, Collections.emptySet())
    }.getMessage
    assert(exMsg == "Found reader features [columnMapping] in reader protocol version 1 but " +
      "these are only supported in reader protocol version 3 and above.")
  }

  test("throws when table does not support writer features but writerFeatures is non-empty") {
    val exMsg = intercept[KernelException] {
      new Protocol(1, 1, Collections.emptySet(), Set("appendOnly").asJava)
    }.getMessage
    assert(exMsg == "Found writer features [appendOnly] in writer protocol version 1 but these " +
      "are only supported in writer protocol version 7 and above.")
  }

  test("throws when table supports reader features but does not support writer features") {
    val exMsg = intercept[KernelException] {
      new Protocol(3, 1, Collections.emptySet(), Collections.emptySet())
    }.getMessage
    assert(exMsg == "Table reader features are supported (current reader protocol version is 3) " +
      "yet table writer features are not (current writer protocol version is 1). Writer protocol " +
      "version must be at least 7 to proceed.")
  }

  test("validate write supported: protocol 1") {
    checkSupported(createTestProtocol(minWriterVersion = 1))
  }

  test("validateWriteSupported: protocol 2") {
    checkSupported(createTestProtocol(minWriterVersion = 2))
  }

  test("validateWriteSupported: protocol 2 with appendOnly") {
    checkSupported(
      createTestProtocol(minWriterVersion = 2),
      metadata = createTestMetadata(withAppendOnly = true))
  }

  test("validateWriteSupported: protocol 2 with invariants") {
    checkUnsupported(
      createTestProtocol(minWriterVersion = 2),
      metadata = createTestMetadata(),
      schema = createTestSchema(includeInvariant = true))
  }

  test("validateWriteSupported: protocol 2, with appendOnly and invariants") {
    checkUnsupported(
      createTestProtocol(minWriterVersion = 2),
      metadata = createTestMetadata(),
      schema = createTestSchema(includeInvariant = true))
  }

  Seq(3, 4, 5, 6).foreach { minWriterVersion =>
    test(s"validateWriteSupported: protocol $minWriterVersion") {
      checkUnsupported(createTestProtocol(minWriterVersion = minWriterVersion))
    }
  }

  test("validateWriteSupported: protocol 7 with no additional writer features") {
    checkSupported(createTestProtocol(minWriterVersion = 7))
  }

  Seq("appendOnly", "inCommitTimestamp", "columnMapping", "typeWidening-preview", "typeWidening")
    .foreach { supportedWriterFeature =>
      test(s"validateWriteSupported: protocol 7 with $supportedWriterFeature") {
        checkSupported(createTestProtocol(minWriterVersion = 7, supportedWriterFeature))
      }
    }

  Seq("invariants", "checkConstraints", "generatedColumns", "allowColumnDefaults", "changeDataFeed",
    "identityColumns", "deletionVectors", "rowTracking", "timestampNtz",
    "domainMetadata", "v2Checkpoint", "icebergCompatV1", "icebergCompatV2", "clustering",
    "vacuumProtocolCheck").foreach { unsupportedWriterFeature =>
    test(s"validateWriteSupported: protocol 7 with $unsupportedWriterFeature") {
      checkUnsupported(createTestProtocol(minWriterVersion = 7, unsupportedWriterFeature))
    }
  }

  ////////////////////
  // Helper Methods //
  ////////////////////

  private def checkSupported(
      protocol: Protocol,
      metadata: Metadata = null,
      schema: StructType = createTestSchema()): Unit = {
    validateWriteSupportedTable(protocol, metadata, schema, "/test/table")
  }

  private def checkUnsupported(
      protocol: Protocol,
      metadata: Metadata = null,
      schema: StructType = createTestSchema()): Unit = {
    intercept[KernelException] {
      validateWriteSupportedTable(protocol, metadata, schema, "/test/table")
    }
  }

  private def createTestProtocol(minWriterVersion: Int, writerFeatures: String*): Protocol = {
    new Protocol(
      // minReaderVersion - it doesn't matter as the read fails anyway before the writer check
      0,
      minWriterVersion,
      // reader features - it doesn't matter as the read fails anyway before the writer check
      Collections.emptySet(),
      writerFeatures.toSet.asJava
    )
  }

  private def createTestMetadata(withAppendOnly: Boolean = false): Metadata = {
    var config: Map[String, String] = Map()
    if (withAppendOnly) {
      config = Map("delta.appendOnly" -> "true");
    }
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      "sss",
      new StructType(),
      new ArrayValue() { // partitionColumns
        override def getSize = 1

        override def getElements: ColumnVector = singletonStringColumnVector("c3")
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize = 1

        override def getKeys: ColumnVector = singletonStringColumnVector("delta.appendOnly")

        override def getValues: ColumnVector =
          singletonStringColumnVector(if (withAppendOnly) "false" else "true")
      }
    )
  }

  private def createTestSchema(
      includeInvariant: Boolean = false): StructType = {
    var structType = new StructType()
      .add("c1", IntegerType.INTEGER)
      .add("c2", StringType.STRING)
    if (includeInvariant) {
      structType = structType.add(
        "c3",
        TimestampType.TIMESTAMP,
        FieldMetadata.builder()
          .putString("delta.invariants", "{\"expression\": { \"expression\": \"x > 3\"} }")
          .build())
    }
    structType
  }
}
