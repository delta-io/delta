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
package io.delta.kernel.internal.tablefeatures

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures.{TABLE_FEATURES, validateWriteSupportedTable}
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.internal.util.VectorUtils.stringVector
import io.delta.kernel.types._
import org.scalatest.funsuite.AnyFunSuite

import java.util.stream.Collectors
import java.util.stream.Collectors.toList
import java.util.{Collections, Optional}
import scala.collection.JavaConverters._

/**
 * Suite that tests Kernel throws error when it receives a unsupported protocol and metadata
 */
class TableFeaturesSuite extends AnyFunSuite {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for [[TableFeature]] implementations                                                  //
  /////////////////////////////////////////////////////////////////////////////////////////////////
  val readerWriterFeatures = Seq("columnMapping", "deletionVectors", "timestampNtz",
    "typeWidening", "typeWidening-preview", "v2Checkpoint", "vacuumProtocolCheck",
    "variantType", "variantType-preview")

  val writerOnlyFeatures = Seq("appendOnly", "invariants", "checkConstraints",
    "generatedColumns", "changeDataFeed", "identityColumns",
    "rowTracking", "domainMetadata", "icebergCompatV2", "inCommitTimestamp")

  val legacyFeatures = Seq("appendOnly", "invariants", "checkConstraints",
    "generatedColumns", "changeDataFeed", "identityColumns", "columnMapping")

  test("basic properties checks") {

    // Check that all features are correctly classified as reader-writer or writer-only
    readerWriterFeatures.foreach { feature =>
      assert(TableFeatures.getTableFeature(feature).isReaderWriterFeature)
    }
    writerOnlyFeatures.foreach { feature =>
      assert(!TableFeatures.getTableFeature(feature).isReaderWriterFeature)
    }

    // Check that legacy features are correctly classified as legacy features
    (readerWriterFeatures ++ writerOnlyFeatures) foreach { feature =>
      if (legacyFeatures.contains(feature)) {
        assert(TableFeatures.getTableFeature(feature).isLegacyFeature)
      } else {
        assert(!TableFeatures.getTableFeature(feature).isLegacyFeature)
      }
    }

    // all expected features are present in list. Just make sure we don't miss any
    // adding to the list. This is the list used to iterate over all features
    assert(
      TableFeatures.TABLE_FEATURES.size() == readerWriterFeatures.size + writerOnlyFeatures.size)
  }

  val testProtocol = new Protocol(1, 2, Collections.emptyList(), Collections.emptyList())
  Seq(
    // Test feature, metadata, expected result
    ("appendOnly", testMetadata(tblProps = Map("delta.appendOnly" -> "true")), true),
    ("appendOnly", testMetadata(tblProps = Map("delta.appendOnly" -> "false")), false),
    ("invariants", testMetadata(includeInvaraint = true), true),
    ("invariants", testMetadata(includeInvaraint = false), false),
    ("checkConstraints", testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")), true),
    ("checkConstraints", testMetadata(), false),
    ("generatedColumns", testMetadata(includeGeneratedColumn = true), true),
    ("generatedColumns", testMetadata(includeGeneratedColumn = false), false),
    ("changeDataFeed",
      testMetadata(tblProps = Map("delta.enableChangeDataFeed" -> "true")), true),
    ("changeDataFeed",
      testMetadata(tblProps = Map("delta.enableChangeDataFeed" -> "false")), false),
    ("identityColumns", testMetadata(includeIdentityColumn = true), true),
    ("identityColumns", testMetadata(includeIdentityColumn = false), false),
    ("columnMapping", testMetadata(tblProps = Map("delta.columnMapping.mode" -> "id")), true),
    ("columnMapping", testMetadata(tblProps = Map("delta.columnMapping.mode" -> "none")), false),
    ("typeWidening-preview",
      testMetadata(tblProps = Map("delta.enableTypeWidening" -> "true")), true),
    ("typeWidening-preview",
      testMetadata(tblProps = Map("delta.enableTypeWidening" -> "false")), false),
    ("typeWidening", testMetadata(tblProps = Map("delta.enableTypeWidening" -> "true")), true),
    ("typeWidening", testMetadata(tblProps = Map("delta.enableTypeWidening" -> "false")), false),
    ("rowTracking", testMetadata(tblProps = Map("delta.enableRowTracking" -> "true")), true),
    ("rowTracking", testMetadata(tblProps = Map("delta.enableRowTracking" -> "false")), false),
    ("deletionVectors",
      testMetadata(tblProps = Map("delta.enableDeletionVectors" -> "true")), true),
    ("deletionVectors",
      testMetadata(tblProps = Map("delta.enableDeletionVectors" -> "false")), false),
    ("timestampNtz", testMetadata(includeTimestampNtzTypeCol = true), true),
    ("timestampNtz", testMetadata(includeTimestampNtzTypeCol = false), false),
    ("v2Checkpoint", testMetadata(tblProps = Map("delta.checkpointPolicy" -> "v2")), true),
    ("v2Checkpoint", testMetadata(tblProps = Map("delta.checkpointPolicy" -> "classic")), false),
    ("icebergCompatV2",
      testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "true")), true),
    ("icebergCompatV2",
      testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "false")), false),
    ("inCommitTimestamp",
      testMetadata(tblProps = Map("delta.enableInCommitTimestamps" -> "true")), true),
    ("inCommitTimestamp",
      testMetadata(tblProps = Map("delta.enableInCommitTimestamps" -> "false")), false)
  ).foreach({ case (feature, metadata, expected) =>
    test(s"metadataRequiresFeatureToBeEnabled - $feature - $metadata") {
      val tableFeature = TableFeatures.getTableFeature(feature)
      assert(tableFeature.isInstanceOf[FeatureAutoEnabledByMetadata])
      assert(tableFeature.asInstanceOf[FeatureAutoEnabledByMetadata]
        .metadataRequiresFeatureToBeEnabled(testProtocol, metadata) == expected)
    }
  })

  Seq("domainMetadata", "vacuumProtocolCheck").foreach { feature =>
    test(s"doesn't support auto enable by metadata: $feature") {
      val tableFeature = TableFeatures.getTableFeature(feature)
      assert(!tableFeature.isInstanceOf[FeatureAutoEnabledByMetadata])
    }
  }

  test("hasKernelReadSupport expected to be true") {
    val results = TABLE_FEATURES.stream()
      .filter(_.isReaderWriterFeature)
      .filter(_.hasKernelReadSupport())
      .collect(toList()).asScala

    val expected = Seq("columnMapping", "v2Checkpoint", "variantType",
      "variantType-preview", "typeWidening", "typeWidening-preview", "deletionVectors",
      "timestampNtz", "vacuumProtocolCheck")

    assert(results.map(_.featureName()).toSet == expected.toSet)
  }

  test("hasKernelWriteSupport expected to be true") {
    val results = TABLE_FEATURES.stream()
      .filter(_.hasKernelWriteSupport(testMetadata()))
      .collect(toList()).asScala

    // checkConstraints, generatedColumns, identityColumns, invariants are writable
    // because the metadata has not been set the info that these features are enabled
    val expected = Seq("columnMapping", "v2Checkpoint", "deletionVectors",
      "vacuumProtocolCheck", "rowTracking", "domainMetadata", "icebergCompatV2",
      "inCommitTimestamp", "appendOnly", "invariants",
      "checkConstraints", "generatedColumns", "identityColumns"
    )

    assert(results.map(_.featureName()).toSet == expected.toSet)
  }

  Seq(
    // Test format: feature, metadata, expected value
    ("invariants", testMetadata(includeInvaraint = true), false),
    ("checkConstraints", testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")), false),
    ("generatedColumns", testMetadata(includeGeneratedColumn = true), false),
    ("identityColumns", testMetadata(includeIdentityColumn = true), false)
  ).foreach({ case (feature, metadata, expected) =>
    test(s"hasKernelWriteSupport - $feature has metadata") {
      val tableFeature = TableFeatures.getTableFeature(feature)
      assert(tableFeature.hasKernelWriteSupport(metadata) == expected)
    }
  })

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Legacy tests (will be modified or deleted in subsequent PRs)                                //
  /////////////////////////////////////////////////////////////////////////////////////////////////
  test("validate write supported: protocol 1") {
    checkSupported(createTestProtocol(minWriterVersion = 1))
  }

  test("validateWriteSupported: protocol 2") {
    checkSupported(createTestProtocol(minWriterVersion = 2))
  }

  test("validateWriteSupported: protocol 2 with appendOnly") {
    checkSupported(
      createTestProtocol(minWriterVersion = 2),
      metadata = testMetadata(tblProps = Map("delta.appendOnly" -> "true")))
  }

  test("validateWriteSupported: protocol 2 with invariants") {
    checkUnsupported(
      createTestProtocol(minWriterVersion = 2),
      metadata = testMetadata(includeInvaraint = true))
  }

  test("validateWriteSupported: protocol 2, with appendOnly and invariants") {
    checkUnsupported(
      createTestProtocol(minWriterVersion = 2),
      metadata = testMetadata(includeInvaraint = true))
  }

  Seq(3, 4, 5, 6).foreach { minWriterVersion =>
    test(s"validateWriteSupported: protocol $minWriterVersion") {
      checkUnsupported(createTestProtocol(minWriterVersion = minWriterVersion))
    }
  }

  test("validateWriteSupported: protocol 7 with no additional writer features") {
    checkSupported(createTestProtocol(minWriterVersion = 7))
  }

  Seq("appendOnly", "inCommitTimestamp", "columnMapping", "typeWidening-preview", "typeWidening",
    "domainMetadata", "rowTracking").foreach { supportedWriterFeature =>
    test(s"validateWriteSupported: protocol 7 with $supportedWriterFeature") {
      val protocol = if (supportedWriterFeature == "rowTracking") {
        createTestProtocol(minWriterVersion = 7, supportedWriterFeature, "domainMetadata")
      } else {
        createTestProtocol(minWriterVersion = 7, supportedWriterFeature)
      }
      checkSupported(protocol)
    }
  }

  Seq("checkConstraints", "generatedColumns", "allowColumnDefaults", "changeDataFeed",
    "identityColumns", "deletionVectors", "timestampNtz", "v2Checkpoint", "icebergCompatV1",
    "icebergCompatV2", "clustering", "vacuumProtocolCheck").foreach { unsupportedWriterFeature =>
    test(s"validateWriteSupported: protocol 7 with $unsupportedWriterFeature") {
      checkUnsupported(createTestProtocol(minWriterVersion = 7, unsupportedWriterFeature))
    }
  }

  test("validateWriteSupported: protocol 7 with invariants, schema doesn't contain invariants") {
    checkSupported(
      createTestProtocol(minWriterVersion = 7, "invariants")
    )
  }

  test("validateWriteSupported: protocol 7 with invariants, schema contains invariants") {
    checkUnsupported(
      createTestProtocol(minWriterVersion = 7, "invariants"),
      metadata = testMetadata(includeInvaraint = true)
    )
  }

  def checkSupported(
    protocol: Protocol,
    metadata: Metadata = testMetadata()): Unit = {
    validateWriteSupportedTable(protocol, metadata, "/test/table")
  }

  def checkUnsupported(
    protocol: Protocol,
    metadata: Metadata = testMetadata()): Unit = {
    intercept[KernelException] {
      validateWriteSupportedTable(protocol, metadata, "/test/table")
    }
  }

  def createTestProtocol(minWriterVersion: Int, writerFeatures: String*): Protocol = {
    new Protocol(
      // minReaderVersion - it doesn't matter as the read fails anyway before the writer check
      0,
      minWriterVersion,
      // reader features - it doesn't matter as the read fails anyway before the writer check
      Collections.emptyList(),
      writerFeatures.toSeq.asJava
    )
  }

  def testMetadata(
      includeInvaraint: Boolean = false,
      includeTimestampNtzTypeCol: Boolean = false,
      includeVariantTypeCol: Boolean = false,
      includeGeneratedColumn: Boolean = false,
      includeIdentityColumn: Boolean = false,
      tblProps: Map[String, String] = Map.empty): Metadata = {
    val testSchema = createTestSchema(
      includeInvaraint,
      includeTimestampNtzTypeCol,
      includeVariantTypeCol,
      includeGeneratedColumn,
      includeIdentityColumn)
    new Metadata(
      "id",
      Optional.of("name"),
      Optional.of("description"),
      new Format("parquet", Collections.emptyMap()),
      testSchema.toJson,
      testSchema,
      new ArrayValue() { // partitionColumns
        override def getSize = 1

        override def getElements: ColumnVector = singletonStringColumnVector("c3")
      },
      Optional.empty(),
      new MapValue() { // conf
        override def getSize = tblProps.size
        override def getKeys: ColumnVector = stringVector(tblProps.toSeq.map(_._1).asJava)
        override def getValues: ColumnVector = stringVector(tblProps.toSeq.map(_._2).asJava)
      }
    )
  }

  def createTestSchema(
      includeInvariant: Boolean = false,
      includeTimestampNtzTypeCol: Boolean = false,
      includeVariantTypeCol: Boolean = false,
      includeGeneratedColumn: Boolean = false,
      includeIdentityColumn: Boolean = false): StructType = {
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
    if (includeTimestampNtzTypeCol) {
      structType = structType.add("c4", TimestampNTZType.TIMESTAMP_NTZ)
    }
    if (includeVariantTypeCol) {
      structType = structType.add("c5", VariantType.VARIANT)
    }
    if (includeGeneratedColumn) {
      structType = structType.add(
        "c6",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putString("delta.generationExpression", "{\"expression\": \"c1 + 1\"}")
          .build())
    }
    if (includeIdentityColumn) {
      structType = structType.add(
        "c7",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putLong("delta.identity.start", 1L)
          .putLong("delta.identity.step", 2L)
          .putBoolean("delta.identity.allowExplicitInsert", true)
          .build())
    }

    structType
  }
}
