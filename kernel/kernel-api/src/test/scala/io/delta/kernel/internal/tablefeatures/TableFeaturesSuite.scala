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

import java.util.{Collections, Optional}
import java.util.Collections.{emptySet, singleton}
import java.util.stream.Collectors.toList

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures.{validateKernelCanReadTheTable, validateKernelCanWriteToTable, TABLE_FEATURES}
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.internal.util.VectorUtils.stringVector
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

/**
 * Suite that tests Kernel throws error when it receives a unsupported protocol and metadata
 */
class TableFeaturesSuite extends AnyFunSuite {

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for [[TableFeature]] implementations                                                  //
  /////////////////////////////////////////////////////////////////////////////////////////////////
  val readerWriterFeatures = Seq(
    "columnMapping",
    "deletionVectors",
    "timestampNtz",
    "typeWidening",
    "typeWidening-preview",
    "v2Checkpoint",
    "vacuumProtocolCheck",
    "variantType",
    "variantType-preview")

  val writerOnlyFeatures = Seq(
    "appendOnly",
    "invariants",
    "checkConstraints",
    "generatedColumns",
    "changeDataFeed",
    "identityColumns",
    "rowTracking",
    "domainMetadata",
    "icebergCompatV2",
    "inCommitTimestamp")

  val legacyFeatures = Seq(
    "appendOnly",
    "invariants",
    "checkConstraints",
    "generatedColumns",
    "changeDataFeed",
    "identityColumns",
    "columnMapping")

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

  val testProtocol = new Protocol(1, 2, emptySet(), emptySet())
  Seq(
    // Test feature, metadata, expected result
    ("appendOnly", testMetadata(tblProps = Map("delta.appendOnly" -> "true")), true),
    ("appendOnly", testMetadata(tblProps = Map("delta.appendOnly" -> "false")), false),
    ("invariants", testMetadata(includeInvariant = true), true),
    ("invariants", testMetadata(includeInvariant = false), false),
    ("checkConstraints", testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")), true),
    ("checkConstraints", testMetadata(), false),
    ("generatedColumns", testMetadata(includeGeneratedColumn = true), true),
    ("generatedColumns", testMetadata(includeGeneratedColumn = false), false),
    ("changeDataFeed", testMetadata(tblProps = Map("delta.enableChangeDataFeed" -> "true")), true),
    (
      "changeDataFeed",
      testMetadata(tblProps = Map("delta.enableChangeDataFeed" -> "false")),
      false),
    ("identityColumns", testMetadata(includeIdentityColumn = true), true),
    ("identityColumns", testMetadata(includeIdentityColumn = false), false),
    ("columnMapping", testMetadata(tblProps = Map("delta.columnMapping.mode" -> "id")), true),
    ("columnMapping", testMetadata(tblProps = Map("delta.columnMapping.mode" -> "none")), false),
    (
      "typeWidening-preview",
      testMetadata(tblProps = Map("delta.enableTypeWidening" -> "true")),
      true),
    (
      "typeWidening-preview",
      testMetadata(tblProps = Map("delta.enableTypeWidening" -> "false")),
      false),
    ("typeWidening", testMetadata(tblProps = Map("delta.enableTypeWidening" -> "true")), true),
    ("typeWidening", testMetadata(tblProps = Map("delta.enableTypeWidening" -> "false")), false),
    ("rowTracking", testMetadata(tblProps = Map("delta.enableRowTracking" -> "true")), true),
    ("rowTracking", testMetadata(tblProps = Map("delta.enableRowTracking" -> "false")), false),
    (
      "deletionVectors",
      testMetadata(tblProps = Map("delta.enableDeletionVectors" -> "true")),
      true),
    (
      "deletionVectors",
      testMetadata(tblProps = Map("delta.enableDeletionVectors" -> "false")),
      false),
    ("timestampNtz", testMetadata(includeTimestampNtzTypeCol = true), true),
    ("timestampNtz", testMetadata(includeTimestampNtzTypeCol = false), false),
    ("v2Checkpoint", testMetadata(tblProps = Map("delta.checkpointPolicy" -> "v2")), true),
    ("v2Checkpoint", testMetadata(tblProps = Map("delta.checkpointPolicy" -> "classic")), false),
    (
      "icebergCompatV2",
      testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "true")),
      true),
    (
      "icebergCompatV2",
      testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "false")),
      false),
    (
      "inCommitTimestamp",
      testMetadata(tblProps = Map("delta.enableInCommitTimestamps" -> "true")),
      true),
    (
      "inCommitTimestamp",
      testMetadata(tblProps = Map("delta.enableInCommitTimestamps" -> "false")),
      false)).foreach({ case (feature, metadata, expected) =>
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

    val expected = Seq(
      "columnMapping",
      "v2Checkpoint",
      "variantType",
      "variantType-preview",
      "typeWidening",
      "typeWidening-preview",
      "deletionVectors",
      "timestampNtz",
      "vacuumProtocolCheck")

    assert(results.map(_.featureName()).toSet == expected.toSet)
  }

  test("hasKernelWriteSupport expected to be true") {
    val results = TABLE_FEATURES.stream()
      .filter(_.hasKernelWriteSupport(testMetadata()))
      .collect(toList()).asScala

    // checkConstraints, generatedColumns, identityColumns, invariants, changeDataFeed,
    // timestampNtz are writable because the metadata has not been set the info that
    // these features are enabled
    val expected = Seq(
      "columnMapping",
      "v2Checkpoint",
      "deletionVectors",
      "vacuumProtocolCheck",
      "rowTracking",
      "domainMetadata",
      "icebergCompatV2",
      "inCommitTimestamp",
      "appendOnly",
      "invariants",
      "checkConstraints",
      "generatedColumns",
      "changeDataFeed",
      "timestampNtz",
      "identityColumns")

    assert(results.map(_.featureName()).toSet == expected.toSet)
  }

  Seq(
    // Test format: feature, metadata, expected value
    ("invariants", testMetadata(includeInvariant = true), false),
    ("checkConstraints", testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")), false),
    ("generatedColumns", testMetadata(includeGeneratedColumn = true), false),
    ("identityColumns", testMetadata(includeIdentityColumn = true), false)).foreach({
    case (feature, metadata, expected) =>
      test(s"hasKernelWriteSupport - $feature has metadata") {
        val tableFeature = TableFeatures.getTableFeature(feature)
        assert(tableFeature.hasKernelWriteSupport(metadata) == expected)
      }
  })

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for validateKernelCanReadTheTable and validateKernelCanWriteToTable                   //
  /////////////////////////////////////////////////////////////////////////////////////////////////

  // Reads: All legacy protocols should be readable by Kernel
  Seq(
    // Test format: protocol (minReaderVersion, minWriterVersion)
    (1, 1),
    (1, 2),
    (1, 3),
    (1, 4),
    (2, 5),
    (2, 6)).foreach {
    case (minReaderVersion, minWriterVersion) =>
      test(s"validateKernelCanReadTheTable: protocol ($minReaderVersion, $minWriterVersion)") {
        val protocol = new Protocol(minReaderVersion, minWriterVersion)
        validateKernelCanReadTheTable(protocol, "/test/table")
      }
  }

  // Reads: Supported table features represented as readerFeatures in the protocol
  Seq(
    "variantType",
    "variantType-preview",
    "deletionVectors",
    "typeWidening",
    "typeWidening-preview",
    "timestampNtz",
    "v2Checkpoint",
    "vacuumProtocolCheck",
    "columnMapping").foreach { feature =>
    test(s"validateKernelCanReadTheTable: protocol 3 with $feature") {
      val protocol = new Protocol(3, 1, singleton(feature), Set().asJava)
      validateKernelCanReadTheTable(protocol, "/test/table")
    }
  }

  // Writes
  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 1",
    new Protocol(1, 1),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 2",
    new Protocol(1, 2),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 2 with appendOnly",
    new Protocol(1, 2),
    testMetadata(tblProps = Map("delta.appendOnly" -> "true")))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 2 with invariants",
    new Protocol(1, 2),
    testMetadata(includeInvariant = true))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 2, with appendOnly and invariants",
    new Protocol(1, 2),
    testMetadata(includeInvariant = true, tblProps = Map("delta.appendOnly" -> "true")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 3",
    new Protocol(1, 3))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 3 with checkConstraints",
    new Protocol(1, 3),
    testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 4",
    new Protocol(1, 4))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 4 with generatedColumns",
    new Protocol(1, 4),
    testMetadata(includeGeneratedColumn = true))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 4 with changeDataFeed",
    new Protocol(1, 4),
    testMetadata(tblProps = Map("delta.enableChangeDataFeed" -> "true")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 5 with columnMapping",
    new Protocol(2, 5),
    testMetadata(tblProps = Map("delta.columnMapping.mode" -> "id")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 6",
    new Protocol(2, 6),
    testMetadata(tblProps = Map("delta.columnMapping.mode" -> "none")))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 6 with identityColumns",
    new Protocol(2, 6),
    testMetadata(includeIdentityColumn = true))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with appendOnly supported",
    new Protocol(1, 7, Set().asJava, singleton("appendOnly")),
    testMetadata(tblProps = Map("delta.appendOnly" -> "true")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with invariants, " +
      "schema doesn't contain invariants",
    new Protocol(1, 7, Set().asJava, singleton("invariants")),
    testMetadata(includeInvariant = false))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with invariants, " +
      "schema contains invariants",
    new Protocol(1, 7, Set().asJava, singleton("invariants")),
    testMetadata(includeInvariant = true))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with checkConstraints, " +
      "metadata doesn't contains any constraints",
    new Protocol(1, 7, Set().asJava, singleton("checkConstraints")),
    testMetadata())

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with checkConstraints, " +
      "metadata contains constraints",
    new Protocol(1, 7, Set().asJava, singleton("checkConstraints")),
    testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with generatedColumns, " +
      "metadata doesn't contains any generated columns",
    new Protocol(1, 7, Set().asJava, singleton("generatedColumns")),
    testMetadata())

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with generatedColumns, " +
      "metadata contains generated columns",
    new Protocol(1, 7, Set().asJava, singleton("generatedColumns")),
    testMetadata(includeGeneratedColumn = true))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with changeDataFeed, " +
      "metadata doesn't contains changeDataFeed",
    new Protocol(1, 7, Set().asJava, singleton("changeDataFeed")),
    testMetadata())

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with changeDataFeed, " +
      "metadata contains changeDataFeed",
    new Protocol(1, 7, Set().asJava, singleton("changeDataFeed")),
    testMetadata(tblProps = Map("delta.enableChangeDataFeed" -> "true")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with columnMapping, " +
      "metadata doesn't contains columnMapping",
    new Protocol(2, 7, Set().asJava, singleton("columnMapping")),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with columnMapping, " +
      "metadata contains columnMapping",
    new Protocol(2, 7, Set().asJava, singleton("columnMapping")),
    testMetadata(tblProps = Map("delta.columnMapping.mode" -> "id")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with identityColumns, " +
      "schema doesn't contains identity columns",
    new Protocol(2, 7, Set().asJava, singleton("identityColumns")),
    testMetadata())

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with identityColumns, " +
      "schema contains identity columns",
    new Protocol(2, 7, Set().asJava, singleton("identityColumns")),
    testMetadata(includeIdentityColumn = true))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with deletionVectors, " +
      "metadata doesn't contains deletionVectors",
    new Protocol(2, 7, Set().asJava, singleton("deletionVectors")),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with deletionVectors, " +
      "metadata contains deletionVectors",
    new Protocol(2, 7, Set().asJava, singleton("deletionVectors")),
    testMetadata(tblProps = Map("delta.enableDeletionVectors" -> "true")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with timestampNtz, " +
      "schema doesn't contains timestampNtz",
    new Protocol(3, 7, singleton("timestampNtz"), singleton("timestampNtz")),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with timestampNtz, " +
      "schema contains timestampNtz",
    new Protocol(3, 7, singleton("timestampNtz"), singleton("timestampNtz")),
    testMetadata(includeTimestampNtzTypeCol = true))

  Seq("typeWidening", "typeWidening-preview").foreach { feature =>
    checkWriteUnsupported(
      s"validateKernelCanWriteToTable: protocol 7 with $feature, " +
        s"metadata doesn't contains $feature",
      new Protocol(3, 7, singleton(feature), singleton(feature)),
      testMetadata())

    checkWriteUnsupported(
      s"validateKernelCanWriteToTable: protocol 7 with $feature, " +
        s"metadata contains $feature",
      new Protocol(3, 7, singleton(feature), singleton(feature)),
      testMetadata(tblProps = Map("delta.enableTypeWidening" -> "true")))
  }

  Seq("variantType", "variantType-preview").foreach { feature =>
    checkWriteUnsupported(
      s"validateKernelCanWriteToTable: protocol 7 with $feature, " +
        s"metadata doesn't contains $feature",
      new Protocol(3, 7, singleton(feature), singleton(feature)),
      testMetadata())

    checkWriteUnsupported(
      s"validateKernelCanWriteToTable: protocol 7 with $feature, " +
        s"metadata contains $feature",
      new Protocol(3, 7, singleton(feature), singleton(feature)),
      testMetadata(tblProps = Map("delta.enableTypeWidening" -> "true")))
  }

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with vacuumProtocolCheck, " +
      "metadata doesn't contains vacuumProtocolCheck",
    new Protocol(3, 7, singleton("vacuumProtocolCheck"), singleton("vacuumProtocolCheck")),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with domainMetadata",
    new Protocol(3, 7, emptySet(), singleton("domainMetadata")),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with rowTracking",
    new Protocol(3, 7, emptySet(), singleton("rowTracking")),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with inCommitTimestamp",
    new Protocol(3, 7, emptySet(), singleton("inCommitTimestamp")),
    testMetadata())

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with icebergCompatV2",
    new Protocol(3, 7, emptySet(), singleton("icebergCompatV2")),
    testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "true")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with v2Checkpoint, " +
      "metadata enables v2Checkpoint",
    new Protocol(3, 7, singleton("v2Checkpoint"), singleton("v2Checkpoint")),
    testMetadata(tblProps = Map("delta.checkpointPolicy" -> "v2")))

  checkWriteSupported(
    "validateKernelCanWriteToTable: protocol 7 with multiple features supported",
    new Protocol(
      3,
      7,
      Set("v2Checkpoint", "columnMapping").asJava,
      Set("v2Checkpoint", "columnMapping", "rowTracking", "domainMetadata").asJava),
    testMetadata(tblProps = Map(
      "delta.checkpointPolicy" -> "v2",
      "delta.columnMapping.mode" -> "id",
      "delta.enableRowTracking" -> "true")))

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with multiple features supported, " +
      "with one of the features not supported by Kernel for writing",
    new Protocol(
      3,
      7,
      Set("v2Checkpoint", "columnMapping", "invariants").asJava,
      Set("v2Checkpoint", "columnMapping", "invariants").asJava),
    testMetadata(
      includeInvariant = true, // unsupported feature
      tblProps = Map(
        "delta.checkpointPolicy" -> "v2",
        "delta.enableRowTracking" -> "true")))

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Test utility methods.                                                                       //
  /////////////////////////////////////////////////////////////////////////////////////////////////
  def checkWriteSupported(
      testDesc: String,
      protocol: Protocol,
      metadata: Metadata = testMetadata()): Unit = {
    test(testDesc) {
      validateKernelCanWriteToTable(protocol, metadata, "/test/table")
    }
  }

  def checkWriteUnsupported(
      testDesc: String,
      protocol: Protocol,
      metadata: Metadata = testMetadata()): Unit = {
    test(testDesc) {
      intercept[KernelException] {
        validateKernelCanWriteToTable(protocol, metadata, "/test/table")
      }
    }
  }

  def testMetadata(
      includeInvariant: Boolean = false,
      includeTimestampNtzTypeCol: Boolean = false,
      includeVariantTypeCol: Boolean = false,
      includeGeneratedColumn: Boolean = false,
      includeIdentityColumn: Boolean = false,
      tblProps: Map[String, String] = Map.empty): Metadata = {
    val testSchema = createTestSchema(
      includeInvariant,
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
      })
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
