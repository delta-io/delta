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

import java.util
import java.util.{Collections, Optional}
import java.util.Collections.{emptySet, singleton}
import java.util.stream.Collectors.toList

import scala.collection.JavaConverters._

import io.delta.kernel.data.{ArrayValue, ColumnVector, MapValue}
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.{Format, Metadata, Protocol}
import io.delta.kernel.internal.tablefeatures.TableFeatures.{validateKernelCanReadTheTable, validateKernelCanWriteToTable, DOMAIN_METADATA_W_FEATURE, TABLE_FEATURES}
import io.delta.kernel.internal.util.InternalUtils.singletonStringColumnVector
import io.delta.kernel.internal.util.VectorUtils.buildColumnVector
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
    "inCommitTimestamp",
    "icebergWriterCompatV1")

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
    // Disable this until we have support to enable row tracking through metadata
    // ("rowTracking", testMetadata(tblProps = Map("delta.enableRowTracking" -> "true")), true),
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
      false),
    (
      "icebergWriterCompatV1",
      testMetadata(tblProps = Map("delta.enableIcebergWriterCompatV1" -> "true")),
      true),
    (
      "icebergWriterCompatV1",
      testMetadata(tblProps = Map("delta.enableIcebergWriterCompatV1" -> "false")),
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

  test("row tracking enable throguh metadata property is not supported") {
    val tableFeature = TableFeatures.getTableFeature("rowTracking")
    val ex = intercept[UnsupportedOperationException] {
      tableFeature.asInstanceOf[FeatureAutoEnabledByMetadata]
        .metadataRequiresFeatureToBeEnabled(
          testProtocol,
          testMetadata(tblProps = Map("delta.enableRowTracking" -> "true")))
    }
    assert(ex.getMessage.contains("Feature `rowTracking` is not yet supported in Kernel."))
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

    // checkConstraints, generatedColumns, identityColumns, invariants and changeDataFeed
    // are writable because the metadata has not been set the info that
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
      "identityColumns",
      "icebergWriterCompatV1")

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

  // Read is supported when all table readerWriter features are supported by the Kernel,
  // but the table has writeOnly table feature unknown to Kernel
  test("validateKernelCanReadTheTable: with writeOnly feature unknown to Kernel") {

    // legacy reader protocol version
    val protocol1 = new Protocol(1, 7, emptySet(), singleton("unknownFeature"))
    validateKernelCanReadTheTable(protocol1, "/test/table")

    // table feature supported reader version
    val protocol2 = new Protocol(
      3,
      7,
      Set("columnMapping", "timestampNtz").asJava,
      Set("columnMapping", "timestampNtz", "unknownFeature").asJava)
    validateKernelCanReadTheTable(protocol2, "/test/table")
  }

  test("validateKernelCanReadTheTable: unknown readerWriter feature to Kernel") {
    val protocol = new Protocol(3, 7, singleton("unknownFeature"), singleton("unknownFeature"))
    val ex = intercept[KernelException] {
      validateKernelCanReadTheTable(protocol, "/test/table")
    }
    assert(ex.getMessage.contains(
      "requires feature \"unknownFeature\" which is unsupported by this version of Delta Kernel"))
  }

  test("validateKernelCanReadTheTable: reader version > 3") {
    val protocol = new Protocol(4, 7, emptySet(), singleton("unknownFeature"))
    val ex = intercept[KernelException] {
      validateKernelCanReadTheTable(protocol, "/test/table")
    }
    assert(ex.getMessage.contains(
      "requires reader version 4 which is unsupported by this version of Delta Kernel"))
  }

  // Writes
  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 8", // beyond the table feature writer version
    new Protocol(3, 8))

  checkWriteUnsupported(
    // beyond the table feature reader/writer version
    "validateKernelCanWriteToTable: protocol 4, 8",
    new Protocol(4, 8))

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
      testMetadata(includeInvariant = true))
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
    "validateKernelCanWriteToTable: protocol 7 with icebergWriterCompatV1",
    new Protocol(3, 7, emptySet(), singleton("icebergWriterCompatV1")),
    testMetadata(tblProps = Map("delta.enableIcebergWriterCompatV1" -> "true")))

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

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with unknown writerOnly feature",
    new Protocol(1, 7, emptySet(), singleton("unknownWriterOnlyFeature")),
    testMetadata())

  checkWriteUnsupported(
    "validateKernelCanWriteToTable: protocol 7 with unknown readerWriter feature",
    new Protocol(
      3,
      7,
      singleton("unknownWriterOnlyFeature"),
      singleton("unknownWriterOnlyFeature")),
    testMetadata())

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests for autoUpgradeProtocolBasedOnMetadata                                                //
  /////////////////////////////////////////////////////////////////////////////////////////////////
  Seq(
    // Test format:
    //  new metadata,
    //  current protocol,
    //  expected protocol,
    //  expected new features added
    (
      testMetadata(tblProps = Map("delta.appendOnly" -> "true")),
      new Protocol(1, 1),
      new Protocol(1, 7, emptySet(), set("appendOnly")),
      set("appendOnly")),
    (
      testMetadata(includeInvariant = true),
      new Protocol(1, 1),
      new Protocol(1, 7, emptySet(), set("invariants")),
      set("invariants")),
    (
      testMetadata(includeInvariant = true, tblProps = Map("delta.appendOnly" -> "true")),
      new Protocol(1, 1),
      new Protocol(1, 2), // (1, 2) covers both appendOnly and invariants
      Set("invariants", "appendOnly").asJava),
    (
      testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")),
      new Protocol(1, 1),
      new Protocol(1, 7, emptySet(), set("checkConstraints")),
      set("checkConstraints")),
    (
      testMetadata(
        includeInvariant = true,
        tblProps = Map("delta.appendOnly" -> "true", "delta.constraints.a" -> "a = b")),
      new Protocol(1, 1),
      new Protocol(1, 3),
      set("appendOnly", "checkConstraints", "invariants")),
    (
      testMetadata(tblProps = Map("delta.constraints.a" -> "a = b")),
      new Protocol(1, 2),
      new Protocol(1, 3), // (1, 3) covers all: appendOnly, invariants, checkConstraints
      set("checkConstraints")),
    (
      testMetadata(tblProps = Map("delta.enableChangeDataFeed" -> "true")),
      new Protocol(1, 1),
      new Protocol(1, 7, emptySet(), set("changeDataFeed")),
      set("changeDataFeed")),
    (
      testMetadata(includeGeneratedColumn = true),
      new Protocol(1, 1),
      new Protocol(1, 7, emptySet(), set("generatedColumns")),
      set("generatedColumns")),
    (
      testMetadata(
        includeGeneratedColumn = true,
        tblProps = Map("delta.enableChangeDataFeed" -> "true")),
      new Protocol(1, 1),
      new Protocol(1, 7, emptySet(), set("generatedColumns", "changeDataFeed")),
      set("generatedColumns", "changeDataFeed")),
    (
      testMetadata(
        includeGeneratedColumn = true,
        tblProps = Map("delta.enableChangeDataFeed" -> "true")),
      new Protocol(1, 2),
      new Protocol(
        1,
        7,
        set(),
        set("generatedColumns", "changeDataFeed", "appendOnly", "invariants")),
      set("generatedColumns", "changeDataFeed")),
    (
      testMetadata(
        includeGeneratedColumn = true,
        tblProps = Map("delta.enableChangeDataFeed" -> "true")),
      new Protocol(1, 3),
      new Protocol(1, 4), // 4 - implicitly supports all features
      set("generatedColumns", "changeDataFeed")),
    (
      testMetadata(tblProps = Map("delta.columnMapping.mode" -> "name")),
      new Protocol(1, 1),
      new Protocol(
        2,
        7,
        set(),
        set("columnMapping")),
      set("columnMapping")),
    (
      testMetadata(tblProps = Map("delta.columnMapping.mode" -> "name")),
      new Protocol(1, 2),
      new Protocol(
        2,
        7,
        set(),
        set("columnMapping", "appendOnly", "invariants")),
      set("columnMapping")),
    (
      testMetadata(tblProps = Map("delta.columnMapping.mode" -> "name")),
      new Protocol(1, 3),
      new Protocol(
        2,
        7,
        set(),
        set("columnMapping", "appendOnly", "invariants", "checkConstraints")),
      set("columnMapping")),
    (
      testMetadata(tblProps = Map("delta.columnMapping.mode" -> "name")),
      new Protocol(1, 4),
      new Protocol(2, 5), // implicitly supports all features
      set("columnMapping")),
    (
      testMetadata(includeIdentityColumn = true),
      new Protocol(1, 1),
      new Protocol(
        1,
        7,
        set(),
        set("identityColumns")),
      set("identityColumns")),
    (
      testMetadata(includeIdentityColumn = true),
      new Protocol(1, 2),
      new Protocol(
        1,
        7,
        set(),
        set("identityColumns", "appendOnly", "invariants")),
      set("identityColumns")),
    (
      testMetadata(includeIdentityColumn = true),
      new Protocol(1, 3),
      new Protocol(
        1,
        7,
        set(),
        set("identityColumns", "appendOnly", "invariants", "checkConstraints")),
      set("identityColumns")),
    (
      testMetadata(includeIdentityColumn = true),
      new Protocol(2, 5),
      new Protocol(2, 6), // implicitly supports all features
      set("identityColumns")),
    (
      testMetadata(includeTimestampNtzTypeCol = true),
      new Protocol(1, 1),
      new Protocol(
        3,
        7,
        set("timestampNtz"),
        set("timestampNtz")),
      set("timestampNtz")),
    (
      testMetadata(includeTimestampNtzTypeCol = true),
      new Protocol(1, 2),
      new Protocol(
        3,
        7,
        set("timestampNtz"),
        set("timestampNtz", "appendOnly", "invariants")),
      set("timestampNtz")),
    (
      testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "true")),
      new Protocol(1, 2),
      new Protocol(
        2,
        7,
        set(),
        set("columnMapping", "appendOnly", "invariants", "icebergCompatV2")),
      set("icebergCompatV2", "columnMapping")),
    (
      testMetadata(tblProps =
        Map("delta.enableIcebergCompatV2" -> "true", "delta.enableDeletionVectors" -> "true")),
      new Protocol(2, 5),
      new Protocol(
        3,
        7,
        set("columnMapping", "deletionVectors"),
        set(
          "columnMapping",
          "appendOnly",
          "invariants",
          "icebergCompatV2",
          "checkConstraints",
          "deletionVectors",
          "generatedColumns",
          "changeDataFeed")),
      set("icebergCompatV2", "deletionVectors")),
    (
      testMetadata(tblProps =
        Map("delta.enableIcebergCompatV2" -> "true")),
      new Protocol(3, 7, set("columnMapping", "deletionVectors"), set("columnMapping")),
      new Protocol(
        3,
        7,
        set("columnMapping", "deletionVectors"),
        set("columnMapping", "icebergCompatV2", "deletionVectors")),
      set("icebergCompatV2")),
    (
      testMetadata(tblProps = Map("delta.enableIcebergWriterCompatV1" -> "true")),
      new Protocol(1, 2),
      new Protocol(
        2,
        7,
        set(),
        set(
          "columnMapping",
          "appendOnly",
          "invariants",
          "icebergCompatV2",
          "icebergWriterCompatV1")),
      set("icebergCompatV2", "columnMapping", "icebergWriterCompatV1"))).foreach {
    case (newMetadata, currentProtocol, expectedProtocol, expectedNewFeatures) =>
      test(s"autoUpgradeProtocolBasedOnMetadata:" +
        s"$currentProtocol -> $expectedProtocol, $expectedNewFeatures") {

        // try with domainMetadata disabled
        val newProtocolAndNewFeaturesEnabled =
          TableFeatures.autoUpgradeProtocolBasedOnMetadata(
            newMetadata,
            /* needDomainMetadataSupport = */ false,
            currentProtocol)
        assert(newProtocolAndNewFeaturesEnabled.isPresent, "expected protocol upgrade")

        val newProtocol = newProtocolAndNewFeaturesEnabled.get()._1
        val newFeaturesEnabled = newProtocolAndNewFeaturesEnabled.get()._2

        assert(newProtocol == expectedProtocol)
        assert(newFeaturesEnabled.asScala.map(_.featureName()).toSet ===
          expectedNewFeatures.asScala)

        // try with domainMetadata enabled
        val newProtocolAndNewFeaturesEnabledWithDM =
          TableFeatures.autoUpgradeProtocolBasedOnMetadata(
            newMetadata,
            /* needDomainMetadataSupport = */ true,
            currentProtocol)

        assert(newProtocolAndNewFeaturesEnabledWithDM.isPresent, "expected protocol upgrade")

        val newProtocolWithDM = newProtocolAndNewFeaturesEnabledWithDM.get()._1
        val newFeaturesEnabledWithDM = newProtocolAndNewFeaturesEnabledWithDM.get()._2

        // reader version should be same as expected protocol as the domain metadata
        // is a writerOnly feature
        assert(newProtocolWithDM.getMinReaderVersion == expectedProtocol.getMinReaderVersion)
        // should be 7 as domainMetadata is enabled
        assert(newProtocolWithDM.getMinWriterVersion === 7)
        assert(newFeaturesEnabledWithDM.asScala.map(_.featureName()).toSet ===
          expectedNewFeatures.asScala ++ Set("domainMetadata"))
        assert(newProtocolWithDM.getImplicitlyAndExplicitlySupportedFeatures.asScala ===
          expectedProtocol.getImplicitlyAndExplicitlySupportedFeatures.asScala
          ++ Set(DOMAIN_METADATA_W_FEATURE))
      }
  }

  // No-op upgrade
  Seq(
    // Test format: new metadata, current protocol
    (testMetadata(), new Protocol(1, 1)),
    (
      // try to enable the writer that is already supported on a protocol
      // that is of legacy protocol
      testMetadata(tblProps = Map("delta.appendOnly" -> "true")),
      new Protocol(1, 7, emptySet(), set("appendOnly"))),
    (
      // try to enable the writer that is already supported on a protocol
      // that is of legacy protocol
      testMetadata(tblProps = Map("delta.appendOnly" -> "true", "delta.constraints.a" -> "a = b")),
      new Protocol(1, 3)),
    (
      // try to enable the reader writer feature that is already supported on a protocol
      // that is of legacy protocol
      testMetadata(tblProps = Map("delta.columnMapping.mode" -> "name")),
      new Protocol(2, 5)),
    (
      // try to enable the feature that is already supported on a protocol
      // that is of partial (writer only) table feature support
      testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "true")),
      new Protocol(2, 7, set(), set("columnMapping", "icebergCompatV2"))),
    (
      // try to enable the feature that is already supported on a protocol
      // that is of table feature support
      testMetadata(tblProps = Map("delta.enableIcebergCompatV2" -> "true")),
      new Protocol(
        3,
        7,
        set("columnMapping", "deletionVectors"),
        set("columnMapping", "deletionVectors", "icebergCompatV2")))).foreach {
    case (newMetadata, currentProtocol) =>
      test(s"autoUpgradeProtocolBasedOnMetadata: no-op upgrade: $currentProtocol") {
        val newProtocolAndNewFeaturesEnabled =
          TableFeatures.autoUpgradeProtocolBasedOnMetadata(
            newMetadata,
            /* needDomainMetadataSupport = */ false,
            currentProtocol)
        assert(!newProtocolAndNewFeaturesEnabled.isPresent, "expected no-op upgrade")
      }
  }

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
        override def getKeys: ColumnVector =
          buildColumnVector(tblProps.toSeq.map(_._1).asJava, StringType.STRING)
        override def getValues: ColumnVector =
          buildColumnVector(tblProps.toSeq.map(_._2).asJava, StringType.STRING)
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

  private def set(elements: String*): java.util.Set[String] = {
    Set(elements: _*).asJava
  }
}
