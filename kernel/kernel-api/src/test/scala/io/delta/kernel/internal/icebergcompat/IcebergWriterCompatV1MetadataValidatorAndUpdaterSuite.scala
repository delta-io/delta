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
package io.delta.kernel.internal.icebergcompat

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.icebergcompat.IcebergWriterCompatV1MetadataValidatorAndUpdater.validateAndUpdateIcebergWriterCompatV1Metadata
import io.delta.kernel.internal.tablefeatures.TableFeature
import io.delta.kernel.internal.tablefeatures.TableFeatures.{COLUMN_MAPPING_RW_FEATURE, ICEBERG_COMPAT_V2_W_FEATURE, ICEBERG_WRITER_COMPAT_V1}
import io.delta.kernel.types.{ByteType, DataType, FieldMetadata, IntegerType, ShortType, StructType}

class IcebergWriterCompatV1MetadataValidatorAndUpdaterSuite
    extends IcebergCompatV2MetadataValidatorAndUpdaterSuiteBase {

  val icebergWriterCompatV1EnabledProps = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true")

  val icebergCompatV2EnabledProps = Map(
    TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true")

  val columnMappingIdModeProps = Map(
    TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")

  override def simpleTypesToSkip: Set[DataType] = Set(ByteType.BYTE, ShortType.SHORT)

  override def getCompatEnabledMetadata(
      schema: StructType,
      partCols: Seq[String] = Seq.empty): Metadata = {
    testMetadata(schema, partCols)
      .withMergedConfiguration((
        icebergWriterCompatV1EnabledProps ++ icebergCompatV2EnabledProps
          ++ columnMappingIdModeProps).asJava)
  }

  override def getCompatEnabledProtocol(tableFeatures: TableFeature*): Protocol = {
    testProtocol(tableFeatures ++
      Seq(ICEBERG_WRITER_COMPAT_V1, ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE): _*)
  }

  override def runValidateAndUpdateIcebergCompatV2Metadata(
      isNewTable: Boolean,
      metadata: Metadata,
      protocol: Protocol): Unit = {
    validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
  }

  test("checks are not enforced when table property is not enabled") {
    // Violate check by including BYTE type column
    val schema = new StructType().add("col", ByteType.BYTE)
    val metadata = testMetadata(schema)
    assert(!TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.fromMetadata(metadata))
    validateAndUpdateIcebergWriterCompatV1Metadata(
      true, /* isNewTable */
      metadata,
      getCompatEnabledProtocol())
  }

  /* --- UNSUPPORTED_TYPES_CHECK tests --- */

  Seq(ByteType.BYTE, ShortType.SHORT).foreach { unsupportedType =>
    Seq(true, false).foreach { isNewTable =>
      test(s"disallowed data types: $unsupportedType, new table = $isNewTable") {
        val schema = new StructType().add("col", unsupportedType)
        val metadata = getCompatEnabledMetadata(schema)
        val protocol = getCompatEnabledProtocol()
        val e = intercept[KernelException] {
          validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          s"icebergWriterCompatV1 does not support the data types: "))
      }
    }
  }

  /* --- CM_ID_MODE_ENABLED tests --- */

  Seq(true, false).foreach { isNewTable =>
    Seq(true, false).foreach { icebergCompatV2Enabled =>
      test(s"column mapping mode `id` is auto enabled when icebergWriterCompatV1 is enabled, " +
        s"isNewTable = $isNewTable, icebergCompatV2Enabled = $icebergCompatV2Enabled") {

        val tblProperties = icebergWriterCompatV1EnabledProps ++
          (if (icebergCompatV2Enabled) {
             icebergCompatV2EnabledProps
           } else {
             Map()
           })
        val metadata = testMetadata(cmTestSchema(), tblProps = tblProperties)
        val protocol = getCompatEnabledProtocol()

        assert(!metadata.getConfiguration.containsKey("delta.columnMapping.mode"))

        if (isNewTable) {
          val updatedMetadata =
            validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
          assert(updatedMetadata.isPresent)
          assert(updatedMetadata.get().getConfiguration.get("delta.columnMapping.mode") == "id")
        } else {
          val e = intercept[KernelException] {
            validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.contains(
            "The value 'none' for the property 'delta.columnMapping.mode' is" +
              " not compatible with icebergWriterCompatV1 requirements"))
        }
      }
    }
  }

  Seq("name", "none").foreach { cmMode =>
    Seq(true, false).foreach { isNewTable =>
      test(s"cannot enable icebergWriterCompatV1 with incompatible column mapping mode " +
        s"`$cmMode`, isNewTable = $isNewTable") {
        val tblProperties = icebergWriterCompatV1EnabledProps ++
          Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)

        val metadata = testMetadata(cmTestSchema(), tblProps = tblProperties)
        val protocol = getCompatEnabledProtocol()

        val e = intercept[KernelException] {
          validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          s"The value '$cmMode' for the property 'delta.columnMapping.mode' is" +
            " not compatible with icebergWriterCompatV1 requirements"))
      }
    }
  }

  /* --- ICEBERG_COMPAT_V2_ENABLED tests --- */

  Seq(true, false).foreach { isNewTable =>
    test(s"icebergCompatV2 is auto enabled when icebergWriterCompatV1 is enabled, " +
      s"isNewTable = $isNewTable") {
      val metadata = testMetadata(
        cmTestSchema(),
        tblProps = icebergWriterCompatV1EnabledProps ++ columnMappingIdModeProps)
      val protocol = getCompatEnabledProtocol()
      assert(!TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(metadata))

      if (isNewTable) {
        val updatedMetadata =
          validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
        assert(updatedMetadata.isPresent)
        assert(TableConfig.ICEBERG_COMPAT_V2_ENABLED.fromMetadata(updatedMetadata.get))
      } else {
        val e = intercept[KernelException] {
          validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          "The value 'false' for the property 'delta.enableIcebergCompatV2' is" +
            " not compatible with icebergWriterCompatV1 requirements"))
      }
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"cannot enable icebergWriterCompatV1 with icebergCompatV2 explicitly disabled, " +
      s"isNewTable = $isNewTable") {
      val tblProperties = icebergWriterCompatV1EnabledProps ++ columnMappingIdModeProps ++
        Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "false")

      val metadata = testMetadata(cmTestSchema(), tblProps = tblProperties)
      val protocol = getCompatEnabledProtocol()

      val e = intercept[KernelException] {
        validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "The value 'false' for the property 'delta.enableIcebergCompatV2' is" +
          " not compatible with icebergWriterCompatV1 requirements"))
    }
  }

  /* --- UNSUPPORTED_FEATURES_CHECK tests --- */

  test("all supported features are allowed") {
    val readerFeatures = Set("columnMapping", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck")
    // TODO add clustering here once it's added to Kernel
    // TODO add typeWidening and typeWidening-preview here once it's no longer blocked
    //  icebergCompatV2
    val writerFeatures = Set(
      "invariants",
      "appendOnly",
      "columnMapping",
      "icebergCompatV2",
      "icebergWriterCompatV1",
      "domainMetadata",
      "vacuumProtocolCheck",
      "v2Checkpoint",
      "inCommitTimestamp",
      // "clustering", add this to this test once we support clustering
      // "typeWidening", add this to this test once we support typeWidening
      // "typeWidening-preview", add this to this test once we support typeWidening
      "timestampNtz")
    val protocol = new Protocol(3, 7, readerFeatures.asJava, writerFeatures.asJava)
    val metadata = getCompatEnabledMetadata(cmTestSchema())
    validateAndUpdateIcebergWriterCompatV1Metadata(true /* isNewTable */, metadata, protocol)
    validateAndUpdateIcebergWriterCompatV1Metadata(false /* isNewTable */, metadata, protocol)
  }

  private def checkUnsupportedOrIncompatibleFeature(
      tableFeature: String,
      expectedErrorMessageContains: String): Unit = {
    val protocol = new Protocol(
      3,
      7,
      Set("columnMapping").asJava,
      Set(
        "columnMapping",
        "icebergCompatV2",
        "icebergWriterCompatV1",
        tableFeature).asJava)
    val metadata = getCompatEnabledMetadata(cmTestSchema())
    Seq(true, false).foreach { isNewTable =>
      val e = intercept[KernelException] {
        validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(expectedErrorMessageContains))
    }
  }

  Seq("clustering", "typeWidening", "typeWidening-preview").foreach {
    unsupportedCompatibleFeature =>
      test(s"cannot enable with compatible UNSUPPORTED feature $unsupportedCompatibleFeature") {
        // We add this test here so that it will fail when we add Kernel support for these features
        // When this happens -> add the feature to the test above
        checkUnsupportedOrIncompatibleFeature(
          unsupportedCompatibleFeature,
          "Unsupported Delta table feature: table requires feature " +
            s""""$unsupportedCompatibleFeature" which is unsupported by this version of """ +
            "Delta Kernel")
      }
  }

  Seq(
    "changeDataFeed",
    "checkConstraints",
    "identityColumns",
    "generatedColumns",
    // "defaultColumns", add this to this test once we support defaultColumns
    "rowTracking",
    // "collations", add this to this test once we support collations
    "variantType").foreach { incompatibleFeature =>
    test(s"cannot enable with incompatible feature $incompatibleFeature") {
      checkUnsupportedOrIncompatibleFeature(
        incompatibleFeature,
        s"Cannot enable table features [$incompatibleFeature] on a " +
          s"table with icebergWriterCompatV1")
    }
  }

  Seq("collations", "defaultColumns").foreach { unsupportedIncompatibleFeature =>
    test(s"cannot enable with incompatible UNSUPPORTED feature $unsupportedIncompatibleFeature") {
      // We add this test here so that it will fail when we add Kernel support for these features
      // When this happens -> add the feature to the test above
      checkUnsupportedOrIncompatibleFeature(
        unsupportedIncompatibleFeature,
        "Unsupported Delta table feature: table requires feature " +
          s""""$unsupportedIncompatibleFeature" which is unsupported by this version of """ +
          "Delta Kernel")
    }
  }

  /* --- INVARIANTS_INACTIVE_CHECK tests --- */
  Seq(true, false).foreach { isNewTable =>
    test(s"cannot enable with invariants active in the schema, isNewTable = $isNewTable") {
      val schema = new StructType()
        .add("c1", IntegerType.INTEGER)
        .add(
          "c2",
          IntegerType.INTEGER,
          FieldMetadata.builder()
            .putString("delta.invariants", "{\"expression\": { \"expression\": \"x > 3\"} }")
            .build())
      val metadata = getCompatEnabledMetadata(schema)
      val protocol = getCompatEnabledProtocol()
      val e = intercept[KernelException] {
        validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "Cannot enable table features [invariants] on a table with icebergWriterCompatV1"))
    }
  }

  /* --- requiredDependencyTableFeatures tests --- */
  Seq(
    ("columnMapping", "icebergCompatV2"),
    ("icebergCompatV2", "columnMapping")).foreach {
    case (featureToIncludeStr, missingFeatureStr) =>
      Seq(true, false).foreach { isNewTable =>
        test(s"protocol is missing required feature $missingFeatureStr, isNewTable = $isNewTable") {
          val metadata = getCompatEnabledMetadata(cmTestSchema())
          val readerFeatures: Set[String] = if ("columnMapping" == featureToIncludeStr) {
            Set("columnMapping")
          } else Set.empty
          val writerFeatures = Set("icebergWriterCompatV1", featureToIncludeStr)
          val protocol = new Protocol(3, 7, readerFeatures.asJava, writerFeatures.asJava)
          val e = intercept[KernelException] {
            validateAndUpdateIcebergWriterCompatV1Metadata(isNewTable, metadata, protocol)
          }
          // Since we run icebergCompatV2 validation as part of ICEBERG_COMPAT_V2_ENABLED.postProcess
          // we actually hit the missing feature error in the icebergCompatV2 checks first
          assert(e.getMessage.contains(
            s"icebergCompatV2: requires the feature '$missingFeatureStr' to be enabled"))
        }
      }
  }
}
