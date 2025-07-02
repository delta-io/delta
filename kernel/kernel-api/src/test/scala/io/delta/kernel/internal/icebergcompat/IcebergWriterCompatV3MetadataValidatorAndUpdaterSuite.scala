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

import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.icebergcompat.IcebergWriterCompatV3MetadataValidatorAndUpdater.validateAndUpdateIcebergWriterCompatV3Metadata
import io.delta.kernel.internal.tablefeatures.TableFeature
import io.delta.kernel.internal.tablefeatures.TableFeatures._
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.types.{ByteType, DataType, DateType, FieldMetadata, IntegerType, LongType, ShortType, StructField, StructType, TimestampNTZType, TypeChange}

class IcebergWriterCompatV3MetadataValidatorAndUpdaterSuite
    extends IcebergCompatV3MetadataValidatorAndUpdaterSuiteBase {
  override def validateAndUpdateIcebergCompatMetadata(
      isNewTable: Boolean,
      metadata: Metadata,
      protocol: Protocol): Optional[Metadata] = {
    validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
  }

  val icebergWriterCompatV3EnabledProps = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED.getKey -> "true")

  val icebergCompatV3EnabledProps = Map(
    TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "true")

  val deletionVectorsEnabledProps = Map(
    TableConfig.DELETION_VECTORS_CREATION_ENABLED.getKey -> "true")

  val variantShreddingEnabledProps = Map(
    TableConfig.VARIANT_SHREDDING_ENABLED.getKey -> "true")

  val columnMappingIdModeProps = Map(
    TableConfig.COLUMN_MAPPING_MODE.getKey -> "id")

  val rowTrackingEnabledProps = Map(
    TableConfig.ROW_TRACKING_ENABLED.getKey -> "true")

  override def getCompatEnabledMetadata(
      schema: StructType,
      columnMappingMode: String = "id",
      partCols: Seq[String] = Seq.empty): Metadata = {
    val result = testMetadata(schema, partCols)
      .withMergedConfiguration((
        icebergWriterCompatV3EnabledProps ++
          icebergCompatV3EnabledProps ++
          columnMappingIdModeProps ++
          rowTrackingEnabledProps).asJava)

    result
  }

  override def getCompatEnabledProtocol(tableFeatures: TableFeature*): Protocol = {
    testProtocol(tableFeatures ++
      Seq(
        ICEBERG_WRITER_COMPAT_V3,
        ICEBERG_COMPAT_V3_W_FEATURE,
        DELETION_VECTORS_RW_FEATURE,
        VARIANT_RW_FEATURE,
        VARIANT_SHREDDING_PREVIEW_RW_FEATURE,
        VARIANT_RW_PREVIEW_FEATURE,
        ROW_TRACKING_W_FEATURE,
        COLUMN_MAPPING_RW_FEATURE): _*)
  }

  /* icebergWriterCompatV3 restricts additional types allowed by icebergCompatV3 */
  override def simpleTypesToSkip: Set[DataType] = Set(ByteType.BYTE, ShortType.SHORT)

  private def checkUnsupportedOrIncompatibleFeature(
      tableFeature: String,
      expectedErrorMessageContains: String): Unit = {
    val protocol = new Protocol(
      3,
      7,
      Set("columnMapping", "rowTracking").asJava,
      Set(
        "columnMapping",
        "icebergCompatV3",
        "icebergWriterCompatV3",
        "deletionVectors",
        "rowTracking",
        "variantType",
        "variantType-preview",
        "variantShredding-preview",
        tableFeature).asJava)
    val metadata = getCompatEnabledMetadata(cmTestSchema())
    Seq(true, false).foreach { isNewTable =>
      val e = intercept[KernelException] {
        validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(expectedErrorMessageContains))
    }
  }

  private def testIncompatibleActiveLegacyFeature(
      activeFeatureMetadata: Metadata,
      tableFeature: String): Unit = {
    Seq(true, false).foreach { isNewTable =>
      test(s"cannot enable with $tableFeature active, isNewTable = $isNewTable") {
        val e = intercept[KernelException] {
          validateAndUpdateIcebergWriterCompatV3Metadata(
            isNewTable,
            activeFeatureMetadata,
            getCompatEnabledProtocol())
        }
        assert(e.getMessage.contains(
          s"Table features [$tableFeature] are incompatible with icebergWriterCompatV3"))
      }
    }
  }

  /* --- CM_ID_MODE_ENABLED and PHYSICAL_NAMES_MATCH_FIELD_IDS_CHECK tests --- */

  Seq(true, false).foreach { isNewTable =>
    Seq(true, false).foreach { icebergCompatV3Enabled =>
      test(s"column mapping mode `id` is auto enabled when icebergWriterCompatV3 is enabled, " +
        s"isNewTable = $isNewTable, icebergCompatV3Enabled = $icebergCompatV3Enabled") {

        val tblProperties = icebergWriterCompatV3EnabledProps ++
          (if (icebergCompatV3Enabled) {
             icebergCompatV3EnabledProps
           } else {
             Map()
           })
        val metadata = testMetadata(cmTestSchema(), tblProps = tblProperties)
        val protocol = getCompatEnabledProtocol()

        assert(!metadata.getConfiguration.containsKey("delta.columnMapping.mode"))

        if (isNewTable) {
          val updatedMetadata =
            validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
          assert(updatedMetadata.isPresent)
          assert(updatedMetadata.get().getConfiguration.get("delta.columnMapping.mode") == "id")
          // We correctly populate the column mapping metadata
          verifyCMTestSchemaHasValidColumnMappingInfo(
            updatedMetadata.get(),
            isNewTable,
            true,
            true)
        } else {
          val e = intercept[KernelException] {
            validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
          }
          assert(e.getMessage.contains(
            "The value 'none' for the property 'delta.columnMapping.mode' is" +
              " not compatible with icebergWriterCompatV3 requirements"))
        }
      }
    }
  }

  test("checks are not enforced when table property is not enabled") {
    // Violate check by including BYTE type column
    val schema = new StructType().add("col", ByteType.BYTE)
    val metadata = testMetadata(schema)
    assert(!TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED.fromMetadata(metadata))
    validateAndUpdateIcebergWriterCompatV3Metadata(
      true, /* isNewTable */
      metadata,
      getCompatEnabledProtocol())
  }

  Seq("name", "none").foreach { cmMode =>
    Seq(true, false).foreach { isNewTable =>
      test(s"cannot enable icebergWriterCompatV3 with incompatible column mapping mode " +
        s"`$cmMode`, isNewTable = $isNewTable") {
        val tblProperties = icebergWriterCompatV3EnabledProps ++
          Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> cmMode)

        val metadata = testMetadata(cmTestSchema(), tblProps = tblProperties)
        val protocol = getCompatEnabledProtocol()

        val e = intercept[KernelException] {
          validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          s"The value '$cmMode' for the property 'delta.columnMapping.mode' is" +
            " not compatible with icebergWriterCompatV3 requirements"))
      }
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"cannot set physicalName to anything other than col-{fieldId}, isNewTable=$isNewTable") {
      val schema = new StructType()
        .add(
          "c1",
          IntegerType.INTEGER,
          FieldMetadata.builder()
            .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1)
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "c1")
            .build())

      val metadata = getCompatEnabledMetadata(schema)
      val protocol = getCompatEnabledProtocol()

      val e = intercept[KernelException] {
        validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "requires column mapping field physical names be equal to "
          + "'col-[fieldId]', but this is not true for the following fields " +
          "[c1(physicalName='c1', columnId=1)]"))
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"can provide correct physicalName=col-{fieldId}, isNewTable=$isNewTable") {
      val schema = new StructType()
        .add(
          "c1",
          IntegerType.INTEGER,
          FieldMetadata.builder()
            .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1)
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "col-1")
            .build())

      val metadata = getCompatEnabledMetadata(schema)
        .withMergedConfiguration(Map(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY -> "1").asJava)
      val protocol = getCompatEnabledProtocol()

      val updatedMetadata =
        validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
      // No metadata update happens
      assert(!updatedMetadata.isPresent)
    }
  }

  /* --- UNSUPPORTED_TYPES_CHECK tests --- */

  Seq(ByteType.BYTE, ShortType.SHORT).foreach { unsupportedType =>
    Seq(true, false).foreach { isNewTable =>
      test(s"disallowed data types: $unsupportedType, new table = $isNewTable") {
        val schema = new StructType().add("col", unsupportedType)
        val metadata = getCompatEnabledMetadata(schema)
        val protocol = getCompatEnabledProtocol()
        val e = intercept[KernelException] {
          validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          s"icebergWriterCompatV3 does not support the data types: "))
      }
    }
  }

  test("compatible type widening is allowed with icebergWriterCompatV3") {
    val schema = new StructType()
      .add(
        new StructField(
          "intToLong",
          IntegerType.INTEGER,
          true,
          FieldMetadata.builder()
            .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1)
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "col-1")
            .build()).withTypeChanges(Seq(new TypeChange(
          IntegerType.INTEGER,
          LongType.LONG)).asJava))

    val metadata = getCompatEnabledMetadata(schema)
      .withMergedConfiguration(Map(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY -> "1").asJava)
    val protocol = getCompatEnabledProtocol(TYPE_WIDENING_RW_FEATURE)

    validateAndUpdateIcebergCompatMetadata(false, metadata, protocol)
  }

  test("incompatible type widening throws exception with icebergWriterCompatV3") {
    val schema = new StructType()
      .add(
        new StructField(
          "dateToTimestamp",
          TimestampNTZType.TIMESTAMP_NTZ,
          true,
          FieldMetadata.builder()
            .putLong(ColumnMapping.COLUMN_MAPPING_ID_KEY, 1)
            .putString(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY, "col-1")
            .build()).withTypeChanges(
          Seq(new TypeChange(DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ)).asJava))

    val metadata = getCompatEnabledMetadata(schema)
      .withMergedConfiguration(Map(ColumnMapping.COLUMN_MAPPING_MAX_COLUMN_ID_KEY -> "1").asJava)
    val protocol = getCompatEnabledProtocol(TYPE_WIDENING_RW_FEATURE, ROW_TRACKING_W_FEATURE)

    val e = intercept[KernelException] {
      validateAndUpdateIcebergCompatMetadata(false, metadata, protocol)
    }
    assert(e.getMessage.contains("icebergCompatV3 does not support type widening present in table"))
  }

  /* --- ICEBERG_COMPAT_V3_ENABLED tests --- */

  Seq(true, false).foreach { isNewTable =>
    test(s"icebergCompatV3 is auto enabled when icebergWriterCompatV3 is enabled, " +
      s"isNewTable = $isNewTable") {
      val metadata = testMetadata(
        cmTestSchema(),
        tblProps =
          icebergWriterCompatV3EnabledProps ++ columnMappingIdModeProps ++ rowTrackingEnabledProps)
      val protocol = getCompatEnabledProtocol()
      assert(!TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(metadata))

      if (isNewTable) {
        val updatedMetadata =
          validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
        assert(updatedMetadata.isPresent)
        assert(TableConfig.ICEBERG_COMPAT_V3_ENABLED.fromMetadata(updatedMetadata.get))
      } else {
        val e = intercept[KernelException] {
          validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          "The value 'false' for the property 'delta.enableIcebergCompatV3' is" +
            " not compatible with icebergWriterCompatV3 requirements"))
      }
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"cannot enable icebergWriterCompatV3 with icebergCompatV3 explicitly disabled, " +
      s"isNewTable = $isNewTable") {
      val tblProperties = icebergWriterCompatV3EnabledProps ++ columnMappingIdModeProps ++
        rowTrackingEnabledProps ++
        Map(TableConfig.ICEBERG_COMPAT_V3_ENABLED.getKey -> "false")

      val metadata = testMetadata(cmTestSchema(), tblProps = tblProperties)
      val protocol = getCompatEnabledProtocol()

      val e = intercept[KernelException] {
        validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "The value 'false' for the property 'delta.enableIcebergCompatV3' is" +
          " not compatible with icebergWriterCompatV3 requirements"))
    }
  }
  /* --- UNSUPPORTED_FEATURES_CHECK tests --- */

  test("all supported features are allowed") {
    val readerFeatures =
      Set("columnMapping", "timestampNtz", "v2Checkpoint", "vacuumProtocolCheck", "rowTracking")
    val writerFeatures = Set(
      // Legacy incompatible features (allowed as long as they are inactive)
      "invariants",
      "checkConstraints",
      "changeDataFeed",
      "identityColumns",
      "generatedColumns",
      // Compatible table features
      "appendOnly",
      "columnMapping",
      "icebergCompatV3",
      "icebergWriterCompatV3",
      "domainMetadata",
      "vacuumProtocolCheck",
      "v2Checkpoint",
      "inCommitTimestamp",
      "clustering",
      "typeWidening",
      "typeWidening-preview",
      "timestampNtz",
      "deletionVectors",
      "rowTracking",
      "variantType",
      "variantType-preview",
      "variantShredding-preview")
    val protocol = new Protocol(3, 7, readerFeatures.asJava, writerFeatures.asJava)
    val metadata = getCompatEnabledMetadata(cmTestSchema())
    validateAndUpdateIcebergWriterCompatV3Metadata(true, metadata, protocol)
    validateAndUpdateIcebergWriterCompatV3Metadata(false, metadata, protocol)
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
  testIncompatibleActiveLegacyFeature(
    getCompatEnabledMetadata(new StructType()
      .add("c1", IntegerType.INTEGER)
      .add(
        "c2",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putString("delta.invariants", "{\"expression\": { \"expression\": \"x > 3\"} }")
          .build())),
    "invariants")

  /* --- CHANGE_DATA_FEED_INACTIVE_CHECK tests --- */
  testIncompatibleActiveLegacyFeature(
    getCompatEnabledMetadata(cmTestSchema())
      .withMergedConfiguration(Map(TableConfig.CHANGE_DATA_FEED_ENABLED.getKey -> "true").asJava),
    "changeDataFeed")

  /* --- CHECK_CONSTRAINTS_INACTIVE_CHECK tests --- */
  testIncompatibleActiveLegacyFeature(
    getCompatEnabledMetadata(cmTestSchema())
      .withMergedConfiguration(Map("delta.constraints.a" -> "a = b").asJava),
    "checkConstraints")

  /* --- IDENTITY_COLUMNS_INACTIVE_CHECK tests --- */
  testIncompatibleActiveLegacyFeature(
    getCompatEnabledMetadata(new StructType()
      .add("c1", IntegerType.INTEGER)
      .add(
        "c2",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putLong("delta.identity.start", 1L)
          .putLong("delta.identity.step", 2L)
          .putBoolean("delta.identity.allowExplicitInsert", true)
          .build())),
    "identityColumns")

  /* --- GENERATED_COLUMNS_INACTIVE_CHECK tests --- */
  testIncompatibleActiveLegacyFeature(
    getCompatEnabledMetadata(new StructType()
      .add("c1", IntegerType.INTEGER)
      .add(
        "c2",
        IntegerType.INTEGER,
        FieldMetadata.builder()
          .putString("delta.generationExpression", "{\"expression\": \"c1 + 1\"}")
          .build())),
    "generatedColumns")

  /* --- requiredDependencyTableFeatures tests --- */
  Seq(
    ("columnMapping", "icebergCompatV3"),
    ("icebergCompatV3", "columnMapping"),
    ("rowTracking", "icebergCompatV3"),
    ("deletionVectors", "icebergCompatV3"),
    ("variantType", "icebergCompatV3")).foreach {
    case (featureToIncludeStr, missingFeatureStr) =>
      Seq(true, false).foreach { isNewTable =>
        test(
          s"protocol is missing required feature $missingFeatureStr when " +
            s"only $featureToIncludeStr present, isNewTable = $isNewTable") {
          val metadata = getCompatEnabledMetadata(cmTestSchema())
          val readerFeatures: Set[String] =
            if (Set("columnMapping", "rowTracking").contains(featureToIncludeStr)) {
              Set(featureToIncludeStr)
            } else Set.empty
          val writerFeatures = Set("icebergWriterCompatV3", featureToIncludeStr)
          val protocol = new Protocol(3, 7, readerFeatures.asJava, writerFeatures.asJava)
          val e = intercept[KernelException] {
            validateAndUpdateIcebergWriterCompatV3Metadata(isNewTable, metadata, protocol)
          }
          // Since we run icebergCompatV3 validation as part of
          // ICEBERG_COMPAT_V3_ENABLED.postProcess we actually hit the missing feature error in the
          // icebergCompatV3 checks first
          assert(e.getMessage.contains(
            s"icebergCompatV3: requires the feature '$missingFeatureStr' to be enabled"))
        }
      }
  }
}
