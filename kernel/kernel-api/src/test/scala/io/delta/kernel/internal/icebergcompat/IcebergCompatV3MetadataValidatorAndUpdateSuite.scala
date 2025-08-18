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
import io.delta.kernel.internal.icebergcompat.IcebergCompatV3MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV3Metadata
import io.delta.kernel.internal.tablefeatures.TableFeature
import io.delta.kernel.internal.tablefeatures.TableFeatures.{COLUMN_MAPPING_RW_FEATURE, ICEBERG_COMPAT_V3_W_FEATURE, ROW_TRACKING_W_FEATURE, TYPE_WIDENING_RW_FEATURE}
import io.delta.kernel.types._

import org.assertj.core.util.Maps

trait IcebergCompatV3MetadataValidatorAndUpdaterSuiteBase
    extends IcebergCompatMetadataValidatorAndUpdaterSuiteBase {

  override def icebergCompatVersion: String = "V3"

  override def supportedDataColumnTypes: Set[DataType] =
    IcebergCompatMetadataValidatorAndUpdaterSuiteBase.SIMPLE_TYPES ++
      IcebergCompatMetadataValidatorAndUpdaterSuiteBase.COMPLEX_TYPES

  override def unsupportedDataColumnTypes: Set[DataType] = Set(VariantType.VARIANT)

  override def unsupportedPartitionColumnTypes: Set[DataType] =
    IcebergCompatMetadataValidatorAndUpdaterSuiteBase.COMPLEX_TYPES

  override def isDeletionVectorsSupported: Boolean = true

  override def withIcebergCompatAndCMEnabled(
      schema: StructType,
      columnMappingMode: String = "name",
      partCols: Seq[String] = Seq.empty): Metadata = {
    testMetadata(
      schema,
      partCols).withIcebergCompatV3AndCMEnabled(columnMappingMode).withMergedConfiguration(
      Maps.newHashMap(TableConfig.ROW_TRACKING_ENABLED.getKey, "true"))
  }
}

class IcebergCompatV3MetadataValidatorAndUpdaterSuite
    extends IcebergCompatV3MetadataValidatorAndUpdaterSuiteBase {

  override def simpleTypesToSkip: Set[DataType] = Set.empty

  override def getCompatEnabledMetadata(
      schema: StructType,
      columnMappingMode: String = "name",
      partCols: Seq[String] = Seq.empty): Metadata = {
    testMetadata(schema, partCols)
      .withIcebergCompatV3AndCMEnabled(columnMappingMode).withMergedConfiguration(
        Maps.newHashMap(TableConfig.ROW_TRACKING_ENABLED.getKey, "true"))
  }

  override def getCompatEnabledProtocol(tableFeatures: TableFeature*): Protocol = {
    testProtocol(tableFeatures ++ Seq(
      ICEBERG_COMPAT_V3_W_FEATURE,
      COLUMN_MAPPING_RW_FEATURE,
      ROW_TRACKING_W_FEATURE): _*)
  }

  override def validateAndUpdateIcebergCompatMetadata(
      isNewTable: Boolean,
      metadata: Metadata,
      protocol: Protocol): Optional[Metadata] = {
    validateAndUpdateIcebergCompatV3Metadata(isNewTable, metadata, protocol)
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"protocol is missing required column mapping feature, isNewTable $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = getCompatEnabledMetadata(schema)
      val protocol =
        new Protocol(3, 7, Set.empty.asJava, Set("icebergCompatV3", "rowTracking").asJava)
      val e = intercept[KernelException] {
        validateAndUpdateIcebergCompatV3Metadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        "icebergCompatV3: requires the feature 'columnMapping' to be enabled."))
    }
  }

  Seq("id", "name").foreach { existingCMMode =>
    Seq(true, false).foreach { isNewTable =>
      test(s"existing column mapping mode `$existingCMMode` is preserved " +
        s"when icebergCompat is enabled, isNewTable = $isNewTable") {
        val metadata = getCompatEnabledMetadata(cmTestSchema(), columnMappingMode = existingCMMode)
        val protocol = getCompatEnabledProtocol()

        assert(metadata.getConfiguration.get("delta.columnMapping.mode") === existingCMMode)

        val updatedMetadata =
          validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
        // No metadata update is needed since already compatible column mapping mode
        assert(!updatedMetadata.isPresent)
      }
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"column mapping and row tracking are auto enabled when icebergCompatV3 is enabled, " +
      s"isNewTable = $isNewTable") {
      val metadata = testMetadata(cmTestSchema()).withIcebergCompatV3Enabled
      val protocol =
        testProtocol(ICEBERG_COMPAT_V3_W_FEATURE, COLUMN_MAPPING_RW_FEATURE, ROW_TRACKING_W_FEATURE)

      assert(!metadata.getConfiguration.containsKey("delta.columnMapping.mode"))
      assert(!metadata.getConfiguration.containsKey("delta.rowTracking.enabled"))

      if (isNewTable) {
        val updatedMetadata =
          validateAndUpdateIcebergCompatV3Metadata(isNewTable, metadata, protocol)
        assert(updatedMetadata.isPresent)
        assert(updatedMetadata.get().getConfiguration.get("delta.columnMapping.mode") == "name")
        assert(TableConfig.ROW_TRACKING_ENABLED.fromMetadata(updatedMetadata.get()))
      } else {
        val e = intercept[KernelException] {
          validateAndUpdateIcebergCompatV3Metadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          "The value 'none' for the property 'delta.columnMapping.mode' is" +
            " not compatible with icebergCompatV3 requirements"))
      }
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(
      s"can't enable icebergCompatV3 on a table with icebergCompatV2 enabled, " +
        s"isNewTable = $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)
      val metadata = getCompatEnabledMetadata(schema)
        .withMergedConfiguration(
          Map("delta.enableIcebergCompatV2" -> "true").asJava)
      val protocol = getCompatEnabledProtocol()

      val ex = intercept[KernelException] {
        validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
      }
      assert(ex.getMessage.contains(
        s"icebergCompat$icebergCompatVersion: Only one IcebergCompat version can be enabled. " +
          "Incompatible version enabled: delta.enableIcebergCompatV2"))
    }
  }
}
