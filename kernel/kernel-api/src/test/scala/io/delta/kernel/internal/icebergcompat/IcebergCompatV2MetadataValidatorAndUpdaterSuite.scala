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
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.icebergcompat.IcebergCompatV2MetadataValidatorAndUpdater.validateAndUpdateIcebergCompatV2Metadata
import io.delta.kernel.internal.tablefeatures.TableFeature
import io.delta.kernel.internal.tablefeatures.TableFeatures.{COLUMN_MAPPING_RW_FEATURE, ICEBERG_COMPAT_V2_W_FEATURE}
import io.delta.kernel.types._

trait IcebergCompatV2MetadataValidatorAndUpdaterSuiteBase
    extends IcebergCompatMetadataValidatorAndUpdaterSuiteBase {

  override def icebergCompatVersion: String = "V2"

  override def supportedDataColumnTypes: Set[DataType] =
    IcebergCompatMetadataValidatorAndUpdaterSuiteBase.SIMPLE_TYPES ++
      IcebergCompatMetadataValidatorAndUpdaterSuiteBase.COMPLEX_TYPES

  override def unsupportedDataColumnTypes: Set[DataType] = Set(VariantType.VARIANT)

  override def unsupportedPartitionColumnTypes: Set[DataType] =
    IcebergCompatMetadataValidatorAndUpdaterSuiteBase.COMPLEX_TYPES

  override def isDeletionVectorsSupported: Boolean = false

  override def requiredTableFeatures: Set[TableFeature] = Set(
    ICEBERG_COMPAT_V2_W_FEATURE,
    COLUMN_MAPPING_RW_FEATURE)

  override def withIcebergCompatAndCMEnabled(
      schema: StructType,
      partCols: Seq[String] = Seq.empty): Metadata = {
    testMetadata(schema, partCols).withIcebergCompatV2AndCMEnabled()
  }
}

class IcebergCompatV2MetadataValidatorAndUpdaterSuite
    extends IcebergCompatV2MetadataValidatorAndUpdaterSuiteBase {

  override def simpleTypesToSkip: Set[DataType] = Set.empty

  override def getCompatEnabledMetadata(
      schema: StructType,
      partCols: Seq[String] = Seq.empty): Metadata = {
    testMetadata(schema, partCols)
      .withIcebergCompatV2AndCMEnabled()
  }

  override def getCompatEnabledProtocol(tableFeatures: TableFeature*): Protocol = {
    testProtocol(tableFeatures ++ Seq(ICEBERG_COMPAT_V2_W_FEATURE, COLUMN_MAPPING_RW_FEATURE): _*)
  }

  override def validateAndUpdateIcebergCompatMetadata(
      isNewTable: Boolean,
      metadata: Metadata,
      protocol: Protocol): Optional[Metadata] = {
    validateAndUpdateIcebergCompatV2Metadata(isNewTable, metadata, protocol)
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"protocol is missing required column mapping feature, isNewTable $isNewTable") {
      val schema = new StructType().add("col", BooleanType.BOOLEAN)

      val metadata = withIcebergCompatAndCMEnabled(schema, partCols = Seq.empty)
      val protocol =
        new Protocol(3, 7, Set.empty.asJava, Set(s"icebergCompat$icebergCompatVersion").asJava)
      val e = intercept[KernelException] {
        validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
      }
      assert(e.getMessage.contains(
        s"icebergCompat$icebergCompatVersion: requires the feature 'columnMapping' to be enabled."))
    }
  }

  Seq(true, false).foreach { isNewTable =>
    test(s"column mapping mode `name` is auto enabled when icebergCompat is enabled, " +
      s"isNewTable = $isNewTable") {
      val metadata = testMetadata(cmTestSchema())
        .withMergedConfiguration(
          Map(s"delta.enableIcebergCompat$icebergCompatVersion" -> "true").asJava)
      val protocol = getCompatEnabledProtocol()

      assert(!metadata.getConfiguration.containsKey("delta.columnMapping.mode"))

      if (isNewTable) {
        val updatedMetadata =
          validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
        assert(updatedMetadata.isPresent)
        assert(updatedMetadata.get().getConfiguration.get("delta.columnMapping.mode") == "name")
      } else {
        val e = intercept[KernelException] {
          validateAndUpdateIcebergCompatMetadata(isNewTable, metadata, protocol)
        }
        assert(e.getMessage.contains(
          "The value 'none' for the property 'delta.columnMapping.mode' is" +
            s" not compatible with icebergCompat$icebergCompatVersion requirements"))
      }
    }
  }
}
