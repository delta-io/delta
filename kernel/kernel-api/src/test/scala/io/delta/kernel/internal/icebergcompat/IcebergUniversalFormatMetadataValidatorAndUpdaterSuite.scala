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

import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.actions.Metadata
import io.delta.kernel.internal.util.ColumnMappingSuiteBase
import io.delta.kernel.types.IntegerType
import io.delta.kernel.types.StructType

import org.scalatest.funsuite.AnyFunSuiteLike

class IcebergUniversalFormatMetadataValidatorAndUpdaterSuite extends AnyFunSuiteLike
    with ColumnMappingSuiteBase {
  test("validateAndUpdate should return empty when UNIVERSAL_FORMAT_ENABLED_FORMATS is not set") {
    val metadata = createMetadata(Map("unrelated_key" -> "unrelated_value"))
    val result = IcebergUniversalFormatMetadataValidatorAndUpdater.validateAndUpdate(metadata)
    assert(result.isEmpty)
  }

  test(
    "validateAndUpdate should return empty when no iceberg in UNIVERSAL_FORMAT_ENABLED_FORMATS") {
    val metadata = createMetadata(Map(
      TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "hudi",
      "unrelated_key" -> "unrelated_value"))
    val result = IcebergUniversalFormatMetadataValidatorAndUpdater.validateAndUpdate(metadata)
    assert(result.isEmpty)
  }

  test("validateAndUpdate should return empty when iceberg is enabled and ICEBERG_COMPAT_V2_ENABLED is true") {
    val metadata = createMetadata(Map(
      TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg,hudi",
      TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
      "unrelated_key" -> "unrelated_value"))
    val result = IcebergUniversalFormatMetadataValidatorAndUpdater.validateAndUpdate(metadata)
    assert(result.isEmpty)
  }

  Seq(
    Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "false"),
    Map[String, String]()).foreach { disabledIcebergCompatV2Enabled =>
    test(s"validateAndUpdate should remove iceberg when ICEBERG_COMPAT_V2_ENABLED $disabledIcebergCompatV2Enabled") {
      val metadata = createMetadata(Map(
        TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg,hudi",
        "unrelated_key" -> "unrelated_value") ++ disabledIcebergCompatV2Enabled)
      val result = IcebergUniversalFormatMetadataValidatorAndUpdater.validateAndUpdate(metadata)
      assert(result.isPresent)
      assert(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetadata(result.get()) == Set(
        "hudi").asJava)

      val updatedConfig = result.get().getConfiguration
      assert(updatedConfig.get("unrelated_key") === "unrelated_value")
    }
  }

  Seq(
    Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "false"),
    Map[String, String]()).foreach { disableIcebergCompatV2Enabled =>
    test(s"validateAndUpdate should remove key entirely when only iceberg is present and not compatible $disableIcebergCompatV2Enabled") {
      val metadata = createMetadata(Map(
        TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg",
        "unrelated_key" -> "unrelated_value"))
      val result = IcebergUniversalFormatMetadataValidatorAndUpdater.validateAndUpdate(metadata)
      assert(result.isPresent)
      val updatedConfig = result.get().getConfiguration
      assert(!updatedConfig.containsKey(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey))
      assert(updatedConfig.get("unrelated_key") === "unrelated_value")
    }
  }

  def createMetadata(tblProps: Map[String, String]): Metadata = {
    val schema = new StructType()
      .add("c1", IntegerType.INTEGER)
    testMetadata(schema, tblProps = tblProps)
  }
}
