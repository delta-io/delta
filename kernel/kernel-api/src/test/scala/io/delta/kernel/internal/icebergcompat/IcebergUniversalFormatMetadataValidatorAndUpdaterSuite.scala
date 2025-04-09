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
import io.delta.kernel.internal.actions.Metadata
import io.delta.kernel.internal.util.ColumnMappingSuiteBase
import io.delta.kernel.types.IntegerType
import io.delta.kernel.types.StructType

import org.scalatest.funsuite.AnyFunSuiteLike

class IcebergUniversalFormatMetadataValidatorAndUpdaterSuite extends AnyFunSuiteLike
    with ColumnMappingSuiteBase {
  test("validateAndUpdate shouldn't throw when when no config is set") {
    val metadata = createMetadata(Map("unrelated_key" -> "unrelated_value"))
    IcebergUniversalFormatMetadataValidatorAndUpdater.validate(
      metadata)
  }

  test(
    "validate shouldn't throw with valid Hudi properties") {
    val metadata = createMetadata(Map(
      TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "hudi",
      "unrelated_key" -> "unrelated_value"))
    IcebergUniversalFormatMetadataValidatorAndUpdater.validate(metadata)
  }

  test(
    "validate shouldn't throw with Iceberg UNIVERSAL_FORMAT_ENABLED_FORMATS when compat is enabled") {
    val metadata = createMetadata(Map(
      TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg,hudi",
      TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
      "unrelated_key" -> "unrelated_value"))
    IcebergUniversalFormatMetadataValidatorAndUpdater.validate(metadata)
  }

  Seq(
    Map(TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "false"),
    Map[String, String]()).foreach { disableIcebergCompatV2Enabled =>
    test(
      s"validate should throw when iceberg compat is not enabled $disableIcebergCompatV2Enabled") {
      val metadata = createMetadata(Map(
        TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg",
        "unrelated_key" -> "unrelated_value"))
      intercept[KernelException] {
        IcebergUniversalFormatMetadataValidatorAndUpdater.validate(metadata)
      }
    }
  }

  def createMetadata(tblProps: Map[String, String] = Map.empty): Metadata = {
    val schema = new StructType()
      .add("c1", IntegerType.INTEGER)
    testMetadata(schema, tblProps = tblProps)
  }
}
