/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import scala.collection.JavaConverters._

import io.delta.kernel.exceptions.KernelException

import org.scalatest.funsuite.AnyFunSuite

class TableConfigSuite extends AnyFunSuite {

  test("check TableConfig.editable is true") {
    TableConfig.validateDeltaProperties(
      Map(
        TableConfig.TOMBSTONE_RETENTION.getKey -> "interval 2 week",
        TableConfig.CHECKPOINT_INTERVAL.getKey -> "20",
        TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> "1",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> "1",
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
        TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg").asJava)
  }

  test("check TableConfig.MAX_COLUMN_ID.editable is false") {
    val e = intercept[KernelException] {
      TableConfig.validateDeltaProperties(
        Map(
          TableConfig.TOMBSTONE_RETENTION.getKey -> "interval 2 week",
          TableConfig.CHECKPOINT_INTERVAL.getKey -> "20",
          TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
          TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey -> "10").asJava)
    }

    assert(e.isInstanceOf[KernelException])
    assert(e.getMessage ===
      s"The Delta table property " +
      s"'${TableConfig.COLUMN_MAPPING_MAX_COLUMN_ID.getKey}'" +
      s" is an internal property and cannot be updated.")
  }

  Seq(
    Map[String, String](),
    Map(TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "")).foreach {
    config =>
      {
        test(
          s"Parsing UNIVERSAL_ENABLED formats returns empty set when key is not present $config") {
          val formats = TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetadata(config.asJava)
          assert(formats.isEmpty)
        }
      }
  }

  test("Parsing UNIVERSAL_ENABLED_FORMATS can parse spaces") {
    val FORMATS_KEY = TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey
    val config = Map(FORMATS_KEY -> "iceberg, hudi ").asJava
    val formats = TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.fromMetadata(config)
    assert(formats == Set("iceberg", "hudi").asJava)
  }
}
