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

import io.delta.kernel.exceptions.{InvalidConfigurationValueException, KernelException}

import org.scalatest.funsuite.AnyFunSuite

class TableConfigSuite extends AnyFunSuite {

  test("check TableConfig.editable is true") {
    TableConfig.validateAndNormalizeDeltaProperties(
      Map(
        TableConfig.TOMBSTONE_RETENTION.getKey -> "interval 2 week",
        TableConfig.CHECKPOINT_INTERVAL.getKey -> "20",
        TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> "1",
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey -> "1",
        TableConfig.COLUMN_MAPPING_MODE.getKey -> "name",
        TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true",
        TableConfig.UNIVERSAL_FORMAT_ENABLED_FORMATS.getKey -> "iceberg",
        TableConfig.PARQUET_COMPRESSION_CODEC.getKey -> "snappy").asJava)
  }

  test("check TableConfig.MAX_COLUMN_ID.editable is false") {
    val e = intercept[KernelException] {
      TableConfig.validateAndNormalizeDeltaProperties(
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

  test("PARQUET_COMPRESSION_CODEC - valid values accepted including mixed case") {
    val validValues = Seq(
      "snappy",
      "SNAPPY",
      "ZSTD",
      "gzip",
      "GZIP",
      "lz4",
      "lz4_raw",
      "LZ4_RAW",
      "uncompressed",
      "UNCOMPRESSED",
      "none",
      "NONE",
      "zstd")
    validValues.foreach { codec =>
      TableConfig.validateAndNormalizeDeltaProperties(
        Map(TableConfig.PARQUET_COMPRESSION_CODEC.getKey -> codec).asJava)
    }
  }

  test("PARQUET_COMPRESSION_CODEC - invalid value throws InvalidConfigurationValueException") {
    val ex = intercept[InvalidConfigurationValueException] {
      TableConfig.validateAndNormalizeDeltaProperties(
        Map(TableConfig.PARQUET_COMPRESSION_CODEC.getKey -> "invalid").asJava)
    }
    assert(ex.getMessage.contains("delta.parquet.compression.codec"))
    assert(ex.getMessage.contains("invalid"))
  }

  test("PARQUET_COMPRESSION_CODEC - fromMetadata returns lowercase regardless of stored case") {
    val config = Map(TableConfig.PARQUET_COMPRESSION_CODEC.getKey -> "SNAPPY").asJava
    val result = TableConfig.PARQUET_COMPRESSION_CODEC.fromMetadata(config)
    assert(result === "snappy")
  }

  test("PARQUET_COMPRESSION_CODEC - fromMetadata returns snappy when property absent") {
    val config = Map.empty[String, String].asJava
    val result = TableConfig.PARQUET_COMPRESSION_CODEC.fromMetadata(config)
    assert(result === "snappy")
  }

  test("PARQUET_COMPRESSION_CODEC - validation normalizes key case") {
    val result = TableConfig.validateAndNormalizeDeltaProperties(
      Map("DELTA.PARQUET.COMPRESSION.CODEC" -> "snappy").asJava)
    assert(result.containsKey("delta.parquet.compression.codec"))
    assert(result.get("delta.parquet.compression.codec") === "snappy")
  }
}
