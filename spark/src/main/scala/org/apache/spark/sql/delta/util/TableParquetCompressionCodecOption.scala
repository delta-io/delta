/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.util

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaOptions}

/**
 * Helper for resolving the Parquet compression codec to use for a Delta write, based on the
 * `delta.parquet.compression.codec` table property defined in the Delta protocol.
 *
 * See: https://github.com/delta-io/delta/blob/master/PROTOCOL.md#table-properties
 */
object TableParquetCompressionCodecOption {
  private val tablePropKey = DeltaConfigs.PARQUET_COMPRESSION_CODEC.key
  private val writerOptionKey = DeltaOptions.COMPRESSION

  /**
   * Determines the writer options to add to indicate the Parquet compression codec of the files
   * being written. The precedence order is:
   * 1. The `compression` DataFrame writer option (case-insensitive). If specified, no extra
   *    options are added; the user-provided value is respected by the Parquet writer.
   * 2. A raw `compression` entry in the table properties (which can be set via
   *    CREATE TABLE ... OPTIONS ('compression' = ...)). This mirrors the behavior of
   *    [[TableParquetVersionOption]].
   * 3. The [[DeltaConfigs.PARQUET_COMPRESSION_CODEC]] (`delta.parquet.compression.codec`) table
   *    property.
   * 4. Spark's default Parquet compression behavior (no override added).
   */
  def getWriterOptions(
      writerOptions: Map[String, String],
      tableProperties: Map[String, String]
  ): Map[String, String] = {
    if (writerOptions.keysIterator.exists(_.equalsIgnoreCase(writerOptionKey))) {
      Map.empty[String, String]
    } else if (tableProperties.contains(writerOptionKey)) {
      Map(writerOptionKey -> tableProperties(writerOptionKey))
    } else {
      DeltaConfigs.PARQUET_COMPRESSION_CODEC
        .fromMap(tableProperties)
        .map(codec => Map(writerOptionKey -> codec))
        .getOrElse(Map.empty[String, String])
    }
  }
}
