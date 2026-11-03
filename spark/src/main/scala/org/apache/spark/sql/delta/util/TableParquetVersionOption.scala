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

import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.parquet.column.ParquetProperties.{WriterVersion => ParquetWriterVersion}
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

object TableParquetVersionOption {
  private val tablePropKey = DeltaConfigs.PARQUET_FORMAT_VERSION.key
  private val writerOptionKey = ParquetOutputFormat.WRITER_VERSION

  /**
   * Determines the writer options to add to indicate the Parquet format version of the files
   * being written. The precedence order is:
   * 1. The ParquetOutputFormat.WRITER_VERSION DataFrame writer option or table property
   *    (e.g. if specified via CREATE TABLE ... OPTIONS).
   * 2. The DeltaConfigs.PARQUET_FORMAT_VERSION table property.
   * 3. The ParquetOutputFormat.WRITER_VERSION Spark configuration.
   * 4. Parquet v1 (the final default).
   */
  def getWriterOptions(
    spark: SparkSession,
    writerOptions: Map[String, String],
    tableProperties: Map[String, String]
  ): Map[String, String] = {
    if (writerOptions.contains(writerOptionKey)
    ) {
      Map.empty[String, String]
    } else if (tableProperties.contains(writerOptionKey)) {
      Map(writerOptionKey -> tableProperties(writerOptionKey))
    } else if (tableProperties.contains(tablePropKey)
    ) {
      val version = ParquetFormatVersion.resolve(tableProperties(tablePropKey))
      getWriterOptions(spark, version)
    } else {
      Map(
        writerOptionKey -> spark.sessionState.conf.getConfString(
          writerOptionKey, writerVersionShortName(ParquetWriterVersion.PARQUET_1_0)
        )
      )
    }
  }

  private def getWriterOptions(
    spark: SparkSession,
    version: ParquetFormatVersion
  ): Map[String, String] = {
    Map(
      writerOptionKey -> writerVersionShortName(ParquetWriterVersion.PARQUET_1_0)
    ) ++ (
      if (version.compare(ParquetFormatVersion.V2_12_0) >= 0) {
        Map(
          writerOptionKey -> writerVersionShortName(ParquetWriterVersion.PARQUET_2_0),
          SQLConf.PARQUET_OUTPUT_TIMESTAMP_TYPE.key -> spark.sessionState.conf.getConf(
            DeltaSQLConf.TABLE_PARQUET_V2_DEFAULT_TIMESTAMP_ENCODING
          )
        )
      } else {
        Map.empty
      }
    )
  }

  def writerVersionShortName(v: ParquetWriterVersion): String = v match {
    case ParquetWriterVersion.PARQUET_1_0 => "v1"
    case ParquetWriterVersion.PARQUET_2_0 => "v2"
    case other => throw new IllegalArgumentException(
      s"Unrecognized ParquetWriterVersion: $other")
  }

}
