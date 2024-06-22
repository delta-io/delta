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

package org.apache.spark.sql.delta.commands.convert

import org.apache.spark.sql.delta.{DeltaErrors, SerializableFileStatus}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaMergingUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.datasources.PartitioningUtils
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetToSparkSchemaConverter}
import org.apache.spark.sql.execution.streaming.FileStreamSink
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A target Parquet table for conversion to a Delta table.
 *
 * @param spark: spark session to use.
 * @param basePath: the root directory of the Parquet table.
 * @param catalogTable: optional catalog table (if exists) of the Parquet table.
 * @param userPartitionSchema: user provided partition schema of the Parquet table.
 */
class ParquetTable(
    val spark: SparkSession,
    val basePath: String,
    val catalogTable: Option[CatalogTable],
    val userPartitionSchema: Option[StructType]) extends ConvertTargetTable with DeltaLogging {

  // Validate user provided partition schema if catalogTable is available.
  if (catalogTable.isDefined && userPartitionSchema.isDefined
    && !catalogTable.get.partitionSchema.equals(userPartitionSchema.get)) {
    throw DeltaErrors.unexpectedPartitionSchemaFromUserException(
      catalogTable.get.partitionSchema, userPartitionSchema.get)
  }

  protected lazy val serializableConf: SerializableConfiguration = {
    // scalastyle:off deltahadoopconfiguration
    new SerializableConfiguration(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
  }

  override val partitionSchema: StructType = {
    userPartitionSchema.orElse(catalogTable.map(_.partitionSchema)).getOrElse(new StructType())
  }

  override lazy val numFiles: Long = fileManifest.numFiles

  def tableSchema: StructType = fileManifest.parquetSchema.get

  override val format: String = "parquet"

  val fileManifest: ConvertTargetFileManifest = {
    val fetchConfig = ParquetSchemaFetchConfig(
      spark.sessionState.conf.isParquetBinaryAsString,
      spark.sessionState.conf.isParquetINT96AsTimestamp,
      spark.sessionState.conf.ignoreCorruptFiles
    )
    if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_METADATA_LOG) &&
      FileStreamSink.hasMetadata(Seq(basePath), serializableConf.value, spark.sessionState.conf)) {
      new MetadataLogFileManifest(spark, basePath, partitionSchema, fetchConfig, serializableConf)
    } else if (spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_CONVERT_USE_CATALOG_PARTITIONS) &&
      catalogTable.isDefined) {
      new CatalogFileManifest(
        spark, basePath, catalogTable.get, partitionSchema, fetchConfig, serializableConf)
    } else {
      new ManualListingFileManifest(
        spark,
        basePath,
        partitionSchema,
        fetchConfig,
        serializableConf)
    }
  }
}
