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

package org.apache.spark.sql.delta

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/** A thin wrapper over the Parquet file format to support columns names without restrictions. */
class DeltaParquetFileFormat(
    val columnMappingMode: DeltaColumnMappingMode,
    val referenceSchema: StructType)
  extends ParquetFileFormat {

  private def prepareSchema(inputSchema: StructType): StructType = {
    DeltaColumnMapping.createPhysicalSchema(inputSchema, referenceSchema, columnMappingMode)
  }

  /**
   * We sometimes need to replace FileFormat within LogicalPlans, so we have to override
   * `equals` to ensure file format changes are captured
   */
  override def equals(other: Any): Boolean = {
    other match {
      case ff: DeltaParquetFileFormat
        => ff.columnMappingMode == columnMappingMode && ff.referenceSchema == referenceSchema
      case _ => false
    }
  }

  override def hashCode(): Int = getClass.hashCode()

  override def buildReaderWithPartitionValues(
      sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    super.buildReaderWithPartitionValues(
      sparkSession,
      prepareSchema(dataSchema),
      prepareSchema(partitionSchema),
      prepareSchema(requiredSchema),
      filters,
      options,
      hadoopConf)
  }

  override def supportFieldName(name: String): Boolean = {
    if (columnMappingMode != NoMapping) true else super.supportFieldName(name)
  }
}
