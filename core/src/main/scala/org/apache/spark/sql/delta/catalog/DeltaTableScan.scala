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

package org.apache.spark.sql.delta.catalog

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{ColumnWithDefaultExprUtils, DeltaColumnMapping, DeltaErrors, DeltaLog, DeltaTableUtils, DeltaTimeTravelSpec, Snapshot}
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaDataSource

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetScan, ParquetScanBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.delta.stats.PrepareDeltaScanBase
import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.DeltaScanGenerator
import org.apache.spark.sql.delta.stats.DeltaScan
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.GeneratedColumn
import org.apache.spark.sql.delta.stats.PreparedDeltaFileIndex
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetPartitionReaderFactory
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.NoMapping
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport
import org.apache.spark.util.SerializableConfiguration

case class DeltaTableScan(
    sparkSession: SparkSession,
    metadata: Metadata,
    referenceSchema: StructType,
    delegatedScan: ParquetScan)
    extends FileScan
    with DeltaLogging {

  def prepareSchema(inputSchema: StructType): StructType = {
    DeltaColumnMapping.createPhysicalSchema(inputSchema, referenceSchema,
      metadata.columnMappingMode)
  }

  override def isSplitable(path: Path): Boolean = delegatedScan.isSplitable(path)

  override def fileIndex: PartitioningAwareFileIndex = delegatedScan.fileIndex

  override def dataSchema: StructType = delegatedScan.dataSchema

  override def readDataSchema: StructType = delegatedScan.readDataSchema

  override def readPartitionSchema: StructType = delegatedScan.readPartitionSchema

  override def partitionFilters: Seq[Expression] = delegatedScan.partitionFilters

  override def dataFilters: Seq[Expression] = delegatedScan.dataFilters

  override def createReaderFactory(): PartitionReaderFactory = {
    val readerFactory = delegatedScan.createReaderFactory()
      .asInstanceOf[ParquetPartitionReaderFactory]
    if (metadata.columnMappingMode != NoMapping) {
      val conf = readerFactory.broadcastedConf.value.value
      val readDataSchemaAsJson = prepareSchema(readDataSchema).json
      conf.set(
        ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
        readDataSchemaAsJson)
      val broadcastedConf = sparkSession.sparkContext.broadcast(
        new SerializableConfiguration(conf))
      readerFactory.copy(broadcastedConf = broadcastedConf)
    } else {
      readerFactory
    }
  }

  override def description(): String = delegatedScan.description()

  override def getMetaData(): Map[String, String] = delegatedScan.getMetaData()
}
