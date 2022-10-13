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

import org.apache.spark.sql.delta.OptimisticTransaction
import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.types.StructType

import org.apache.hadoop.fs.Path

case class DeltaTableScan(
    sparkSession: SparkSession,
    delegatedScan: ParquetScan,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    transaction: Option[OptimisticTransaction])
    extends FileScan
    with DeltaLogging {

  override def isSplitable(path: Path): Boolean = delegatedScan.isSplitable(path)

  override def fileIndex: PartitioningAwareFileIndex = delegatedScan.fileIndex

  override def dataSchema: StructType = delegatedScan.dataSchema

  /**
   * Override so that we have the logical schema and not the physical schema.
   */
  override def readSchema(): StructType = {
    if (delegatedScan.pushedAggregate.nonEmpty) readDataSchema else super.readSchema()
  }

  override def partitionFilters: Seq[Expression] = delegatedScan.partitionFilters

  override def dataFilters: Seq[Expression] = delegatedScan.dataFilters

  override def createReaderFactory(): PartitionReaderFactory = delegatedScan.createReaderFactory()

  override def getMetaData(): Map[String, String] = if (delegatedScan.pushedAggregate.nonEmpty) {
    // If there is a pushed aggregation, we know column mapping is not enabled so it's safe to
    // pull the metadata from the delegated scan
    delegatedScan.getMetaData()
  } else {
    // Be safe and use the logical schema for the metadata
    super.getMetaData()
  }
}
