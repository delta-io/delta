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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScanBuilder
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
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan

class DeltaScanBuilder(
    sparkSession: SparkSession,
    deltaFileIndex: TahoeFileIndex,
    tableSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends ParquetScanBuilder(sparkSession, deltaFileIndex, tableSchema, tableSchema, options)
  with DeltaLogging {

  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator = {
    // The first case means that we've fixed the table snapshot for time travel
    if (index.isTimeTravelQuery) return index.getSnapshot
    val scanGenerator = OptimisticTransaction.getActive().map(_.getDeltaScanGenerator(index))
      .getOrElse {
        val snapshot = index.getSnapshot
        snapshot
      }
    scanGenerator
  }

  /**
   * Helper method to generate a [[PreparedDeltaFileIndex]]
   */
  protected def getPreparedIndex(
      preparedScan: DeltaScan,
      fileIndex: TahoeLogFileIndex): PreparedDeltaFileIndex = {
    assert(fileIndex.partitionFilters.isEmpty,
      "Partition filters should have been extracted by DeltaAnalysis.")
    PreparedDeltaFileIndex(
      sparkSession,
      fileIndex.deltaLog,
      fileIndex.path,
      preparedScan,
      fileIndex.partitionSchema,
      fileIndex.versionToUse)
  }

  override def build(): Scan = {
    val parquetScan = super.build().asInstanceOf[ParquetScan]
    if (deltaFileIndex.isInstanceOf[TahoeLogFileIndex]) {
      val logFileIndex = deltaFileIndex.asInstanceOf[TahoeLogFileIndex]
      val scanGenerator = getDeltaScanGenerator(logFileIndex)
      val preparedScan = scanGenerator.filesForScan(partitionFilters ++ dataFilters)
      val preparedIndex = getPreparedIndex(preparedScan, logFileIndex)
      parquetScan.copy(fileIndex = preparedIndex)
    }
    parquetScan
  }
}
