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

import org.apache.spark.sql.delta.{GeneratedColumn, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.stats.{DeltaScan, DeltaScanGenerator, PreparedDeltaFileIndex, PrepareDeltaScanBase}

import org.apache.spark.sql.delta.metering.DeltaLogging

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetScan, ParquetScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaScanBuilder(
    sparkSession: SparkSession,
    deltaFileIndex: TahoeFileIndex,
    metadata: Metadata,
    tableSchema: StructType,
    readSchema: StructType,
    options: CaseInsensitiveStringMap)
  extends ParquetScanBuilder(sparkSession, deltaFileIndex, readSchema, readSchema, options)
  with PredicateHelper
  with DeltaLogging {

  protected def getDeltaScanGenerator(index: TahoeLogFileIndex): DeltaScanGenerator = {
    // The first case means that we've fixed the table snapshot for time travel
    if (index.isTimeTravelQuery) return index.getSnapshot
    val scanGenerator = OptimisticTransaction.getActive().map(_.getDeltaScanGenerator(index))
      .getOrElse {
        index.getSnapshot
      }
    // Test compatibility with PrepareDeltaScan
    import PrepareDeltaScanBase._
    if (onGetDeltaScanGeneratorCallback != null) onGetDeltaScanGeneratorCallback(scanGenerator)
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

  override def pushFilters(filters: Seq[Expression]): Seq[Expression] = {
    // V2 scans don't filter out non-determinstic expressions like V1 scans, so filter those out
    // ourselves right now to allow metrics to be collected in non-determinstic filter UDFs
    val (deterministicFilters, nonDeterministicFilters) = filters.partition((_.deterministic))
    val filtersToEvaluate = super.pushFilters(deterministicFilters)
    filtersToEvaluate ++ nonDeterministicFilters
  }

  override def build(): Scan = {
    var parquetScan = super.build().asInstanceOf[ParquetScan]
    var transaction: Option[OptimisticTransaction] = None
    if (deltaFileIndex.isInstanceOf[TahoeLogFileIndex]) {
      val logFileIndex = deltaFileIndex.asInstanceOf[TahoeLogFileIndex]
      val scanGenerator = getDeltaScanGenerator(logFileIndex)
      if (scanGenerator.isInstanceOf[OptimisticTransaction]) {
        transaction = Some(scanGenerator.asInstanceOf[OptimisticTransaction])
      }

      val filters = partitionFilters ++ dataFilters
      val generatedPartitionFilters =
        if (GeneratedColumn.partitionFilterOptimizationEnabled(sparkSession)) {
          // Create a fake relation to resolve partition filters against, not sure
          // why this is really needed
          val relation = LogicalRelation(logFileIndex.deltaLog.createRelation(
            snapshotToUseOpt = Some(scanGenerator.snapshotToScan)))
          val generatedFilters = GeneratedColumn.generatePartitionFilters(
            sparkSession, scanGenerator.snapshotToScan, filters, relation)
          // Split predicates as this is what the tests are looking for to match V1 behavior
          generatedFilters.flatMap(splitConjunctivePredicates)
        } else {
          Seq.empty
        }

      val preparedScan = scanGenerator.filesForScan(filters ++ generatedPartitionFilters)
      val preparedIndex = getPreparedIndex(preparedScan, logFileIndex)
      parquetScan = parquetScan.copy(fileIndex = preparedIndex,
        partitionFilters = partitionFilters ++ generatedPartitionFilters)
    }
    DeltaTableScan(sparkSession, metadata, tableSchema, parquetScan, transaction)
  }
}
