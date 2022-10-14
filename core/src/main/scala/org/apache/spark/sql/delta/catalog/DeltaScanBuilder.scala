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

import org.apache.spark.sql.delta.{DeltaColumnMapping, GeneratedColumn, OptimisticTransaction, NoMapping}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.files.{TahoeFileIndex, TahoeLogFileIndex}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.stats.{DeltaScan, DeltaScanGenerator, PreparedDeltaFileIndex, PrepareDeltaScanBase}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.parquet.{ParquetScan, ParquetScanBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex


/**
 * The scan builder for V2 Delta table reads. Mostly serves the same purpose as the
 * PrepareDeltaScan optimization rule. The scan building happens after the PreCBO rules.
 *
 * @param deltaFileIndex The file index for the read
 * @param metadata Metadata for the table to check the column mapping mode
 * @param tableSchema The schema of the table with all Delta metadata
 * @param readSchema The schema of the table with Delta metadata removed
 * @param options File system options for the scan
 */
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

  protected def prepareSchema(inputSchema: StructType): StructType = {
    DeltaColumnMapping.createPhysicalSchema(inputSchema, tableSchema,
      metadata.columnMappingMode)
  }

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

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    // Pushed aggregations don't work with column mapping right now
    if (metadata.columnMappingMode != NoMapping) {
      return false
    } else {
      return super.pushAggregation(aggregation)
    }
  }

  override def build(): Scan = {
    var parquetScan = super.build().asInstanceOf[ParquetScan]
    var transaction: Option[OptimisticTransaction] = None
    var generatedPartitionFilters = Seq.empty[Expression]
    var preparedIndex: PartitioningAwareFileIndex = parquetScan.fileIndex

    if (deltaFileIndex.isInstanceOf[TahoeLogFileIndex]) {
      val logFileIndex = deltaFileIndex.asInstanceOf[TahoeLogFileIndex]
      val scanGenerator = getDeltaScanGenerator(logFileIndex)

      // If we are already in a transaction, store the scan with the transaction so
      // we don't double add the read files and predicates in WriteIntoDelta
      if (scanGenerator.isInstanceOf[OptimisticTransaction]) {
        transaction = Some(scanGenerator.asInstanceOf[OptimisticTransaction])
      }

      val filters = partitionFilters ++ dataFilters
      generatedPartitionFilters =
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

      val preparedScan = withStatusCode("DELTA", "Filtering files for query") {
        scanGenerator.filesForScan(filters ++ generatedPartitionFilters)
      }
      preparedIndex = getPreparedIndex(preparedScan, logFileIndex)
    }

    // Pull the pruned schemas from the parquet scan
    val readDataSchema = parquetScan.readDataSchema
    val readPartitionSchema = parquetScan.readPartitionSchema

    // Update schemas with the prepared index and column mapping for the physical scan
    parquetScan = parquetScan.copy(
      fileIndex = preparedIndex,
      partitionFilters = partitionFilters ++ generatedPartitionFilters,
      dataSchema = prepareSchema(readSchema),
      readDataSchema = prepareSchema(parquetScan.readDataSchema),
      readPartitionSchema = prepareSchema(parquetScan.readPartitionSchema))

    DeltaTableScan(sparkSession, parquetScan, readDataSchema, readPartitionSchema, transaction)
  }
}
