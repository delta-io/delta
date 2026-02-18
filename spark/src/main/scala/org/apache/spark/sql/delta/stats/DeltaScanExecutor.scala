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

package org.apache.spark.sql.delta.stats

import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.implicits._
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.stats.DeltaStatistics._

import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.SparkUserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, LongType, StructType}

/**
 * Executes scan operations against a Delta table's file list.
 *
 * Owns the three execution paths:
 *   1. No filters  -> [[getAllFiles]]
 *   2. Partition only -> [[filterOnPartitions]]
 *   3. Data skipping  -> [[getDataSkippedFiles]]
 *
 * V1 and V2 share [[DefaultDeltaScanExecutor]]; differences are captured
 * through the injected dependencies (stats column resolution, state, etc.).
 */
private[delta] trait DeltaScanExecutor {

  /** Returns all files, optionally keeping numRecords stats. */
  def getAllFiles(keepNumRecords: Boolean): Seq[AddFile]

  /** Filters files by partition predicates only. */
  def filterOnPartitions(
      partitionFilters: Seq[Expression],
      keepNumRecords: Boolean): (Seq[AddFile], DataSize)

  /**
   * Applies data skipping + partition filters and collects size metrics.
   *
   * @param partitionFilters  Rewritten partition filter column
   * @param verifiedSkippingExpr Ready-to-use data skipping expression (already includes
   *                             stats verification: dataFilter || !statsVerification)
   * @param keepNumRecords    Whether to retain numRecords stats in the output
   */
  def getDataSkippedFiles(
      partitionFilters: Column,
      verifiedSkippingExpr: Column,
      keepNumRecords: Boolean): (Seq[AddFile], Seq[DataSize])
}

/**
 * Default implementation shared by V1 and V2.
 *
 * All state access is provided through lazy by-name parameters so that
 * the executor never triggers state reconstruction until actually needed.
 *
 * @param spark                      SparkSession (for UDF registration)
 * @param withStats                  Lazily provides the file list with parsed statistics
 * @param allFiles                   Lazily provides the raw file list
 * @param getStatsColumnOpt          Resolves a [[StatsColumn]] to a [[Column]], or None
 * @param partitionSchema            Schema of the partition columns
 */
private[delta] class DefaultDeltaScanExecutor(
    spark: SparkSession,
    withStats: => DataFrame,
    allFiles: => Dataset[AddFile],
    getStatsColumnOpt: StatsColumn => Option[Column],
    partitionSchema: StructType)
  extends DeltaScanExecutor with DeltaLogging {

  import DataSkippingReader._

  /** Resolves a stats column, returning a null literal if the column doesn't exist. */
  private def getStatsColumnOrNullLiteral(
      statType: String, pathToColumn: Seq[String] = Nil): Column =
    getStatsColumnOpt(StatsColumn(statType, pathToColumn, LongType)).getOrElse(lit(null))

  private def buildSizeCollectorFilter(): (ArrayAccumulator, Column => Column) = {
    val bytesCompressed = col("size")
    val rows = getStatsColumnOrNullLiteral(NUM_RECORDS)
    val dvCardinality = coalesce(col("deletionVector.cardinality"), lit(0L))
    val logicalRows = (rows - dvCardinality).as("logicalRows")

    val accumulator = new ArrayAccumulator(4)
    spark.sparkContext.register(accumulator)

    val collector = (include: Boolean,
                     bytesCompressed: java.lang.Long,
                     logicalRows: java.lang.Long,
                     rows: java.lang.Long) => {
      if (include) {
        accumulator.add((0, bytesCompressed))
        accumulator.add((1, Option(rows).map(_.toLong).getOrElse(-1L)))
        accumulator.add((2, 1))
        accumulator.add((3, Option(logicalRows).map(_.toLong).getOrElse(-1L)))
      }
      include
    }
    val collectorUdf = SparkUserDefinedFunction(
      f = collector,
      dataType = BooleanType,
      inputEncoders = sizeCollectorInputEncoders,
      deterministic = false)

    (accumulator, collectorUdf(_: Column, bytesCompressed, logicalRows, rows))
  }

  // ---------------------------------------------------------------------------
  // DataFrame-returning methods (no collect). V2 uses these for zero-copy.
  // ---------------------------------------------------------------------------

  /** Returns all files as a DataFrame. */
  def getAllFilesAsDF: DataFrame = withStats

  /** Filters by partition predicates and returns a DataFrame (no collect). */
  def filterOnPartitionsAsDF(partitionFilters: Seq[Expression]): DataFrame =
    PartitionFilterUtils.filterFileList(partitionSchema, withStats, partitionFilters)

  /**
   * Applies partition + data-skipping filters and returns a DataFrame (no collect).
   *
   * Unlike [[getDataSkippedFiles]], this does NOT attach size-collector accumulators
   * (they are only useful when collecting on the driver).
   */
  def getDataSkippedFilesAsDF(
      partitionFilters: Column,
      verifiedSkippingExpr: Column): DataFrame =
    withStats.where(partitionFilters && verifiedSkippingExpr)

  // ---------------------------------------------------------------------------
  // Collecting methods (V1 materialized path). Delegates to DF methods above.
  // ---------------------------------------------------------------------------

  override def getAllFiles(keepNumRecords: Boolean): Seq[AddFile] = recordFrameProfile(
      "Delta", "DeltaScanExecutor.getAllFiles") {
    val ds = if (keepNumRecords) {
      withStats
        .withColumn("stats", to_json(struct(col("stats.numRecords") as "numRecords")))
    } else {
      allFiles.withColumn("stats", nullStringLiteral)
    }
    ds.toDF().as[AddFile].collect().toSeq
  }

  override def filterOnPartitions(
      partitionFilters: Seq[Expression],
      keepNumRecords: Boolean): (Seq[AddFile], DataSize) = recordFrameProfile(
      "Delta", "DeltaScanExecutor.filterOnPartitions") {
    val df = if (keepNumRecords) {
      val filteredFiles =
        PartitionFilterUtils.filterFileList(partitionSchema, withStats, partitionFilters)
      filteredFiles
        .withColumn("stats", to_json(struct(col("stats.numRecords") as "numRecords")))
    } else {
      val filteredFiles =
        PartitionFilterUtils.filterFileList(partitionSchema, allFiles.toDF(), partitionFilters)
      filteredFiles
        .withColumn("stats", nullStringLiteral)
    }
    val files = df.as[AddFile].collect()
    val sizeInBytesByPartitionFilters = files.map(_.size).sum
    files.toSeq -> DataSize(Some(sizeInBytesByPartitionFilters), None, Some(files.size))
  }

  override def getDataSkippedFiles(
      partitionFilters: Column,
      verifiedSkippingExpr: Column,
      keepNumRecords: Boolean): (Seq[AddFile], Seq[DataSize]) = recordFrameProfile(
      "Delta", "DeltaScanExecutor.getDataSkippedFiles") {
    val (totalSize, totalFilter) = buildSizeCollectorFilter()
    val (partitionSize, partitionFilter) = buildSizeCollectorFilter()
    val (scanSize, scanFilter) = buildSizeCollectorFilter()

    val filteredFiles = withStats.where(
        totalFilter(trueLiteral) &&
          partitionFilter(partitionFilters) &&
          scanFilter(verifiedSkippingExpr)
      )

    val statsColumn = if (keepNumRecords) {
      to_json(struct(col("stats.numRecords") as "numRecords"))
    } else nullStringLiteral

    val files =
      recordFrameProfile("Delta", "DeltaScanExecutor.getDataSkippedFiles.collectFiles") {
      val df = filteredFiles.withColumn("stats", statsColumn)
      df.as[AddFile].collect()
    }
    files.toSeq -> Seq(DataSize(totalSize), DataSize(partitionSize), DataSize(scanSize))
  }
}
