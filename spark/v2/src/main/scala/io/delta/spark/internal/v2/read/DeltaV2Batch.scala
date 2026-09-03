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

package io.delta.spark.internal.v2.read

import java.util.{List => JList, Objects}

import io.delta.spark.internal.v2.DeltaV2Logging
import io.delta.spark.internal.v2.utils.PartitionUtils
import org.apache.hadoop.conf.Configuration
import io.delta.kernel.{Snapshot => KernelSnapshot}
import io.delta.kernel.expressions.{Predicate => KernelPredicate}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

/**
 * Package-private batch implementation for Delta's Spark DataSource V2 read path.
 *
 * This class must remain package-private so callers outside `v2.read` depend only on Spark's
 * public connector interfaces instead of coupling to Delta's internal V2 implementation.
 */
// Fields read across instances in equals/hashCode are `private val`, since object-private
// `private[this]` cannot read `that.field`. Other retained fields are `private[this]`; the two
// filter-array params are consumed only to derive the equality sets, so they take no `val`.
private[read] final class DeltaV2Batch(
    private val kernelSnapshot: KernelSnapshot,
    private val dataSchema: StructType,
    private val partitionSchema: StructType,
    private val readDataSchema: StructType,
    private[this] val ddlOrderedReadOutputSchema: StructType,
    private val partitionedFiles: JList[PartitionedFile],
    kernelPushedFilters: Array[KernelPredicate],
    // Data-column filters only: the sole filters passed to the Parquet reader factory. Partition
    // columns are not stored in Parquet; their values come from PartitionedFile.partitionValues.
    private[this] val dataFilters: Array[Filter],
    // Full filter set (partition + data). Used only to build the equals/hashCode identity set so
    // batches selecting different file sets stay distinct; never passed to the reader factory.
    allFilters: Array[Filter],
    private[this] val totalBytes: Long,
    private[this] val readerOptions: Map[String, String],
    private[this] val hadoopConf: Configuration)
    extends Batch
    with DeltaV2Logging {

  // Use Objects.requireNonNull (throws NullPointerException) rather than Scala's require (throws
  // IllegalArgumentException) to preserve the exact null-check behavior of the original Java class.
  Objects.requireNonNull(kernelSnapshot, "kernelSnapshot is null")
  Objects.requireNonNull(dataSchema, "dataSchema is null")
  Objects.requireNonNull(partitionSchema, "partitionSchema is null")
  Objects.requireNonNull(readDataSchema, "readDataSchema is null")
  Objects.requireNonNull(ddlOrderedReadOutputSchema, "ddlOrderedReadOutputSchema is null")
  Objects.requireNonNull(partitionedFiles, "partitionedFiles is null")
  Objects.requireNonNull(kernelPushedFilters, "kernelPushedFilters is null")
  Objects.requireNonNull(dataFilters, "dataFilters is null")
  Objects.requireNonNull(allFilters, "allFilters is null")
  Objects.requireNonNull(readerOptions, "readerOptions is null")
  Objects.requireNonNull(hadoopConf, "hadoopConf is null")

  // Order-insensitive equality keys. Filters are AND-ed, so order does not affect the scan.
  private val kernelPushedFilterSet = kernelPushedFilters.toSet
  private val filterSet = allFilters.toSet
  private[this] val sqlConf: SQLConf = SQLConf.get

  override def planInputPartitions(): Array[InputPartition] = {
    recordFrameProfile("batchScan.planInputPartitions") {
      PartitionUtils.planInputPartitions(
        SparkSession.active,
        partitionedFiles,
        totalBytes,
        hadoopConf,
        sqlConf)
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    // Non-CDC plain table scan. Write-time CDF streaming reads route through
    // DeltaV2MicroBatchStream; read-time CDF batch reads use a dedicated batch implementation.
    recordFrameProfile("batchScan.createReaderFactory") {
      PartitionUtils.createDeltaParquetReaderFactory(
        kernelSnapshot,
        dataSchema,
        partitionSchema,
        readDataSchema,
        ddlOrderedReadOutputSchema,
        dataFilters,
        readerOptions,
        hadoopConf,
        sqlConf)
    }
  }

  override def equals(obj: Any): Boolean = obj match {
    case that: DeltaV2Batch =>
      (this eq that) || (
        kernelSnapshot == that.kernelSnapshot &&
        readDataSchema == that.readDataSchema &&
        dataSchema == that.dataSchema &&
        partitionSchema == that.partitionSchema &&
        kernelPushedFilterSet == that.kernelPushedFilterSet &&
        filterSet == that.filterSet &&
        partitionedFiles.size() == that.partitionedFiles.size()
      )
    case _ => false
  }

  override def hashCode(): Int =
    Seq(
      kernelSnapshot,
      readDataSchema,
      dataSchema,
      partitionSchema,
      kernelPushedFilterSet,
      filterSet,
      partitionedFiles.size()).hashCode()
}
