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

package org.apache.spark.sql.delta.files

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{DeltaLog, DeltaTableUtils, Snapshot}
import org.apache.spark.sql.delta.actions.SingleAction
import org.apache.spark.sql.delta.logging.DeltaLogKeys
import org.apache.spark.sql.delta.sources.IndexedFile
import org.apache.spark.sql.delta.stats.DataSkippingReader
import org.apache.spark.sql.delta.util.StateCache

import org.apache.spark.internal.MDC
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions._

/**
 * Converts a `Snapshot` into the initial set of files read when starting a new streaming query.
 * The list of files that represent the table at the time the query starts are selected by:
 * - Adding `version` and `index` to each file to enable splitting of the initial state into
 *   multiple batches.
 * - Filtering files that don't match partition predicates, while preserving the aforementioned
 *   indexing.
 */
class DeltaSourceSnapshot(
    val spark: SparkSession,
    val snapshot: Snapshot,
    val filters: Seq[Expression])
  extends StateCache {

  protected val version = snapshot.version
  protected val path = snapshot.path

  protected lazy val (partitionFilters, dataFilters) = {
    val partitionCols = snapshot.metadata.partitionColumns
    val (part, data) = filters.partition { e =>
      DeltaTableUtils.isPredicatePartitionColumnsOnly(e, partitionCols, spark)
    }
    logInfo(log"Classified filters: partition: ${MDC(DeltaLogKeys.PARTITION_FILTER, part)}, " +
      log"data: ${MDC(DeltaLogKeys.DATA_FILTER, data)}")
    (part, data)
  }

  private[delta] def filteredFiles: Dataset[IndexedFile] = {
    import spark.implicits.rddToDatasetHolder
    import org.apache.spark.sql.delta.implicits._

    val initialFiles = snapshot.allFiles
        // This allows us to control the number of partitions created from the sort instead of
        // using the shufflePartitions setting
        .repartitionByRange(snapshot.getNumPartitions, col("modificationTime"), col("path"))
        .sort("modificationTime", "path")
        .rdd.zipWithIndex()
        .toDF("add", "index")
        // Stats aren't used for streaming reads right now, so decrease
        // the size of the files by nulling out the stats if they exist
        .withColumn("add", col("add").withField("stats", DataSkippingReader.nullStringLiteral))
        .withColumn("remove", SingleAction.nullLitForRemoveFile)
        .withColumn("cdc", SingleAction.nullLitForAddCDCFile)
        .withColumn("version", lit(version))
        .withColumn("isLast", lit(false))
        .withColumn("shouldSkip", lit(false))

    DeltaLog.filterFileList(
      snapshot.metadata.partitionSchema,
      initialFiles,
      partitionFilters,
      Seq("add")).as[IndexedFile]
  }

  private lazy val cachedState = {
    cacheDS(filteredFiles, s"Delta Source Snapshot #$version - ${snapshot.redactedPath}")
  }

  def iterator(): Iterator[IndexedFile] = {
    cachedState.getDS.toLocalIterator().asScala
  }

  def close(unpersistSnapshot: Boolean): Unit = {
    uncache()
    if (unpersistSnapshot) {
      snapshot.uncache()
    }
  }
}
