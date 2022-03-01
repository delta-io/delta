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

import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.delta.actions.{AddFile, Metadata}
import org.apache.spark.sql.delta.util.StateCache
import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types.StringType

/**
 * Adds the ability to use statistics to filter the set of files based on predicates
 * to a [[org.apache.spark.sql.delta.Snapshot]] of a given Delta table.
 */
trait DataSkippingReader
  extends DeltaScanGenerator
    with StatisticsCollection
    with ReadsMetadataFields
    with StateCache {

  // Snapshot will provide all of these
  def allFiles: Dataset[AddFile]
  def version: Long
  def metadata: Metadata
  def sizeInBytes: Long
  def numOfFiles: Long
  def redactedPath: String

  private def withStatsInternal0: DataFrame = {
    val implicits = spark.implicits
    import implicits._
    allFiles.withColumn("stats", from_json($"stats", statsSchema))
  }

  private lazy val withStatsCache =
    cacheDS(withStatsInternal0, s"Delta Table State with Stats #$version - $redactedPath")

  // TODO(scott): proper implementation
  override def filesWithStatsForScan(partitionFilters: Seq[Expression]): DataFrame = {
    DeltaLog.filterFileList(metadata.partitionSchema, withStatsCache.getDS, partitionFilters)
  }
}
