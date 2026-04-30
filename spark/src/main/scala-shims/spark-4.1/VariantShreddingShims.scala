/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.shims

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.util.HadoopInputFile

import org.apache.spark.sql.delta.stats.{DeltaTaskStatisticsTracker, VariantStatsData}
import org.apache.spark.sql.execution.datasources.{DataSourceUtils, OutputWriter, WriteTaskStatsTracker}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFooterReader
import org.apache.spark.sql.internal.SQLConf

/**
 * Shim for variant shredding configs to handle API changes between Spark versions.
 * Spark 4.1 supports shredded writes, and by extension the collection of variant stats.
 */
object VariantShreddingShims {
  def getVariantInferShreddingSchemaOptions(enableVariantShredding: Boolean)
    : Map[String, String] = {
    Map(SQLConf.VARIANT_INFER_SHREDDING_SCHEMA.key -> enableVariantShredding.toString)
  }

  // Extract variant stats from the parquet footer of a written file and inject data into the
  // variant stats collection expressions.
  def extractAndInjectVariantStats(
      writer: OutputWriter,
      trackers: Seq[WriteTaskStatsTracker],
      parquetRebaseModeInRead: String,
      hadoopConf: Configuration): Unit = {
    val hasVariantTrackers = trackers.exists {
      case t: DeltaTaskStatisticsTracker =>
        t.minVariantStatsExpressions.nonEmpty || t.maxVariantStatsExpressions.nonEmpty
      case _ => false
    }
    if (!hasVariantTrackers) return
    val footer = try {
      ParquetFooterReader.readFooter(
        HadoopInputFile.fromPath(new Path(writer.path()), hadoopConf),
        ParquetMetadataConverter.NO_FILTER)
    } catch {
      case NonFatal(_) => return
    }
    val keyValueMetadata = footer.getFileMetaData.getKeyValueMetaData
    val rebaseSpec = DataSourceUtils.datetimeRebaseSpec(
      k => keyValueMetadata.get(k),
      parquetRebaseModeInRead)
    val dateRebaseFunc =
      DataSourceUtils.createDateRebaseFuncInRead(rebaseSpec.mode, "Parquet")
    val timestampRebaseFunc =
      DataSourceUtils.createTimestampRebaseFuncInRead(rebaseSpec, "Parquet")
    val variantStatsData = VariantStatsData(footer, dateRebaseFunc, timestampRebaseFunc)
    trackers.foreach {
      case tracker: DeltaTaskStatisticsTracker =>
        tracker.minVariantStatsExpressions.foreach(
          _.addVariantStatsData(variantStatsData))
        tracker.maxVariantStatsExpressions.foreach(
          _.addVariantStatsData(variantStatsData))
      case _ =>
    }
  }
}
