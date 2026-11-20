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
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.col

/**
 * Reader-side data skipping used during conflict detection (row-level concurrency Case 1: writers
 * touching disjoint data ranges).
 *
 * Mixed into [[DataSkippingReaderBase]] as a self-typed trait that keeps the conflict-detection
 * logic in one place, while sharing the scan path's stats parsing ([[parseStats]]) and filter
 * eligibility ([[DataSkippingReader.isIneligibleForDataSkipping]]). It is opt-in: the caller
 * ([[org.apache.spark.sql.delta.ConflictChecker]]) invokes these methods only behind
 * `DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED`, and each is one-way safe -- a file
 * is dropped only when its stats *prove* it cannot match, so a real conflict is never a false
 * negative.
 */
trait ConflictDataSkippingReader extends DeltaLogging { self: DataSkippingReaderBase =>

  /**
   * Builds a single [[DataSkippingPredicate]] (skipping expression + the stats it references) from
   * `dataFilters`, with [[filesForScan]]'s eligibility filtering, per-filter construction and
   * conjunction fold. Returns None when stats skipping is unavailable or no eligible filter yields
   * a predicate. The filters are AND-combined, so callers must pass filters from a single logical
   * read (predicates from independent reads have OR, not AND, semantics).
   */
  private[delta] def buildDataSkippingPredicate(
      dataFilters: Seq[Expression]): Option[DataSkippingPredicate] = {
    // Stats-skipping conf, inlined from DataSkippingReaderBase.useStats (file-private, so not
    // visible here). Reading it inline avoids widening that member's visibility.
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)) return None
    // Same eligibility as filesForScan: drop subquery / non-deterministic / metadata filters, so we
    // never build a skipping predicate that could wrongly exclude a matching file. Dropping a
    // conjunct only widens the predicate.
    val eligibleFilters =
      dataFilters.filterNot(DataSkippingReader.isIneligibleForDataSkipping)
    val constructDataFilters = new DataFiltersBuilder(
      spark = spark,
      dataSkippingType = DeltaDataSkippingType.dataSkippingOnlyV1,
      getStatsColumnOpt = (s: StatsColumn) => getStatsColumnOpt(s))
    eligibleFilters
      .flatMap(f => constructDataFilters(f))
      .reduceOption((skip1, skip2) => DataSkippingPredicate(
        skip1.expr && skip2.expr, skip1.referencedStats ++ skip2.referencedStats))
  }

  /**
   * Builds the condition a file must satisfy to possibly match ANY of several INDEPENDENT reads.
   * Each inner `Seq[Expression]` is one logical read's data filters (AND-combined via
   * [[buildDataSkippingPredicate]]); the reads are OR-combined, matching read semantics -- a file
   * is a candidate if it could match any one of them. Returns None when nothing can be skipped: no
   * reads, a table without stats, or a read with no usable skipping predicate (empty / ineligible
   * filters match every file). Needs no DataFrame, so callers can check whether skipping applies
   * before building one.
   *
   * One-way safe: each read contributes `expr || !verifyStatsForFilter(...)` exactly as
   * [[getDataSkippedFiles]], so a file is dropped only when its stats *prove* it fails EVERY read
   * -- a real conflict is never a false negative. Files with missing/insufficient stats are kept.
   */
  private[delta] def conflictSkippingCondition(
      dataFiltersPerRead: Seq[Seq[Expression]]): Option[Column] = {
    if (dataFiltersPerRead.isEmpty || schema.isEmpty) return None
    val perReadPredicates = dataFiltersPerRead.map { dataFilters =>
      if (dataFilters.isEmpty) None else buildDataSkippingPredicate(dataFilters)
    }
    if (perReadPredicates.exists(_.isEmpty)) return None
    // Per read, `expr || !verifyStatsForFilter(...)` keeps any file whose referenced stats are
    // missing/NULL (mirrors getDataSkippedFiles); OR the reads so a match against any one keeps it.
    Some(perReadPredicates.flatten
      .map(pred => pred.expr || !verifyStatsForFilter(pred.referencedStats))
      .reduce(_ || _))
  }

  /**
   * Lazily applies a [[conflictSkippingCondition]] to `filesDf`. No job is launched here, so
   * callers can fuse the filter into their own query, e.g. with a partition filter. The returned
   * DataFrame keeps the input's columns, with `stats` still the original JSON string.
   */
  private[delta] def applyConflictSkippingCondition(
      filesDf: DataFrame,
      condition: Column): DataFrame = {
    // The skipping expressions reference the parsed `stats` struct; keep the raw JSON aside so the
    // result still has the AddFile shape.
    val rawStatsCol = "__conflict_skipping_raw_stats"
    filesDf
      .withColumn(rawStatsCol, col("stats"))
      .withColumn("stats", parseStats(col("stats")))
      .where(condition)
      .withColumn("stats", col(rawStatsCol))
      .drop(rawStatsCol)
  }
}
