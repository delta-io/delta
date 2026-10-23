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
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.expressions.DecodeNestedZ85EncodedVariant
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.functions.{col, from_json}

/**
 * Reader-side data skipping used during conflict detection (row-level concurrency Case 1: writers
 * touching disjoint data ranges).
 *
 * Mixed into [[DataSkippingReaderBase]] as a self-typed trait so this feature is a single isolated
 * unit that only ADDS methods -- it does not modify any existing data-skipping code. It is opt-in:
 * the caller ([[org.apache.spark.sql.delta.ConflictChecker]]) invokes these methods only behind
 * `DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_ENABLED`, and each is one-way safe -- a file
 * is dropped only when its stats *prove* it cannot match, so a real conflict is never a false
 * negative.
 */
trait ConflictDataSkippingReader extends DeltaLogging { self: DataSkippingReaderBase =>

  /**
   * Parses (and, when the schema contains VariantType, Z85-decodes) the JSON `statsCol` into the
   * `statsSchema` struct, mirroring [[withStatsInternal0]]. Deliberately kept private to this trait
   * rather than shared with `withStatsInternal0`, so the existing stats path stays untouched.
   */
  private def parseAndDecodeStats(statsCol: Column): Column = {
    val parsedStats = from_json(statsCol, statsSchema)
    // Only use DecodeNestedZ85EncodedVariant if the schema contains VariantType.
    // This avoids performance overhead for tables without variant columns.
    // `DecodeNestedZ85EncodedVariant` is a temporary workaround since the Spark 4.1 from_json
    // expression has no way to decode a VariantVal from an encoded Z85 string.
    // TODO: Add Z85 decoding to Variant in Spark 4.2 and use that from_json option here.
    if (SchemaUtils.checkForVariantTypeColumnsRecursively(statsSchema)) {
      Column(DecodeNestedZ85EncodedVariant(parsedStats.expr))
    } else {
      parsedStats
    }
  }

  /**
   * Builds a single [[DataSkippingPredicate]] (skipping expression + the stats it references) from
   * `dataFilters`, mirroring [[filesForScan]]'s eligibility filtering, per-filter construction and
   * conjunction fold. Returns None when stats skipping is unavailable or no eligible filter yields
   * a predicate. The filters are AND-combined, so callers must pass filters from a single logical
   * read (predicates from independent reads have OR, not AND, semantics).
   */
  private[delta] def buildDataSkippingPredicate(
      dataFilters: Seq[Expression]): Option[DataSkippingPredicate] = {
    import DeltaTableUtils._
    // Stats-skipping conf, inlined from DataSkippingReaderBase.useStats (file-private, so not
    // visible here). Reading it inline avoids widening that member's visibility.
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)) return None
    // Mirror filesForScan eligibility: drop subquery / non-deterministic / metadata filters, so we
    // never build a skipping predicate that could wrongly exclude a matching file.
    val eligibleFilters = dataFilters.filterNot { f =>
      containsSubquery(f) || !f.deterministic || f.exists {
        case MetadataAttribute(_) => true
        case _ => false
      }
    }
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
   * Conflict-detection helper (reader-side data skipping): returns the subset of `files` whose
   * statistics do NOT prove they fail `dataFilters` -- i.e. the files that could still match and
   * therefore must be treated as conflicts. Files with missing/insufficient stats (or when skipping
   * is unavailable) are kept.
   *
   * `dataFilters` must come from a single logical read: they are AND-combined via
   * [[buildDataSkippingPredicate]]. This is the one-read case of
   * [[filterFilesMatchingAnyReadPredicate]]; callers with multiple independent reads should use
   * that method.
   */
  private[delta] def filterFilesByDataSkipping(
      files: Seq[AddFile],
      dataFilters: Seq[Expression]): Seq[AddFile] =
    filterFilesMatchingAnyReadPredicate(files, Seq(dataFilters))

  /**
   * Conflict-detection helper (reader-side data skipping) over several INDEPENDENT reads: returns
   * the subset of `files` that could still match ANY read and therefore must be treated as
   * conflicts. Collecting wrapper around [[filterFilesDfMatchingAnyReadPredicate]].
   */
  private[delta] def filterFilesMatchingAnyReadPredicate(
      files: Seq[AddFile],
      dataFiltersPerRead: Seq[Seq[Expression]]): Seq[AddFile] = {
    import org.apache.spark.sql.delta.implicits._
    if (files.isEmpty) return files
    filterFilesDfMatchingAnyReadPredicate(files.toDF(spark), dataFiltersPerRead) match {
      case None => files
      case Some(df) =>
        val survivingPaths = recordFrameProfile(
            "Delta", "DataSkippingReader.filterFilesMatchingAnyReadPredicate") {
          df.select("path").collect().map(_.getString(0)).toSet
        }
        files.filter(f => survivingPaths.contains(f.path))
    }
  }

  /**
   * Lazily restricts `filesDf` (a DataFrame of [[AddFile]]s with JSON `stats`) to the files that
   * could still match ANY of several INDEPENDENT reads, or returns None when nothing can be
   * skipped. Combines [[conflictSkippingCondition]] and [[applyConflictSkippingCondition]].
   */
  private[delta] def filterFilesDfMatchingAnyReadPredicate(
      filesDf: DataFrame,
      dataFiltersPerRead: Seq[Seq[Expression]]): Option[DataFrame] =
    conflictSkippingCondition(dataFiltersPerRead).map(applyConflictSkippingCondition(filesDf, _))

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
      .withColumn("stats", parseAndDecodeStats(col("stats")))
      .where(condition)
      .withColumn("stats", col(rawStatsCol))
      .drop(rawStatsCol)
  }
}
