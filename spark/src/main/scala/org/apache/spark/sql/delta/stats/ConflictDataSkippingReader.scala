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

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.ClassicColumnConversions._
import org.apache.spark.sql.delta.DeltaTableUtils
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.expressions.DecodeNestedZ85EncodedVariant
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkContext
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
   * Filters `dataFilters` to those safe to use for conflict-time skipping: no subqueries, fully
   * deterministic, no metadata attributes. Mirrors [[filesForScan]] eligibility so we never build a
   * predicate (stats or value-exact) that could wrongly exclude a matching file. Shared by the
   * stats tier ([[buildDataSkippingPredicate]]) and the value-exact tier
   * ([[filterFilesByValueExactScan]]).
   */
  private def eligibleSkippingFilters(dataFilters: Seq[Expression]): Seq[Expression] = {
    import DeltaTableUtils._
    dataFilters.filterNot { f =>
      containsSubquery(f) || !f.deterministic || f.exists {
        case MetadataAttribute(_) => true
        case _ => false
      }
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
    // Stats-skipping conf, inlined from DataSkippingReaderBase.useStats (file-private, so not
    // visible here). Reading it inline avoids widening that member's visibility.
    if (!spark.sessionState.conf.getConf(DeltaSQLConf.DELTA_STATS_SKIPPING)) return None
    val eligibleFilters = eligibleSkippingFilters(dataFilters)
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
   * that method so all reads are evaluated in a single Spark job.
   */
  private[delta] def filterFilesByDataSkipping(
      files: Seq[AddFile],
      dataFilters: Seq[Expression]): Seq[AddFile] =
    filterFilesMatchingAnyReadPredicate(files, Seq(dataFilters))

  /**
   * Conflict-detection helper (reader-side data skipping) over several INDEPENDENT reads: returns
   * the subset of `files` that could still match ANY read and therefore must be treated as
   * conflicts. Each inner `Seq[Expression]` is one logical read's data filters (AND-combined); the
   * reads are OR-combined, matching read semantics -- a file is a candidate if it could match any
   * one of them.
   *
   * Two tiers, both one-way safe (a file is dropped only when it is *proven* to fail EVERY read, so
   * a real conflict is never a false negative) and both fail-safe (any error falls back to keeping
   * all `files`, since skipping is a pure optimization over correct conservative detection):
   *   1. [[filterFilesByStatsSkipping]] -- column min/max stats, no data I/O (always on with the
   *      feature). Cannot resolve predicates min/max does not model (modulo, non-range exprs).
   *   2. [[filterFilesByValueExactScan]] -- opt-in via
   *      [[DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_VALUE_EXACT_ENABLED]]: for the files
   *      tier 1 could not rule out, reads their actual rows and evaluates the real predicates,
   *      dropping any file with zero matching rows. Resolves the stats-inconclusive cases at the
   *      cost of reading the (already narrowed) added files during commit.
   */
  private[delta] def filterFilesMatchingAnyReadPredicate(
      files: Seq[AddFile],
      dataFiltersPerRead: Seq[Seq[Expression]]): Seq[AddFile] = {
    // Tier 1 (cheap, no data I/O): column min/max stats skipping.
    val statsSurvivors = filterFilesByStatsSkipping(files, dataFiltersPerRead)
    // Tier 2 (opt-in, reads data): for files stats could NOT rule out, evaluate the real read
    // predicates against their actual rows. Resolves predicates min/max cannot skip (e.g. modulo).
    if (statsSurvivors.isEmpty ||
        !spark.sessionState.conf.getConf(
          DeltaSQLConf.DELTA_CONFLICT_DETECTION_DATA_SKIPPING_VALUE_EXACT_ENABLED)) {
      statsSurvivors
    } else {
      filterFilesByValueExactScan(statsSurvivors, dataFiltersPerRead)
    }
  }

  /**
   * Tier 1 -- column min/max stats skipping (see [[filterFilesMatchingAnyReadPredicate]]). Returns
   * the subset of `files` whose stats do NOT prove they fail every read.
   *
   * All reads are evaluated in a SINGLE Spark job: the per-read skipping predicates are OR-ed
   * together into one `where` clause, rather than filtering per read and unioning the survivors
   * (which launched one job per read). Note we cannot instead flatten every read's filters into one
   * predicate -- that would AND them (read1 AND read2), the opposite of the OR we need.
   *
   * One-way safe: each read contributes `expr || !verifyStatsForFilter(...)` exactly as
   * [[getDataSkippedFiles]], so a file is dropped only when its stats *prove* it fails EVERY read.
   * Files with missing/insufficient stats, a read with no usable skipping predicate (empty /
   * ineligible filters -> matches everything), or a table without stats are all kept. Returns the
   * original [[AddFile]]s (matched by path). Any failure falls back to keeping all `files`.
   */
  private def filterFilesByStatsSkipping(
      files: Seq[AddFile],
      dataFiltersPerRead: Seq[Seq[Expression]]): Seq[AddFile] = {
    import org.apache.spark.sql.delta.implicits._
    if (files.isEmpty || dataFiltersPerRead.isEmpty || schema.isEmpty) return files
    try {
      // One skipping predicate per read. A read with no usable predicate (empty or ineligible
      // filters) matches every file -> nothing can be skipped, so keep all files without a job.
      val perReadPredicates = dataFiltersPerRead.map { dataFilters =>
        if (dataFilters.isEmpty) None else buildDataSkippingPredicate(dataFilters)
      }
      if (perReadPredicates.exists(_.isEmpty)) return files
      // Survive if the file could match ANY read. Per read, `expr || !verifyStatsForFilter(...)`
      // keeps any file whose referenced stats are missing/NULL (mirrors getDataSkippedFiles): only
      // skip when stats prove no match. OR the reads so a match against any one keeps the file.
      val survivorCondition = perReadPredicates.flatten
        .map(pred => pred.expr || !verifyStatsForFilter(pred.referencedStats))
        .reduce(_ || _)
      val survivingPaths = recordFrameProfile(
          "Delta", "DataSkippingReader.filterFilesMatchingAnyReadPredicate") {
        files.toDF(spark)
          .withColumn("stats", parseAndDecodeStats(col("stats")))
          .where(survivorCondition)
          .select("path")
          .collect()
          .map(_.getString(0))
          .toSet
      }
      files.filter(f => survivingPaths.contains(f.path))
    } catch {
      case NonFatal(e) =>
        // Optimization only: never let a skipping failure abort a commit. Fall back to the default
        // (feature-off) behavior of treating every added file as a conflict candidate.
        logWarning(log"Conflict-time data skipping failed to evaluate; falling back to treating " +
          log"all added files as conflict candidates", e)
        files
    }
  }

  /**
   * Tier 2 -- value-exact scan (see [[filterFilesMatchingAnyReadPredicate]]). For the `files` that
   * tier 1 stats skipping could not rule out, read their ACTUAL data and evaluate the real read
   * predicates, keeping only files that genuinely have >= 1 matching row. This resolves predicates
   * column min/max cannot skip (e.g. `a % 2 = 1`, other non-range expressions): a file whose stats
   * range spans the predicate but whose values never satisfy it is dropped here, avoiding an
   * unnecessary conflict.
   *
   * Reads are OR-combined, AND within a read -- the same semantics tier 1 approximates. A read with
   * no eligible (deterministic, subquery-free, non-metadata) filter matches everything, so we
   * cannot prove non-match and keep all `files`. Using only the eligible filters is a sound
   * over-approximation of the true read predicate (dropping a conjunct only makes it match more),
   * so a real conflict is never a false negative.
   *
   * Runs a SINGLE, short-circuiting Spark job over the (already stats-narrowed) files: does ANY of
   * them still hold a row matching the read predicates? If so, keep them ALL as conflict
   * candidates; if not, drop them all. That is exactly what the caller
   * ([[org.apache.spark.sql.delta.ConflictChecker]]) consumes -- an unpartitioned table collapses
   * the survivors to `headOption`, and a partitioned table runs this before its partition filter,
   * so in both cases only "can anything still match?" matters, never which file. Deliberately an
   * existence check rather than per-file attribution: we never map a scanned row back to its
   * [[AddFile]], so there is no `_metadata.file_path` distinct/collect and no chance that a
   * path-canonicalization mismatch drops a genuinely-matching file (which would be a missed
   * conflict). One-way safe: dropping all files is sound only because the scan proved no candidate
   * has a matching row. Any failure falls back to keeping all `files`.
   */
  private[delta] def filterFilesByValueExactScan(
      files: Seq[AddFile],
      dataFiltersPerRead: Seq[Seq[Expression]]): Seq[AddFile] = {
    if (files.isEmpty || dataFiltersPerRead.isEmpty || schema.isEmpty) return files
    try {
      // AND within a read over its eligible filters; a read with none matches everything, so we
      // cannot prove any file fails it -> keep all files (no job).
      val perReadEligible = dataFiltersPerRead.map(eligibleSkippingFilters)
      if (perReadEligible.exists(_.isEmpty)) return files
      val snapshot = snapshotToScan
      val df = snapshot.deltaLog.createDataFrame(
        snapshot, files, actionTypeOpt = Some("conflictDetectionValueExact"))
      // Rebind each filter's attributes to the fresh DataFrame's columns by name, AND within a
      // read, OR across reads -- a file matches if any read's predicate matches any of its rows.
      val condition = perReadEligible
        .map(filters => filters.map(f => rebindToDataFrame(f, df)).reduce(_ && _))
        .reduce(_ || _)
      // The caller only needs a boolean (see the scaladoc), so run one short-circuiting existence
      // check instead of attributing rows back to files: keep all candidates if any row matches,
      // else drop them all.
      val sc = spark.sparkContext
      val prevJobDesc = sc.getLocalProperty(SparkContext.SPARK_JOB_DESCRIPTION)
      val anyRowMatches =
        try {
          sc.setJobDescription("Delta conflict detection: value-exact data skipping")
          recordFrameProfile("Delta", "DataSkippingReader.filterFilesByValueExactScan") {
            !df.where(condition).isEmpty
          }
        } finally {
          sc.setJobDescription(prevJobDesc)
        }
      if (anyRowMatches) files else Nil
    } catch {
      case NonFatal(e) =>
        // Optimization only: never let a value-exact scan failure abort a commit. Fall back to
        // keeping every stats-surviving file as a conflict candidate.
        logWarning(log"Conflict-time value-exact scan failed to evaluate; falling back to " +
          log"keeping all stats-surviving files as conflict candidates", e)
        files
    }
  }

  /**
   * Rebinds `e`'s attribute references to `df`'s output columns by name, so a read predicate
   * resolved against the transaction's plan can be evaluated on a freshly built DataFrame. Nested
   * accesses (`GetStructField` over a top-level struct column) are preserved. If a referenced name
   * is not a column of `df`, `df.col` throws and the caller's fail-safe keeps all files.
   */
  private def rebindToDataFrame(e: Expression, df: DataFrame): Column =
    Column(e.transform { case a: AttributeReference => df.col(a.name).expr })
}
