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

import org.apache.spark.sql.Column
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
   * that method so all reads are evaluated in a single Spark job.
   */
  private[delta] def filterFilesByDataSkipping(
      files: Seq[AddFile],
      dataFilters: Seq[Expression]): Seq[AddFile] =
    filterFilesMatchingAnyReadPredicate(files, Seq(dataFilters))

  /**
   * Conflict-detection helper (reader-side data skipping) over several INDEPENDENT reads: returns
   * the subset of `files` that could still match ANY read and therefore must be treated as
   * conflicts. Each inner `Seq[Expression]` is one logical read's data filters (AND-combined via
   * [[buildDataSkippingPredicate]]); the reads are OR-combined, matching read semantics -- a file
   * is a candidate if it could match any one of them.
   *
   * All reads are evaluated in a SINGLE Spark job: the per-read skipping predicates are OR-ed
   * together into one `where` clause, rather than filtering per read and unioning the survivors
   * (which launched one job per read). Note we cannot instead flatten every read's filters into one
   * predicate -- that would AND them (read1 AND read2), the opposite of the OR we need.
   *
   * One-way safe: each read contributes `expr || !verifyStatsForFilter(...)` exactly as
   * [[getDataSkippedFiles]], so a file is dropped only when its stats *prove* it fails EVERY read
   * -- a real conflict is never a false negative. Files with missing/insufficient stats, a
   * read with no usable skipping predicate (empty / ineligible filters -> matches everything), or a
   * table without stats are all kept. Returns the original [[AddFile]]s (matched by path).
   *
   * Fail-safe: skipping here is a pure optimization over correct (conservative) conflict detection,
   * so if building or evaluating the predicate throws we fall back to the default behavior of
   * keeping all `files` as conflict candidates rather than failing the commit.
   */
  private[delta] def filterFilesMatchingAnyReadPredicate(
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
}
