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

package org.apache.spark.sql.delta.amt

import org.apache.spark.sql.delta.MergeIntoSQLTestUtils

/**
 * Generates AMT (`adaptiveMetadata-preview`) variants of the MERGE INTO test suites.
 *
 * Each MERGE is bracketed by the [[AMTDMLTestUtils]] checkpoints.
 *
 * The MERGE AMT mixin should come after all other mixins of a suite, so its dimension should be
 * the last of every generator config it is used in. This is for 2 reasons:
 * 1. Visibility: some mixins declare beforeAll public, and a later mixin overriding it as
 *    protected (which AMTDMLTestUtils does) fails to compile. This is a latent issue for the MERGE
 *    suites.
 * 2. Commit orders: some mixins perform extra commits. To maintain a stable commit order, we
 *    prefer to have AMT commits wrap them all.
 */
trait MergeIntoAMTMixin
  extends AMTDMLTestUtils
  with MergeIntoSQLTestUtils {

  override def excluded: Seq[String] = super.excluded ++ Seq(
    // scalastyle:off line.size.limit
    // AMT tables are always catalog-managed, so the path-based (catalogManaged=false) analysis-
    // snapshot-reuse variants are not applicable.
    "merge SQL command reuses analysis snapshot in SQL environments (catalogManaged=false)",
    "merge SQL command does not reuse analysis snapshot when config is disabled (catalogManaged=false)",
    // This test strips record-count stats from the target files (AddFile.stats = null) to exercise
    // Delta's graceful missing-stats handling. AMT cannot represent such files: its manifest
    // requires a per-file physical record count (DataEntry.fromAddFile throws on a stats-less
    // AddFile), so the post-commit AMT checkpoint fails before the assertion is reached.
    "merge logs error if number of records are missing in stats",
    // RowTrackingMerge: these tests create a table with delta.enableRowTracking = false (one to
    // assert row tracking stays off, one to later enable it via backfill). AMT mandates row
    // tracking, so table creation fails with DELTA_ADAPTIVE_METADATA_REQUIRES_DEPENDENT_FEATURE_
    // ENABLED. Structural AMT invariant (row tracking cannot be disabled), not a MERGE bug.
    "Row tracking marked as not preserved when row tracking disabled",
    "MERGE preserves Row Tracking on tables enabled using backfill"
    // scalastyle:on line.size.limit
  )

  abstract override def executeMerge(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): Unit =
    withAMTCheckpointsAround(target) {
      super.executeMerge(target, source, condition, update, insert)
    }

  abstract override def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit =
    withAMTCheckpointsAround(tgt) {
      super.executeMerge(tgt, src, cond, clauses: _*)
    }

  abstract override def executeMergeWithSchemaEvolution(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit =
    withAMTCheckpointsAround(tgt) {
      super.executeMergeWithSchemaEvolution(tgt, src, cond, clauses: _*)
    }
}
