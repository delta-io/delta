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

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, DeltaConfigs, DeltaLog, DeltaTestUtils, MergeIntoSQLTestUtils}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.coordinatedcommits.{CatalogOwnedCommitCoordinatorProvider, CatalogOwnedTableUtils, TrackingInMemoryCommitCoordinatorBuilder}

import org.apache.spark.SparkConf

/**
 * Generates AMT (`adaptiveMetadata-preview`) variants of the MERGE INTO test suites.
 */
trait MergeIntoAMTMixin
  extends AMTCheckpointTestBase
  with MergeIntoSQLTestUtils {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(defaultPropertyKey(AdaptiveMetadataTableFeature), FEATURE_PROP_SUPPORTED)
    // Disable periodic checkpoints so the only AMT checkpoints are the two that
    // withAMTCheckpointsAroundMerge brackets each MERGE with.
    .set(DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey, Int.MaxValue.toString)

  // Register the in-memory commit coordinator before super.beforeAll(). Some MERGE suites (e.g.
  // RowTrackingMergeSuiteBase) create catalog-managed helper tables in their own beforeAll(), which
  // runs before the beforeEach() that normally registers the coordinator; without it, creating
  // those AMT tables fails with "Couldn't locate commit coordinator for ...".
  override def beforeAll(): Unit = {
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    catalogOwnedCoordinatorBackfillBatchSize.foreach { batchSize =>
      CatalogOwnedCommitCoordinatorProvider.registerBuilder(
        catalogName = CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING,
        commitCoordinatorBuilder = TrackingInMemoryCommitCoordinatorBuilder(batchSize))
    }
    super.beforeAll()
  }

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

  // Bracket each MERGE with two checkpoints: a full one before (so the MERGE reads an AMT-backed
  // target, exercising AMTCheckpointProvider.fromCheckpoint) and an incremental one after. The
  // invariant: the post-MERGE checkpoint is a pure metadata reorganization -- the live file set is
  // identical immediately before and after it. Only applies when the target resolves to an
  // AMT-backed Delta table; temp views, non-Delta / not-yet-created targets and analysis-error
  // negative cases run the MERGE unchanged (and a throwing MERGE skips the post-checkpoint/assert).
  private def withAMTCheckpointsAroundMerge(target: String)(runMerge: => Unit): Unit = {
    resolveAMTDeltaLog(target) match {
      case Some(deltaLog) =>
        commitCheckpoint(deltaLog, incremental = false)
        runMerge
        val liveBeforeCheckpoint = liveFileKeys(deltaLog)
        commitCheckpoint(deltaLog, incremental = true)
        val liveAfterCheckpoint = liveFileKeys(deltaLog)
        assert(liveAfterCheckpoint == liveBeforeCheckpoint,
          "AMT checkpoint after MERGE changed the live file set: only-before = " +
            s"${liveBeforeCheckpoint -- liveAfterCheckpoint}, only-after = " +
            s"${liveAfterCheckpoint -- liveBeforeCheckpoint}")
      case None =>
        runMerge
    }
  }

  // Resolves `target` to its DeltaLog, or None if it is not a resolvable AMT-backed Delta table
  // (temp view, non-Delta target, not-yet-created table, analysis-error negative case).
  private def resolveAMTDeltaLog(target: String): Option[DeltaLog] = {
    val deltaLogOpt =
      try {
        DeltaTestUtils.getTableIdentifierOrPath(target) match {
          case DeltaTestUtils.TableIdentifierOrPath.Identifier(id, _) =>
            Some(DeltaLog.forTable(spark, id))
          case DeltaTestUtils.TableIdentifierOrPath.Path(path, _) =>
            Some(DeltaLog.forTable(spark, path))
        }
      } catch {
        case NonFatal(_) => None
      }
    deltaLogOpt.filter(_.update().protocol.isFeatureSupported(AdaptiveMetadataTableFeature))
  }

  // The set of live files, keyed the way AMT identifies them (path + deletion-vector object
  // identity) so a DV re-encoded by the checkpoint still compares equal.
  private def liveFileKeys(deltaLog: DeltaLog) =
    deltaLog.update().allFiles.collect()
      .map(_.toUniqueFileActionTuple(deltaLog.dataPath, useObjectIdentity = true)).toSet

  abstract override def executeMerge(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): Unit =
    withAMTCheckpointsAroundMerge(target) {
      super.executeMerge(target, source, condition, update, insert)
    }

  abstract override def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit =
    withAMTCheckpointsAroundMerge(tgt) {
      super.executeMerge(tgt, src, cond, clauses: _*)
    }

  abstract override def executeMergeWithSchemaEvolution(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit =
    withAMTCheckpointsAroundMerge(tgt) {
      super.executeMergeWithSchemaEvolution(tgt, src, cond, clauses: _*)
    }
}
