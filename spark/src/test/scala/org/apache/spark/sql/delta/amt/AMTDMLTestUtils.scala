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

import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, DeltaConfigs, DeltaLog, DeltaTable, DeltaTestUtils, RowCommitVersion, RowId, Snapshot}
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.coordinatedcommits.{CatalogOwnedCommitCoordinatorProvider, CatalogOwnedTableUtils, TrackingInMemoryCommitCoordinatorBuilder}
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.functions.col

/**
 * Shared harness for running the generated DML test suites against AMT (`adaptiveMetadata-preview`)
 * tables.
 *
 * Each DML on an AMT target is bracketed by two AMT checkpoints: a full one before, so the DML
 * reads the target through [[AMTCheckpointProvider]], and an incremental one after, which must
 * leave the target's live files and rows unchanged.
 */
trait AMTDMLTestUtils extends AMTCheckpointTestBase {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(defaultPropertyKey(AdaptiveMetadataTableFeature), FEATURE_PROP_SUPPORTED)
    // Disable periodic checkpoints so the only AMT checkpoints are the ones the harness commits
    // around each DML.
    .set(DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey, Int.MaxValue.toString)
    // With the default cap, a small table's tree is a single root without leaves, and entries in
    // the root carry no back reference. One entry per leaf gives any table with more than one file
    // real leaves, so the files a DML removes carry back references into them.
    .set(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key, "1")

  // Register the in-memory commit coordinator before super.beforeAll(). Some suites (e.g.
  // RowTrackingMergeSuiteBase) create catalog-managed tables in their own beforeAll(), which runs
  // before the beforeEach() that normally registers the coordinator.
  override protected def beforeAll(): Unit = {
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    catalogOwnedCoordinatorBackfillBatchSize.foreach { batchSize =>
      CatalogOwnedCommitCoordinatorProvider.registerBuilder(
        catalogName = CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING,
        commitCoordinatorBuilder = TrackingInMemoryCommitCoordinatorBuilder(batchSize))
    }
    super.beforeAll()
  }

  /** The AMT table a DML target resolves to. */
  case class AMTTarget(deltaLog: DeltaLog, catalogTable: CatalogTable) {
    def snapshot: Snapshot = deltaLog.update(catalogTableOpt = Some(catalogTable))
  }

  // Resolves a DML target (optionally aliased) to the AMT table it writes. Temp views resolve to
  // the Delta table they read, so DMLs through a view are covered too. Returns None only when the
  // target does not resolve to a Delta table (a non-Delta target, or a negative case whose target
  // does not exist). Every Delta table these suites create is an AMT table, so a Delta target
  // without AMT fails instead of running the DML without the checkpoints.
  protected def resolveAMTTarget(target: String): Option[AMTTarget] = {
    val (tableName, _) = DeltaTestUtils.parseTableAndAlias(target)
    val fileIndexOpt =
      try {
        spark.table(tableName).queryExecution.analyzed
          .collectFirst { case DeltaTable(index: TahoeLogFileIndex) => index }
      } catch {
        case _: AnalysisException => None
      }
    fileIndexOpt.map { index =>
      val catalogTable = index.catalogTableOpt.getOrElse(
        fail(s"DML target $target resolves to a Delta table accessed by path, but AMT tables " +
          "are catalog-managed and accessed by name"))
      val amtTarget = AMTTarget(index.deltaLog, catalogTable)
      assert(AMTUtils.amtEnabled(amtTarget.snapshot),
        s"DML target $target resolves to a Delta table without AMT")
      amtTarget
    }
  }

  // Runs `runDML` on `target`. When the target is an AMT table, a full checkpoint before makes the
  // DML read the target through a freshly written tree, and an incremental one after folds the
  // DML's commits into that tree. The incremental checkpoint is a pure metadata reorganization, so
  // the live files and the rows (with their row tracking values) must be the same across it.
  protected def withAMTCheckpointsAround(target: String)(runDML: => Unit): Unit = {
    resolveAMTTarget(target) match {
      case Some(amtTarget) =>
        val fullProvider = commitHarnessCheckpoint(amtTarget, AMTTriggerMode.CheckpointIntervalFull)
        assert(fullProvider.checkpointAction.contentRoot.lastManifestCommitWithFullRewrite
          .contains(fullProvider.checkpointVersion),
          "A full checkpoint must record itself as the last full rewrite")
        runDML
        val allFilesBeforeCheckpoint = allFilesUniqueTuples(amtTarget)
        val rowsBeforeCheckpoint = tableDfWithRowTrackingColumns(amtTarget).collect()
        val incrementalProvider =
          commitHarnessCheckpoint(amtTarget, AMTTriggerMode.CheckpointIntervalIncremental)
        assert(incrementalProvider.checkpointAction.contentRoot.lastManifestCommitWithFullRewrite
          .contains(fullProvider.checkpointVersion),
          "The incremental checkpoint must build on the full checkpoint before the DML")
        val allFilesAfterCheckpoint = allFilesUniqueTuples(amtTarget)
        assert(allFilesAfterCheckpoint == allFilesBeforeCheckpoint,
          "AMT checkpoint after the DML changed the live file set: only-before = " +
            s"${allFilesBeforeCheckpoint -- allFilesAfterCheckpoint}, only-after = " +
            s"${allFilesAfterCheckpoint -- allFilesBeforeCheckpoint}")
        checkAnswer(tableDfWithRowTrackingColumns(amtTarget), rowsBeforeCheckpoint)
      case None =>
        runDML
    }
  }

  // The set of live files, keyed the way AMT identifies them (path + deletion-vector object
  // identity) so a DV re-encoded by the checkpoint still compares equal.
  private def allFilesUniqueTuples(target: AMTTarget) =
    target.snapshot.allFiles.collect()
      .map(_.toUniqueFileActionTuple(target.deltaLog.dataPath, useObjectIdentity = true)).toSet

  // The target's rows with their row IDs and row commit versions (AMT mandates row tracking).
  private def tableDfWithRowTrackingColumns(target: AMTTarget): DataFrame =
    spark.table(target.catalogTable.identifier.quotedString)
      .select(
        col("*"), col(RowId.QUALIFIED_COLUMN_NAME), col(RowCommitVersion.QUALIFIED_COLUMN_NAME))

  // Commits an AMT checkpoint the way the post-commit checkpoint hook does: an OPTIMIZE CHECKPOINT
  // commit on top of the latest version. Returns the provider the table then reads through.
  private def commitHarnessCheckpoint(
      target: AMTTarget,
      triggerMode: AMTTriggerMode): AMTCheckpointProvider = {
    val snapshotBefore = target.snapshot
    target.deltaLog.checkpoint(snapshotBefore, Some(target.catalogTable), Some(triggerMode))
    val snapshotAfter = target.snapshot
    assert(snapshotAfter.version == snapshotBefore.version + 1,
      s"Expected the $triggerMode checkpoint of v${snapshotBefore.version} to commit " +
        s"v${snapshotBefore.version + 1}, but the table is at v${snapshotAfter.version}")
    val provider = amtProvider(snapshotAfter).getOrElse(
      fail(s"The $triggerMode checkpoint left the table without an AMT-backed snapshot"))
    assert(provider.checkpointVersion == snapshotBefore.version,
      s"The $triggerMode checkpoint describes v${provider.checkpointVersion}, " +
        s"not v${snapshotBefore.version}")
    assert(
      provider.checkpointAction.contentRoot.isIncremental.contains(triggerMode.isIncremental),
      s"The $triggerMode checkpoint wrote a tree tagged isIncremental = " +
        provider.checkpointAction.contentRoot.isIncremental)
    provider
  }
}
