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

import java.io.File

// scalastyle:off import.ordering.noEmptyLine
import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.{CommitStats, CurrentTransactionInfo, DeltaOperations, Snapshot}
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.functions.col

class AMTCheckpointWriteSuite extends AMTCheckpointTestBase {

  test("interval boundary emits a follow-up OPTIMIZE CHECKPOINT commit carrying the Checkpoint") {
    withTable("amt_inline_emit") {
      val name = "amt_inline_emit"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1: not an interval boundary.
      sql(s"INSERT INTO $name VALUES (2)") // v2: interval boundary -> schedule maintenance.

      val deltaLog = deltaLogForName(name)
      val path = tablePath(name)
      val snapshot = deltaLog.update()
      // The AMT is written by a follow-up OPTIMIZE CHECKPOINT commit at v3, not inline at v2.
      assert(snapshot.version == 3, "A follow-up OPTIMIZE CHECKPOINT commit lands at v3.")

      // Manifest tree exists on disk: exactly one root, at least one leaf.
      assert(rootFiles(path).size == 1, "Exactly one root manifest must be written.")
      assert(leafFiles(path).nonEmpty, "At least one leaf manifest must be written.")

      // The v2 business commit carries only the user AddFile, no Checkpoint.
      val v2Actions = actionsAt(deltaLog, 2)
      assert(v2Actions.exists(_.isInstanceOf[AddFile]), "v2 carries the user AddFile.")
      assert(v2Actions.collect { case c: Checkpoint => c }.isEmpty,
        s"v2 must not carry a Checkpoint action; got: $v2Actions")

      // The v3 follow-up commit carries a single Checkpoint action and no user AddFile.
      val v3Actions = actionsAt(deltaLog, 3)
      val checkpoints = v3Actions.collect { case c: Checkpoint => c }
      assert(checkpoints.size == 1, s"Expected one Checkpoint action at v3, got: $v3Actions")
      assert(!v3Actions.exists(_.isInstanceOf[AddFile]), "v3 carries no user AddFile.")
      // The Checkpoint describes state as of v2 (the version whose maintenance it fulfills).
      assert(checkpoints.head.version == 2,
        s"Checkpoint must describe state as of v2; got ${checkpoints.head.version}")

      // The Checkpoint's contentRoot points at the on-disk root file.
      val rootName = new File(checkpoints.head.contentRoot.path).getName
      assert(isRootFileName(rootName),
        s"contentRoot must point at a root manifest file; got ${checkpoints.head.contentRoot.path}")
      assert(rootFiles(path).exists(_.getName == rootName))
    }
  }

  test("no emission on a vanilla (non-AMT) table") {
    withTable("amt_vanilla") {
      val name = "amt_vanilla"
      sql(
        s"""CREATE TABLE $name (id INT) USING DELTA
           |TBLPROPERTIES ('delta.checkpointInterval' = '2')""".stripMargin)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)") // interval boundary, but no AMT feature.

      val deltaLog = deltaLogForName(name)
      val path = tablePath(name)
      assert(rootFiles(path).isEmpty && leafFiles(path).isEmpty,
        "No AMT artifacts on a vanilla table.")
      assert(checkpointsAt(deltaLog, 2).isEmpty, "No Checkpoint action on a vanilla table.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("no emission on a non-interval commit") {
    withTable("amt_non_interval") {
      val name = "amt_non_interval"
      createAMTTable(name, checkpointInterval = 10) // interval far from the versions we write.
      sql(s"INSERT INTO $name VALUES (1)") // v1: 1 % 10 != 0.

      val deltaLog = deltaLogForName(name)
      assert(checkpointsAt(deltaLog, 1).isEmpty, "v1 is not an interval boundary; no emission.")
      assert(rootFiles(tablePath(name)).isEmpty,
        "No manifest tree written off an interval boundary.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("leaf cardinality respects AMT_ENTRIES_PER_LEAF") {
    withTable("amt_entries_per_leaf") {
      val name = "amt_entries_per_leaf"
      createAMTTable(name, checkpointInterval = 2)
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "1") {
        sql(s"INSERT INTO $name VALUES (1)") // v1: one data file.
        sql(s"INSERT INTO $name VALUES (2)") // v2: one more data file -> 2 live files.
      }
      val path = tablePath(name)
      // Two live AddFiles, one per leaf -> two leaves, one root.
      assert(leafFiles(path).size == 2, s"Expected 2 leaves, got ${leafFiles(path).size}.")
      assert(rootFiles(path).size == 1)
    }
  }

  /**
   * Rewrites the whole manifest tree of `tableName` from scratch (the OPTIMIZE-checkpoint write
   * path) and returns the write result plus the read snapshot it rewrote.
   */
  private def runFullRewrite(tableName: String): (AMTWriteResult, Snapshot) = {
    val snapshot = deltaLogForName(tableName).update()
    val manager = new AMTWriterManager(snapshot, DeltaOperations.OptimizeCheckpoint(
      incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
    val txnInfo = new CurrentTransactionInfo(
      txnId = "txn",
      readPredicates = Vector.empty,
      readFiles = Set.empty,
      readWholeTable = false,
      readAppIds = Set.empty,
      metadata = snapshot.metadata,
      protocol = snapshot.protocol,
      actions = Seq.empty,
      readSnapshot = snapshot,
      commitInfo = None,
      readRowIdHighWatermark = 0L,
      catalogTable = None,
      domainMetadata = Seq.empty,
      op = DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
    val result = manager.writeAMT(
      commitVersion = snapshot.version + 1,
      currentTransactionInfo = txnInfo,
      preCommitLogSegment = snapshot.logSegment)
    assert(result.isDefined, "A full rewrite must emit a manifest tree.")
    (result.get, snapshot)
  }

  test("full rewrite reconstructs identical table state") {
    withTable("amt_full_rewrite") {
      val name = "amt_full_rewrite"
      createAMTTable(name, checkpointInterval = 100) // Interval far away: no incremental emission.
      sql(s"INSERT INTO $name VALUES (1)") // v1
      sql(s"INSERT INTO $name VALUES (2)") // v2
      sql(s"INSERT INTO $name VALUES (3)") // v3 -- 3 live files.

      // Small leaf size so the rewrite splits the files across multiple leaves.
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "2") {
        val (result, snapshot) = runFullRewrite(name)
        val before = snapshot.allFiles.collect().map(_.path).toSet

        // One root, at least one leaf, and the rewritten tree reconstructs the same file set.
        val provider =
          AMTCheckpointProvider.fromCheckpoint(spark, deltaLogForName(name), result.checkpoint)
        assert(rootFiles(tablePath(name)).size == 1)
        assert(provider.leaves.nonEmpty)
        val reconstructed = provider
          .loadActionsForStateReconstruction(spark, deltaLogForName(name)).get
          .where(col("add").isNotNull)
          .select("add.path")
          .collect()
          .map(_.getString(0))
          .toSet
        assert(reconstructed == before, s"File set changed: before=$before after=$reconstructed")
      }
    }
  }

  /** Parses the `delta.commit.stats` [[CommitStats]] logged for `version`, or fails. */
  private def commitStatsAt(f: => Unit, version: Long): CommitStats = {
    Log4jUsageLogger.track(f)
      .filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
        e.tags.get("opType").contains("delta.commit.stats"))
      .map(e => JsonUtils.fromJson[CommitStats](e.blob))
      .find(_.commitVersion == version)
      .getOrElse(fail(s"No commit stats logged for version $version."))
  }

  test("the follow-up OPTIMIZE CHECKPOINT commit stats carry AMT write metrics") {
    withTable("amt_commit_stats") {
      val name = "amt_commit_stats"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1: below the interval, no maintenance.

      // v2 hits the interval boundary; the AMT is written by the follow-up commit at v3, so the
      // AMT write metrics are recorded on v3's stats, not v2's.
      val allStats = Log4jUsageLogger.track {
        sql(s"INSERT INTO $name VALUES (2)")
      }.filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
          e.tags.get("opType").contains("delta.commit.stats"))
        .map(e => JsonUtils.fromJson[CommitStats](e.blob))

      val v2Stats = allStats.find(_.commitVersion == 2).getOrElse(fail("No stats for v2."))
      assert(v2Stats.amtWriteMetrics.isEmpty, "v2 defers the AMT; its stats carry no AMT metrics.")

      val v3Stats = allStats.find(_.commitVersion == 3).getOrElse(fail("No stats for v3."))
      val metrics = v3Stats.amtWriteMetrics
        .getOrElse(fail("The follow-up commit's stats should carry AMT write metrics."))
      assert(metrics.attempts.size == 1, s"Expected one AMT write attempt, got ${metrics.attempts}")
      // The first AMT has no prior tree to build on, so it is always a full rewrite.
      assert(metrics.attempts.head.trigger == AMTTriggerMode.CheckpointIntervalFull.name)
      assert(metrics.attempts.head.materializeDurationMs >= 0L)
    }
  }

  test("commit stats carry no AMT write metrics when no AMT is emitted") {
    withTable("amt_no_commit_stats") {
      val name = "amt_no_commit_stats"
      createAMTTable(name, checkpointInterval = 100) // interval far away, so v1 emits no AMT.

      val commitStats = commitStatsAt(sql(s"INSERT INTO $name VALUES (1)"), version = 1)

      assert(commitStats.amtWriteMetrics.isEmpty,
        "Commit stats must not carry AMT write metrics when no AMT is emitted.")
    }
  }
}
