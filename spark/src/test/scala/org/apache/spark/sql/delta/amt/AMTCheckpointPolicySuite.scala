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

import com.databricks.spark.util.{Log4jUsageLogger, MetricDefinitions}
import org.apache.spark.sql.delta.{AdaptiveMetadataTableFeature, CommitStats, DeltaLog}
import org.apache.spark.sql.delta.DeltaOperations
import org.apache.spark.sql.delta.actions.Checkpoint
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.catalyst.TableIdentifier

/**
 * Emission-policy scenarios for AMT (`adaptiveMetadata-preview`): the checkpoint-interval trigger
 * (deferred to a follow-up OPTIMIZE CHECKPOINT commit), the full-vs-incremental rewrite cadence.
 */
class AMTCheckpointPolicySuite extends AMTCheckpointTestBase {

  /** The Checkpoint action emitted at exactly `version`, or fails. */
  private def requireCheckpointAt(deltaLog: DeltaLog, version: Long): Checkpoint =
    checkpointAt(deltaLog, version).getOrElse(
      fail(s"Expected a Checkpoint at v$version."))

  /** Every AMT [[Checkpoint]] emitted in `[0, latestVersion]`, in commit order. */
  private def allCheckpoints(deltaLog: DeltaLog): Seq[Checkpoint] =
    allCheckpointsWithCommitVersion(deltaLog).map(_._2)

  /**
   * Every AMT [[Checkpoint]] emitted in `[0, latestVersion]`, paired with the version of the commit
   * that carries it (the manifest commit version), in commit order.
   */
  private def allCheckpointsWithCommitVersion(deltaLog: DeltaLog): Seq[(Long, Checkpoint)] = {
    val latest = deltaLog.update().version
    (0L to latest).flatMap(v => checkpointAt(deltaLog, v).map(cp => (v, cp)))
  }

  /** The trigger name recorded in the AMT write metrics of the commit `f` produces at `version`. */
  private def amtTriggerNameAt(f: => Unit, version: Long): String =
    amtWriteMetricsAt(f, version).trigger

  /** The AMT write metrics logged for the commit `f` produces at `version`. */
  private def amtWriteMetricsAt(f: => Unit, version: Long): SingleAMTWriteMetrics = {
    Log4jUsageLogger.track(f)
      .filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
        e.tags.get("opType").contains("delta.commit.stats"))
      .map(e => JsonUtils.fromJson[CommitStats](e.blob))
      .find(_.commitVersion == version)
      .flatMap(_.amtWriteMetrics)
      .flatMap(_.attempts.headOption)
      .getOrElse(fail(s"No AMT write metrics logged for version $version."))
  }

  /**
   * Writes a full AMT via a direct OPTIMIZE CHECKPOINT commit (the same operation the post-commit
   * checkpoint hook issues), landing a full rewrite that describes the latest committed version.
   * Used to bootstrap a full anchor at an arbitrary (off-interval-grid) version.
   */
  private def writeFullOptimizeCheckpoint(tableName: String): Unit = {
    val deltaLog = deltaLogForName(tableName)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
    deltaLog.startTransaction(Some(catalogTable), Some(deltaLog.update())).commit(
      Seq.empty,
      DeltaOperations.OptimizeCheckpoint(
        incremental = false, triggerName = AMTTriggerMode.CheckpointIntervalFull.name))
  }


  /**
   * One expected AMT checkpoint in a deterministic emission timeline.
   *
   * @param checkpointedVersion   the table version the emitted Checkpoint describes (the
   *                              triggering business commit)
   * @param manifestCommitVersion the version of the commit that carries the Checkpoint action -- in
   *                              deferred mode the follow-up OPTIMIZE CHECKPOINT commit, which
   *                              lands at checkpointedVersion + 1
   * @param incremental           whether the rewrite is incremental (false = full rewrite)
   * @param lastFullRewrite       expected `lastManifestCommitWithFullRewrite` marker on the
   *                              checkpoint
   */
  private case class ExpectedCheckpoint(
      checkpointedVersion: Long,
      manifestCommitVersion: Long,
      incremental: Boolean,
      lastFullRewrite: Long)

  /**
   * Asserts that the AMT checkpoints emitted on `deltaLog` over `[0, latest]` are exactly
   * `expected`, in order, matching on described version, manifest commit version, incremental flag,
   * and last-full-rewrite marker. Because every checkpoint is read back off disk, this verifies the
   * full-vs-incremental cadence and the marker tracking across the whole timeline in one shot.
   */
  private def assertCheckpointTimeline(
      deltaLog: DeltaLog, expected: Seq[ExpectedCheckpoint]): Unit = {
    val checkpoints = allCheckpointsWithCommitVersion(deltaLog)
    assert(checkpoints.map(_._2.version) == expected.map(_.checkpointedVersion),
      s"Emitted checkpoint versions ${checkpoints.map(_._2.version)} must match " +
        s"${expected.map(_.checkpointedVersion)}.")
    assert(checkpoints.map(_._1) == expected.map(_.manifestCommitVersion),
      s"Manifest commit versions ${checkpoints.map(_._1)} must match " +
        s"${expected.map(_.manifestCommitVersion)}.")
    checkpoints.zip(expected).foreach { case ((_, cp), exp) =>
      assert(cp.contentRoot.isIncremental.contains(exp.incremental),
        s"Checkpoint describing v${exp.checkpointedVersion}: expected incremental=" +
          s"${exp.incremental}, got ${cp.contentRoot.isIncremental}")
      assert(cp.contentRoot.lastManifestCommitWithFullRewrite.contains(exp.lastFullRewrite),
        s"Checkpoint describing v${exp.checkpointedVersion}: expected lastFullRewrite=" +
          s"${exp.lastFullRewrite}, got ${cp.contentRoot.lastManifestCommitWithFullRewrite}")
    }
  }

  test("no AMT is emitted below the checkpoint interval") {
    withTable("amt_below_interval") {
      val name = "amt_below_interval"
      createAMTTable(name, checkpointInterval = 10)
      sql(s"INSERT INTO $name VALUES (1)") // v1: 1 % 10 != 0.

      val deltaLog = deltaLogForName(name)
      val path = tablePath(name)
      assert(deltaLog.update().version == 1, "No follow-up commit below the interval.")
      assert(rootFiles(path).isEmpty && leafFiles(path).isEmpty, "No manifest tree written.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("the interval boundary emits a follow-up OPTIMIZE CHECKPOINT commit at V+1") {
    withTable("amt_interval_followup") {
      val name = "amt_interval_followup"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)") // v1.
      sql(s"INSERT INTO $name VALUES (2)") // v2: boundary -> follow-up at v3.

      val deltaLog = deltaLogForName(name)
      assert(deltaLog.update().version == 3, "The follow-up OPTIMIZE CHECKPOINT lands at v3.")
      assert(checkpointAt(deltaLog, 2).isEmpty, "v2 (business commit) carries no Checkpoint.")
      // The Checkpoint rides in v3 and describes state as of v2.
      assert(requireCheckpointAt(deltaLog, 3).version == 2)
      assert(amtProvider(deltaLog.update()).isDefined)
    }
  }

  test("the first AMT is a full rewrite even off the full-rewrite boundary") {
    withTable("amt_first_full") {
      val name = "amt_first_full"
      // interval 2, default multiplier 5 -> v2 is NOT a 5x boundary, but it is the first AMT.
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")
      val v3Trigger = amtTriggerNameAt(sql(s"INSERT INTO $name VALUES (2)"), version = 3)
      assert(v3Trigger == AMTTriggerMode.CheckpointIntervalFull.name,
        s"The first AMT must be a full rewrite; got $v3Trigger")

      // The full rewrite records its own version as the last-full-rewrite marker.
      val deltaLog = deltaLogForName(name)
      assert(requireCheckpointAt(deltaLog, 3)
        .contentRoot.lastManifestCommitWithFullRewrite.contains(2L))
      assert(requireCheckpointAt(deltaLog, 3).contentRoot.isIncremental.contains(false))
    }
  }

  test("an incremental rewrite happens off the full-rewrite boundary; a full one lands on it") {
    withTable("amt_full_vs_incremental") {
      val name = "amt_full_vs_incremental"
      // interval 2, multiplier 2 -> fullRewriteSpan = 4. Timeline (deferred follow-up commits):
      //   commit 0             CREATE TABLE
      //   commit 1  INSERT-1
      //   commit 2  INSERT-2   (interval boundary -> full checkpoint; first AMT)
      //   commit 3  full checkpoint          (describes v2,  lastFull=2)
      //   commit 4  INSERT-3   (interval boundary -> incremental checkpoint)
      //   commit 5  incremental checkpoint   (describes v4,  lastFull=2)
      //   commit 6  INSERT-4   (interval boundary -> full checkpoint)
      //   commit 7  full checkpoint          (describes v6,  lastFull=6)
      //   commit 8  INSERT-5   (interval boundary -> incremental checkpoint)
      //   commit 9  incremental checkpoint   (describes v8,  lastFull=6)
      //   commit 10 INSERT-6   (interval boundary -> full checkpoint)
      //   commit 11 full checkpoint          (describes v10, lastFull=10)
      //   commit 12 INSERT-7   (interval boundary -> incremental checkpoint)
      //   commit 13 incremental checkpoint   (describes v12, lastFull=10)
      //   commit 14 INSERT-8   (interval boundary -> full checkpoint)
      //   commit 15 full checkpoint          (describes v14, lastFull=14)
      //   commit 16 INSERT-9   (interval boundary -> incremental checkpoint)
      //   commit 17 incremental checkpoint   (describes v16, lastFull=14)
      createAMTTable(name, checkpointInterval = 2)
      val deltaLog = deltaLogForName(name)
      withSQLConf(DeltaSQLConf.AMT_FULL_REWRITE_CHECKPOINT_INTERVAL_MULTIPLIER.key -> "2") {
        // 9 single-row INSERTs. INSERT-1 lands at v1 (below the boundary); INSERT-2..INSERT-9 each
        // land on an interval boundary (v2, v4, ..., v16) and trigger a follow-up checkpoint.
        (1 to 9).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      }
      // ExpectedCheckpoint(checkpointedVersion, manifestCommitVersion, incremental,
      //   lastFullRewrite).
      assertCheckpointTimeline(deltaLog, Seq(
        ExpectedCheckpoint(2, 3, incremental = false, lastFullRewrite = 2),
        ExpectedCheckpoint(4, 5, incremental = true, lastFullRewrite = 2),
        ExpectedCheckpoint(6, 7, incremental = false, lastFullRewrite = 6),
        ExpectedCheckpoint(8, 9, incremental = true, lastFullRewrite = 6),
        ExpectedCheckpoint(10, 11, incremental = false, lastFullRewrite = 10),
        ExpectedCheckpoint(12, 13, incremental = true, lastFullRewrite = 10),
        ExpectedCheckpoint(14, 15, incremental = false, lastFullRewrite = 14),
        ExpectedCheckpoint(16, 17, incremental = true, lastFullRewrite = 14)))
    }
  }

  test("lastManifestCommitWithFullRewrite tracks the most recent full rewrite") {
    withTable("amt_last_full_rewrite") {
      val name = "amt_last_full_rewrite"
      // interval 2, multiplier 3 -> fullRewriteSpan = 6. Timeline (deferred follow-up commits):
      //   commit 0             CREATE TABLE
      //   commit 1  INSERT-1
      //   commit 2  INSERT-2   (interval boundary -> full checkpoint; first AMT)
      //   commit 3  full checkpoint          (describes v2,  lastFull=2)
      //   commit 4  INSERT-3   (interval boundary -> incremental checkpoint)
      //   commit 5  incremental checkpoint   (describes v4,  lastFull=2)
      //   commit 6  INSERT-4   (interval boundary -> incremental checkpoint)
      //   commit 7  incremental checkpoint   (describes v6,  lastFull=2)
      //   commit 8  INSERT-5   (interval boundary -> full checkpoint)
      //   commit 9  full checkpoint          (describes v8,  lastFull=8)
      //   commit 10 INSERT-6   (interval boundary -> incremental checkpoint)
      //   commit 11 incremental checkpoint   (describes v10, lastFull=8)
      //   commit 12 INSERT-7   (interval boundary -> incremental checkpoint)
      //   commit 13 incremental checkpoint   (describes v12, lastFull=8)
      //   commit 14 INSERT-8   (interval boundary -> full checkpoint)
      //   commit 15 full checkpoint          (describes v14, lastFull=14)
      //   commit 16 INSERT-9   (interval boundary -> incremental checkpoint)
      //   commit 17 incremental checkpoint   (describes v16, lastFull=14)
      createAMTTable(name, checkpointInterval = 2)
      val deltaLog = deltaLogForName(name)
      withSQLConf(DeltaSQLConf.AMT_FULL_REWRITE_CHECKPOINT_INTERVAL_MULTIPLIER.key -> "3") {
        // 9 single-row INSERTs. INSERT-1 lands at v1 (below the boundary); INSERT-2..INSERT-9 each
        // land on an interval boundary (v2, v4, ..., v16) and trigger a follow-up checkpoint.
        (1 to 9).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      }
      // ExpectedCheckpoint(checkpointedVersion, manifestCommitVersion, incremental,
      //   lastFullRewrite).
      assertCheckpointTimeline(deltaLog, Seq(
        ExpectedCheckpoint(2, 3, incremental = false, lastFullRewrite = 2),
        ExpectedCheckpoint(4, 5, incremental = true, lastFullRewrite = 2),
        ExpectedCheckpoint(6, 7, incremental = true, lastFullRewrite = 2),
        ExpectedCheckpoint(8, 9, incremental = false, lastFullRewrite = 8),
        ExpectedCheckpoint(10, 11, incremental = true, lastFullRewrite = 8),
        ExpectedCheckpoint(12, 13, incremental = true, lastFullRewrite = 8),
        ExpectedCheckpoint(14, 15, incremental = false, lastFullRewrite = 14),
        ExpectedCheckpoint(16, 17, incremental = true, lastFullRewrite = 14)))
    }
  }

  test("a large commit does not write AMT inline until a full AMT already exists") {
    withTable("amt_inline_needs_full") {
      val name = "amt_inline_needs_full"
      // Threshold 1 so every commit is "large" enough to inline. Interval 2.
      createAMTTable(name, checkpointInterval = 2)
      withSQLConf(
          DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
            -> "1") {
        // v1 is the first commit: no prior AMT, so it must NOT inline even though it is "large".
        sql(s"INSERT INTO $name VALUES (1)")
        val deltaLog = deltaLogForName(name)
        assert(checkpointAt(deltaLog, 1).isEmpty,
          "The first large commit must not write an AMT inline (no full AMT exists yet).")

        // v2 is the interval boundary: the first (full) AMT is emitted via the deferred follow-up
        // OPTIMIZE CHECKPOINT commit at v3, not inline in v2.
        sql(s"INSERT INTO $name VALUES (2)")
        assert(checkpointAt(deltaLog, 2).isEmpty, "v2 must not write an AMT inline.")
        assert(deltaLog.update().version == 3, "The first full AMT lands as a follow-up at v3.")
        assert(requireCheckpointAt(deltaLog, 3).contentRoot.isIncremental.contains(false),
          "The first AMT is a full rewrite.")

        // Now a full AMT exists. The next large commit (v4) writes its AMT inline (incrementally).
        val v4Metrics = amtWriteMetricsAt(sql(s"INSERT INTO $name VALUES (3)"), version = 4)
        assert(v4Metrics.trigger == AMTTriggerMode.InlineWithLargeCommitIncremental.name,
          s"Once a full AMT exists, a large commit inlines its AMT; got ${v4Metrics.trigger}")
        assert(v4Metrics.incremental == "true",
          "The usage log must report incremental=true for the inline AMT write.")
        assert(requireCheckpointAt(deltaLog, 4).contentRoot.isIncremental.contains(true),
          "The inline AMT is incremental.")
      }
    }
  }

  test("a full rewrite follows up when inline writes cross the full-rewrite span") {
    withTable("amt_inline_full_followup") {
      val name = "amt_inline_full_followup"
      // Interval 2, multiplier 2 -> fullRewriteSpan = 4. Threshold 1 so every large commit inlines
      // once a full AMT exists. The first full AMT is the deferred follow-up; subsequent large
      // commits inline incrementally, and when an inline commit lands a full span past the last
      // full rewrite a follow-up full OPTIMIZE CHECKPOINT commit is scheduled.
      createAMTTable(name, checkpointInterval = 2)
      val deltaLog = deltaLogForName(name)
      withSQLConf(
          DeltaSQLConf.AMT_FULL_REWRITE_CHECKPOINT_INTERVAL_MULTIPLIER.key -> "2",
          DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
            -> "1") {
        (1 to 8).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      }
      // At least one full rewrite lands after the first, driven by the inline-triggered follow-up,
      // and it correctly advances the last-full-rewrite marker.
      val checkpoints = allCheckpoints(deltaLog)
      val fulls = checkpoints.filter(_.contentRoot.isIncremental.contains(false))
      assert(fulls.size >= 2,
        s"Expected a follow-up full rewrite after inline writes crossed the span; got " +
          s"${checkpoints.map(cp => (cp.version, cp.contentRoot.isIncremental))}.")
      // Every full rewrite is at least a span past the previous one (anchored, not per-commit).
      val fullRewriteSpan = 2 * 2
      fulls.map(_.version).sliding(2).foreach {
        case Seq(prev, next) =>
          assert(next - prev >= fullRewriteSpan,
            s"Full rewrite at v$next is only ${next - prev} versions after v$prev (span " +
              s"$fullRewriteSpan).")
        case _ => ()
      }
    }
  }

  // A table whose full anchor sits off the interval grid (here the first full AMT is written by a
  // direct OPTIMIZE CHECKPOINT at v2, describing v1) and whose commits otherwise all write their
  // AMT inline must still get a periodic full rewrite. The span commit at v11 (11 - anchor 1 = 10 =
  // 2 * interval) is where the full is due; whether it inlines or not, a full checkpoint must land
  // describing v11.
  //   - interval boundary inline = false: v11 is a small commit -> deferred follow-up path, where
  //     case-1 cannot fire (11 - lastAMT 10 = 1) and case-1b emits the full instead.
  //   - interval boundary inline = true:  v11 is a large commit -> inlines an incremental AMT and
  //     planMaintenanceAfterInlineWrite schedules the full follow-up (the tree is materialized
  //     twice at v11 and v12, the accepted cost of never inlining a large commit's actions).
  Seq(true, false).foreach { boundaryInline =>
    test("[full checkpoint off boundary] table which only gets inline commits except at interval " +
        s"boundary is not starved of full checkpoint [interval boundary inline = $boundaryInline]"
        ) {
      withTable("amt_offgrid_full") {
        val name = "amt_offgrid_full"
        // interval 5, multiplier 2 -> fullRewriteSpan = 10.
        //   commit 0             CREATE TABLE
        //   commit 1  INSERT     (data commit; no full tree yet, so it does not write an AMT)
        //   commit 2  OPTIMIZE CHECKPOINT full        (describes v1,  lastFull=1; off-grid anchor)
        //   commit 3..10 INSERT range(8)  (large -> inline incremental each)
        //   commit 11 INSERT     (interval boundary: large inline OR small deferred, per parameter)
        //   commit 12 full checkpoint                (describes v11, lastFull=11)
        createAMTTable(name, checkpointInterval = 5)
        val deltaLog = deltaLogForName(name)
        withSQLConf(
            DeltaSQLConf.AMT_FULL_REWRITE_CHECKPOINT_INTERVAL_MULTIPLIER.key -> "2",
            // A large commit (>= 4 actions) inlines its AMT once a full tree exists; a small one
            // does not. maxRecordsPerFile 1 with optimized writes off makes an N-row insert commit
            // N AddFile actions, so row count controls the action count directly.
            DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
              -> "4",
            DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key -> "false",
            "spark.sql.files.maxRecordsPerFile" -> "1") {
          // v1: a data commit off the interval boundary. No full tree exists yet, so it writes no
          // AMT inline. v2: write the first (full) AMT via a direct OPTIMIZE CHECKPOINT commit, the
          // same operation the checkpoint hook issues; it describes v1 and anchors lastFull off the
          // interval grid.
          sql(s"INSERT INTO $name VALUES (1)") // v1.
          writeFullOptimizeCheckpoint(name) // v2 -> full checkpoint describing v1.
          assert(deltaLog.update().version == 2, "The bootstrap OPTIMIZE CHECKPOINT lands at v2.")
          assert(requireCheckpointAt(deltaLog, 2).contentRoot.isIncremental.contains(false),
            "The bootstrap AMT is a full rewrite anchoring lastFull off the interval grid.")

          // v3..v10: every commit is large, so each inlines an incremental AMT (a full tree now
          // exists). The inline checkpoints ride on v3..v10; none crosses the span yet.
          (1 to 8).foreach(_ => sql(s"INSERT INTO $name SELECT * FROM range(8)"))
          assert(deltaLog.update().version == 10, "Eight inline commits land at v3..v10.")

          // v11 is a full span past the anchor (11 - 1 = 10). Per the parameter it either inlines
          // (large) or takes the deferred path (small); either way a full must follow at v12.
          if (boundaryInline) {
            sql(s"INSERT INTO $name SELECT * FROM range(8)") // v11 large -> inline incr + full v12.
          } else {
            sql(s"INSERT INTO $name VALUES (0)") // v11 small -> deferred; case-1b full v12.
          }
        }
        // ExpectedCheckpoint(describedVersion, manifestCommitVersion, incremental, lastFullRewrite)
        // The inline incremental describing v11 is present only when v11 itself inlines.
        val v11Inline =
          if (boundaryInline) {
            Seq(ExpectedCheckpoint(11, 11, incremental = true, lastFullRewrite = 1))
          } else {
            Seq.empty
          }
        val inlineIncrementals =
          (3L to 10L).map(v => ExpectedCheckpoint(v, v, incremental = true, lastFullRewrite = 1))
        assertCheckpointTimeline(deltaLog,
          Seq(ExpectedCheckpoint(1, 2, incremental = false, lastFullRewrite = 1)) ++
            inlineIncrementals ++
            v11Inline ++
            Seq(ExpectedCheckpoint(11, 12, incremental = false, lastFullRewrite = 11)))
      }
    }
  }

}
