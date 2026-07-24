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
import org.apache.spark.sql.delta.actions.Checkpoint
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils._
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.JsonUtils

/**
 * Emission-policy scenarios for AMT (`adaptiveMetadata-preview`): the checkpoint-interval trigger
 * (deferred to a follow-up OPTIMIZE CHECKPOINT commit), the full-vs-incremental rewrite cadence.
 */
class AMTCheckpointPolicySuite extends AMTCheckpointTestBase {

  /** The Checkpoint action emitted at exactly `version`, or fails. */
  private def checkpointAt(deltaLog: DeltaLog, version: Long): Checkpoint =
    checkpointsAt(deltaLog, version) match {
      case Seq(cp) => cp
      case other => fail(s"Expected exactly one Checkpoint at v$version, got: $other")
    }

  /** Every AMT [[Checkpoint]] emitted in `[0, latestVersion]`, in commit order. */
  private def allCheckpoints(deltaLog: DeltaLog): Seq[Checkpoint] =
    allCheckpointsWithCommitVersion(deltaLog).map(_._2)

  /**
   * Every AMT [[Checkpoint]] emitted in `[0, latestVersion]`, paired with the version of the commit
   * that carries it (the manifest commit version), in commit order.
   */
  private def allCheckpointsWithCommitVersion(deltaLog: DeltaLog): Seq[(Long, Checkpoint)] = {
    val latest = deltaLog.update().version
    (0L to latest).flatMap(v => checkpointsAt(deltaLog, v).map(cp => (v, cp)))
  }

  /** The trigger name recorded in the AMT write metrics of the commit `f` produces at `version`. */
  private def amtTriggerNameAt(f: => Unit, version: Long): String = {
    Log4jUsageLogger.track(f)
      .filter(e => e.metric == MetricDefinitions.EVENT_TAHOE.name &&
        e.tags.get("opType").contains("delta.commit.stats"))
      .map(e => JsonUtils.fromJson[CommitStats](e.blob))
      .find(_.commitVersion == version)
      .flatMap(_.amtWriteMetrics)
      .flatMap(_.attempts.headOption)
      .map(_.trigger)
      .getOrElse(fail(s"No AMT write metrics logged for version $version."))
  }


  /**
   * One expected AMT checkpoint in a deterministic emission timeline.
   *
   * @param describedVersion      the table version the emitted Checkpoint describes (the
   *                              triggering business commit)
   * @param manifestCommitVersion the version of the commit that carries the Checkpoint action -- in
   *                              deferred mode the follow-up OPTIMIZE CHECKPOINT commit, which
   *                              lands at describedVersion + 1
   * @param incremental           whether the rewrite is incremental (false = full rewrite)
   * @param lastFullRewrite       expected `lastManifestCommitWithFullRewrite` marker on the
   *                              checkpoint
   */
  private case class ExpectedCheckpoint(
      describedVersion: Long,
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
    assert(checkpoints.map(_._2.version) == expected.map(_.describedVersion),
      s"Emitted checkpoint versions ${checkpoints.map(_._2.version)} must match " +
        s"${expected.map(_.describedVersion)}.")
    assert(checkpoints.map(_._1) == expected.map(_.manifestCommitVersion),
      s"Manifest commit versions ${checkpoints.map(_._1)} must match " +
        s"${expected.map(_.manifestCommitVersion)}.")
    checkpoints.zip(expected).foreach { case ((_, cp), exp) =>
      assert(cp.contentRoot.isIncremental.contains(exp.incremental),
        s"Checkpoint describing v${exp.describedVersion}: expected incremental=" +
          s"${exp.incremental}, got ${cp.contentRoot.isIncremental}")
      assert(cp.contentRoot.lastManifestCommitWithFullRewrite.contains(exp.lastFullRewrite),
        s"Checkpoint describing v${exp.describedVersion}: expected lastFullRewrite=" +
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
      assert(checkpointsAt(deltaLog, 2).isEmpty, "v2 (business commit) carries no Checkpoint.")
      // The Checkpoint rides in v3 and describes state as of v2.
      assert(checkpointAt(deltaLog, 3).version == 2)
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
      assert(checkpointAt(deltaLog, 3).contentRoot.lastManifestCommitWithFullRewrite.contains(2L))
      assert(checkpointAt(deltaLog, 3).contentRoot.isIncremental.contains(false))
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
      // ExpectedCheckpoint(describedVersion, manifestCommitVersion, incremental, lastFullRewrite).
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
      // ExpectedCheckpoint(describedVersion, manifestCommitVersion, incremental, lastFullRewrite).
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

}
