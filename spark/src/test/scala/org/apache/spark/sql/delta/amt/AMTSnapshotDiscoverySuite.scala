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

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.util.FileNames

class AMTSnapshotDiscoverySuite extends AMTCheckpointTestBase {

  ////////////////////////////
  // Cold snapshot discovery
  ////////////////////////////

  ///////////////////////////
  // deltaLog.update()
  ///////////////////////////

  ///////////////////////////
  // Post commit snapshot
  ///////////////////////////

  test("emission installs an AMTCheckpointProvider on the post-commit snapshot") {
    withTable("amt_provider_install") {
      val name = "amt_provider_install"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)") // v2: interval boundary.
      // The first AMT is always a deferred follow-up OPTIMIZE CHECKPOINT commit: it lands at v3
      // describing state as of v2 (the first, full AMT can never be inline).
      val checkpointVersion = 2L
      val snapshotVersion = 3L

      val deltaLog = deltaLogForName(name)
      val snapshot = deltaLog.unsafeVolatileSnapshot
      assert(snapshot.version == snapshotVersion)
      val provider = amtProvider(snapshot)
      assert(provider.isDefined, "Post-emission snapshot must expose an AMTCheckpointProvider.")
      assert(provider.get.checkpointVersion == checkpointVersion)
      assert(provider.get.checkpointAction.contentRoot.path ==
        checkpointsAt(deltaLog, snapshotVersion).head.contentRoot.path)
      assert(provider.get.leaves.nonEmpty, "Provider must list the tree's leaves.")

      // Now that a full AMT exists, force an inline AMT commit and assert the provider installs
      // from that business commit itself (no follow-up commit). Threshold 1 makes v4 inline.
      withSQLConf(
          DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
            -> "1") {
        sql(s"INSERT INTO $name VALUES (3)") // v4: inline AMT, no follow-up.
      }
      val inlineSnapshot = deltaLog.unsafeVolatileSnapshot
      assert(inlineSnapshot.version == 4L, "The inline AMT rides in the v4 business commit.")
      val inlineProvider = amtProvider(inlineSnapshot).getOrElse(
        fail("The inline-emission snapshot must expose an AMTCheckpointProvider."))
      assert(inlineProvider.checkpointVersion == 4L, "The inline Checkpoint describes v4.")
      assert(inlineProvider.checkpointAction.contentRoot.path ==
        checkpointsAt(deltaLog, 4L).head.contentRoot.path)
      assert(inlineProvider.leaves.nonEmpty, "Provider must list the tree's leaves.")
    }
  }

  test("an emitted AMT installs the provider and trims the log segment") {
    withTable("amt_log_segment") {
      val name = "amt_log_segment"
      createAMTTable(name, checkpointInterval = 3)
      // The interval boundary is v3; the first AMT is a deferred follow-up OPTIMIZE CHECKPOINT
      // commit at v4 describing state as of v3, and the log segment trims to deltas after v3.
      (1 to 3).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      val checkpointVersion = 3L
      val snapshotVersion = 4L

      val deltaLog = deltaLogForName(name)
      val snapshot = deltaLog.unsafeVolatileSnapshot
      assert(snapshot.version == snapshotVersion)
      val provider = amtProvider(snapshot).getOrElse(
        fail("The post-emission snapshot must expose an AMTCheckpointProvider."))
      assert(provider.checkpointVersion == checkpointVersion,
        "The Checkpoint describes state as of v3.")
      // The log segment keeps only deltas strictly after the checkpoint version (v3).
      val segmentDeltaVersions = snapshot.logSegment.deltas.map(f => FileNames.deltaVersion(f))
      assert(segmentDeltaVersions.forall(_ > checkpointVersion),
        s"Log segment must trim deltas up to the checkpoint version; got $segmentDeltaVersions.")

      // Now that a full AMT exists, force an inline AMT commit and assert the same install + trim
      // behavior when the Checkpoint rides in the business commit. Threshold 1 makes v5 inline.
      withSQLConf(
          DeltaSQLConf.AMT_LARGE_COMMIT_ACTIONS_COUNT_THRESHOLD_FOR_INLINE_MANIFEST_COMMIT.key
            -> "1") {
        sql(s"INSERT INTO $name VALUES (4)") // v5: inline AMT, no follow-up.
      }
      val inlineSnapshot = deltaLog.unsafeVolatileSnapshot
      assert(inlineSnapshot.version == 5L, "The inline AMT rides in the v5 business commit.")
      val inlineProvider = amtProvider(inlineSnapshot).getOrElse(
        fail("The inline-emission snapshot must expose an AMTCheckpointProvider."))
      assert(inlineProvider.checkpointVersion == 5L, "The inline Checkpoint describes v5.")
      val inlineDeltaVersions = inlineSnapshot.logSegment.deltas.map(f => FileNames.deltaVersion(f))
      assert(inlineDeltaVersions.forall(_ > 5L),
        s"Log segment must trim deltas up to the inline checkpoint version; got " +
          s"$inlineDeltaVersions.")
    }
  }
}
