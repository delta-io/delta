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

  testInlineAndDeferred("emission installs an AMTCheckpointProvider on the post-commit snapshot") {
    mode =>
    withTable("amt_provider_install") {
      val name = "amt_provider_install"
      createAMTTable(name, checkpointInterval = 2)
      sql(s"INSERT INTO $name VALUES (1)")
      sql(s"INSERT INTO $name VALUES (2)") // v2: interval boundary.
      // Inline: the AMT rides in the v2 business commit (checkpointVersion == 2).
      // Deferred: a follow-up OPTIMIZE CHECKPOINT commit lands at v3 describing state as of v2.
      val checkpointVersion = 2L
      val snapshotVersion = checkpointVersion + mode.followUpCommits

      val deltaLog = deltaLogForName(name)
      val snapshot = deltaLog.unsafeVolatileSnapshot
      assert(snapshot.version == snapshotVersion)
      val provider = amtProvider(snapshot)
      assert(provider.isDefined, "Post-emission snapshot must expose an AMTCheckpointProvider.")
      assert(provider.get.checkpointVersion == checkpointVersion)
      assert(provider.get.checkpointAction.contentRoot.path ==
        checkpointsAt(deltaLog, snapshotVersion).head.contentRoot.path)
      assert(provider.get.leaves.nonEmpty, "Provider must list the tree's leaves.")
    }
  }

  testInlineAndDeferred("an emitted AMT installs the provider and trims the log segment") { mode =>
    withTable("amt_log_segment") {
      val name = "amt_log_segment"
      createAMTTable(name, checkpointInterval = 3)
      // The interval boundary is v3. Inline: the AMT rides in the v3 business commit. Deferred: a
      // follow-up OPTIMIZE CHECKPOINT commit lands at v4 describing state as of v3. Either way the
      // Checkpoint describes state as of v3, and the log segment trims to deltas after v3.
      (1 to 3).foreach(i => sql(s"INSERT INTO $name VALUES ($i)"))
      val checkpointVersion = 3L
      val snapshotVersion = checkpointVersion + mode.followUpCommits

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
    }
  }
}
