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

import org.apache.spark.sql.delta.{DeltaLog, Snapshot}
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

  test("inline emission installs an AMTCheckpointProvider on the post-commit snapshot") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 2)
      sql(s"INSERT INTO delta.`$path` VALUES (1)")
      sql(s"INSERT INTO delta.`$path` VALUES (2)") // v2: emit.

      val deltaLog = DeltaLog.forTable(spark, path)
      val snapshot = deltaLog.unsafeVolatileSnapshot
      assert(snapshot.version == 2)
      val provider = amtProvider(snapshot)
      assert(provider.isDefined, "Post-emission snapshot must expose an AMTCheckpointProvider.")
      assert(provider.get.checkpointVersion == 2)
      assert(provider.get.checkpointAction.contentRoot.path ==
        checkpointsAt(deltaLog, 2).head.contentRoot.path)
      assert(provider.get.leaves.nonEmpty, "Provider must list the tree's leaves.")
    }
  }

  test("post-commit LogSegment carries the AMT provider and only post-checkpoint deltas") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 3)
      // After each commit, the post-commit snapshot's log segment holds the deltas after the latest
      // checkpoint. A checkpoint emits at every interval boundary (v3, v6); the provider persists
      // across the intervening commits (deltaLog.update() is a no-op for AMT, so the cached
      // post-commit snapshot is returned as-is). Expected (version -> delta count):
      //   v0=1, v1=2, v2=3, v3=0 (checkpoint), v4=1, v5=2, v6=0 (checkpoint).
      val expected = Seq(0L -> 1, 1L -> 2, 2L -> 3, 3L -> 0, 4L -> 1, 5L -> 2, 6L -> 0)
      val deltaLog = DeltaLog.forTable(spark, path)

      def assertSegment(snapshot: Snapshot, expectedVersion: Long, expectedDeltas: Int): Unit = {
        assert(snapshot.version == expectedVersion)
        assert(snapshot.logSegment.deltas.size == expectedDeltas,
          s"v$expectedVersion: expected $expectedDeltas deltas, " +
          s"got ${snapshot.logSegment.deltas.map(f => FileNames.deltaVersion(f))}.")
        // A provider is installed exactly on the checkpoint-boundary commits (v3, v6).
        val isBoundary = expectedVersion > 0 && expectedVersion % 3 == 0
        if (isBoundary) {
          assert(amtProvider(snapshot).exists(_.checkpointVersion == expectedVersion),
            s"v$expectedVersion: expected an AMT provider at this checkpoint version.")
        }
      }

      assertSegment(deltaLog.unsafeVolatileSnapshot, expected.head._1, expected.head._2)
      expected.tail.foreach { case (version, deltas) =>
        sql(s"INSERT INTO delta.`$path` VALUES ($version)")
        assertSegment(DeltaLog.forTable(spark, path).unsafeVolatileSnapshot, version, deltas)
      }
    }
  }
}
