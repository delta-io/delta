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

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{AddFile, Checkpoint}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

class AMTCheckpointWriteSuite extends AMTCheckpointTestBase {

  test("inline emission embeds a Checkpoint action in the same commit at the interval boundary") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 2)
      sql(s"INSERT INTO delta.`$path` VALUES (1)") // v1: not an interval boundary.
      sql(s"INSERT INTO delta.`$path` VALUES (2)") // v2: interval boundary -> emit.

      val deltaLog = DeltaLog.forTable(spark, path)
      val snapshot = deltaLog.update()
      assert(snapshot.version == 2, "No follow-up commit: emission rides in the v2 commit itself.")

      // Manifest tree exists on disk: exactly one root, at least one leaf.
      assert(rootFiles(path).size == 1, "Exactly one root manifest must be written.")
      assert(leafFiles(path).nonEmpty, "At least one leaf manifest must be written.")

      // The v2 commit JSON carries BOTH the user's AddFile and a single Checkpoint action.
      val v2Actions = actionsAt(deltaLog, 2)
      val checkpoints = v2Actions.collect { case c: Checkpoint => c }
      assert(checkpoints.size == 1, s"Expected one Checkpoint action at v2, got: $v2Actions")
      assert(v2Actions.exists(_.isInstanceOf[AddFile]), "User AddFile must be in the same commit.")

      // The Checkpoint's contentRoot points at the on-disk root file.
      val rootName = new File(checkpoints.head.contentRoot.path).getName
      assert(isRootFileName(rootName),
        s"contentRoot must point at a root manifest file; got ${checkpoints.head.contentRoot.path}")
      assert(rootFiles(path).exists(_.getName == rootName))
    }
  }

  test("no emission on a vanilla (non-AMT) table") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(
        s"""CREATE TABLE delta.`$path` (id INT) USING DELTA
           |TBLPROPERTIES ('delta.checkpointInterval' = '2')""".stripMargin)
      sql(s"INSERT INTO delta.`$path` VALUES (1)")
      sql(s"INSERT INTO delta.`$path` VALUES (2)") // interval boundary, but no AMT feature.

      val deltaLog = DeltaLog.forTable(spark, path)
      assert(rootFiles(path).isEmpty && leafFiles(path).isEmpty,
        "No AMT artifacts on a vanilla table.")
      assert(checkpointsAt(deltaLog, 2).isEmpty, "No Checkpoint action on a vanilla table.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("no emission on a non-interval commit") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 10) // interval far from the versions we write.
      sql(s"INSERT INTO delta.`$path` VALUES (1)") // v1: 1 % 10 != 0.

      val deltaLog = DeltaLog.forTable(spark, path)
      assert(checkpointsAt(deltaLog, 1).isEmpty, "v1 is not an interval boundary; no emission.")
      assert(rootFiles(path).isEmpty, "No manifest tree written off an interval boundary.")
      assert(amtProvider(deltaLog.update()).isEmpty)
    }
  }

  test("leaf cardinality respects AMT_ENTRIES_PER_LEAF") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      createAMTTable(path, checkpointInterval = 2)
      withSQLConf(DeltaSQLConf.AMT_ENTRIES_PER_LEAF.key -> "1") {
        sql(s"INSERT INTO delta.`$path` VALUES (1)") // v1: one data file.
        sql(s"INSERT INTO delta.`$path` VALUES (2)") // v2: one more data file -> 2 live files.
      }
      // Two live AddFiles, one per leaf -> two leaves, one root.
      assert(leafFiles(path).size == 2, s"Expected 2 leaves, got ${leafFiles(path).size}.")
      assert(rootFiles(path).size == 1)
    }
  }
}
