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

package org.apache.spark.sql.delta

import org.apache.spark.sql.delta.coordinatedcommits.CatalogOwnedTestBaseSuite
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.FileNames

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Verifies that [[org.apache.spark.sql.delta.hooks.LogCompactionHook]] honours the protocol
 * requirement that a log compaction file may only be produced for versions already published
 * (backfilled) in `_delta_log` (see PROTOCOL.md, "Maintenance Operations on Catalog-managed
 * Tables").
 *
 * On catalog-managed (catalog-owned / coordinated-commits) tables, committed versions can still be
 * staged under `_delta_log/_staged_commits`. The hook synchronously backfills the window's commits
 * before compacting (the same way checkpoint writing publishes commits via
 * `Snapshot.ensureCommitFilesBackfilled`), so every produced compaction covers only published
 * versions.
 */
class LogCompactionWithCatalogOwnedSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest
  with CatalogOwnedTestBaseSuite {

  // Batched backfill so committed versions would otherwise be staged when the hook runs.
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(2)

  test("hook backfills then compacts, producing compactions over published versions") {
    withSQLConf(
      DeltaSQLConf.DELTALOG_MINOR_COMPACTION_USE_FOR_WRITES.key -> "true",
      DeltaConfigs.LOG_COMPACTION_INTERVAL.defaultTablePropertyKey -> "5",
      // High checkpoint interval so no checkpoint interferes with the produced windows.
      DeltaConfigs.CHECKPOINT_INTERVAL.defaultTablePropertyKey -> "100") {
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        (0L to 10L).foreach { v =>
          spark.range(v, v + 1).write.format("delta").mode("append").save(path)
        }
        val deltaLog = DeltaLog.forTable(spark, path)
        val fs = deltaLog.logPath.getFileSystem(deltaLog.newDeltaHadoopConf())

        val compactions = fs.listStatus(deltaLog.logPath)
          .filter(FileNames.isCompactedDeltaFile)
          .map(f => FileNames.compactedDeltaVersions(f.getPath))
          .sorted

        // The hook backfills before compacting, so it produces the same windows it would on a
        // filesystem table: v5 -> [1, 5] and v10 -> [6, 10].
        assert(compactions.toSeq === Seq((1L, 5L), (6L, 10L)))

        // Every produced compaction must cover only published (backfilled) versions: the backfilled
        // `_delta_log/<endVersion>.json` must exist.
        compactions.foreach { case (startV, endV) =>
          assert(fs.exists(FileNames.unsafeDeltaFile(deltaLog.logPath, endV)),
            s"compaction [$startV, $endV] covers a non-published (unbackfilled) end version")
        }

        // The compaction files are used for snapshot construction and the table reads correctly.
        DeltaLog.clearCache()
        val snapshot = DeltaLog.forTable(spark, path).unsafeVolatileSnapshot
        assert(snapshot.logSegment.deltas.exists(f => FileNames.isCompactedDeltaFile(f.getPath)))
        checkAnswer(spark.read.format("delta").load(path), spark.range(11).toDF())
      }
    }
  }
}
