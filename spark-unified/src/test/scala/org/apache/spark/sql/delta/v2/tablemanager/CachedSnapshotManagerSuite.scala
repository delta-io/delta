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

package org.apache.spark.sql.delta.v2.tablemanager

import java.io.File
import java.util.Optional
import java.util.concurrent.ConcurrentLinkedQueue

import io.delta.spark.internal.v2.exception.VersionNotFoundException
import io.delta.spark.internal.v2.kernel.KernelEngineFactory

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class CachedSnapshotManagerSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  private def createDeltaTable(dir: File, numRows: Int = 10): Unit = {
    spark.range(numRows).write.format("delta").save(dir.getCanonicalPath)
  }

  private def appendToDeltaTable(dir: File, numRows: Int = 5): Unit = {
    spark.range(numRows).write.format("delta").mode("append").save(dir.getCanonicalPath)
  }

  private def createManager(dir: File): CachedSnapshotManager = {
    new CachedSnapshotManager(
      new Path(dir.getCanonicalPath),
      catalogTableOpt = None,
      sessionInvariantFsOptions = Map.empty)
  }

  // === Cold start ============================================

  test("cold start loads latest snapshot") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        val snapshot = mgr.loadLatestSnapshot()
        assert(snapshot != null)
        val kernelSnap = DeltaV2Snapshot.getKernelSnapshot(snapshot)
        assert(kernelSnap.getVersion == 0L)
      } finally {
        mgr.retire()
      }
    }
  }

  test("cold start with multi-version table loads latest") {
    withTempDir { dir =>
      createDeltaTable(dir)
      appendToDeltaTable(dir)
      appendToDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        val snapshot = mgr.loadLatestSnapshot()
        val kernelSnap = DeltaV2Snapshot.getKernelSnapshot(snapshot)
        assert(kernelSnap.getVersion == 2L)
      } finally {
        mgr.retire()
      }
    }
  }

  // === Warm hit (cache reuse) =================================

  test("second loadLatestSnapshot reuses cached snapshot at same version") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val first = mgr.loadLatestSnapshot()
          val second = mgr.loadLatestSnapshot()
          val k1 = DeltaV2Snapshot.getKernelSnapshot(first)
          val k2 = DeltaV2Snapshot.getKernelSnapshot(second)
          assert(k1 eq k2, "Expected same SnapshotImpl instance on warm hit")
          val firstFileCount = first.allFiles.count()
          assert(firstFileCount > 0L)
          assert(second.allFiles.count() == firstFileCount)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === Staleness triggers full reload ==========================

  test("stale rebuild after append advances version via full reload") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val snap1 = mgr.loadLatestSnapshot()
          val k1 = DeltaV2Snapshot.getKernelSnapshot(snap1)
          assert(k1.getVersion == 0L)

          appendToDeltaTable(dir)

          val snap2 = mgr.loadLatestSnapshot()
          val k2 = DeltaV2Snapshot.getKernelSnapshot(snap2)
          assert(k2.getVersion == 1L)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === Version-specific load ==================================

  test("loadSnapshotAt returns specific version") {
    withTempDir { dir =>
      createDeltaTable(dir)
      appendToDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        val snapV0 = mgr.loadSnapshotAt(0L)
        val snapV1 = mgr.loadSnapshotAt(1L)
        assert(DeltaV2Snapshot.getKernelSnapshot(snapV0).getVersion == 0L)
        assert(DeltaV2Snapshot.getKernelSnapshot(snapV1).getVersion == 1L)
      } finally {
        mgr.retire()
      }
    }
  }

  test("loadSnapshotAt reuses the matching cached snapshot") {
    withTempDir { dir =>
      createDeltaTable(dir)
      appendToDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        mgr.loadLatestSnapshot()
        val cached = mgr.currentSnapshotForTesting
        assert(cached.getVersion == 1L)

        val loaded = DeltaV2Snapshot.getKernelSnapshot(mgr.loadSnapshotAt(1L))
        assert(loaded eq cached, "Matching version should reuse the cached snapshot")
        assert(mgr.currentSnapshotForTesting eq cached)
      } finally {
        mgr.retire()
      }
    }
  }

  // === Retire lifecycle =======================================

  test("retire discards current cached snapshot") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      mgr.loadLatestSnapshot()
      assert(mgr.currentSnapshotForTesting != null)

      mgr.retire()

      assert(mgr.isRetired)
      assert(mgr.currentSnapshotForTesting == null)
    }
  }

  test("retire is idempotent") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      mgr.loadLatestSnapshot()

      mgr.retire()
      mgr.retire()

      assert(mgr.isRetired)
    }
  }

  test("loadLatestSnapshot after retire returns fresh uncached snapshot") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      mgr.loadLatestSnapshot()
      mgr.retire()

      val snap = mgr.loadLatestSnapshot()
      val k = DeltaV2Snapshot.getKernelSnapshot(snap)
      assert(k.getVersion == 0L)
      assert(mgr.currentSnapshotForTesting == null, "Retired manager should not cache snapshots")
    }
  }

  // === Table identity validation ==============================

  test("table identity is captured on first load") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        assert(mgr.tableIdForTesting == null)
        mgr.loadLatestSnapshot()
        assert(mgr.tableIdForTesting != null)
      } finally {
        mgr.retire()
      }
    }
  }

  test("table identity mismatch fails after the table is recreated at the same path") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          mgr.loadLatestSnapshot()
          Utils.deleteRecursively(dir)
          createDeltaTable(dir)

          val error = intercept[IllegalStateException] {
            mgr.loadLatestSnapshot()
          }
          assert(error.getMessage.contains("Table identity mismatch"))
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === Validation timestamp tracking ==========================

  test("lastValidatedAtMs is recorded after a successful acquire") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        assert(mgr.lastValidatedAtMsForTesting == -1L)

        mgr.loadLatestSnapshot()

        val afterFirst = mgr.lastValidatedAtMsForTesting
        assert(afterFirst > 0L)
      } finally {
        mgr.retire()
      }
    }
  }

  // === installSnapshot same-version dedup =====================

  test("installSnapshot deduplicates same-version refresh") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          mgr.loadLatestSnapshot()
          val firstSnap = mgr.currentSnapshotForTesting
          assert(firstSnap.getVersion == 0L)

          val second = mgr.loadLatestSnapshot()
          val secondSnap = mgr.currentSnapshotForTesting
          assert(firstSnap eq secondSnap, "Same version should keep existing instance")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("installSnapshot keeps a newer cached snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val stale = DeltaV2Snapshot.getKernelSnapshot(mgr.loadSnapshotAt(0L))
          mgr.loadLatestSnapshot()
          appendToDeltaTable(dir)
          mgr.loadLatestSnapshot()
          val current = mgr.currentSnapshotForTesting
          assert(current.getVersion == 1L)

          assert(mgr.installSnapshot(stale, System.currentTimeMillis()) eq current)
          assert(mgr.currentSnapshotForTesting eq current)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("delegates history, version, and commit-range requests") {
    withTempDir { dir =>
      createDeltaTable(dir)
      appendToDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        val activeCommit = mgr.getActiveCommitAtTime(
          Long.MaxValue,
          /* canReturnLastCommit= */ true,
          /* mustBeRecreatable= */ true,
          /* canReturnEarliestCommit= */ false)
        assert(activeCommit.getVersion == 1L)

        mgr.checkVersionExists(1L, mustBeRecreatable = true, allowOutOfRange = false)
        intercept[VersionNotFoundException] {
          mgr.checkVersionExists(2L, mustBeRecreatable = true, allowOutOfRange = false)
        }

        // scalastyle:off deltahadoopconfiguration
        val kernelEngine =
          KernelEngineFactory.createDefaultEngine(spark.sessionState.newHadoopConf())
        // scalastyle:on deltahadoopconfiguration
        val changes = mgr.getTableChanges(kernelEngine, 0L, Optional.of(1L))
        assert(changes != null)
      } finally {
        mgr.retire()
      }
    }
  }

  // === Concurrency correctness ================================

  test("concurrent loadLatestSnapshot converges to latest version") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        appendToDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val threads = (1 to 8).map { _ =>
            new Thread(() => {
              mgr.loadLatestSnapshot()
            })
          }
          threads.foreach(_.start())
          threads.foreach(_.join())

          val cached = mgr.currentSnapshotForTesting
          assert(cached != null)
          assert(cached.getVersion == 2L, "All threads should converge on the latest version")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("retire during concurrent loads returns usable uncached snapshots") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        mgr.loadLatestSnapshot()

        val ready = new java.util.concurrent.CountDownLatch(4)
        val start = new java.util.concurrent.CountDownLatch(1)
        val snapshots = new ConcurrentLinkedQueue[Snapshot]()
        val failures = new ConcurrentLinkedQueue[Throwable]()
        val loaders = (1 to 4).map { _ =>
          new Thread(() => {
            try {
              ready.countDown()
              start.await()
              snapshots.add(mgr.loadLatestSnapshot())
            } catch {
              case failure: Throwable => failures.add(failure)
            }
          })
        }
        loaders.foreach(_.start())

        ready.await()
        start.countDown()
        mgr.retire()

        loaders.foreach(_.join())
        assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
        assert(snapshots.size() == loaders.size)
        while (!snapshots.isEmpty) {
          assert(DeltaV2Snapshot.getKernelSnapshot(snapshots.poll()).getVersion == 0L)
        }
        assert(mgr.isRetired)
        assert(mgr.currentSnapshotForTesting == null, "Retired manager must not retain a snapshot")
      }
    }
  }

  test("concurrent loads never regress cached version") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          mgr.loadLatestSnapshot()
          assert(
            mgr.currentSnapshotForTesting.getVersion == 0L)

          appendToDeltaTable(dir)

          val threads = (1 to 8).map { _ =>
            new Thread(() => {
              mgr.loadLatestSnapshot()
            })
          }
          threads.foreach(_.start())
          threads.foreach(_.join())

          val finalVersion = mgr.currentSnapshotForTesting.getVersion
          assert(finalVersion == 1L, s"Cached version must not regress; got $finalVersion")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("concurrent table identity validation is consistent") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val threads = (1 to 8).map { _ =>
            new Thread(() => {
              mgr.loadLatestSnapshot()
            })
          }
          threads.foreach(_.start())
          threads.foreach(_.join())

          val tableId = mgr.tableIdForTesting
          assert(tableId != null)

          val threads2 = (1 to 4).map { _ =>
            new Thread(() => {
              mgr.loadLatestSnapshot()
            })
          }
          threads2.foreach(_.start())
          threads2.foreach(_.join())

          assert(
            mgr.tableIdForTesting == tableId,
            "Table identity must remain stable across concurrent loads")
        } finally {
          mgr.retire()
        }
      }
    }
  }
}
