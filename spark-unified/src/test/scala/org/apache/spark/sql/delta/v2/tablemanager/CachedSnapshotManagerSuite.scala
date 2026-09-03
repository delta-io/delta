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
        val cached = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
        assert(cached.getVersion == 1L)

        val loaded = DeltaV2Snapshot.getKernelSnapshot(mgr.loadSnapshotAt(1L))
        assert(loaded eq cached, "Matching version should reuse the cached snapshot")
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
      val beforeRetire = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())

      mgr.retire()

      val afterRetire = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
      assert(afterRetire ne beforeRetire)
    }
  }

  test("retire is idempotent") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      mgr.loadLatestSnapshot()

      mgr.retire()
      mgr.retire()

      assert(DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot()).getVersion == 0L)
    }
  }

  test("loadLatestSnapshot after retire returns fresh uncached snapshot") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      mgr.loadLatestSnapshot()
      mgr.retire()

      val first = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
      val second = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
      assert(first.getVersion == 0L)
      assert(second.getVersion == 0L)
      assert(first ne second, "Retired manager should not cache snapshots")
    }
  }

  // === Table identity validation ==============================

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

  // === installSnapshot same-version dedup =====================

  test("installSnapshot deduplicates same-version refresh") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val firstSnap = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
          assert(firstSnap.getVersion == 0L)

          val secondSnap = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
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
          val current = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
          assert(current.getVersion == 1L)

          assert(mgr.installSnapshot(stale, System.currentTimeMillis()) eq current)
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
          val snapshots = new ConcurrentLinkedQueue[Snapshot]()
          val failures = new ConcurrentLinkedQueue[Throwable]()
          val threads = (1 to 8).map { _ =>
            new Thread(() => {
              try {
                snapshots.add(mgr.loadLatestSnapshot())
              } catch {
                case failure: Throwable => failures.add(failure)
              }
            })
          }
          threads.foreach(_.start())
          threads.foreach(_.join())

          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          assert(snapshots.size() == threads.size)
          while (!snapshots.isEmpty) {
            assert(DeltaV2Snapshot.getKernelSnapshot(snapshots.poll()).getVersion == 2L)
          }
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
        val firstAfterRetire = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
        val secondAfterRetire = DeltaV2Snapshot.getKernelSnapshot(mgr.loadLatestSnapshot())
        assert(firstAfterRetire ne secondAfterRetire)
      }
    }
  }

  test("concurrent table identity validation is consistent") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val snapshots = new ConcurrentLinkedQueue[Snapshot]()
          val failures = new ConcurrentLinkedQueue[Throwable]()
          val threads = (1 to 8).map { _ =>
            new Thread(() => {
              try {
                snapshots.add(mgr.loadLatestSnapshot())
              } catch {
                case failure: Throwable => failures.add(failure)
              }
            })
          }
          threads.foreach(_.start())
          threads.foreach(_.join())

          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          assert(snapshots.size() == threads.size)
          val tableIds = Seq.newBuilder[String]
          while (!snapshots.isEmpty) {
            tableIds += DeltaV2Snapshot.getKernelSnapshot(snapshots.poll()).getMetadata.getId
          }
          assert(tableIds.result().distinct.size == 1)
        } finally {
          mgr.retire()
        }
      }
    }
  }
}
