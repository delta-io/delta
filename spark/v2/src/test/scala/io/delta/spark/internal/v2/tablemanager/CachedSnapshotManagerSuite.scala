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

package io.delta.spark.internal.v2.tablemanager

import java.io.File
import java.util.Optional
import java.util.concurrent.ConcurrentLinkedQueue

import io.delta.spark.internal.v2.kernel.KernelEngineFactory
import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.sql.delta.{DeltaUnsupportedOperationException, Snapshot}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession

class CachedSnapshotManagerSuite
    extends QueryTest
    with SharedSparkSession {

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(
        StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
        classOf[DeltaSparkSessionExtension].getName)
      .set(
        SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
        classOf[DeltaCatalog].getName)
  }

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
          assert(snap2 ne snap1, "A new Kernel snapshot must install a new DeltaV2Snapshot")
          assert(k2 ne k1)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("previously returned snapshot remains usable after installing a newer snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        val previous = mgr.loadLatestSnapshot()
        val previousFileCount = previous.allFiles.count()

        appendToDeltaTable(dir)
        val current = mgr.loadLatestSnapshot()

        assert(previous.version == 0L)
        assert(current.version == 1L)
        assert(previous.allFiles.count() == previousFileCount)
        assert(current.allFiles.count() > previousFileCount)
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
        val cached = mgr.loadLatestSnapshot()
        assert(cached.version == 1L)

        val loaded = mgr.loadSnapshotAt(1L)
        assert(loaded eq cached, "Matching version should reuse the cached snapshot")
      } finally {
        mgr.retire()
      }
    }
  }

  test("loadSnapshotAt does not replace a non-stale cached latest snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        val cachedLatest = mgr.loadLatestSnapshot()
        appendToDeltaTable(dir)

        val versioned = mgr.loadSnapshotAt(1L)
        val latestAgain = mgr.loadLatestSnapshot()
        assert(versioned.version == 1L)
        assert(latestAgain eq cachedLatest)
        assert(latestAgain.version == 0L)
      }
    }
  }

  // === Retire lifecycle =======================================

  test("retire does not change snapshot management behavior") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        val beforeRetire = mgr.loadLatestSnapshot()

        mgr.retire()
        mgr.retire()
        appendToDeltaTable(dir)

        val afterRetire = mgr.loadLatestSnapshot()
        assert(beforeRetire.version == 0L)
        assert(afterRetire.version == 1L)
        assert(afterRetire ne beforeRetire)
      }
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
          JavaUtils.deleteRecursively(dir)
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
          val firstSnap = mgr.loadLatestSnapshot()
          assert(firstSnap.version == 0L)

          val secondSnap = mgr.loadLatestSnapshot()
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
          val current = mgr.loadLatestSnapshot()
          assert(current.version == 1L)

          assert(mgr.installSnapshot(stale, System.currentTimeMillis()) eq current)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("history, version, and commit-range operations are unsupported") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      val historyError = intercept[DeltaUnsupportedOperationException] {
        mgr.getActiveCommitAtTime(
          Long.MaxValue,
          /* canReturnLastCommit= */ true,
          /* mustBeRecreatable= */ true,
          /* canReturnEarliestCommit= */ false)
      }
      assert(historyError.getErrorClass == "INTERNAL_ERROR")
      val versionError = intercept[DeltaUnsupportedOperationException] {
        mgr.checkVersionExists(0L, mustBeRecreatable = true, allowOutOfRange = false)
      }
      assert(versionError.getErrorClass == "INTERNAL_ERROR")

      // scalastyle:off deltahadoopconfiguration
      val kernelEngine =
        KernelEngineFactory.createDefaultEngine(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val changesError = intercept[DeltaUnsupportedOperationException] {
        mgr.getTableChanges(kernelEngine, 0L, Optional.empty())
      }
      assert(changesError.getErrorClass == "INTERNAL_ERROR")
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
                val snapshot = mgr.loadLatestSnapshot()
                assert(snapshot.allFiles.count() > 0L)
                snapshots.add(snapshot)
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

  test("concurrent versioned loads reuse current and do not replace latest") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        appendToDeltaTable(dir)
        val mgr = createManager(dir)
        val cachedLatest = mgr.loadLatestSnapshot()
        val currentResults = new ConcurrentLinkedQueue[Snapshot]()
        val historicalResults = new ConcurrentLinkedQueue[Snapshot]()
        val failures = new ConcurrentLinkedQueue[Throwable]()

        val threads = (1 to 8).map { index =>
          new Thread(() => {
            try {
              val result = if (index % 2 == 0) {
                mgr.loadSnapshotAt(2L)
              } else {
                mgr.loadSnapshotAt(0L)
              }
              assert(result.allFiles.count() > 0L)
              if (index % 2 == 0) currentResults.add(result) else historicalResults.add(result)
            } catch {
              case failure: Throwable => failures.add(failure)
            }
          })
        }
        threads.foreach(_.start())
        threads.foreach(_.join())

        assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
        assert(currentResults.size() == 4)
        assert(historicalResults.size() == 4)
        while (!currentResults.isEmpty) assert(currentResults.poll() eq cachedLatest)
        while (!historicalResults.isEmpty) assert(historicalResults.poll().version == 0L)
        assert(mgr.loadLatestSnapshot() eq cachedLatest)
      }
    }
  }
}
