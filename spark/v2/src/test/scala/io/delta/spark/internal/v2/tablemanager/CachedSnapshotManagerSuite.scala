/*
 * Copyright (2026) The Delta Lake Project Authors.
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
import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

// format: off
// scalastyle:off import.ordering.noEmptyLine
// scalastyle:off import.ordering.wrongOrderInGroup
import io.delta.spark.internal.v2.kernel.KernelEngineFactory

import io.delta.sql.DeltaSparkSessionExtension

import org.apache.spark.sql.delta.DeltaIllegalStateException
import org.apache.spark.sql.delta.DeltaUnsupportedOperationException
import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.spark.internal.v2.exception.VersionNotFoundException
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
// scalastyle:on import.ordering.noEmptyLine
// scalastyle:on import.ordering.wrongOrderInGroup

class CachedSnapshotManagerSuite
    extends QueryTest
    with SharedSparkSession
{

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      .set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key, classOf[DeltaSparkSessionExtension].getName)
      .set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
  }
// format: on

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
        assert(mgr.loadLatestSnapshot() eq snapV1)
      } finally {
        mgr.retire()
      }
    }
  }

  test("loadSnapshotAt reuses the matching cached snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val cached = mgr.loadLatestSnapshot()
          assert(cached.version == 1L)
          appendToDeltaTable(dir)

          val loaded = mgr.loadSnapshotAt(1L)
          assert(loaded eq cached, "Matching version should reuse the cached snapshot")
          assert(mgr.loadLatestSnapshot().version == 2L)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("newer loadSnapshotAt refreshes latest despite a non-stale cached snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        val cachedLatest = mgr.loadLatestSnapshot()
        appendToDeltaTable(dir)

        val versioned = mgr.loadSnapshotAt(1L)
        val latestAgain = mgr.loadLatestSnapshot()
        assert(versioned.version == 1L)
        assert(versioned eq latestAgain)
        assert(latestAgain ne cachedLatest)
      }
    }
  }

  test("newer loadSnapshotAt keeps a refreshed later snapshot cached") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        val cachedLatest = mgr.loadLatestSnapshot()
        appendToDeltaTable(dir)
        appendToDeltaTable(dir)

        val versioned = mgr.loadSnapshotAt(1L)
        val latestAgain = mgr.loadLatestSnapshot()
        assert(versioned.version == 1L)
        assert(latestAgain.version == 2L)
        assert(latestAgain ne versioned)
        assert(latestAgain ne cachedLatest)
      }
    }
  }

  test("older loadSnapshotAt preserves a non-stale cached latest snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        val mgr = createManager(dir)
        val cachedLatest = mgr.loadLatestSnapshot()
        appendToDeltaTable(dir)

        val versioned = mgr.loadSnapshotAt(0L)
        val latestAgain = mgr.loadLatestSnapshot()
        assert(versioned.version == 0L)
        assert(latestAgain eq cachedLatest)
        assert(latestAgain.version == 1L)
      }
    }
  }

  test("older loadSnapshotAt refreshes a stale cached latest snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        val mgr = createManager(dir)
        val cachedLatest = mgr.loadLatestSnapshot()
        appendToDeltaTable(dir)

        val versioned = mgr.loadSnapshotAt(0L)
        val latestAgain = mgr.loadLatestSnapshot()
        assert(versioned.version == 0L)
        assert(latestAgain.version == 2L)
        assert(latestAgain ne versioned)
        assert(latestAgain ne cachedLatest)
      }
    }
  }

  // === Retire lifecycle =======================================

  Seq("0", "60000").foreach { stalenessLimit =>
    test(s"retire preserves newer exact-version refresh with staleness $stalenessLimit") {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> stalenessLimit) {
        withTempDir { dir =>
          createDeltaTable(dir)
          val mgr = createManager(dir)
          val beforeRetire = mgr.loadLatestSnapshot()

          mgr.retire()
          appendToDeltaTable(dir)

          val afterRetire = mgr.loadSnapshotAt(1L)
          assert(beforeRetire.version == 0L)
          assert(afterRetire.version == 1L)
          assert(afterRetire ne beforeRetire)
          assert(mgr.loadLatestSnapshot() eq afterRetire)
        }
      }
    }

    test(s"retire preserves newer historical refresh with staleness $stalenessLimit") {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> stalenessLimit) {
        withTempDir { dir =>
          createDeltaTable(dir)
          val mgr = createManager(dir)
          val beforeRetire = mgr.loadLatestSnapshot()

          mgr.retire()
          appendToDeltaTable(dir)
          appendToDeltaTable(dir)

          val historical = mgr.loadSnapshotAt(1L)
          val latest = mgr.loadLatestSnapshot()
          assert(beforeRetire.version == 0L)
          assert(historical.version == 1L)
          assert(latest.version == 2L)
          assert(latest ne historical)
          assert(latest ne beforeRetire)
        }
      }
    }

    test(s"retire preserves latest refresh when requested version is missing: $stalenessLimit") {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> stalenessLimit) {
        withTempDir { dir =>
          createDeltaTable(dir)
          val mgr = createManager(dir)
          val beforeRetire = mgr.loadLatestSnapshot()

          mgr.retire()
          appendToDeltaTable(dir)

          val error = intercept[VersionNotFoundException] {
            mgr.loadSnapshotAt(2L)
          }
          assert(error.getUserVersion == 2L)
          assert(error.getEarliest == 0L)
          assert(error.getLatest == 1L)
          val latest = mgr.loadLatestSnapshot()
          assert(latest.version == 1L)
          assert(latest ne beforeRetire)
        }
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

          val error = intercept[DeltaIllegalStateException] {
            mgr.loadLatestSnapshot()
          }
          assert(error.getErrorClass == "INTERNAL_ERROR")
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

          val kernelSnapshot = DeltaV2Snapshot.getKernelSnapshot(firstSnap)
          val secondSnap = mgr.install(kernelSnapshot, System.currentTimeMillis())
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

          assert(mgr.install(stale, System.currentTimeMillis()) eq current)
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
      val kernelEngine = KernelEngineFactory.createDefaultEngine(spark.sessionState.newHadoopConf())
      // scalastyle:on deltahadoopconfiguration
      val changesError = intercept[DeltaUnsupportedOperationException] {
        mgr.getTableChanges(kernelEngine, 0L, Optional.empty())
      }
      assert(changesError.getErrorClass == "INTERNAL_ERROR")
    }
  }

  // === Concurrency correctness ================================

  test("concurrent appends and loadLatestSnapshot calls observe monotonically newer versions") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val failures = new ConcurrentLinkedQueue[Throwable]()
          val committedVersion = new AtomicLong(mgr.loadLatestSnapshot().version)
          val maxVersionSeen = new AtomicLong(committedVersion.get())
          val writerDone = new AtomicBoolean(false)
          val versionObserved = Array.fill(3)(new CountDownLatch(1))
          val readers = (1 to 4).map { _ =>
            new Thread(() => {
              try {
                var lastSeenVersion = -1L
                while (!writerDone.get() || lastSeenVersion < committedVersion.get()) {
                  val minimumVersion = maxVersionSeen.get()
                  val observedVersion = mgr.loadLatestSnapshot().version
                  assert(observedVersion >= lastSeenVersion)
                  assert(observedVersion >= minimumVersion)
                  lastSeenVersion = observedVersion
                  maxVersionSeen.getAndUpdate(current => math.max(current, observedVersion))
                  if (observedVersion > 0L) {
                    versionObserved((observedVersion - 1L).toInt).countDown()
                  }
                }
              } catch {
                case failure: Throwable => failures.add(failure)
              }
            })
          }
          val appender = new Thread(() => {
            try {
              (1L to 3L).foreach { version =>
                appendToDeltaTable(dir)
                committedVersion.set(version)
                val observed = versionObserved((version - 1L).toInt).await(30L, TimeUnit.SECONDS)
                assert(observed, s"No reader observed committed version $version")
              }
            } catch {
              case failure: Throwable => failures.add(failure)
            } finally {
              writerDone.set(true)
            }
          })
          val threads = appender +: readers
          threads.foreach(_.start())
          threads.foreach(_.join())

          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          val latest = mgr.loadLatestSnapshot()
          assert(latest.version == 3L)
          assert(latest.allFiles.count() > 0L)
          assert(maxVersionSeen.get() == latest.version)
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

  test("concurrent versioned loads advance and preserve the latest cached snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        val initial = mgr.loadLatestSnapshot()
        appendToDeltaTable(dir)
        appendToDeltaTable(dir)
        val currentResults = new ConcurrentLinkedQueue[Snapshot]()
        val intermediateResults = new ConcurrentLinkedQueue[Snapshot]()
        val historicalResults = new ConcurrentLinkedQueue[Snapshot]()
        val failures = new ConcurrentLinkedQueue[Throwable]()

        val threads = (1 to 12).map { index =>
          new Thread(() => {
            try {
              val requestedVersion = index % 3
              val result = mgr.loadSnapshotAt(requestedVersion)
              assert(result.version == requestedVersion)
              requestedVersion match {
                case 0 => historicalResults.add(result)
                case 1 => intermediateResults.add(result)
                case 2 => currentResults.add(result)
              }
            } catch {
              case failure: Throwable => failures.add(failure)
            }
          })
        }
        threads.foreach(_.start())
        threads.foreach(_.join())

        assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
        assert(currentResults.size() == 4)
        assert(intermediateResults.size() == 4)
        assert(historicalResults.size() == 4)
        val cachedLatest = mgr.loadLatestSnapshot()
        assert(cachedLatest.version == 2L)
        assert(cachedLatest ne initial)
        assert(cachedLatest.allFiles.count() > 0L)
        while (!currentResults.isEmpty) assert(currentResults.poll() eq cachedLatest)
        while (!intermediateResults.isEmpty) assert(intermediateResults.poll().version == 1L)
        while (!historicalResults.isEmpty) assert(historicalResults.poll().version == 0L)
        assert(mgr.loadLatestSnapshot() eq cachedLatest)
      }
    }
  }
}
