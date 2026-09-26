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
import java.net.URI
import java.util.Optional
import java.util.concurrent.{
  ConcurrentLinkedQueue,
  CopyOnWriteArrayList,
  CountDownLatch,
  CyclicBarrier,
  TimeUnit
}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

// format: off
// scalastyle:off import.ordering.noEmptyLine
// scalastyle:off import.ordering.wrongOrderInGroup
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.v2.interop.{DeltaV2QueryContext, DeltaV2SnapshotManager}
import io.delta.spark.internal.v2.kernel.{KernelContext, KernelEngineFactory}
import io.delta.kernel.exceptions.KernelException

import io.delta.sql.{DeltaSparkSessionExtensionV1 => DeltaSparkSessionExtension}

import org.apache.spark.sql.delta.Snapshot
import org.apache.spark.sql.delta.catalog.{DeltaCatalogV1 => DeltaCatalog}
import org.apache.spark.sql.delta.sources.DeltaSQLConf

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FSDataInputStream, Path, RawLocalFileSystem}

import org.apache.spark.SparkConf
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.StructType
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

  private def createManager(
      dir: File,
      kernelContext: KernelContext = KernelContext(Map.empty, LogStore.createLogStore(spark))
  ): CachedSnapshotManager = {
    new CachedSnapshotManager(
      new Path(dir.getCanonicalPath),
      kernelContext,
      new AtomicReference[CatalogTable]())
  }

  private def setRecordingMarkers(session: SparkSession, sessionMarker: String): Unit = {
    session.conf.set(
      CachedSnapshotManagerRecordingFileSystem.SessionMarkerKey,
      sessionMarker)
    session.conf.set(
      CachedSnapshotManagerRecordingFileSystem.InvariantMarkerKey,
      s"session-$sessionMarker")
  }

  private def assertRecordingFileSystemObserved(expectedSessionMarker: String): Unit = {
    val observations = CachedSnapshotManagerRecordingFileSystem.currentThreadObservations
    assert(observations.nonEmpty)
    assert(observations.forall { case (_, sessionMarker, invariantMarker) =>
      sessionMarker == expectedSessionMarker && invariantMarker == "table-option"
    }, s"Unexpected filesystem observations: $observations")
  }

  private def startAndJoinThreads(threads: Seq[Thread]): Unit = {
    threads.foreach(_.setDaemon(true))
    threads.foreach(_.start())
    threads.foreach(_.join(TimeUnit.SECONDS.toMillis(30L)))
    val alive = threads.filter(_.isAlive)
    alive.foreach(_.interrupt())
    assert(alive.isEmpty, s"Threads did not terminate: ${alive.map(_.getName).mkString(", ")}")
  }

  private def awaitClockAfter(timestampMs: Long): Unit = {
    val deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L)
    while (System.currentTimeMillis() <= timestampMs && System.nanoTime() < deadlineNanos) {
      Thread.`yield`()
    }
    assert(
      System.currentTimeMillis() > timestampMs,
      s"Wall clock did not advance beyond $timestampMs")
  }

  test("contextual APIs use query catalog metadata while legacy APIs use the latest metadata") {
    withTempDir { dir =>
      val legacyCatalogTable = CatalogTable(
        identifier = TableIdentifier("legacy_table"),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty,
        schema = new StructType())
      val queryCatalogTable = CatalogTable(
        identifier = TableIdentifier("query_table"),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty,
        schema = new StructType())
      val observedCatalogTables = new ConcurrentLinkedQueue[Option[CatalogTable]]()
      val kernelContext = KernelContext(Map.empty, LogStore.createLogStore(spark))
      val manager = new CachedSnapshotManager(
        new Path(dir.getCanonicalPath),
        kernelContext,
        new AtomicReference[CatalogTable](legacyCatalogTable)) {
        override private[tablemanager] def createUncachedSnapshotManager(
            catalogTableOpt: Option[CatalogTable]): DeltaV2SnapshotManager = {
          observedCatalogTables.add(catalogTableOpt)
          throw new IllegalStateException("recorded catalog metadata")
        }
      }
      val queryContext = DeltaV2QueryContext(Some(queryCatalogTable))
      val kernelEngine = kernelContext.getDefaultEngine()

      def assertRoutesWith(
          expectedCatalogTableOpt: Option[CatalogTable])(
          operation: => Any): Unit = {
        val error = intercept[IllegalStateException](operation)
        assert(error.getMessage === "recorded catalog metadata")
        assert(observedCatalogTables.poll() === expectedCatalogTableOpt)
      }

      val legacyOperations = Seq[() => Any](
        () => manager.loadLatestSnapshot(),
        () => manager.loadSnapshotAt(0L),
        () => manager.getActiveCommitAtTime(0L, false, false, false),
        () => manager.checkVersionExists(0L, false, false),
        () => manager.getTableChanges(kernelEngine, 0L, Optional.empty()))
      legacyOperations.foreach(operation =>
        assertRoutesWith(Some(legacyCatalogTable))(operation()))

      def contextualOperations(context: DeltaV2QueryContext): Seq[() => Any] = Seq(
        () => manager.loadLatestSnapshot(context),
        () => manager.loadSnapshotAt(0L, context),
        () => manager.getActiveCommitAtTime(0L, false, false, false, context),
        () => manager.checkVersionExists(0L, false, false, context),
        () => manager.getTableChanges(kernelEngine, 0L, Optional.empty(), context))

      contextualOperations(queryContext).foreach(operation =>
        assertRoutesWith(Some(queryCatalogTable))(operation()))
      contextualOperations(DeltaV2QueryContext.empty).foreach(operation =>
        assertRoutesWith(None)(operation()))
      assert(observedCatalogTables.isEmpty)
    }
  }

  private def assertWaitsForSnapshotLock(thread: Thread): Unit = {
    val deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L)
    var stackTrace = thread.getStackTrace.toSeq
    while (thread.isAlive &&
        !stackTrace.exists(_.getMethodName == "lockInterruptibly") &&
        System.nanoTime() < deadlineNanos) {
      Thread.sleep(10L)
      stackTrace = thread.getStackTrace.toSeq
    }
    assert(
      stackTrace.exists(_.getMethodName == "lockInterruptibly"),
      s"${thread.getName} did not wait for the snapshot lock: ${stackTrace.mkString(", ")}")
  }

  // === Cold start ============================================

  test("cold start loads latest snapshot") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        val snapshot = mgr.loadLatestSnapshot()
        assert(snapshot != null)
        assert(snapshot.version == 0L)
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
        assert(snapshot.version == 2L)
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
          assert(first eq second, "Expected same snapshot instance on warm hit")
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
          assert(snap1.version == 0L)

          appendToDeltaTable(dir)

          val snap2 = mgr.loadLatestSnapshot()
          assert(snap2.version == 1L)
          assert(snap2 ne snap1, "A newly loaded snapshot must replace the cached facade")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("cached snapshot manager preserves KernelContext session invariance") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val originalSession = SparkSession.active
      val constructionSession = spark.newSession()
      val firstOperationSession = spark.newSession()
      val secondOperationSession = spark.newSession()
      val invariantOptions = Map(
        "fs.file.impl" -> classOf[CachedSnapshotManagerRecordingFileSystem].getName,
        "fs.file.impl.disable.cache" -> "true",
        CachedSnapshotManagerRecordingFileSystem.InvariantMarkerKey -> "table-option")
      var manager: CachedSnapshotManager = null

      try {
        SparkSession.setActiveSession(constructionSession)
        setRecordingMarkers(constructionSession, "construction")
        val kernelContext =
          KernelContext(invariantOptions, LogStore.createLogStore(constructionSession))
        manager = createManager(dir, kernelContext)

        SparkSession.setActiveSession(firstOperationSession)
        setRecordingMarkers(firstOperationSession, "first-operation")
        CachedSnapshotManagerRecordingFileSystem.clear()
        assert(manager.loadLatestSnapshot().version == 0L)
        assertRecordingFileSystemObserved("first-operation")
        val retainedEngine = kernelContext.getDefaultEngine()

        appendToDeltaTable(dir)
        SparkSession.setActiveSession(secondOperationSession)
        setRecordingMarkers(secondOperationSession, "second-operation")
        CachedSnapshotManagerRecordingFileSystem.clear()
        assert(manager.loadSnapshotAt(1L).version == 1L)
        assert(kernelContext.getDefaultEngine() eq retainedEngine)
        val expectedSessionMarker =
          "first-operation"
        assertRecordingFileSystemObserved(expectedSessionMarker)
      } finally {
        if (manager != null) manager.retire()
        SparkSession.setActiveSession(originalSession)
      }
    }
  }

  test("superseded and retired snapshots release persisted state and remain usable") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val beforePreviousStatsRddIds = spark.sparkContext.getPersistentRDDs.keySet
          val previous = mgr.loadLatestSnapshot()
          val previousFileCount = previous.allFiles.count()
          val previousStatsCount = previous.withStats.count()
          val previousRddIds =
            spark.sparkContext.getPersistentRDDs.keySet -- beforePreviousStatsRddIds
          assert(previousRddIds.nonEmpty)

          appendToDeltaTable(dir)
          val current = mgr.loadLatestSnapshot()
          assert(previousRddIds.intersect(spark.sparkContext.getPersistentRDDs.keySet).isEmpty)
          val beforeCurrentStatsRddIds = spark.sparkContext.getPersistentRDDs.keySet
          val currentStatsCount = current.withStats.count()
          val currentRddIds =
            spark.sparkContext.getPersistentRDDs.keySet -- beforeCurrentStatsRddIds
          assert(currentRddIds.nonEmpty)

          assert(previous.version == 0L)
          assert(current.version == 1L)
          assert(previous.allFiles.count() == previousFileCount)
          assert(previous.withStats.count() == previousStatsCount)
          assert(current.allFiles.count() > previousFileCount)

          mgr.retire()
          assert(currentRddIds.intersect(spark.sparkContext.getPersistentRDDs.keySet).isEmpty)
          assert(current.withStats.count() == currentStatsCount)
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
        assert(snapV0.version == 0L)
        assert(snapV1.version == 1L)
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
        try {
          val cachedLatest = mgr.loadLatestSnapshot()
          appendToDeltaTable(dir)

          val versioned = mgr.loadSnapshotAt(1L)
          val latestAgain = mgr.loadLatestSnapshot()
          assert(versioned.version == 1L)
          assert(versioned eq latestAgain)
          assert(latestAgain ne cachedLatest)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("newer loadSnapshotAt keeps a refreshed later snapshot cached") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val cachedLatest = mgr.loadLatestSnapshot()
          appendToDeltaTable(dir)
          appendToDeltaTable(dir)

          val versioned = mgr.loadSnapshotAt(1L)
          val latestAgain = mgr.loadLatestSnapshot()
          assert(versioned.version == 1L)
          assert(latestAgain.version == 2L)
          assert(latestAgain ne versioned)
          assert(latestAgain ne cachedLatest)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("older loadSnapshotAt preserves a non-stale cached latest snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val cachedLatest = mgr.loadLatestSnapshot()
          appendToDeltaTable(dir)

          val versioned = mgr.loadSnapshotAt(0L)
          val latestAgain = mgr.loadLatestSnapshot()
          assert(versioned.version == 0L)
          assert(latestAgain eq cachedLatest)
          assert(latestAgain.version == 1L)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("older loadSnapshotAt refreshes a stale cached latest snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val cachedLatest = mgr.loadLatestSnapshot()
          appendToDeltaTable(dir)

          val versioned = mgr.loadSnapshotAt(0L)
          val latestAgain = mgr.loadLatestSnapshot()
          assert(versioned.version == 0L)
          assert(latestAgain.version == 2L)
          assert(latestAgain ne versioned)
          assert(latestAgain ne cachedLatest)
        } finally {
          mgr.retire()
        }
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
          try {
            val beforeRetire = mgr.loadLatestSnapshot()

            mgr.retire()
            appendToDeltaTable(dir)

            val afterRetire = mgr.loadSnapshotAt(1L)
            assert(beforeRetire.version == 0L)
            assert(afterRetire.version == 1L)
            assert(afterRetire ne beforeRetire)
            assert(mgr.loadLatestSnapshot() eq afterRetire)
          } finally {
            mgr.retire()
          }
        }
      }
    }

    test(s"retire preserves newer historical refresh with staleness $stalenessLimit") {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> stalenessLimit) {
        withTempDir { dir =>
          createDeltaTable(dir)
          val mgr = createManager(dir)
          try {
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
          } finally {
            mgr.retire()
          }
        }
      }
    }

    test(s"failed exact load preserves latest refresh: $stalenessLimit") {
      withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> stalenessLimit) {
        withTempDir { dir =>
          createDeltaTable(dir)
          val mgr = createManager(dir)
          try {
            val beforeRetire = mgr.loadLatestSnapshot()

            mgr.retire()
            appendToDeltaTable(dir)

            intercept[KernelException] {
              mgr.loadSnapshotAt(2L)
            }
            val latest = mgr.loadLatestSnapshot()
            assert(latest.version == 1L)
            assert(latest ne beforeRetire)
          } finally {
            mgr.retire()
          }
        }
      }
    }
  }

  // === Table identity validation ==============================

  test("table recreation at the same path replaces the cached snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val previous = mgr.loadLatestSnapshot()
          JavaUtils.deleteRecursively(dir)
          createDeltaTable(dir)

          val replacement = mgr.loadLatestSnapshot()
          assert(replacement.version == 0L)
          assert(replacement.metadata.id != previous.metadata.id)
          assert(replacement ne previous)
          assert(mgr.loadLatestSnapshot() eq replacement)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === Refresh deduplication ==================================

  test("same-version refresh reuses the cached snapshot") {
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

  test("history, version, and commit-range operations delegate to an uncached manager") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        val activeCommit = mgr.getActiveCommitAtTime(
          Long.MaxValue,
          /* canReturnLastCommit= */ true,
          /* mustBeRecreatable= */ true,
          /* canReturnEarliestCommit= */ false)
        assert(activeCommit.getVersion == 0L)

        mgr.checkVersionExists(0L, mustBeRecreatable = true, allowOutOfRange = false)

        // scalastyle:off deltahadoopconfiguration
        val kernelEngine =
          KernelEngineFactory.createDefaultEngine(spark.sessionState.newHadoopConf())
        // scalastyle:on deltahadoopconfiguration
        val changes = mgr.getTableChanges(kernelEngine, 0L, Optional.empty())
        assert(changes != null)
      } finally {
        mgr.retire()
      }
    }
  }

  // === Concurrency correctness ================================

  test("concurrent refreshes serialize and the later caller advances the cached snapshot") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val kernelContext = KernelContext(
          Map(
            "fs.file.impl" -> classOf[CachedSnapshotManagerBlockingFileSystem].getName,
            "fs.file.impl.disable.cache" -> "true"),
          LogStore.createLogStore(spark))
        val manager = createManager(dir, kernelContext)
        val failures = new ConcurrentLinkedQueue[Throwable]()
        val staleResult = new AtomicReference[Snapshot]()
        val newerResult = new AtomicReference[Snapshot]()
        val secondaryPhase = new CyclicBarrier(2)
        val staleThread = new Thread(() => {
          try {
            staleResult.set(manager.loadLatestSnapshot())
          } catch {
            case NonFatal(failure) => failures.add(failure)
          }
        }, "stale-refresh")
        val newerThread = new Thread(() => {
          try {
            secondaryPhase.await(30L, TimeUnit.SECONDS)
            newerResult.set(manager.loadLatestSnapshot())
          } catch {
            case NonFatal(failure) => failures.add(failure)
          }
        }, "newer-refresh")
        staleThread.setDaemon(true)
        newerThread.setDaemon(true)

        try {
          assert(manager.loadLatestSnapshot().version == 0L)
          appendToDeltaTable(dir)
          CachedSnapshotManagerBlockingFileSystem.arm(staleThread.getName)
          staleThread.start()
          assert(
            CachedSnapshotManagerBlockingFileSystem.awaitCapturedListing(),
            "stale refresh did not capture a transaction-log listing")
          val staleListingCapturedAtMs = System.currentTimeMillis()

          newerThread.start()
          awaitClockAfter(staleListingCapturedAtMs)
          secondaryPhase.await(30L, TimeUnit.SECONDS)
          appendToDeltaTable(dir)
          assertWaitsForSnapshotLock(newerThread)
          assert(newerResult.get() == null)

          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          staleThread.join(TimeUnit.SECONDS.toMillis(30L))
          newerThread.join(TimeUnit.SECONDS.toMillis(30L))
          assert(!staleThread.isAlive, "stale refresh thread did not terminate")
          assert(!newerThread.isAlive, "newer refresh thread did not terminate")
          assert(failures.isEmpty, s"Concurrent refresh failed: ${failures.toArray.mkString(", ")}")
          assert(staleResult.get().version == 1L)
          assert(newerResult.get().version == 2L)
          assert(manager.loadSnapshotAt(2L) eq newerResult.get())
          assert(
            CachedSnapshotManagerBlockingFileSystem.listingThreadNames.distinct.sorted ==
              Seq("newer-refresh", "stale-refresh"),
            "the later freshness boundary should trigger a second reconstruction")
        } finally {
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          staleThread.interrupt()
          newerThread.interrupt()
          manager.retire()
        }
      }
    }
  }

  test("concurrent cold latest loads share one refresh across Spark sessions") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val kernelContext = KernelContext(
          Map(
            "fs.file.impl" -> classOf[CachedSnapshotManagerBlockingFileSystem].getName,
            "fs.file.impl.disable.cache" -> "true"),
          LogStore.createLogStore(spark))
        val manager = createManager(dir, kernelContext)
        val operationSessions = Seq(spark.newSession(), spark.newSession())
        operationSessions.foreach(
          _.conf.set(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key, "60s"))
        val startBarrier = new CyclicBarrier(operationSessions.size)
        val snapshots = new ConcurrentLinkedQueue[Snapshot]()
        val allFileSessions = new ConcurrentLinkedQueue[SparkSession]()
        val failures = new ConcurrentLinkedQueue[Throwable]()
        val loadsCompleted = new CountDownLatch(operationSessions.size)
        val materializeAllFiles = new CountDownLatch(1)
        val threads = operationSessions.zipWithIndex.map { case (operationSession, index) =>
          new Thread(() => {
            SparkSession.setActiveSession(operationSession)
            try {
              startBarrier.await(30L, TimeUnit.SECONDS)
              val snapshot = manager.loadLatestSnapshot()
              snapshots.add(snapshot)
              loadsCompleted.countDown()
              assert(materializeAllFiles.await(30L, TimeUnit.SECONDS))
              val allFiles = snapshot.allFiles
              assert(allFiles.count() > 0L)
              allFileSessions.add(allFiles.sparkSession)
            } catch {
              case failure: Throwable => failures.add(failure)
            } finally {
              SparkSession.clearActiveSession()
            }
          }, s"cold-latest-$index")
        }
        threads.foreach(_.setDaemon(true))

        try {
          CachedSnapshotManagerBlockingFileSystem.armFirstListing()
          threads.foreach(_.start())
          assert(
            CachedSnapshotManagerBlockingFileSystem.awaitCapturedListing(),
            "cold refresh did not capture a transaction-log listing")
          val lockWinner = CachedSnapshotManagerBlockingFileSystem.listingThreadNames.head
          assertWaitsForSnapshotLock(threads.find(_.getName != lockWinner).get)

          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          assert(
            loadsCompleted.await(30L, TimeUnit.SECONDS),
            "concurrent cold refreshes did not complete")
          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          assert(snapshots.size() == operationSessions.size)
          val returnedSnapshots = snapshots.asScala.toSeq
          assert(returnedSnapshots.map(_.version).distinct == Seq(0L))
          assert(returnedSnapshots.forall(_ eq returnedSnapshots.head))
          assert(
            CachedSnapshotManagerBlockingFileSystem.listingThreadNames.distinct.size == 1,
            "only the lock winner should reconstruct the cold snapshot")

          materializeAllFiles.countDown()
          threads.foreach(_.join(TimeUnit.SECONDS.toMillis(30L)))
          assert(
            threads.forall(thread => !thread.isAlive),
            "concurrent cold refresh threads did not terminate")
          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          assert(allFileSessions.size() == operationSessions.size)
          assert(allFileSessions.asScala.forall(operationSessions.contains))
          assert(allFileSessions.asScala.toSeq.distinct.size == 1)
          assert(!allFileSessions.asScala.exists(_ eq spark))
        } finally {
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          materializeAllFiles.countDown()
          threads.foreach(_.interrupt())
          manager.retire()
        }
      }
    }
  }

  test("concurrent cold latest and time-travel loads share one upper-bound refresh") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val kernelContext = KernelContext(
          Map(
            "fs.file.impl" -> classOf[CachedSnapshotManagerBlockingFileSystem].getName,
            "fs.file.impl.disable.cache" -> "true"),
          LogStore.createLogStore(spark))
        val manager = createManager(dir, kernelContext)
        val operationSessions = Seq(spark.newSession(), spark.newSession())
        operationSessions.foreach(
          _.conf.set(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key, "60s"))
        val startBarrier = new CyclicBarrier(operationSessions.size)
        val latestResult = new AtomicReference[Snapshot]()
        val timeTravelResult = new AtomicReference[Snapshot]()
        val failures = new ConcurrentLinkedQueue[Throwable]()
        val threads = operationSessions.zipWithIndex.map { case (operationSession, index) =>
          new Thread(() => {
            SparkSession.setActiveSession(operationSession)
            try {
              startBarrier.await(30L, TimeUnit.SECONDS)
              val result = if (index == 0) {
                manager.loadLatestSnapshot()
              } else {
                manager.loadSnapshotAt(0L)
              }
              if (index == 0) latestResult.set(result) else timeTravelResult.set(result)
            } catch {
              case failure: Throwable => failures.add(failure)
            } finally {
              SparkSession.clearActiveSession()
            }
          }, if (index == 0) "mixed-latest" else "mixed-time-travel")
        }
        threads.foreach(_.setDaemon(true))

        try {
          CachedSnapshotManagerBlockingFileSystem.armFirstListing()
          threads.foreach(_.start())
          assert(
            CachedSnapshotManagerBlockingFileSystem.awaitCapturedListing(),
            "mixed refresh did not capture a transaction-log listing")
          val lockWinner = CachedSnapshotManagerBlockingFileSystem.listingThreadNames.head
          assertWaitsForSnapshotLock(threads.find(_.getName != lockWinner).get)

          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          threads.foreach(_.join(TimeUnit.SECONDS.toMillis(30L)))
          assert(
            threads.forall(thread => !thread.isAlive),
            "mixed refresh threads did not terminate")
          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          assert(latestResult.get().version == 0L)
          assert(timeTravelResult.get().version == 0L)
          assert(latestResult.get() eq timeTravelResult.get())
          assert(manager.loadSnapshotAt(0L) eq latestResult.get())
          assert(
            CachedSnapshotManagerBlockingFileSystem.listingThreadNames.distinct.size == 1,
            "the waiter should reuse the upper bound installed by the lock winner")
        } finally {
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          threads.foreach(_.interrupt())
          manager.retire()
        }
      }
    }
  }

  test("exact load advances when latest upper bound misses a concurrent append") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val kernelContext = KernelContext(
          Map(
            "fs.file.impl" -> classOf[CachedSnapshotManagerBlockingFileSystem].getName,
            "fs.file.impl.disable.cache" -> "true"),
          LogStore.createLogStore(spark))
        val manager = createManager(dir, kernelContext)
        val result = new AtomicReference[Snapshot]()
        val failure = new AtomicReference[Throwable]()
        val loadThread = new Thread(() => {
          try {
            result.set(manager.loadSnapshotAt(2L))
          } catch {
            case NonFatal(error) => failure.set(error)
          }
        }, "stale-upper-bound")
        loadThread.setDaemon(true)

        try {
          assert(manager.loadLatestSnapshot().version == 0L)
          appendToDeltaTable(dir)
          CachedSnapshotManagerBlockingFileSystem.arm(loadThread.getName)
          loadThread.start()
          assert(
            CachedSnapshotManagerBlockingFileSystem.awaitCapturedListing(),
            "latest load did not capture its stale transaction-log listing")

          appendToDeltaTable(dir)
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          loadThread.join(TimeUnit.SECONDS.toMillis(30L))
          assert(!loadThread.isAlive, "exact fallback load did not terminate")
          assert(failure.get() == null, s"Exact fallback load failed: ${failure.get()}")
          assert(result.get().version == 2L)
          assert(manager.loadSnapshotAt(2L) eq result.get())

          val listingsBeforeLatestValidation =
            CachedSnapshotManagerBlockingFileSystem.listingThreadNames.size
          assert(manager.loadLatestSnapshot() eq result.get())
          assert(
            CachedSnapshotManagerBlockingFileSystem.listingThreadNames.size >
              listingsBeforeLatestValidation,
            "an exact-version installation must not be treated as a validated latest snapshot")
        } finally {
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          loadThread.interrupt()
          manager.retire()
        }
      }
    }
  }

  test("concurrent latest and historical loads preserve the installed upper bound") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val kernelContext = KernelContext(
          Map(
            "fs.file.impl" -> classOf[CachedSnapshotManagerBlockingFileSystem].getName,
            "fs.file.impl.disable.cache" -> "true"),
          LogStore.createLogStore(spark))
        val manager = createManager(dir, kernelContext)
        val initial = manager.loadLatestSnapshot()
        appendToDeltaTable(dir)
        appendToDeltaTable(dir)
        val latestSession = spark.newSession()
        val historicalSession = spark.newSession()
        val startBarrier = new CyclicBarrier(2)
        val latestResult = new AtomicReference[Snapshot]()
        val historicalResult = new AtomicReference[Snapshot]()
        val latestAllFilesSession = new AtomicReference[SparkSession]()
        val historicalAllFilesSession = new AtomicReference[SparkSession]()
        val failures = new ConcurrentLinkedQueue[Throwable]()
        val latestThread = new Thread(() => {
          SparkSession.setActiveSession(latestSession)
          try {
            startBarrier.await(30L, TimeUnit.SECONDS)
            val snapshot = manager.loadLatestSnapshot()
            latestResult.set(snapshot)
            latestAllFilesSession.set(snapshot.allFiles.sparkSession)
          } catch {
            case failure: Throwable => failures.add(failure)
          } finally {
            SparkSession.clearActiveSession()
          }
        }, "latest-upper-bound")
        val historicalThread = new Thread(() => {
          SparkSession.setActiveSession(historicalSession)
          try {
            startBarrier.await(30L, TimeUnit.SECONDS)
            val snapshot = manager.loadSnapshotAt(1L)
            historicalResult.set(snapshot)
            historicalAllFilesSession.set(snapshot.allFiles.sparkSession)
          } catch {
            case failure: Throwable => failures.add(failure)
          } finally {
            SparkSession.clearActiveSession()
          }
        }, "historical-version")
        val threads = Seq(latestThread, historicalThread)
        threads.foreach(_.setDaemon(true))

        try {
          CachedSnapshotManagerBlockingFileSystem.armFirstListing()
          threads.foreach(_.start())
          assert(
            CachedSnapshotManagerBlockingFileSystem.awaitCapturedListing(),
            "upper-bound refresh did not capture a transaction-log listing")
          val lockWinner = CachedSnapshotManagerBlockingFileSystem.listingThreadNames.head
          assertWaitsForSnapshotLock(threads.find(_.getName != lockWinner).get)

          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          threads.foreach(_.join(TimeUnit.SECONDS.toMillis(30L)))
          assert(
            threads.forall(thread => !thread.isAlive),
            "latest and historical refresh threads did not terminate")
          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          assert(latestResult.get().version == 2L)
          assert(historicalResult.get().version == 1L)
          assert(latestResult.get() ne initial)
          assert(historicalResult.get() ne latestResult.get())
          assert(manager.loadSnapshotAt(2L) eq latestResult.get())
          assert(latestResult.get().allFiles.count() > historicalResult.get().allFiles.count())
          assert(latestAllFilesSession.get() eq latestSession)
          assert(historicalAllFilesSession.get() eq historicalSession)
        } finally {
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          threads.foreach(_.interrupt())
          manager.retire()
        }
      }
    }
  }

  test("retire waits for an in-flight refresh and preserves manager reuse") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val kernelContext = KernelContext(
          Map(
            "fs.file.impl" -> classOf[CachedSnapshotManagerBlockingFileSystem].getName,
            "fs.file.impl.disable.cache" -> "true"),
          LogStore.createLogStore(spark))
        val manager = createManager(dir, kernelContext)
        manager.loadLatestSnapshot()
        appendToDeltaTable(dir)
        val refreshed = new AtomicReference[Snapshot]()
        val failures = new ConcurrentLinkedQueue[Throwable]()
        val refreshThread = new Thread(() => {
          SparkSession.setActiveSession(spark)
          try {
            refreshed.set(manager.loadLatestSnapshot())
          } catch {
            case failure: Throwable => failures.add(failure)
          } finally {
            SparkSession.clearActiveSession()
          }
        }, "refresh-before-retire")
        val retireThread = new Thread(() => {
          try {
            manager.retire()
          } catch {
            case failure: Throwable => failures.add(failure)
          }
        }, "concurrent-retire")
        val threads = Seq(refreshThread, retireThread)
        threads.foreach(_.setDaemon(true))

        try {
          CachedSnapshotManagerBlockingFileSystem.arm(refreshThread.getName)
          refreshThread.start()
          assert(
            CachedSnapshotManagerBlockingFileSystem.awaitCapturedListing(),
            "refresh did not capture a transaction-log listing")
          retireThread.start()
          assertWaitsForSnapshotLock(retireThread)

          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          threads.foreach(_.join(TimeUnit.SECONDS.toMillis(30L)))
          assert(
            threads.forall(thread => !thread.isAlive),
            "refresh and retire threads did not terminate")
          assert(
            failures.isEmpty,
            s"Concurrent lifecycle failed: ${failures.toArray.mkString(", ")}")
          assert(refreshed.get().version == 1L)
          assert(manager.loadSnapshotAt(1L) eq refreshed.get())
          assert(refreshed.get().allFiles.count() > 0L)

          appendToDeltaTable(dir)
          val next = manager.loadSnapshotAt(2L)
          assert(next.version == 2L)
          assert(next ne refreshed.get())
        } finally {
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          threads.foreach(_.interrupt())
          manager.retire()
        }
      }
    }
  }

  test("interrupting a snapshot-lock waiter leaves refresh and lock healthy") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val kernelContext = KernelContext(
          Map(
            "fs.file.impl" -> classOf[CachedSnapshotManagerBlockingFileSystem].getName,
            "fs.file.impl.disable.cache" -> "true"),
          LogStore.createLogStore(spark))
        val manager = createManager(dir, kernelContext)
        manager.loadLatestSnapshot()
        appendToDeltaTable(dir)
        val refreshed = new AtomicReference[Snapshot]()
        val refreshFailure = new AtomicReference[Throwable]()
        val waiterFailure = new AtomicReference[Throwable]()
        val refreshThread = new Thread(() => {
          SparkSession.setActiveSession(spark)
          try {
            refreshed.set(manager.loadLatestSnapshot())
          } catch {
            case failure: Throwable => refreshFailure.set(failure)
          } finally {
            SparkSession.clearActiveSession()
          }
        }, "refresh-lock-owner")
        val waiterThread = new Thread(() => {
          SparkSession.setActiveSession(spark)
          try {
            manager.loadLatestSnapshot()
          } catch {
            case failure: Throwable => waiterFailure.set(failure)
          } finally {
            SparkSession.clearActiveSession()
          }
        }, "interrupted-lock-waiter")
        val threads = Seq(refreshThread, waiterThread)
        threads.foreach(_.setDaemon(true))

        try {
          CachedSnapshotManagerBlockingFileSystem.arm(refreshThread.getName)
          refreshThread.start()
          assert(
            CachedSnapshotManagerBlockingFileSystem.awaitCapturedListing(),
            "refresh did not capture a transaction-log listing")
          waiterThread.start()
          assertWaitsForSnapshotLock(waiterThread)
          waiterThread.interrupt()
          waiterThread.join(TimeUnit.SECONDS.toMillis(30L))
          assert(!waiterThread.isAlive, "interrupted lock waiter did not terminate")
          assert(waiterFailure.get().isInstanceOf[InterruptedException])

          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          refreshThread.join(TimeUnit.SECONDS.toMillis(30L))
          assert(!refreshThread.isAlive, "refresh lock owner did not terminate")
          assert(refreshFailure.get() == null)
          assert(refreshed.get().version == 1L)

          appendToDeltaTable(dir)
          val next = manager.loadSnapshotAt(2L)
          assert(next.version == 2L)
          assert(next ne refreshed.get())
        } finally {
          CachedSnapshotManagerBlockingFileSystem.releaseListing()
          threads.foreach(_.interrupt())
          manager.retire()
        }
      }
    }
  }

  test("concurrent appends and loadLatestSnapshot calls observe monotonically newer versions") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val versions = 1L to 3L
          val numReaders = 4
          val failures = new ConcurrentLinkedQueue[Throwable]()
          val maxVersionSeen = new AtomicLong(mgr.loadLatestSnapshot().version)
          val roundStarted = new CyclicBarrier(numReaders + 1)
          val roundFinished = new CyclicBarrier(numReaders + 1)
          val readers = (1 to numReaders).map { _ =>
            new Thread(() => {
              try {
                var lastSeenVersion = -1L
                versions.foreach { _ =>
                  roundStarted.await(30L, TimeUnit.SECONDS)
                  val minimumVersion = maxVersionSeen.get()
                  try {
                    val observedVersion = mgr.loadLatestSnapshot().version
                    assert(observedVersion >= lastSeenVersion)
                    assert(observedVersion >= minimumVersion)
                    lastSeenVersion = observedVersion
                    maxVersionSeen.getAndUpdate(current => math.max(current, observedVersion))
                  } finally {
                    roundFinished.await(30L, TimeUnit.SECONDS)
                  }
                }
              } catch {
                case failure: Throwable => failures.add(failure)
              }
            })
          }
          val appender = new Thread(() => {
            try {
              versions.foreach { _ =>
                roundStarted.await(30L, TimeUnit.SECONDS)
                try {
                  appendToDeltaTable(dir)
                } finally {
                  roundFinished.await(30L, TimeUnit.SECONDS)
                }
              }
            } catch {
              case failure: Throwable => failures.add(failure)
            }
          })
          val threads = appender +: readers
          startAndJoinThreads(threads)

          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          val latest = mgr.loadLatestSnapshot()
          assert(latest.version == 3L)
          assert(latest.allFiles.count() > 0L)
          assert(latest.version >= maxVersionSeen.get())
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("concurrent cold loads return one table identity") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val startBarrier = new CyclicBarrier(8)
          val snapshots = new ConcurrentLinkedQueue[Snapshot]()
          val failures = new ConcurrentLinkedQueue[Throwable]()
          val threads = (1 to 8).map { _ =>
            new Thread(() => {
              try {
                startBarrier.await(30L, TimeUnit.SECONDS)
                snapshots.add(mgr.loadLatestSnapshot())
              } catch {
                case failure: Throwable => failures.add(failure)
              }
            })
          }
          startAndJoinThreads(threads)

          assert(failures.isEmpty, s"Concurrent loads failed: ${failures.toArray.mkString(", ")}")
          assert(snapshots.size() == threads.size)
          val tableIds = Seq.newBuilder[String]
          while (!snapshots.isEmpty) {
            tableIds += snapshots.poll().metadata.id
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
        try {
          val initial = mgr.loadLatestSnapshot()
          appendToDeltaTable(dir)
          appendToDeltaTable(dir)
          val currentResults = new ConcurrentLinkedQueue[Snapshot]()
          val intermediateResults = new ConcurrentLinkedQueue[Snapshot]()
          val historicalResults = new ConcurrentLinkedQueue[Snapshot]()
          val failures = new ConcurrentLinkedQueue[Throwable]()
          val numThreads = 12
          val startBarrier = new CyclicBarrier(numThreads)

          val threads = (1 to numThreads).map { index =>
            new Thread(() => {
              try {
                startBarrier.await(30L, TimeUnit.SECONDS)
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
          startAndJoinThreads(threads)

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
        } finally {
          mgr.retire()
        }
      }
    }
  }
}

private[tablemanager] object CachedSnapshotManagerRecordingFileSystem {
  val SessionMarkerKey = "fs.cached-snapshot-manager-test.session-marker"
  val InvariantMarkerKey = "fs.cached-snapshot-manager-test.table-option"

  private val observations = new CopyOnWriteArrayList[(String, String, String)]()

  def clear(): Unit = observations.clear()

  def record(operation: String, sessionMarker: String, invariantMarker: String): Unit =
    observations.add((operation, sessionMarker, invariantMarker))

  def currentThreadObservations: Seq[(String, String, String)] =
    observations.asScala.toSeq
}

private[tablemanager] class CachedSnapshotManagerRecordingFileSystem extends RawLocalFileSystem {
  private var sessionMarker: String = _
  private var invariantMarker: String = _

  override def initialize(name: URI, hadoopConf: Configuration): Unit = {
    sessionMarker =
      hadoopConf.get(CachedSnapshotManagerRecordingFileSystem.SessionMarkerKey)
    invariantMarker =
      hadoopConf.get(CachedSnapshotManagerRecordingFileSystem.InvariantMarkerKey)
    super.initialize(name, hadoopConf)
  }

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    record("open")
    super.open(path, bufferSize)
  }

  override def getFileStatus(path: Path): FileStatus = {
    record("getFileStatus")
    super.getFileStatus(path)
  }

  override def listStatus(path: Path): Array[FileStatus] = {
    record("listStatus")
    super.listStatus(path)
  }

  private def record(operation: String): Unit =
    CachedSnapshotManagerRecordingFileSystem.record(
      operation,
      sessionMarker,
      invariantMarker)
}

private[tablemanager] object CachedSnapshotManagerBlockingFileSystem {
  private val AnyThread = "*"
  private val targetThreadName = new AtomicReference[String]()
  private val listingThreads = new CopyOnWriteArrayList[String]()
  @volatile private var capturedListing = new CountDownLatch(0)
  @volatile private var releaseCapturedListing = new CountDownLatch(0)

  def arm(threadName: String): Unit = synchronized {
    listingThreads.clear()
    targetThreadName.set(threadName)
    capturedListing = new CountDownLatch(1)
    releaseCapturedListing = new CountDownLatch(1)
  }

  def armFirstListing(): Unit = arm(AnyThread)

  def listingThreadNames: Seq[String] = listingThreads.asScala.toSeq

  def awaitCapturedListing(): Boolean =
    capturedListing.await(30L, TimeUnit.SECONDS)

  def releaseListing(): Unit =
    releaseCapturedListing.countDown()

  def blockAfterListing(path: Path): Unit = {
    if (path.getName == "_delta_log") {
      listingThreads.add(Thread.currentThread().getName)
    }
    val expectedThreadName = targetThreadName.get()
    if (path.getName == "_delta_log" &&
        (expectedThreadName == AnyThread || expectedThreadName == Thread.currentThread().getName) &&
        targetThreadName.compareAndSet(expectedThreadName, null)) {
      capturedListing.countDown()
      if (!releaseCapturedListing.await(30L, TimeUnit.SECONDS)) {
        throw new IllegalStateException(
          "Timed out waiting to release captured transaction-log list")
      }
    }
  }
}

private[tablemanager] class CachedSnapshotManagerBlockingFileSystem extends RawLocalFileSystem {
  override def listStatus(path: Path): Array[FileStatus] = {
    val statuses = super.listStatus(path)
    CachedSnapshotManagerBlockingFileSystem.blockAfterListing(path)
    statuses
  }
}
