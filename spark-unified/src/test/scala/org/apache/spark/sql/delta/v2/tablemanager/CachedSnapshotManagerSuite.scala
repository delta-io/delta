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
import java.lang.reflect.{InvocationHandler, Method, Proxy}
import java.util.{Collections, Optional, UUID}

import org.apache.spark.sql.delta.{
  CatalogOwnedTableFeature, DeltaLog
}
import org.apache.spark.sql.delta.actions.{
  TableFeatureProtocolUtils
}
import org.apache.spark.sql.delta.coordinatedcommits.{
  CatalogOwnedCommitCoordinatorProvider,
  TrackingCommitCoordinatorClient,
  TrackingInMemoryCommitCoordinatorBuilder
}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.v2.interop.DeltaV2Snapshot
import io.delta.spark.internal.v2.kernel.KernelEngineFactory
import io.delta.spark.internal.v2.snapshot.unitycatalog.{
  UCManagedTableSnapshotManager,
  UCTableInfo
}
import org.apache.hadoop.fs.Path
// scalastyle:off import.ordering.noEmptyLine
import io.delta.kernel.engine.{
  Engine => KernelEngine
}
import io.delta.kernel.internal.{
  SnapshotImpl => KernelSnapshot
}
import io.delta.kernel.unitycatalog.{
  UCCatalogManagedClient => ShadedUCClient,
  UCTableIdentifier => ShadedUCTableId
}

// scalastyle:on import.ordering.noEmptyLine
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType
}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class CachedSnapshotManagerSuite
    extends QueryTest
    with SharedSparkSession {

  private def createDeltaTable(dir: File, numRows: Int = 10): Unit = {
    spark.range(numRows).write.format("delta").save(dir.getCanonicalPath)
  }

  private def appendToDeltaTable(dir: File, numRows: Int = 5): Unit = {
    spark.range(numRows).write.format("delta").mode("append").save(dir.getCanonicalPath)
  }

  private def createManager(dir: File): CachedSnapshotManager = {
    new CachedSnapshotManager(new Path(dir.getCanonicalPath),
      catalogTableOpt = None, sessionInvariantFsOptions = Map.empty)
  }

  /** A plain catalog table with no UC metadata (no tableId). */
  private def dummyCatalogTable(dir: File): CatalogTable = {
    new CatalogTable(
      identifier = TableIdentifier("test_table", Some("default")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(dir.toURI)),
      schema = new StructType(),
      provider = Some("delta"))
  }

  private def createCatalogManager(
      dir: File): CachedSnapshotManager = {
    new CachedSnapshotManager(
      new Path(dir.getCanonicalPath),
      catalogTableOpt = Some(dummyCatalogTable(dir)),
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

  // === Retire lifecycle =======================================

  test("retire closes current snapshot") {
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

  // === Incremental build ======================================

  test("incremental build advances version after append") {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
          .key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          val snap1 = mgr.loadLatestSnapshot()
          val v1 = DeltaV2Snapshot
            .getKernelSnapshot(snap1).getVersion
          assert(v1 == 0L)

          appendToDeltaTable(dir)

          val snap2 = mgr.loadLatestSnapshot()
          val v2 = DeltaV2Snapshot
            .getKernelSnapshot(snap2).getVersion
          assert(v2 == 1L,
            "Incremental build should advance to v1")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("incremental build through multiple appends") {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
          .key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          mgr.loadLatestSnapshot()

          appendToDeltaTable(dir)
          appendToDeltaTable(dir)
          appendToDeltaTable(dir)

          val snap = mgr.loadLatestSnapshot()
          val version = DeltaV2Snapshot
            .getKernelSnapshot(snap).getVersion
          assert(version == 3L,
            "Incremental build should reach v3")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === Validation timestamp tracking ==========================

  test("lastValidatedAtMs advances on successful acquire") {
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
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        mgr.loadLatestSnapshot()
        val firstSnap = mgr.currentSnapshotForTesting
        assert(firstSnap.getVersion == 0L)

        val second = mgr.loadLatestSnapshot()
        val secondSnap = mgr.currentSnapshotForTesting
        assert(firstSnap eq secondSnap,
          "Same version should keep existing instance")
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
          assert(cached.getVersion == 2L,
            "All threads should converge on the latest version")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("retire during concurrent loads does not leak snapshots") {
    withSQLConf(DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT.key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        mgr.loadLatestSnapshot()

        val latch = new java.util.concurrent.CountDownLatch(1)
        val loaders = (1 to 4).map { _ =>
          new Thread(() => {
            latch.await()
            try { mgr.loadLatestSnapshot() }
            catch { case _: Exception => }
          })
        }
        loaders.foreach(_.start())

        latch.countDown()
        Thread.sleep(5)
        mgr.retire()

        loaders.foreach(_.join())
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

  // === Slice 2: DeltaV2SnapshotManager default =============

  test("loadLatestSnapshotFrom: default throws UnsupportedOperationException") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        val snap = mgr.loadLatestSnapshot()
        val kernelSnap = DeltaV2Snapshot.getKernelSnapshot(snap)
        val ex = intercept[UnsupportedOperationException] {
          mgr.loadLatestSnapshotFrom(null, kernelSnap)
        }
        assert(ex.getMessage.contains("Incremental snapshot update not supported"))
      } finally {
        mgr.retire()
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

          assert(mgr.tableIdForTesting == tableId,
            "Table identity must remain stable across concurrent loads")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === Identity-close bug regression ========================

  test("installSnapshot with identity reference does not" +
      " close the live snapshot") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val mgr = createManager(dir)
      try {
        mgr.loadLatestSnapshot()
        val live = mgr.currentSnapshotForTesting
        assert(live != null)
        assert(live.getVersion == 0L)

        val now = System.currentTimeMillis()
        val returned = mgr.installSnapshot(live, now)

        assert(returned eq live,
          "installSnapshot should return the live snapshot")
        assert(mgr.currentSnapshotForTesting eq live,
          "Cached snapshot must still be the same object")
        // Verify the snapshot is still usable (not closed)
        assert(live.getVersion == 0L)
        assert(mgr.lastValidatedAtMsForTesting == now)
      } finally {
        mgr.retire()
      }
    }
  }

  test("installSnapshot closes a distinct same-version" +
      " snapshot") {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
          .key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createManager(dir)
        try {
          mgr.loadLatestSnapshot()
          val live = mgr.currentSnapshotForTesting
          assert(live.getVersion == 0L)

          // Load a second, distinct v0 snapshot
          val other = mgr.loadLatestUncached()
          assert(other.getVersion == 0L)
          assert(other ne live)

          val now = System.currentTimeMillis()
          val returned = mgr.installSnapshot(other, now)

          assert(returned eq live,
            "Should keep existing on same version")
          assert(mgr.currentSnapshotForTesting eq live)
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === Non-UC catalog routing ================================

  test("non-UC catalog table falls back to path-based" +
      " incremental build") {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
          .key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createCatalogManager(dir)
        try {
          val snap1 = mgr.loadLatestSnapshot()
          val v1 = DeltaV2Snapshot
            .getKernelSnapshot(snap1).getVersion
          assert(v1 == 0L)

          appendToDeltaTable(dir)

          val snap2 = mgr.loadLatestSnapshot()
          val v2 = DeltaV2Snapshot
            .getKernelSnapshot(snap2).getVersion
          assert(v2 == 1L,
            "Non-UC catalog table should advance via" +
              " filesystem incremental build")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  test("non-UC catalog table: no-change reuses cached" +
      " snapshot") {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
          .key -> "60000") {
      withTempDir { dir =>
        createDeltaTable(dir)
        val mgr = createCatalogManager(dir)
        try {
          val snap1 = mgr.loadLatestSnapshot()
          val snap2 = mgr.loadLatestSnapshot()
          val k1 =
            DeltaV2Snapshot.getKernelSnapshot(snap1)
          val k2 =
            DeltaV2Snapshot.getKernelSnapshot(snap2)
          assert(k1 eq k2,
            "Should reuse cached snapshot on warm hit")
        } finally {
          mgr.retire()
        }
      }
    }
  }

  // === UCManagedTableSnapshotManager delegation ==============

  /**
   * Proxy-based stub of the shaded [[ShadedUCClient]].
   * Only `loadSnapshotFrom` is exercised; other methods
   * throw [[UnsupportedOperationException]].
   */
  private def stubUCClient(
      fn: (KernelEngine, KernelSnapshot) =>
        KernelSnapshot): ShadedUCClient = {
    val cls = classOf[ShadedUCClient]
    Proxy.newProxyInstance(
      cls.getClassLoader,
      Array[Class[_]](cls),
      new InvocationHandler {
        override def invoke(
            proxy: AnyRef,
            method: Method,
            args: Array[AnyRef]): AnyRef = {
          method.getName match {
            case "loadSnapshotFrom" =>
              fn(
                args(0).asInstanceOf[KernelEngine],
                args(1).asInstanceOf[KernelSnapshot])
            case _ =>
              throw new UnsupportedOperationException(
                s"Stub: ${method.getName}")
          }
        }
      }).asInstanceOf[ShadedUCClient]
  }

  private def dummyTableInfo(
      dir: File): UCTableInfo = {
    new UCTableInfo(
      "test-table-id",
      dir.getCanonicalPath,
      new ShadedUCTableId("cat", "sch", "tbl"),
      "https://uc.test/api/2.1/unity-catalog",
      Collections.emptyMap[String, String]())
  }

  private def withKernelEngine[T](f: KernelEngine => T): T = {
    // scalastyle:off deltahadoopconfiguration
    val conf = spark.sessionState
      .newHadoopConfWithOptions(Map.empty)
    // scalastyle:on deltahadoopconfiguration
    val engine =
      KernelEngineFactory.createDefaultEngine(conf)
    try f(engine) finally engine.close()
  }

  test("UCManagedTableSnapshotManager: identity on" +
      " no-advance from client") {
    withTempDir { dir =>
      createDeltaTable(dir)
      val pathMgr = createManager(dir)
      try {
        pathMgr.loadLatestSnapshot()
        val existing = pathMgr.currentSnapshotForTesting

        val client = stubUCClient { (_, snap) => snap }
        val info = dummyTableInfo(dir)
        withKernelEngine { engine =>
          val ucMgr =
            new UCManagedTableSnapshotManager(
              client, info, engine)
          val result =
            ucMgr.loadLatestSnapshotFrom(
              engine, existing)
          assert(result eq existing,
            "Should return same object on no-advance")
        }
      } finally {
        pathMgr.retire()
      }
    }
  }

  test("UCManagedTableSnapshotManager: delegation" +
      " returns updated snapshot version") {
    withSQLConf(
        DeltaSQLConf.DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
          .key -> "0") {
      withTempDir { dir =>
        createDeltaTable(dir)
        appendToDeltaTable(dir)
        val pathMgr = createManager(dir)
        try {
          val snapV0 = pathMgr.loadSnapshotAt(0L)
          val v0Kernel =
            DeltaV2Snapshot.getKernelSnapshot(snapV0)
          assert(v0Kernel.getVersion == 0L)

          val snapV1 = pathMgr.loadSnapshotAt(1L)
          val v1Kernel =
            DeltaV2Snapshot.getKernelSnapshot(snapV1)
          assert(v1Kernel.getVersion == 1L)

          val client = stubUCClient { (_, _) =>
            v1Kernel
          }
          val info = dummyTableInfo(dir)
          withKernelEngine { engine =>
            val ucMgr =
              new UCManagedTableSnapshotManager(
                client, info, engine)
            val result =
              ucMgr.loadLatestSnapshotFrom(
                engine, v0Kernel)
            assert(result ne v0Kernel,
              "Should return a different object")
            assert(result.getVersion == 1L,
              "Should delegate to client result")
          }
        } finally {
          pathMgr.retire()
        }
      }
    }
  }

  // === UC-routed CachedSnapshotManager tests ===============

  private val catalogManagedFeatureKey =
    TableFeatureProtocolUtils.propertyKey(
      CatalogOwnedTableFeature)

  /**
   * Constructs a [[CatalogTable]] with UC identity from a
   * SQL-created catalog-managed table. Injects a synthetic
   * `tableId` via `ignoredProperties` because the in-memory
   * test catalog does not populate it.
   */
  private def catalogTableWithUCIdentity(
      tableName: String): CatalogTable = {
    val raw = spark.sessionState.catalog
      .getTableMetadata(
        TableIdentifier(tableName, Some("default")))
    raw.copy(
      ignoredProperties =
        raw.ignoredProperties + (
          CatalogTable.UNITY_CATALOG_RESOURCE_ID ->
            UUID.randomUUID().toString))
  }

  private def withUCCoordinator[T](
      f: TrackingCommitCoordinatorClient => T): T = {
    // batchSize = 1 backfills every commit
    // immediately so that getCommits returns only
    // the latestTableVersion pointer without staged
    // commit paths -- the Rust kernel's setLogTail
    // cannot parse the _staged_commits path format
    // used by the tracking coordinator.
    val builder =
      TrackingInMemoryCommitCoordinatorBuilder(
        batchSize = 1)
    val coordinator = builder
      .trackingInMemoryCommitCoordinatorClient
      .asInstanceOf[TrackingCommitCoordinatorClient]
    CatalogOwnedCommitCoordinatorProvider
      .clearBuilders()
    CatalogOwnedCommitCoordinatorProvider
      .registerBuilder("spark_catalog", builder)
    try {
      f(coordinator)
    } finally {
      CatalogOwnedCommitCoordinatorProvider
        .clearBuilders()
      DeltaLog.clearCache()
    }
  }

  private def createCatalogManagedTable(
      tableName: String): Unit = {
    sql(
      s"""CREATE TABLE spark_catalog.default.$tableName
         |USING delta
         |TBLPROPERTIES (
         |  '$catalogManagedFeatureKey' =
         |  '${TableFeatureProtocolUtils
              .FEATURE_PROP_SUPPORTED}'
         |)
         |AS SELECT CAST(1 AS INT) AS id""".stripMargin)
  }

  private def uniqueTableName(): String =
    "uc_csm_" +
      UUID.randomUUID().toString.replace("-", "_")

  test("UC-routed: stale rebuild advances version" +
      " through UCClientEdge") {
    withUCCoordinator { coordinator =>
      val tableName = uniqueTableName()
      withSQLConf(
          DeltaSQLConf.CATALOG_OWNED_ALLOW_CREATE_OR_UPGRADE
            .key -> "true",
          DeltaSQLConf
            .CATALOG_MANAGED_ALLOW_NON_UC_MANAGED_TABLE_CREATION
            .key -> "true",
          DeltaSQLConf
            .DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
            .key -> "0") {
        withTable(tableName) {
          createCatalogManagedTable(tableName)
          val ct = catalogTableWithUCIdentity(tableName)
          val tblPath = new Path(ct.location)

          DeltaLog.clearCache()
          coordinator.reset()

          val mgr = new CachedSnapshotManager(
            tblPath,
            catalogTableOpt = Some(ct),
            sessionInvariantFsOptions = Map.empty)
          try {
            val s1 = mgr.loadLatestSnapshot()
            val v1 = DeltaV2Snapshot
              .getKernelSnapshot(s1).getVersion
            assert(v1 == 0L)
            assert(
              coordinator.numGetCommitsCalled
                .get() > 0,
              "Initial load must route through" +
                " UCClientEdge.getCommits")

            coordinator.reset()
            sql(s"INSERT INTO " +
              s"spark_catalog.default.$tableName " +
              s"VALUES (2)")

            coordinator.reset()
            val s2 = mgr.loadLatestSnapshot()
            val v2 = DeltaV2Snapshot
              .getKernelSnapshot(s2).getVersion
            assert(v2 == 1L,
              "Incremental UC rebuild should" +
                " advance to v1")
            assert(
              coordinator.numGetCommitsCalled
                .get() > 0,
              "Incremental rebuild must call" +
                " getCommits via UCClientEdge")
            assert(
              coordinator.lastGetCommitsStartVersion
                .get() == 1L,
              "Incremental UC rebuild must send" +
                " startVersion = existingVersion" +
                " + 1, not full-reload 0")
          } finally {
            mgr.retire()
          }
        }
      }
    }
  }

  test("UC-routed: no-change refresh reuses" +
      " cached snapshot") {
    withUCCoordinator { coordinator =>
      val tableName = uniqueTableName()
      withSQLConf(
          DeltaSQLConf.CATALOG_OWNED_ALLOW_CREATE_OR_UPGRADE
            .key -> "true",
          DeltaSQLConf
            .CATALOG_MANAGED_ALLOW_NON_UC_MANAGED_TABLE_CREATION
            .key -> "true",
          DeltaSQLConf
            .DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
            .key -> "0") {
        withTable(tableName) {
          createCatalogManagedTable(tableName)
          val ct = catalogTableWithUCIdentity(tableName)
          val tblPath = new Path(ct.location)

          DeltaLog.clearCache()
          coordinator.reset()

          val mgr = new CachedSnapshotManager(
            tblPath,
            catalogTableOpt = Some(ct),
            sessionInvariantFsOptions = Map.empty)
          try {
            val s1 = mgr.loadLatestSnapshot()
            val k1 = DeltaV2Snapshot
              .getKernelSnapshot(s1)
            assert(k1.getVersion == 0L)

            coordinator.reset()
            val s2 = mgr.loadLatestSnapshot()
            val k2 = DeltaV2Snapshot
              .getKernelSnapshot(s2)
            assert(k2.getVersion == 0L)
            assert(k1 eq k2,
              "No-change UC refresh must reuse" +
                " the identical cached snapshot")
          } finally {
            mgr.retire()
          }
        }
      }
    }
  }

  test("UC-routed: failed incremental refresh" +
      " propagates error, preserves cached snapshot") {
    withUCCoordinator { coordinator =>
      val tableName = uniqueTableName()
      withSQLConf(
          DeltaSQLConf.CATALOG_OWNED_ALLOW_CREATE_OR_UPGRADE
            .key -> "true",
          DeltaSQLConf
            .CATALOG_MANAGED_ALLOW_NON_UC_MANAGED_TABLE_CREATION
            .key -> "true",
          DeltaSQLConf
            .DELTA_ASYNC_UPDATE_STALENESS_TIME_LIMIT
            .key -> "0") {
        withTable(tableName) {
          createCatalogManagedTable(tableName)
          val ct = catalogTableWithUCIdentity(tableName)
          val tblPath = new Path(ct.location)

          DeltaLog.clearCache()
          coordinator.reset()

          val mgr = new CachedSnapshotManager(
            tblPath,
            catalogTableOpt = Some(ct),
            sessionInvariantFsOptions = Map.empty)
          try {
            val s1 = mgr.loadLatestSnapshot()
            val cached = mgr.currentSnapshotForTesting
            assert(cached != null)
            assert(cached.getVersion == 0L)
            val validatedBefore =
              mgr.lastValidatedAtMsForTesting

            // Remove the coordinator so the next
            // incrementalBuildUC fails when
            // UCClientEdge.getCommitCoordinator
            // cannot locate the builder.
            CatalogOwnedCommitCoordinatorProvider
              .clearBuilders()

            val ex =
              intercept[IllegalStateException] {
                mgr.loadLatestSnapshot()
              }
            assert(ex.getMessage.contains(
              "Couldn't locate commit coordinator"))

            // The previously installed snapshot
            // remains usable.
            val stillCached =
              mgr.currentSnapshotForTesting
            assert(stillCached eq cached,
              "Failed refresh must not evict" +
                " the cached snapshot")
            assert(stillCached.getVersion == 0L,
              "Cached snapshot must remain" +
                " usable after failure")
            assert(
              mgr.lastValidatedAtMsForTesting ==
                validatedBefore,
              "Failed refresh must not update" +
                " lastValidatedAtMs")
          } finally {
            mgr.retire()
          }
        }
      }
    }
  }
}
