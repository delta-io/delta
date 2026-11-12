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

package org.apache.spark.sql.delta.hooks

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog, MaintenanceOperation}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata}
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.hooks.metrics.SimpleMockServer
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class UpdateExternalTableMetadataHookSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private val CONF_KEY = DeltaSQLConf.DELTA_UC_EXTERNAL_TABLE_METADATA_SYNC_ENABLED.key

  // Unique per test, so a catalog's confs never leak into another test.
  private val catalogCounter = new AtomicInteger(0)

  /**
   * Runs `testBody` with a Delta table and an EXTERNAL `CatalogTable` for it in a catalog of
   * class `catalogClass` that points at a mock UC server. The mock answers `{}`, so the hook's
   * load fails after reaching the server; the tests only count requests.
   */
  private def withMockCatalog(
      catalogClass: String = UCCommitCoordinatorBuilder.UNITY_CATALOG_CONNECTOR_CLASS)(
      testBody: (SimpleMockServer, DeltaLog, CatalogTable) => Unit): Unit = {
    val catalogName = s"ext_cat_${catalogCounter.incrementAndGet()}"
    val mockServer = new SimpleMockServer(0)
    mockServer.start()
    try {
      spark.conf.set(s"spark.sql.catalog.$catalogName", catalogClass)
      spark.conf.set(s"spark.sql.catalog.$catalogName.uri",
        s"http://localhost:${mockServer.getPort()}")
      spark.conf.set(s"spark.sql.catalog.$catalogName.deltaRestApi.enabled", "true")
      spark.conf.set(s"spark.sql.catalog.$catalogName.auth.type", "static")
      spark.conf.set(s"spark.sql.catalog.$catalogName.auth.token", "test-token")
      withTempDir { dir =>
        spark.range(1).write.format("delta").save(dir.getCanonicalPath)
        val deltaLog = DeltaLog.forTable(spark, dir.getCanonicalPath)
        val ct = CatalogTable(
          identifier = TableIdentifier("t", Some("d"), Some(catalogName)),
          tableType = CatalogTableType.EXTERNAL,
          storage = CatalogStorageFormat.empty,
          schema = new StructType())
        testBody(mockServer, deltaLog, ct)
      }
    } finally {
      mockServer.stop()
    }
  }

  private def runHook(deltaLog: DeltaLog, ct: CatalogTable, actions: Seq[Action]): Unit = {
    val snapshot = deltaLog.update()
    UpdateExternalTableMetadataHook(ct).run(spark, CommittedTransaction(
      txnId = "test-txn", deltaLog = deltaLog, catalogTable = Some(ct),
      readSnapshot = snapshot, committedVersion = snapshot.version, committedActions = actions,
      postCommitSnapshot = snapshot, postCommitHooks = Seq.empty, txnExecutionTimeMs = 0L,
      maintenanceOperation = MaintenanceOperation(), partitionsAddedToOpt = None,
      isBlindAppend = false))
    assert(UpdateExternalTableMetadataHook.awaitCompletionForTesting(10000))
  }

  test("run(): loads the catalog entry after a metadata-changing commit") {
    withMockCatalog() { (mockServer, deltaLog, ct) =>
      runHook(deltaLog, ct, Seq(deltaLog.update().metadata))
      assert(mockServer.getRequestCount() == 1)
      assert(mockServer.getLastRequestPath().endsWith(
        s"/delta/v1/catalogs/${ct.identifier.catalog.get}/schemas/d/tables/t"))
    }
  }

  test("run(): sends nothing when disabled, outside Unity Catalog, or for a data-only commit") {
    val addFile = AddFile("f1.parquet", Map.empty, 1L, 0L, dataChange = true)
    Seq(
      ("disabled", UCCommitCoordinatorBuilder.UNITY_CATALOG_CONNECTOR_CLASS, false, false),
      ("not Unity Catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog", true, false),
      ("data-only commit", UCCommitCoordinatorBuilder.UNITY_CATALOG_CONNECTOR_CLASS, true, true)
    ).foreach { case (label, catalogClass, enabled, dataOnly) =>
      withSQLConf(CONF_KEY -> enabled.toString) {
        withMockCatalog(catalogClass) { (mockServer, deltaLog, ct) =>
          runHook(deltaLog, ct, if (dataOnly) Seq(addFile) else Seq(deltaLog.update().metadata))
          assert(mockServer.getRequestCount() == 0, label)
        }
      }
    }
  }

  test("changesTableMetadata: metadata, protocol, and clustering changes count") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      val snapshot = DeltaLog.forTable(spark, dir.getCanonicalPath).update()
      Seq(
        Seq(snapshot.metadata) -> true,
        Seq(snapshot.protocol) -> true,
        Seq(DomainMetadata("delta.clustering", "{}", removed = false)) -> true,
        Seq(DomainMetadata("delta.rowTracking", "{}", removed = false)) -> false,
        Seq(AddFile("f1.parquet", Map.empty, 1L, 0L, dataChange = true)) -> false
      ).foreach { case (actions, expected) =>
        assert(UpdateExternalTableMetadataHook.changesTableMetadata(actions) == expected,
          actions.toString)
      }
    }
  }
}
