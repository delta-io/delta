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

import java.util
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import io.delta.storage.commit.uniform.UniformMetadata

import org.apache.spark.sql.delta.{CommittedTransaction, DeltaLog, MaintenanceOperation}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.{AbstractDeltaCatalogClient, CatalogTableEntry}
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.hooks.metrics.SimpleMockServer
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class UpdateUnityCatalogSuite extends QueryTest
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

  private def runHook(
      deltaLog: DeltaLog,
      ct: CatalogTable,
      actions: Seq[Action],
      hook: UpdateUnityCatalog = null): Unit = {
    val snapshot = deltaLog.update()
    Option(hook).getOrElse(UpdateUnityCatalog(ct)).run(spark, CommittedTransaction(
      txnId = "test-txn", deltaLog = deltaLog, catalogTable = Some(ct),
      readSnapshot = snapshot, committedVersion = snapshot.version, committedActions = actions,
      postCommitSnapshot = snapshot, postCommitHooks = Seq.empty, txnExecutionTimeMs = 0L,
      maintenanceOperation = MaintenanceOperation(), partitionsAddedToOpt = None,
      isBlindAppend = false))
    assert(UpdateUnityCatalog.awaitCompletionForTesting(10000))
  }

  test("run(): loads the catalog entry after a metadata-changing commit") {
    withMockCatalog() { (mockServer, deltaLog, ct) =>
      runHook(deltaLog, ct, Seq(deltaLog.update().metadata))
      assert(mockServer.getRequestCount() == 1)
      assert(mockServer.getLastRequestPath().endsWith(
        s"/delta/v1/catalogs/${ct.identifier.catalog.get}/schemas/d/tables/t"))
    }
  }

  test("run(): sends nothing when disabled or for a data-only commit") {
    val addFile = AddFile("f1.parquet", Map.empty, 1L, 0L, dataChange = true)
    Seq(
      ("disabled", false, false),
      ("data-only commit", true, true)
    ).foreach { case (label, enabled, dataOnly) =>
      withSQLConf(CONF_KEY -> enabled.toString) {
        withMockCatalog() { (mockServer, deltaLog, ct) =>
          runHook(deltaLog, ct, if (dataOnly) Seq(addFile) else Seq(deltaLog.update().metadata))
          assert(mockServer.getRequestCount() == 0, label)
        }
      }
    }
  }

  test("the factory picks the hook by catalog and table type") {
    val unityCatalog = UCCommitCoordinatorBuilder.UNITY_CATALOG_CONNECTOR_CLASS
    Seq(
      ("EXTERNAL in Unity Catalog", unityCatalog, CatalogTableType.EXTERNAL, true),
      ("MANAGED in Unity Catalog", unityCatalog, CatalogTableType.MANAGED, false),
      ("EXTERNAL elsewhere", "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        CatalogTableType.EXTERNAL, false)
    ).foreach { case (label, catalogClass, tableType, isUnityCatalogHook) =>
      withMockCatalog(catalogClass) { (_, _, ct) =>
        val hook = UpdateCatalogFactory.getUpdateCatalogHook(ct.copy(tableType = tableType), spark)
        assert(hook.isInstanceOf[UpdateUnityCatalog] == isUnityCatalogHook, label)
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
        assert(UpdateUnityCatalog.changesTableMetadata(actions) == expected,
          actions.toString)
      }
    }
  }

  /** Serves the given entries in turn and answers each update with the next given result. */
  private class FakeCatalogClient(entries: Seq[CatalogTableEntry], updateResults: Seq[Boolean])
      extends AbstractDeltaCatalogClient {
    var loads = 0
    val updates = ArrayBuffer.empty[
      (CatalogTableEntry, Metadata, Metadata, Option[Protocol], Seq[DomainMetadata])]

    override def loadTableEntry(ct: CatalogTable): Option[CatalogTableEntry] = {
      loads += 1
      Some(entries(math.min(loads, entries.size) - 1))
    }

    override def updateTableMetadata(
        ct: CatalogTable,
        entry: CatalogTableEntry,
        oldMetadata: Metadata,
        newMetadata: Metadata,
        newProtocol: Option[Protocol],
        domainMetadata: Seq[DomainMetadata],
        commitVersion: Long,
        commitTimestampMs: Long): Boolean = {
      updates += ((entry, oldMetadata, newMetadata, newProtocol, domainMetadata))
      updateResults(updates.size - 1)
    }

    override def tableExists(ident: Identifier): Boolean = throw new UnsupportedOperationException
    override def loadTable(ident: Identifier): Table = throw new UnsupportedOperationException
    override def createStagingTable(
        ident: Identifier,
        properties: util.Map[String, String]): util.Map[String, String] =
      throw new UnsupportedOperationException
    override def loadTableAndBuildReplaceProps(
        ident: Identifier,
        properties: util.Map[String, String]): util.Map[String, String] =
      throw new UnsupportedOperationException
    override def createTable(
        ident: Identifier,
        table: CatalogTable,
        metadata: Metadata,
        domainMetadata: Seq[DomainMetadata],
        protocol: Protocol,
        lastCommitTimestampMs: Long,
        lastCommitVersion: Long,
        uniformMetadata: Option[UniformMetadata]): Unit = throw new UnsupportedOperationException
    override def reportMetrics(
        ct: CatalogTable,
        committedActions: Seq[Action],
        committedVersion: Long,
        snapshotHistogram: Option[FileSizeHistogram]): Unit =
      throw new UnsupportedOperationException
  }

  private def hookWith(ct: CatalogTable, client: AbstractDeltaCatalogClient): UpdateUnityCatalog =
    new UpdateUnityCatalog(ct) {
      override protected def createClient(
          spark: org.apache.spark.sql.SparkSession,
          catalogName: String): Option[AbstractDeltaCatalogClient] = Some(client)
    }

  private def entryOf(
      ct: CatalogTable,
      deltaLog: DeltaLog,
      etag: String,
      properties: Map[String, String] = Map.empty): CatalogTableEntry =
    CatalogTableEntry(
      ct.copy(
        storage = CatalogStorageFormat.empty.copy(locationUri = Some(deltaLog.dataPath.toUri)),
        properties = properties),
      UUID.randomUUID(),
      etag)

  test("run(): reloads the entry and retries once when the update conflicts") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val client = new FakeCatalogClient(
        Seq(entryOf(ct, deltaLog, "etag-1"), entryOf(ct, deltaLog, "etag-2")),
        Seq(false, true))

      runHook(deltaLog, ct, Seq(deltaLog.update().metadata), hookWith(ct, client))

      assert(client.loads === 2)
      assert(client.updates.map(_._1.etag) === Seq("etag-1", "etag-2"))
    }
  }

  test("run(): gives up after a second conflict") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val client = new FakeCatalogClient(Seq(entryOf(ct, deltaLog, "etag-1")), Seq(false, false))

      runHook(deltaLog, ct, Seq(deltaLog.update().metadata), hookWith(ct, client))

      assert(client.loads === 2 && client.updates.size === 2)
    }
  }

  test("run(): sends the protocol only if the entry's differs, and not what UC derives") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val snapshot = deltaLog.update()
      val derived = snapshot.getProperties.toMap.filter { case (k, _) =>
        k.startsWith("delta.min") || k.startsWith("delta.feature.")
      }
      val userProperties = Map("user" -> "v")
      val client = new FakeCatalogClient(
        Seq(entryOf(ct, deltaLog, "etag-1", userProperties ++ Map(
          "delta.lastUpdateVersion" -> "0", "clusteringColumns" -> "[]") ++ derived),
          entryOf(ct, deltaLog, "etag-2", userProperties)),
        Seq(true, true))

      runHook(deltaLog, ct, Seq(snapshot.metadata), hookWith(ct, client))
      runHook(deltaLog, ct, Seq(snapshot.metadata), hookWith(ct, client))

      // The first entry already has the protocol; the second doesn't.
      val (_, oldMetadata, _, firstProtocol, _) = client.updates(0)
      val (_, _, _, secondProtocol, _) = client.updates(1)
      assert(firstProtocol.isEmpty && secondProtocol === Some(snapshot.protocol))
      // What UC derives itself is left out, so it can't be removed.
      assert(oldMetadata.configuration === userProperties)
    }
  }

  test("clusteringUpdate: sets, clears, or leaves the clustering domain") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      sql(s"CREATE TABLE delta.`$path` (id BIGINT) USING delta CLUSTER BY (id)")
      val clustered = DeltaLog.forTable(spark, path).update()
      val domain = clustered.domainMetadata
        .find(_.domain == ClusteringMetadataDomain.domainName).get
      val columns = ClusteringMetadataDomain.unapply(domain).get._1.clusteringColumns
      val entryColumns = Map("clusteringColumns" -> JsonUtils.toJson(columns))

      assert(UpdateUnityCatalog.clusteringUpdate(clustered, Map.empty) === Some(domain))
      assert(UpdateUnityCatalog.clusteringUpdate(
        clustered, Map("clusteringColumns" -> """[["other"]]""")) === Some(domain))
      assert(UpdateUnityCatalog.clusteringUpdate(clustered, entryColumns) === None)

      withTempDir { plain =>
        spark.range(1).write.format("delta").save(plain.getCanonicalPath)
        val unclustered = DeltaLog.forTable(spark, plain.getCanonicalPath).update()
        assert(UpdateUnityCatalog.clusteringUpdate(unclustered, Map.empty) === None)
        assert(UpdateUnityCatalog.clusteringUpdate(unclustered, entryColumns) ===
          Some(DomainMetadata(ClusteringMetadataDomain.domainName, "{}", removed = true)))
      }
    }
  }
}
