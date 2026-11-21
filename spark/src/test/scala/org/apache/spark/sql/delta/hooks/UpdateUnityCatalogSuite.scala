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

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient.{UC_ETAG_KEY, UC_EXTERNAL_TABLE_ID_KEY}
import io.delta.storage.commit.uniform.UniformMetadata

import org.apache.spark.sql.delta.{CatalogManagedTableMaintenanceOperation, CommittedTransaction, DeltaLog, IcebergConstants, MaintenanceOperation}
import org.apache.spark.sql.delta.actions.{Action, AddFile, DomainMetadata, Metadata, Protocol}
import org.apache.spark.sql.delta.catalog.AbstractDeltaCatalogClient
import org.apache.spark.sql.delta.clustering.ClusteringMetadataDomain
import org.apache.spark.sql.delta.coordinatedcommits.UCCommitCoordinatorBuilder
import org.apache.spark.sql.delta.hooks.metrics.SimpleMockServer
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.stats.FileSizeHistogram
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, V1Table}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class UpdateUnityCatalogSuite extends QueryTest
  with SharedSparkSession
  with DeltaSQLCommandTest {

  private val CONF_KEY = DeltaSQLConf.DELTA_UC_EXTERNAL_TABLE_METADATA_SYNC_ENABLED.key

  // Unique per test, so a catalog's confs never leak into another test.
  private val catalogCounter = new AtomicInteger(0)

  private val tableId = UUID.fromString("00000000-0000-0000-0000-000000000003")

  /** A table as loaded for reads: the entry's id and etag, and its properties, are in storage. */
  private def loadedTable(
      ct: CatalogTable,
      etag: String,
      location: Option[java.net.URI] = None,
      properties: Map[String, String] = Map.empty): CatalogTable =
    ct.copy(storage = CatalogStorageFormat.empty.copy(
      locationUri = location,
      properties = properties ++
        Map(
          UC_EXTERNAL_TABLE_ID_KEY -> tableId.toString,
          UC_ETAG_KEY -> etag)))

  /**
   * Runs `testBody` with a Delta table and an EXTERNAL `CatalogTable` for it in a catalog of
   * class `catalogClass` that points at a mock UC server. The mock answers `{}`, so the hook's
   * update fails after reaching the server; the tests only count requests.
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
        val ct = loadedTable(
          CatalogTable(
            identifier = TableIdentifier("t", Some("d"), Some(catalogName)),
            tableType = CatalogTableType.EXTERNAL,
            storage = CatalogStorageFormat.empty,
            schema = new StructType()),
          etag = "etag-1",
          location = Some(deltaLog.dataPath.toUri))
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
  }

  test("run(): updates the entry after a metadata-changing commit, without loading it first") {
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

  test("the factory picks the hook by catalog, table type and etag") {
    val unityCatalog = UCCommitCoordinatorBuilder.UNITY_CATALOG_CONNECTOR_CLASS
    Seq(
      ("EXTERNAL in Unity Catalog", unityCatalog, CatalogTableType.EXTERNAL, true, true),
      ("EXTERNAL loaded without an etag", unityCatalog, CatalogTableType.EXTERNAL, false, false),
      ("MANAGED in Unity Catalog", unityCatalog, CatalogTableType.MANAGED, true, false),
      ("EXTERNAL elsewhere", "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        CatalogTableType.EXTERNAL, true, false)
    ).foreach { case (label, catalogClass, tableType, hasEtag, isUnityCatalogHook) =>
      withMockCatalog(catalogClass) { (_, _, ct) =>
        val table = ct.copy(
          tableType = tableType,
          storage = if (hasEtag) ct.storage else CatalogStorageFormat.empty)
        val hook = UpdateCatalogFactory.getUpdateCatalogHook(table, spark)
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

  test("withEntryProperties keeps the entry's properties, not credentials or bookkeeping") {
    val ct = loadedTable(
      CatalogTable(
        identifier = TableIdentifier("t", Some("d"), Some("c")),
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat.empty,
        schema = new StructType()),
      etag = "etag-1",
      properties = Map(
        "user" -> "v",
        "delta.minReaderVersion" -> "1",
        "delta.lastUpdateVersion" -> "3",
        "fs.s3a.access.key" -> "secret",
        "fs.unitycatalog.auth.oauth.clientSecret" -> "secret",
        CatalogManagedTableMaintenanceOperation.ALLOWED_OPERATIONS_PROPERTY -> "DATA_CLEANUP",
        IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP -> "s3://m",
        IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP -> "1",
        IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_TIMESTAMP_PROP -> "2"))

    val view = UpdateUnityCatalog.withEntryProperties(ct)

    assert(view.properties === Map(
      "user" -> "v", "delta.minReaderVersion" -> "1", "delta.lastUpdateVersion" -> "3"))
    // The table's storage, with the etag and id the update is conditional on, is untouched.
    assert(view.storage === ct.storage)
  }

  /** Serves the given tables in turn on a reload, and answers each update with the next result. */
  private class FakeCatalogClient(
      reloads: Seq[Option[CatalogTable]],
      updateResults: Seq[Boolean],
      updateFailure: Option[RuntimeException] = None)
      extends AbstractDeltaCatalogClient {
    var refetches = 0
    // Whether each reload fetched credentials: it should not.
    val reloadsWithCredentials = ArrayBuffer.empty[Boolean]
    val updates = ArrayBuffer.empty[
      (CatalogTable, Metadata, Metadata, Option[Protocol], Seq[DomainMetadata])]

    override def loadTable(ident: Identifier, fetchCredentials: Boolean): Table = {
      refetches += 1
      reloadsWithCredentials += fetchCredentials
      reloads(math.min(refetches, reloads.size) - 1)
        .map(V1Table(_)).getOrElse(throw new NoSuchTableException(ident))
    }

    override def updateTableMetadata(
        ct: CatalogTable,
        oldMetadata: Metadata,
        newMetadata: Metadata,
        newProtocol: Option[Protocol],
        domainMetadata: Seq[DomainMetadata],
        commitVersion: Long,
        commitTimestampMs: Long): Boolean = {
      updates += ((ct, oldMetadata, newMetadata, newProtocol, domainMetadata))
      updateFailure.foreach(e => throw e)
      updateResults(math.min(updates.size, updateResults.size) - 1)
    }

    override def tableExists(ident: Identifier): Boolean = throw new UnsupportedOperationException
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

  private def etagOf(ct: CatalogTable): String =
    ct.storage.properties(UC_ETAG_KEY)

  test("run(): reloads the table and retries when the update conflicts") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val client = new FakeCatalogClient(
        Seq(Some(loadedTable(ct, "etag-2", ct.storage.locationUri))), Seq(false, true))

      runHook(deltaLog, ct, Seq(deltaLog.update().metadata), hookWith(ct, client))

      assert(client.refetches === 1 && client.reloadsWithCredentials === Seq(false))
      assert(client.updates.map(u => etagOf(u._1)) === Seq("etag-1", "etag-2"))
    }
  }

  test("run(): retries a limited number of times, then gives up") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val client = new FakeCatalogClient(
        Seq(Some(loadedTable(ct, "etag-2", ct.storage.locationUri))), Seq(false))

      runHook(deltaLog, ct, Seq(deltaLog.update().metadata), hookWith(ct, client))

      assert(client.updates.size === 1 + UpdateUnityCatalog.ETAG_RETRIES)
      assert(client.refetches === UpdateUnityCatalog.ETAG_RETRIES)
    }
  }

  test("run(): stops when the table is gone or is another table") {
    val elsewhere = Some(new java.net.URI("file:/somewhere/else"))
    Seq(
      ("gone", (_: CatalogTable) => None),
      ("another table", (ct: CatalogTable) => Some(loadedTable(ct, "etag-2", elsewhere)))
    ).foreach { case (label, reloaded) =>
      withMockCatalog() { (_, deltaLog, ct) =>
        val client = new FakeCatalogClient(Seq(reloaded(ct)), Seq(false))

        runHook(deltaLog, ct, Seq(deltaLog.update().metadata), hookWith(ct, client))

        assert(client.updates.size === 1 && client.refetches === 1, label)
      }
    }
  }

  test("run(): a failing update or reload never fails the commit") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val failingUpdate = new FakeCatalogClient(
        Seq(None), Seq(true), Some(new RuntimeException("UC is down")))
      runHook(deltaLog, ct, Seq(deltaLog.update().metadata), hookWith(ct, failingUpdate))
      assert(failingUpdate.updates.size === 1)

      val failingReload = new FakeCatalogClient(Seq(None), Seq(false)) {
        override def loadTable(ident: Identifier, fetchCredentials: Boolean): Table =
          throw new RuntimeException("UC is down")
      }
      runHook(deltaLog, ct, Seq(deltaLog.update().metadata), hookWith(ct, failingReload))
      assert(failingReload.updates.size === 1)
    }
  }

  test("run(): sends the protocol only if the entry's differs, and not what UC derives") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val snapshot = deltaLog.update()
      val derived = snapshot.getProperties.toMap.filter { case (k, _) =>
        k.startsWith("delta.min") || k.startsWith("delta.feature.")
      }
      val userProperties = Map("user" -> "v")
      val withProtocol = loadedTable(ct, "etag-1", ct.storage.locationUri,
        userProperties ++ Map(
          "delta.lastUpdateVersion" -> "0", "clusteringColumns" -> "[]") ++ derived)
      val withoutProtocol = loadedTable(ct, "etag-1", ct.storage.locationUri, userProperties)
      val client = new FakeCatalogClient(Seq(None), Seq(true))

      runHook(deltaLog, withProtocol, Seq(snapshot.metadata), hookWith(withProtocol, client))
      runHook(deltaLog, withoutProtocol, Seq(snapshot.metadata), hookWith(withoutProtocol, client))

      // The first entry already has the protocol; the second doesn't.
      val (_, oldMetadata, _, firstProtocol, _) = client.updates(0)
      val (_, _, _, secondProtocol, _) = client.updates(1)
      assert(firstProtocol.isEmpty && secondProtocol === Some(snapshot.protocol))
      // What UC derives itself is left out, so it can't be removed.
      assert(oldMetadata.configuration === userProperties)
    }
  }

  test("run(): sends nothing when the entry already matches the table") {
    withMockCatalog() { (_, deltaLog, ct) =>
      val snapshot = deltaLog.update()
      val upToDate = loadedTable(ct, "etag-1", ct.storage.locationUri, snapshot.getProperties.toMap)
        .copy(schema = snapshot.schema)
      val client = new FakeCatalogClient(Seq(None), Seq(true))

      runHook(deltaLog, upToDate, Seq(snapshot.metadata), hookWith(upToDate, client))

      assert(client.updates.isEmpty && client.refetches === 0)
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
