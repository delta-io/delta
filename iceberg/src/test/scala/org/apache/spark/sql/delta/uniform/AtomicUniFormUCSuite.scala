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

package org.apache.spark.sql.delta.uniform

// scalastyle:off import.ordering.noEmptyLine
import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaConfigs.{
  COORDINATED_COMMITS_COORDINATOR_CONF,
  COORDINATED_COMMITS_COORDINATOR_NAME
}
import shadedForDelta.org.apache.iceberg.hadoop.HadoopTables
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.coordinatedcommits.{
  CatalogOwnedCommitCoordinatorBuilder,
  CommitCoordinatorProvider,
  InMemoryUCClient,
  InMemoryUCCommitCoordinator
}
import io.delta.storage.commit.{CommitCoordinatorClient => JCommitCoordinatorClient}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.util.JsonUtils
import io.delta.storage.commit.{TableIdentifier => UCTableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.test.SharedSparkSession

/**
 * End-to-end test for atomic UniForm: verifies that when a Delta table has
 * UniForm Iceberg enabled and uses a UC-backed commit coordinator, an INSERT
 * stores the pre-generated [[UniformMetadata]] (DeltaUniformIceberg) in the
 * in-memory UC coordinator where it can be retrieved by table ID.
 */
class AtomicUniFormUCSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  // ── Test commit coordinator ───────────────────────────────────────────────

  /**
   * Subclass of [[UCCommitCoordinatorClient]] that overrides [[registerTable]]
   * to auto-assign a UC table ID (UUID), making it compatible with the
   * standard Delta CREATE TABLE flow without needing a pre-registered UC table.
   *
   * All commit traffic flows through the real [[UCCommitCoordinatorClient]] →
   * [[InMemoryUCClient]] → [[InMemoryUCCommitCoordinator]] path, so
   * [[UniformMetadata]] is stored in the coordinator and can be fetched by ID.
   */
  private class TestUCBackedCommitCoordinator(ucClient: InMemoryUCClient)
      extends UCCommitCoordinatorClient(Collections.emptyMap(), ucClient) {

    @volatile var lastRegisteredTableId: String = _

    override def registerTable(
        logPath: Path,
        tableIdentifier: Optional[UCTableIdentifier],
        currentVersion: Long,
        currentMetadata: AbstractMetadata,
        currentProtocol: AbstractProtocol): java.util.Map[String, String] = {
      val tableId = UUID.randomUUID().toString
      lastRegisteredTableId = tableId
      Map(UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableId).asJava
    }
  }

  // ── Shared state (re-created per test) ──────────────────────────────────

  private var ucCoordinator: InMemoryUCCommitCoordinator = _
  private var testCoordinator: TestUCBackedCommitCoordinator = _

  // ── Before / After ───────────────────────────────────────────────────────

  override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaLog.clearCache()
    CommitCoordinatorProvider.clearAllBuilders()
    ucCoordinator = new InMemoryUCCommitCoordinator()
    val ucClient = new InMemoryUCClient("test-metastore", ucCoordinator)
    testCoordinator = new TestUCBackedCommitCoordinator(ucClient)
    CommitCoordinatorProvider.registerBuilder(new CatalogOwnedCommitCoordinatorBuilder {
      override def getName: String = "unity-catalog"
      override def build(
          spark: SparkSession,
          conf: Map[String, String]): JCommitCoordinatorClient = testCoordinator
      override def buildForCatalog(
          spark: SparkSession,
          catalogName: String): JCommitCoordinatorClient = testCoordinator
    })
  }

  override def afterEach(): Unit = {
    super.afterEach()
    CommitCoordinatorProvider.clearAllBuilders()
    DeltaLog.clearCache()
  }

  // ── Test ─────────────────────────────────────────────────────────────────

  test("INSERT on UniForm table stores IcebergMetadata in UC coordinator") {
    withTable("tbl") {
      sql(
        s"""CREATE TABLE tbl (id INT) USING delta
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg',
           |  '${COORDINATED_COMMITS_COORDINATOR_NAME.key}' = 'unity-catalog',
           |  '${COORDINATED_COMMITS_COORDINATOR_CONF.key}' =
           |      '${JsonUtils.toJson(Map.empty[String, String])}'
           |)""".stripMargin)

      sql("INSERT INTO tbl VALUES (1)")

      // The table ID was assigned by registerTable and stored in Delta's tableConf.
      val tableId = testCoordinator.lastRegisteredTableId
      assert(tableId != null, "Table should have been registered with a UC table ID")

      // Fetch the UniformMetadata from the in-memory UC coordinator, simulating
      // what a real catalog query would return.
      val uniformMetaOpt = ucCoordinator.getUniformMetadata(tableId)
      assert(uniformMetaOpt.isDefined,
        "UC coordinator should have stored UniformMetadata from the INSERT commit")
      assert(uniformMetaOpt.get.getIcebergMetadata.isPresent,
        "UniformMetadata should contain IcebergMetadata from the pre-commit conversion")

      // Verify the Iceberg metadata content: load the actual metadata file and check
      // that the file count matches the Delta snapshot.
      val metadataPath = uniformMetaOpt.get.getIcebergMetadata.get.getMetadataLocation
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("tbl"))
      val snapshot = deltaLog.snapshot
      val icebergTable = new HadoopTables(deltaLog.newDeltaHadoopConf()).load(metadataPath)
      val numFilesInIceberg =
        icebergTable.currentSnapshot().summary().get("total-data-files").toInt
      assert(
        numFilesInIceberg == snapshot.numOfFiles,
        s"Iceberg total-data-files ($numFilesInIceberg) must equal " +
          s"Delta numOfFiles (${snapshot.numOfFiles})")
    }
  }
}
