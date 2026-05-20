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

import java.util.{Collections, Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.storage.commit.{CommitCoordinatorClient => JCommitCoordinatorClient}
import io.delta.storage.commit.{TableIdentifier => UCTableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkSessionSwitch}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.{DeltaLog, IcebergConstants}
import org.apache.spark.sql.delta.DeltaConfigs.{
  COORDINATED_COMMITS_COORDINATOR_CONF,
  COORDINATED_COMMITS_COORDINATOR_NAME
}
import org.apache.spark.sql.delta.NonSparkReadIceberg
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.coordinatedcommits.{
  CatalogOwnedCommitCoordinatorBuilder,
  CommitCoordinatorProvider,
  InMemoryUCClient,
  InMemoryUCCommitCoordinator,
  UCCommitCoordinatorBuilder
}
import org.apache.spark.sql.delta.icebergShaded.IcebergConverter
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.uniform.hms.HMSTest
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.spark.sql.internal.SQLConf

/**
 * This trait allows the tests to write with Delta
 * using a in-memory HiveMetaStore as catalog,
 * and read from the same HiveMetaStore with Iceberg.
 */
trait WriteDeltaHMSReadIceberg extends UniFormE2ETest
  with DeltaSQLCommandTest with HMSTest with SparkSessionSwitch {

  override protected def sparkConf: SparkConf =
    setupSparkConfWithHMS(super.sparkConf)
      .set(DeltaSQLConf.DELTA_UNIFORM_ICEBERG_SYNC_CONVERT_ENABLED.key, "true")
      .set(DeltaSQLConf.DELTA_OPTIMIZE_WRITE_ENABLED.key, "true")
      .set(DeltaSQLConf.DELTA_AUTO_COMPACT_ENABLED.key, "true")

  private var _readerSparkSession: Option[SparkSession] = None
  /**
   * Verify the result by reading from the reader session and compare the result to the expected.
   *
   * @param table  write table name
   * @param fields fields to verify, separated by comma. E.g., "col1, col2"
   * @param orderBy fields to order the results, separated by comma.
   * @param expect expected result
   */
  protected override def readAndVerify(
      table: String, fields: String, orderBy: String, expect: Seq[Row]): Unit = {
    val translated = tableNameForRead(table)
    withSession(readerSparkSession) { session =>
      checkAnswer(session.sql(s"SELECT $fields FROM $translated ORDER BY $orderBy"), expect)
    }
  }

  protected def readerSparkSession: SparkSession = {
    if (_readerSparkSession.isEmpty) {
      // call to newSession makes sure
      // [[SparkSession.getOrCreate]] gives a new session
      // and [[SparkContext.getOrCreate]] uses a new context
      _readerSparkSession = Some(newSession(createIcebergSparkSession))
    }
    _readerSparkSession.get
  }
}

/**
 * A [[DeltaCatalog]] subclass that enriches [[CatalogTable]] with the last converted Iceberg
 * metadata from [[InMemoryUCCommitCoordinator]] before loading the table. This simulates what
 * the real UC catalog does: returning
 * [[IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP]]
 * and [[IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP]] as catalog table
 * properties so that [[IcebergConverter]] can perform incremental conversion.
 */
class UCBackedDeltaCatalog extends DeltaCatalog {
  override def loadCatalogTable(ident: Identifier, catalogTable: CatalogTable): Table = {
    val enriched = UCBackedDeltaCatalog.currentCoordinator.flatMap { coordinator =>
      val deltaLog = DeltaLog.forTable(spark, new Path(catalogTable.location))
      val tableConf = deltaLog.update().metadata.coordinatedCommitsTableConf
      tableConf.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY).flatMap { tableId =>
        coordinator.getUniformMetadata(tableId)
          .filter(_.getIcebergMetadata.isPresent)
          .map { meta =>
            val icebergMeta = meta.getIcebergMetadata.get
            catalogTable.copy(properties = catalogTable.properties +
              (IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP->
                icebergMeta.getMetadataLocation) +
              (IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP ->
                icebergMeta.getConvertedDeltaVersion.toString))
          }
      }
    }.getOrElse(catalogTable)
    super.loadCatalogTable(ident, enriched)
  }
}

object UCBackedDeltaCatalog {
  @volatile var currentCoordinator: Option[InMemoryUCCommitCoordinator] = None
}

/**
 * Trait that wires up an in-memory UC commit coordinator for UniForm E2E testing.
 *
 * Mix this into a concrete suite that already extends [[UniFormE2EIcebergSuiteBase]] (or any
 * other [[UniFormE2ETest]] subclass) to redirect every [[readAndVerify]] call through the
 * native Iceberg reader backed by the in-memory UC coordinator
 *
 * Concrete suites must call [[requiredTableProperties]] inside their
 * [[UniFormE2EIcebergSuiteBase.extraTableProperties]] override to inject the coordinator
 * name and conf into every `CREATE TABLE` statement.
 */
trait WriteDeltaUCCCReadIceberg extends UniFormE2ETest
  with DeltaSQLCommandTest
  with NonSparkReadIceberg {

  /**
   * Use customized catalog so that uniform property is injected into catalogTable
   */
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[UCBackedDeltaCatalog].getName)

  /**
   * A [[UCCommitCoordinatorClient]] subclass that overrides [[registerTable]] to auto-assign
   * a UC table ID, simulating what the UC catalog does during CREATE TABLE.
   */
  private class TestUCBackedCommitCoordinator(ucClient: InMemoryUCClient)
    extends UCCommitCoordinatorClient(Collections.emptyMap(), ucClient) {

    @volatile var lastRegisteredTableId: String = _

    /**
     * Delta blocks setting `COORDINATED_COMMITS_TABLE_CONF` in TBLPROPERTIES, so this trait
     * simulates what the real UC catalog does: a [[CatalogOwnedCommitCoordinatorBuilder]] returns
     * a single [[TestUCBackedCommitCoordinator]] instance whose [[registerTable]] auto-assigns a
     * UUID.  Returning the same instance from every [[build]]/[[buildForCatalog]] call ensures
     * that [[UCCommitCoordinatorClient.semanticEquals]] (which uses reference equality on `conf`)
     * returns true and Delta does not reject intra-test metadata updates.
     */
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

  protected var ucCommitCoordinator: InMemoryUCCommitCoordinator = _
  private var testCoordinator: TestUCBackedCommitCoordinator = _

  abstract override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaLog.clearCache()
    CommitCoordinatorProvider.clearAllBuilders()
    ucCommitCoordinator = new InMemoryUCCommitCoordinator()
    UCBackedDeltaCatalog.currentCoordinator = Some(ucCommitCoordinator)
    val ucClient = new InMemoryUCClient("test-metastore", ucCommitCoordinator)
    testCoordinator = new TestUCBackedCommitCoordinator(ucClient)
    CommitCoordinatorProvider.registerBuilder(new CatalogOwnedCommitCoordinatorBuilder {
      override def getName: String = UCCommitCoordinatorBuilder.getName
      override def build(
          spark: SparkSession, conf: Map[String, String]): JCommitCoordinatorClient =
        testCoordinator
      override def buildForCatalog(
          spark: SparkSession, catalogName: String): JCommitCoordinatorClient =
        testCoordinator
    })
  }

  abstract override def afterEach(): Unit = {
    UCBackedDeltaCatalog.currentCoordinator = None
    CommitCoordinatorProvider.clearAllBuilders()
    DeltaLog.clearCache()
    super.afterEach()
  }

  /**
   * Returns the TBLPROPERTIES SQL fragment required to enable the UC commit coordinator.
   * Concrete suites should append this to their [[extraTableProperties]] override.
   */
  def requiredTableProperties: String =
    s", '${COORDINATED_COMMITS_COORDINATOR_NAME.key}' = '${UCCommitCoordinatorBuilder.getName}'" +
      s", '${COORDINATED_COMMITS_COORDINATOR_CONF.key}' = " +
      s"'${JsonUtils.toJson(Map.empty[String, String])}'"

  override protected def readAndVerify(
      table: String, fields: String, orderBy: String, expect: Seq[Row]): Unit = {
    val tableId = testCoordinator.lastRegisteredTableId
    assert(tableId != null,
      s"No table UUID assigned for '$table' - table was not created with CC properties")
    val schema = DeltaLog.forTable(spark, TableIdentifier(table)).update().schema
    val uniformMetadata = ucCommitCoordinator.getUniformMetadata(tableId)
    assert(uniformMetadata.isDefined,
      s"No UniForm metadata found for table '$table' (ID $tableId)")
    assert(uniformMetadata.get.getIcebergMetadata.isPresent,
      s"No Iceberg metadata found for table '$table' (ID $tableId)")
    val icebergMetadataPath = uniformMetadata.get.getIcebergMetadata.get.getMetadataLocation
    verifyReadByPath(icebergMetadataPath, schema, fields, orderBy, expect)
  }
}

/**
 * Concrete E2E suite that runs all [[UniFormE2EIcebergSuiteBase]] tests with tables backed
 * by an in-memory UC commit coordinator, reading results via the native Iceberg reader.
 */
class UniFormE2EIcebergUCSuite extends UniFormE2EIcebergSuiteBase
    with WriteDeltaUCCCReadIceberg {
  // No test should go here. Please add tests in [[UniFormE2EIcebergSuiteBase]]
  override def extraTableProperties(compatVersion: Int): String =
    super.extraTableProperties(compatVersion) + requiredTableProperties
}
