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

import java.util.{Collections, UUID}

import io.delta.storage.commit.{CommitCoordinatorClient => JCommitCoordinatorClient}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkSessionSwitch}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.{Identifier, Table}
import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, DeltaLog, DeltaOperations, IcebergConstants}
import org.apache.spark.sql.delta.NonSparkReadIceberg
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.coordinatedcommits.{
  CatalogOwnedCommitCoordinatorBuilder,
  CatalogOwnedCommitCoordinatorProvider,
  CatalogOwnedTableUtils,
  InMemoryUCClient,
  InMemoryUCCommitCoordinator,
  TrackingInMemoryCommitCoordinatorBuilder,
  UCCommitCoordinatorBuilder
}
import org.apache.spark.sql.delta.icebergShaded.IcebergConverter
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.uniform.hms.HMSTest
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
      val tableConf = deltaLog.update().metadata.configuration
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
 * native Iceberg reader backed by the in-memory UC coordinator.
 *
 * Concrete suites must call [[requiredTableProperties]] inside their
 * [[UniFormE2EIcebergSuiteBase.extraTableProperties]] override to enable
 * [[CatalogOwnedTableFeature]] in every `CREATE TABLE` statement.
 */
trait WriteDeltaUCCCReadIceberg extends UniFormE2ETest
  with DeltaSQLCommandTest
  with NonSparkReadIceberg {
  this: UniFormE2EIcebergSuiteBase =>

  /**
   * Use customized catalog so that uniform property is injected into catalogTable.
   */
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(
      SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      classOf[UCBackedDeltaCatalog].getName)

  protected var ucCommitCoordinator: InMemoryUCCommitCoordinator = _
  private var testCoordinator: UCCommitCoordinatorClient = _
  private var currentTableId: String = _

  abstract override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaLog.clearCache()
    ucCommitCoordinator = new InMemoryUCCommitCoordinator()
    UCBackedDeltaCatalog.currentCoordinator = Some(ucCommitCoordinator)
    val ucClient = new InMemoryUCClient("test-metastore", ucCommitCoordinator)
    testCoordinator = new UCCommitCoordinatorClient(Collections.emptyMap(), ucClient)
    registerBootstrapCatalogOwnedProvider()
  }

  private def registerBootstrapCatalogOwnedProvider(): Unit = {
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      catalogName = CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING,
      commitCoordinatorBuilder = TrackingInMemoryCommitCoordinatorBuilder(batchSize = 1))
  }

  private def registerUCBackedCatalogOwnedProvider(): Unit = {
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      catalogName = CatalogOwnedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING,
      commitCoordinatorBuilder = new CatalogOwnedCommitCoordinatorBuilder {
        override def getName: String = UCCommitCoordinatorBuilder.getName
        override def build(
            spark: SparkSession, conf: Map[String, String]): JCommitCoordinatorClient =
          testCoordinator
        override def buildForCatalog(
            spark: SparkSession, catalogName: String): JCommitCoordinatorClient =
          testCoordinator
      })
  }

  private def assignUCTableIdIfNeeded(tableName: String): Unit = {
    val tableIdentifier = TableIdentifier(tableName)
    if (!spark.sessionState.catalog.tableExists(tableIdentifier)) {
      return
    }
    val catalogTable = spark.sessionState.catalog.getTableMetadata(tableIdentifier)
    val deltaLog = DeltaLog.forTable(spark, catalogTable)
    val snapshot = deltaLog.update(catalogTableOpt = Some(catalogTable))
    if (!snapshot.isCatalogOwned) {
      return
    }
    snapshot.metadata.configuration.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY) match {
      case Some(tableId) =>
        currentTableId = tableId
        registerUCBackedCatalogOwnedProvider()
      case None =>
        val tableId = UUID.randomUUID().toString
        val newMetadata = snapshot.metadata.copy(
          configuration = snapshot.metadata.configuration +
            (UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableId))
        deltaLog.startTransaction(Some(catalogTable))
          .commit(Seq(newMetadata), DeltaOperations.ManualUpdate)
        currentTableId = tableId
        DeltaLog.clearCache()
        registerUCBackedCatalogOwnedProvider()
    }
  }

  abstract override def afterEach(): Unit = {
    UCBackedDeltaCatalog.currentCoordinator = None
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    DeltaLog.clearCache()
    super.afterEach()
  }

  /**
   * Returns the TBLPROPERTIES SQL fragment required to enable CatalogOwned commits.
   * Concrete suites should append this to their [[extraTableProperties]] override.
   */
  def requiredTableProperties: String =
    s", '${TableFeatureProtocolUtils.propertyKey(CatalogOwnedTableFeature)}' = " +
      s"'${TableFeatureProtocolUtils.FEATURE_PROP_SUPPORTED}'"

  override protected def write(sqlText: String): DataFrame = {
    val result = super.write(sqlText)
    assignUCTableIdIfNeeded(testTableName)
    result
  }

  override protected def readAndVerify(
      table: String, fields: String, orderBy: String, expect: Seq[Row]): Unit = {
    val tableId = currentTableId
    assert(tableId != null,
      s"No table UUID assigned for '$table' - table was not created with CatalogOwned commits")
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
  // No test should go here. Please add tests in [[UniFormE2EIcebergSuiteBase]].
  // CatalogManaged tables block REORG in production.
  override protected def supportsReorgFromV1ToV2: Boolean = false

  override def extraTableProperties(compatVersion: Int): String =
    super.extraTableProperties(compatVersion) + requiredTableProperties
}
