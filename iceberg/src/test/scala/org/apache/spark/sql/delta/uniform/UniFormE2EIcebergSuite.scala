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

import com.databricks.spark.util.Log4jUsageLogger
import io.delta.storage.commit.{CommitCoordinatorClient => JCommitCoordinatorClient}
import io.delta.storage.commit.{TableIdentifier => UCTableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkSessionSwitch}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog}
import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, DeltaTestUtils, IcebergConstants}
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
 * A [[SessionCatalog]] wrapper that overrides [[getTableMetadata]] to inject fresh Iceberg
 * metadata from [[UCEnrichedSessionCatalog.currentCoordinator]] on every call. This mirrors what
 * [[UCDeltaCatalogClientImpl.toV1Table]] does in production. Installed via reflection into
 * both [[org.apache.spark.sql.internal.SessionState.catalog]] (for the V1 path used by
 * [[org.apache.spark.sql.delta.icebergShaded.IcebergConverter.refreshCatalogTableIfNeeded]])
 * and the [[org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog]] delegate (for
 * the V2 write path that goes through [[DeltaCatalog.loadTable]]). Both injections happen in
 * [[WriteDeltaUCCCReadIceberg.beforeEach]].
 */
private class UCEnrichedSessionCatalog(
    spark: SparkSession,
    delegate: SessionCatalog,
    fr: FunctionRegistry,
    tfr: TableFunctionRegistry)
  extends SessionCatalog(delegate.externalCatalog, fr, tfr) {

  setCurrentDatabase(delegate.getCurrentDatabase)

  override def getTableMetadata(name: TableIdentifier): CatalogTable = {
    val base = delegate.getTableMetadata(name)
    val forbiddenKeys = base.properties.keys.filter(_.startsWith("deltaUniformIceberg."))
    assert(forbiddenKeys.isEmpty,
      s"deltaUniformIceberg.* keys must only appear in storage.properties, " +
        s"never in catalogTable.properties. Found: ${forbiddenKeys.mkString(", ")}")
    UCEnrichedSessionCatalog.currentCoordinator.flatMap { coordinator =>
      scala.util.Try {
        val deltaLog = DeltaLog.forTable(spark, new Path(base.location))
        val tableConf = deltaLog.unsafeVolatileSnapshot.metadata.coordinatedCommitsTableConf
        tableConf.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY).flatMap { tableId =>
          coordinator.getUniformMetadata(tableId)
            .filter(_.getIcebergMetadata.isPresent)
            .map { meta =>
              val icebergMeta = meta.getIcebergMetadata.get
              base.copy(storage = base.storage.copy(
                properties = base.storage.properties +
                  (IcebergConstants.CATALOG_TABLE_ICEBERG_METADATA_LOCATION_PROP ->
                    icebergMeta.getMetadataLocation) +
                  (IcebergConstants.CATALOG_TABLE_ICEBERG_CONVERTED_DELTA_VERSION_PROP ->
                    icebergMeta.getConvertedDeltaVersion.toString)
              ))
            }
        }
      }.toOption.flatten
    }.getOrElse(base)
  }
}

private object UCEnrichedSessionCatalog {
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
      classOf[DeltaCatalog].getName)

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
  private var origSessionCatalog: SessionCatalog = _
  private var origV2Catalog: SessionCatalog = _

  abstract override def beforeEach(): Unit = {
    super.beforeEach()
    DeltaLog.clearCache()
    CommitCoordinatorProvider.clearAllBuilders()
    ucCommitCoordinator = new InMemoryUCCommitCoordinator()
    UCEnrichedSessionCatalog.currentCoordinator = Some(ucCommitCoordinator)
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

    // Inject UCEnrichedSessionCatalog into two places so all catalog reads:
    // whether from the
    // V1 path (refreshCatalogTableIfNeeded -> spark.sessionState.catalog.getTableMetadata) or
    // V2 write path (DeltaCatalog.loadTable -> V2SessionCatalog.catalog.getTableMetadata)
    // see up-to-date coordinator Iceberg metadata,
    // mirroring what toV1Table does in production for UCDeltaCatalogClientImpl
    origSessionCatalog = spark.sessionState.catalog  // force lazy init of the field
    val fr = reflectField(origSessionCatalog, "functionRegistry")
      .get(origSessionCatalog).asInstanceOf[FunctionRegistry]
    val tfr = reflectField(origSessionCatalog, "tableFunctionRegistry")
      .get(origSessionCatalog).asInstanceOf[TableFunctionRegistry]
    val enriched = new UCEnrichedSessionCatalog(spark, origSessionCatalog, fr, tfr)

    // V1 path: spark.sessionState.catalog (used by refreshCatalogTableIfNeeded)
    reflectField(spark.sessionState, "catalog").set(spark.sessionState, enriched)

    // V2 path: V2SessionCatalog holds a direct reference to SessionCatalog captured at
    // session construction; it is unaffected by the above swap, so replace it separately.
    val v2Cat = spark.sessionState.catalogManager.catalog("spark_catalog")
    val v2Delegate = reflectField(v2Cat, "delegate").get(v2Cat).asInstanceOf[AnyRef]
    val v2CatalogField = reflectField(v2Delegate, "catalog")
    origV2Catalog = v2CatalogField.get(v2Delegate).asInstanceOf[SessionCatalog]
    v2CatalogField.set(v2Delegate, enriched)
  }

  abstract override def afterEach(): Unit = {
    if (origV2Catalog != null) {
      val v2Cat = spark.sessionState.catalogManager.catalog("spark_catalog")
      val v2Delegate = reflectField(v2Cat, "delegate").get(v2Cat).asInstanceOf[AnyRef]
      reflectField(v2Delegate, "catalog").set(v2Delegate, origV2Catalog)
      origV2Catalog = null
    }
    if (origSessionCatalog != null) {
      reflectField(spark.sessionState, "catalog").set(spark.sessionState, origSessionCatalog)
      origSessionCatalog = null
    }
    UCEnrichedSessionCatalog.currentCoordinator = None
    CommitCoordinatorProvider.clearAllBuilders()
    DeltaLog.clearCache()
    super.afterEach()
  }

  private def reflectField(obj: AnyRef, name: String): java.lang.reflect.Field = {
    var cls: Class[_] = obj.getClass
    while (cls != null) {
      try {
        val f = cls.getDeclaredField(name)
        f.setAccessible(true)
        return f
      } catch {
        case _: NoSuchFieldException => cls = cls.getSuperclass
      }
    }
    throw new NoSuchFieldException(s"Field '$name' not found in ${obj.getClass.getName}")
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
  // Tests that don't require CC infrastructure should be added in [[UniFormE2EIcebergSuiteBase]].
  // Tests that specifically exercise the UC commit coordinator path may be added here.
  override def extraTableProperties(compatVersion: Int): String =
    super.extraTableProperties(compatVersion) + requiredTableProperties

  test("conflict resolution refreshes catalogTable with fresh uniform metadata " +
    "for incremental Iceberg conversion") {
    val tableName = "test_cc_conflict_iceberg_refresh"
    withTable(tableName) {
      // Create a CC + UniForm table.
      write(
        s"""CREATE TABLE $tableName (id INT) USING DELTA
           |TBLPROPERTIES (
           |  'delta.columnMapping.mode' = 'name',
           |  'delta.enableIcebergCompatV2' = 'true',
           |  'delta.universalFormat.enabledFormats' = 'iceberg'
           |  $requiredTableProperties
           |)""".stripMargin)

      // v1: insert row 1 - triggers atomic Iceberg conversion at v1.
      write(s"INSERT INTO $tableName VALUES (1)")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
      val v1Snapshot = deltaLog.update()
      val v1CatalogTable =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))

      // Start a transaction with v1 snapshot and v1 Iceberg catalog.
      val txn = deltaLog.startTransaction(Some(v1CatalogTable), Some(v1Snapshot))

      // v2: insert row 2 - the "winning" commit that the stale txn will conflict with.
      // Now v1 snapshot and v1 Iceberg catalog becomes stale so txn's commit would hit
      // conflict.
      write(s"INSERT INTO $tableName VALUES (2)")

      // Try to commit the transaction as v2, hit a conflict, then retry as v3.
      // During conflict resolution, getCommits returns v2's UniformMetadata, which
      // refreshes catalogTable to convertedDeltaVersion=2. The retry commit then converts
      // only v3 incrementally (fromVersion=3, toVersion=3) rather than from stale v1.
      val events = Log4jUsageLogger.track {
        txn.commit(Seq.empty, DeltaOperations.ManualUpdate)
      }

      // Latest version is now 3. There are two deltaCommitRange events: one from the failed
      // first attempt (v2, using stale v1 base) and one from the successful retry (v3).
      // The retry must use the refreshed catalog (convertedDeltaVersion=2 from v2's
      // UniformMetadata), so conversion covers only v3 (fromVersion=3, toVersion=3).
      // Without the fix, the retry would still use the stale v1 base and fromVersion would be 2.
      val latestVersion = deltaLog.update().version
      val rangeEvents = DeltaTestUtils.filterUsageRecords(
        events, "delta.iceberg.conversion.deltaCommitRange")
      assert(rangeEvents.size == 2,
        s"Expected 2 deltaCommitRange events (failed attempt + retry), got ${rangeEvents.size}")
      val retryEventData = JsonUtils.fromJson[Map[String, Any]](rangeEvents.last.blob)
      // Jackson deserializes small JSON integers as Int, but latestVersion is Long;
      // use Number.longValue for a type-safe comparison.
      assert(retryEventData("fromVersion").asInstanceOf[Number].longValue === latestVersion,
        s"Expected fromVersion=$latestVersion (fresh v2 base), " +
          s"got ${retryEventData("fromVersion")} (stale v1 base would give ${latestVersion - 1})")
      assert(retryEventData("toVersion").asInstanceOf[Number].longValue === latestVersion,
        s"Expected toVersion=$latestVersion")
    }
  }
}
