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

package org.apache.spark.sql.delta.coordinatedcommits

import java.util.Optional
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.sql.delta.{CatalogOwnedTableFeature, DeltaConfigs, DeltaLog, DeltaTestUtilsBase}
import org.apache.spark.sql.delta.actions.{CommitInfo, Metadata, Protocol, TableFeatureProtocolUtils}
import org.apache.spark.sql.delta.util.{DeltaCommitFileProvider, JsonUtils}
import io.delta.storage.LogStore
import io.delta.storage.commit.{CommitCoordinatorClient, CommitResponse, GetCommitsResponse => JGetCommitsResponse, TableDescriptor, TableIdentifier, UpdatedActions}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.test.SharedSparkSession

// This trait is built to serve as a base trait for tests built for both CatalogOwned
// and commit-coordinators table feature.
trait CommitCoordinatorUtilBase {
  /**
   * Runs a specific test with commit coordinator feature unset.
   */
  def testWithDefaultCommitCoordinatorUnset(testName: String)(f: => Unit)

  /**
   * Runs the function `f` with commit coordinator table feature unset.
   * Any table created in function `f` have CatalogOwned/CoordinatedCommits disabled by default.
   */
  def withoutDefaultCCTableFeature(f: => Unit): Unit

  /**
   * Runs the function `f` with commit coordinator table feature set.
   * Any table created in function `f` have CatalogOwned/CoordinatedCommits enabled by default.`
   */
  def withDefaultCCTableFeature(f: => Unit): Unit

  /** Run the test with different backfill batch sizes: 1, 2, 10 */
  def testWithDifferentBackfillInterval(testName: String)(f: Int => Unit): Unit

  /** Register a builder to the appropriate builder provider. */
  def registerBuilder(builder: CommitCoordinatorBuilder): Unit

  /** Clear relevant table feature commit coordinator builders that are registered. */
  def clearBuilders(): Unit

  /** Returns the properties string to be used in the table creation for test. */
  def propertiesString: String

  /**
   * Returns true if this test is about CatalogOwned table feature.
   * Returns false if this test is about CoordinatedCommits tabel feature.
   */
  def isCatalogOwnedTest: Boolean
}

trait CatalogOwnedTestBaseSuite
  extends SparkFunSuite
  with DeltaTestUtilsBase
  with CommitCoordinatorUtilBase
  with SharedSparkSession {

  val defaultCatalogOwnedFeatureEnabledKey =
    TableFeatureProtocolUtils.defaultPropertyKey(CatalogOwnedTableFeature)

  // If this config is not overridden, newly created table is not CatalogOwned by default.
  def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = None

  def catalogOwnedDefaultCreationEnabledInTests: Boolean =
    catalogOwnedCoordinatorBackfillBatchSize.nonEmpty

  override protected def sparkConf: SparkConf = {
    if (catalogOwnedDefaultCreationEnabledInTests) {
      super.sparkConf.set(defaultCatalogOwnedFeatureEnabledKey, "supported")
    } else {
      super.sparkConf
    }
  }

  override def clearBuilders(): Unit = {
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
  }

  override def propertiesString: String =
    s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    catalogOwnedCoordinatorBackfillBatchSize.foreach { batchSize =>
      CatalogOwnedCommitCoordinatorProvider.registerBuilder(
        "spark_catalog", TrackingInMemoryCommitCoordinatorBuilder(batchSize = batchSize))
    }
    DeltaLog.clearCache()
  }

  override def testWithDefaultCommitCoordinatorUnset(testName: String)(f: => Unit): Unit = {
    test(testName) {
      withoutDefaultCCTableFeature {
        f
      }
    }
  }

  override def withDefaultCCTableFeature(f: => Unit): Unit = {
    val oldConfig = spark.conf.getOption(defaultCatalogOwnedFeatureEnabledKey)
    spark.conf.set(defaultCatalogOwnedFeatureEnabledKey, "supported")
    try { f } finally {
      if (oldConfig.isDefined) {
        spark.conf.set(defaultCatalogOwnedFeatureEnabledKey, oldConfig.get)
      }
    }
  }

  override def withoutDefaultCCTableFeature(f: => Unit): Unit = {
    val oldConfig = spark.conf.getOption(defaultCatalogOwnedFeatureEnabledKey)
    spark.conf.unset(defaultCatalogOwnedFeatureEnabledKey)
    try { f } finally {
      if (oldConfig.isDefined) {
        spark.conf.set(defaultCatalogOwnedFeatureEnabledKey, oldConfig.get)
      }
    }
  }

  override def testWithDifferentBackfillInterval(testName: String)(f: Int => Unit): Unit = {
    Seq(1, 2, 10).foreach { backfillBatchSize =>
      test(s"$testName [Backfill batch size: $backfillBatchSize]") {
        CatalogOwnedCommitCoordinatorProvider.clearBuilders()
        CatalogOwnedCommitCoordinatorProvider.registerBuilder(
          "spark_catalog", TrackingInMemoryCommitCoordinatorBuilder(batchSize = backfillBatchSize))
        f(backfillBatchSize)
      }
    }
  }

  override def registerBuilder(builder: CommitCoordinatorBuilder): Unit = {
    assert(builder.isInstanceOf[CatalogOwnedCommitCoordinatorBuilder],
      s"builder $builder(${builder.getName}) must be CatalogOwnedCommitCoordinatorBuilder")
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      "spark_catalog", builder.asInstanceOf[CatalogOwnedCommitCoordinatorBuilder])
  }

  override def isCatalogOwnedTest: Boolean = true
}

trait CoordinatedCommitsTestUtils
  extends DeltaTestUtilsBase
  with CommitCoordinatorUtilBase { self: SparkFunSuite with SharedSparkSession =>

  protected val defaultCommitsCoordinatorName = "tracking-in-memory"
  protected val defaultCommitsCoordinatorConf = Map("randomConf" -> "randomConfValue")

  def getCoordinatedCommitsDefaultProperties(withICT: Boolean = false): Map[String, String] = {
    val coordinatedCommitsConfJson = JsonUtils.toJson(defaultCommitsCoordinatorConf)
    val properties = Map(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> defaultCommitsCoordinatorName,
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key -> coordinatedCommitsConfJson,
      DeltaConfigs.COORDINATED_COMMITS_TABLE_CONF.key -> "{}")
    if (withICT) {
      properties + (DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key -> "true")
    } else {
      properties
    }
  }

  override def testWithDefaultCommitCoordinatorUnset(testName: String)(f: => Unit): Unit = {
    test(testName) {
      withoutDefaultCCTableFeature {
        f
      }
    }
  }

  override def withDefaultCCTableFeature(f: => Unit): Unit = {
    val confJson = JsonUtils.toJson(defaultCommitsCoordinatorConf)
    withSQLConf(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
        defaultCommitsCoordinatorName,
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey -> confJson) {
      f
    }
  }

  override def withoutDefaultCCTableFeature(f: => Unit): Unit = {
    val defaultCoordinatedCommitsConfs = CoordinatedCommitsUtils
      .getDefaultCCConfigurations(spark, withDefaultKey = true)
    defaultCoordinatedCommitsConfs.foreach { case (defaultKey, _) =>
      spark.conf.unset(defaultKey)
    }
    try { f } finally {
      defaultCoordinatedCommitsConfs.foreach { case (defaultKey, oldValue) =>
        spark.conf.set(defaultKey, oldValue)
      }
    }
  }

  def withCustomCoordinatedCommitsTableProperties(
      commitCoordinatorName: String,
      conf: Map[String, String] = Map("randomConf" -> "randomConfValue"))(f: => Unit): Unit = {
    withSQLConf(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
        commitCoordinatorName,
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey ->
        JsonUtils.toJson(conf)) {
      f
    }
  }

  override def testWithDifferentBackfillInterval(testName: String)(f: Int => Unit): Unit = {
    Seq(1, 2, 10).foreach { backfillBatchSize =>
      test(s"$testName [Backfill batch size: $backfillBatchSize]") {
        CommitCoordinatorProvider.clearNonDefaultBuilders()
        CommitCoordinatorProvider.registerBuilder(
          TrackingInMemoryCommitCoordinatorBuilder(backfillBatchSize))
        CommitCoordinatorProvider.registerBuilder(
          InMemoryCommitCoordinatorBuilder(backfillBatchSize))
        f(backfillBatchSize)
      }
    }
  }

  override def registerBuilder(builder: CommitCoordinatorBuilder): Unit = {
    CommitCoordinatorProvider.registerBuilder(builder)
  }

  override def clearBuilders(): Unit = {
    CommitCoordinatorProvider.clearNonDefaultBuilders()
  }

  override def propertiesString: String = {
    val coordinatedCommitsConfJson = JsonUtils.toJson(defaultCommitsCoordinatorConf)
    s"('${DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key}' =" +
      s"'$defaultCommitsCoordinatorName', " +
      s"'${DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.key}' = '$coordinatedCommitsConfJson')"
  }

  override def isCatalogOwnedTest: Boolean = false

  /**
   * Run the test against a [[TrackingCommitCoordinatorClient]] with backfill batch size =
   * `batchBackfillSize`
   */
  def testWithCoordinatedCommits(backfillBatchSize: Int)(testName: String)(f: => Unit): Unit = {
    test(s"$testName [Backfill batch size: $backfillBatchSize]") {
      CommitCoordinatorProvider.clearNonDefaultBuilders()
      CommitCoordinatorProvider.registerBuilder(
        TrackingInMemoryCommitCoordinatorBuilder(backfillBatchSize))
      withDefaultCCTableFeature {
        f
      }
    }
  }

  /** Run the test with:
   * 1. Without coordinated-commits
   * 2. With coordinated-commits with different backfill batch sizes
   */
  def testWithDifferentBackfillIntervalOptional(testName: String)(f: Option[Int] => Unit): Unit = {
    test(s"$testName [Backfill batch size: None]") {
      f(None)
    }
    testWithDifferentBackfillInterval(testName) { backfillBatchSize =>
      val coordinatedCommitsCoordinatorJson = JsonUtils.toJson(defaultCommitsCoordinatorConf)
      withSQLConf(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey ->
            defaultCommitsCoordinatorName,
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey ->
            coordinatedCommitsCoordinatorJson) {
        f(Some(backfillBatchSize))
      }
    }
  }

  def getUpdatedActionsForZerothCommit(
      commitInfo: CommitInfo,
      oldMetadata: Metadata = Metadata()): UpdatedActions = {
    val newMetadataConfiguration =
      oldMetadata.configuration +
        (DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.key -> defaultCommitsCoordinatorName)
    val newMetadata = oldMetadata.copy(configuration = newMetadataConfiguration)
    new UpdatedActions(commitInfo, newMetadata, Protocol(), oldMetadata, Protocol())
  }

  def getUpdatedActionsForNonZerothCommit(commitInfo: CommitInfo): UpdatedActions = {
    val updatedActions = getUpdatedActionsForZerothCommit(commitInfo)
    new UpdatedActions(
      updatedActions.getCommitInfo,
      updatedActions.getNewMetadata,
      updatedActions.getNewProtocol,
      updatedActions.getNewMetadata,
      updatedActions.getOldProtocol
    )
  }

}

case class TrackingInMemoryCommitCoordinatorBuilder(
    batchSize: Long,
    defaultCommitCoordinatorClientOpt: Option[CommitCoordinatorClient] = None,
    defaultCommitCoordinatorName: String = "tracking-in-memory")
  extends CatalogOwnedCommitCoordinatorBuilder {
  lazy val trackingInMemoryCommitCoordinatorClient =
    defaultCommitCoordinatorClientOpt.getOrElse {
      new TrackingCommitCoordinatorClient(
        new PredictableUuidInMemoryCommitCoordinatorClient(batchSize))
    }

  override def getName: String = defaultCommitCoordinatorName
  override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
    trackingInMemoryCommitCoordinatorClient
  }

  override def buildForCatalog(
      spark: SparkSession, catalogName: String): CommitCoordinatorClient = {
    trackingInMemoryCommitCoordinatorClient
  }
}

case class TrackingGenericInMemoryCommitCoordinatorBuilder(
    builderName: String, realBuilder: CommitCoordinatorBuilder)
  extends CommitCoordinatorBuilder {
  override def getName: String = builderName

  override def build(spark: SparkSession, conf: Map[String, String]): CommitCoordinatorClient = {
    new TrackingCommitCoordinatorClient(realBuilder.build(spark, conf))
  }
}

class PredictableUuidInMemoryCommitCoordinatorClient(batchSize: Long)
  extends InMemoryCommitCoordinator(batchSize) {

  var nextUuidSuffix = 1L
  override def generateUUID(): String = {
    nextUuidSuffix += 1
    s"uuid-${nextUuidSuffix - 1}"
  }
}

object TrackingCommitCoordinatorClient {
  private val insideOperation = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
}

class TrackingCommitCoordinatorClient(
    val delegatingCommitCoordinatorClient: CommitCoordinatorClient)
  extends CommitCoordinatorClient {

  val numCommitsCalled = new AtomicInteger(0)
  val numGetCommitsCalled = new AtomicInteger(0)
  val numBackfillToVersionCalled = new AtomicInteger(0)
  val numRegisterTableCalled = new AtomicInteger(0)

  def recordOperation[T](op: String)(f: => T): T = {
    val oldInsideOperation = TrackingCommitCoordinatorClient.insideOperation.get()
    try {
      if (!TrackingCommitCoordinatorClient.insideOperation.get()) {
        op match {
          case "commit" => numCommitsCalled.incrementAndGet()
          case "getCommits" => numGetCommitsCalled.incrementAndGet()
          case "backfillToVersion" => numBackfillToVersionCalled.incrementAndGet()
          case "registerTable" => numRegisterTableCalled.incrementAndGet()
          case _ => ()
        }
      }
      TrackingCommitCoordinatorClient.insideOperation.set(true)
      f
    } finally {
      TrackingCommitCoordinatorClient.insideOperation.set(oldInsideOperation)
    }
  }

  override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      tableDesc: TableDescriptor,
      commitVersion: Long,
      actions: java.util.Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = recordOperation("commit") {
    delegatingCommitCoordinatorClient.commit(
      logStore,
      hadoopConf,
      tableDesc,
      commitVersion,
      actions,
      updatedActions)
  }

  override def getCommits(
      tableDesc: TableDescriptor,
      startVersion: java.lang.Long,
      endVersion: java.lang.Long): JGetCommitsResponse = recordOperation("getCommits") {
    delegatingCommitCoordinatorClient.getCommits(tableDesc, startVersion, endVersion)
  }

  override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      tableDesc: TableDescriptor,
      version: Long,
      lastKnownBackfilledVersion: java.lang.Long): Unit = recordOperation("backfillToVersion") {
    delegatingCommitCoordinatorClient.backfillToVersion(
      logStore,
      hadoopConf,
      tableDesc,
      version,
      lastKnownBackfilledVersion)
  }

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = {
    other match {
      case otherTracking: TrackingCommitCoordinatorClient =>
        delegatingCommitCoordinatorClient.semanticEquals(
          otherTracking.delegatingCommitCoordinatorClient)
      case _ =>
        delegatingCommitCoordinatorClient.semanticEquals(other)
    }
  }

  def reset(): Unit = {
    numCommitsCalled.set(0)
    numGetCommitsCalled.set(0)
    numBackfillToVersionCalled.set(0)
  }

  override def registerTable(
      logPath: Path,
      tableIdentifier: Optional[TableIdentifier],
      currentVersion: Long,
      currentMetadata: AbstractMetadata,
      currentProtocol: AbstractProtocol): java.util.Map[String, String] =
    recordOperation("registerTable") {
      delegatingCommitCoordinatorClient.registerTable(
        logPath, tableIdentifier, currentVersion, currentMetadata, currentProtocol)
    }
}

/**
 * A helper class which enables coordinated-commits for the test suite based on the given
 * `coordinatedCommitsBackfillBatchSize` conf.
 */
trait CoordinatedCommitsBaseSuite
  extends SparkFunSuite
  with SharedSparkSession
  with CoordinatedCommitsTestUtils {

  // If this config is not overridden, coordinated commits are disabled.
  def coordinatedCommitsBackfillBatchSize: Option[Int] = None

  final def coordinatedCommitsEnabledInTests: Boolean = coordinatedCommitsBackfillBatchSize.nonEmpty

  // Keeps track of the number of table names pointing to the location.
  protected val locRefCount: mutable.Map[String, Int] = mutable.Map.empty

  // In case some tests reuse the table path/name with DROP table, this method can be used to
  // clean the table data in the commit coordinator. Note that we should call this before
  // the table actually gets DROP.
  def deleteTableFromCommitCoordinator(tableName: String): Unit = {
    val location = try {
      spark.sql(s"describe detail $tableName")
        .select("location")
        .first()
        .getAs[String](0)
    } catch {
      case NonFatal(_) =>
        // Ignore if the table does not exist/broken.
        return
    }
    deleteTableFromCommitCoordinator(new Path(location))
  }

  def deleteTableFromCommitCoordinator(path: Path): Unit = {
    val cc = CommitCoordinatorProvider.getCommitCoordinatorClient(
      defaultCommitsCoordinatorName, defaultCommitsCoordinatorConf, spark)
    assert(
      cc.isInstanceOf[TrackingCommitCoordinatorClient],
      s"Please implement delete/drop method for coordinator: ${cc.getClass.getName}")

    val locKey = path.toString.stripPrefix("file:")
    if (locRefCount.contains(locKey)) {
      locRefCount(locKey) -= 1
    }
    // When we create an external table in a location where some table already existed, two table
    // names could be pointing to the same location. We should only clean up the table data in the
    // commit coordinator when the last table name pointing to the location is dropped.
    if (locRefCount.getOrElse(locKey, 0) == 0) {
      val logPath = new Path(path, "_delta_log")
      cc.asInstanceOf[TrackingCommitCoordinatorClient]
        .delegatingCommitCoordinatorClient
        .asInstanceOf[InMemoryCommitCoordinator]
        .dropTable(logPath)
    }
    DeltaLog.clearCache()
  }

  override protected def sparkConf: SparkConf = {
    if (coordinatedCommitsBackfillBatchSize.nonEmpty) {
      val coordinatedCommitsCoordinatorJson = JsonUtils.toJson(defaultCommitsCoordinatorConf)
      super.sparkConf
        .set(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey,
          defaultCommitsCoordinatorName)
        .set(
          DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_CONF.defaultTablePropertyKey,
          coordinatedCommitsCoordinatorJson)
    } else {
      super.sparkConf
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitCoordinatorProvider.clearNonDefaultBuilders()
    coordinatedCommitsBackfillBatchSize.foreach { batchSize =>
      CommitCoordinatorProvider.registerBuilder(TrackingInMemoryCommitCoordinatorBuilder(batchSize))
    }
    DeltaLog.clearCache()
  }

  protected def isICTEnabledForNewTables: Boolean = {
    spark.conf.getOption(
      DeltaConfigs.COORDINATED_COMMITS_COORDINATOR_NAME.defaultTablePropertyKey).nonEmpty ||
      spark.conf.getOption(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey).contains("true")
  }
}
