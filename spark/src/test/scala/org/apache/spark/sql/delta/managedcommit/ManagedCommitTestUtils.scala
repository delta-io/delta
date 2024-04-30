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

package org.apache.spark.sql.delta.managedcommit

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaLog, DeltaTestUtilsBase}
import org.apache.spark.sql.delta.DeltaConfigs.MANAGED_COMMIT_OWNER_NAME
import org.apache.spark.sql.delta.actions.{Action, CommitInfo, Metadata, Protocol}
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.test.SharedSparkSession

trait ManagedCommitTestUtils
  extends DeltaTestUtilsBase { self: SparkFunSuite with SharedSparkSession =>

  /**
   * Runs a specific test with managed commits default properties unset.
   * Any table created in this test won't have managed commits enabled by default.
   */
  def testWithDefaultCommitOwnerUnset(testName: String)(f: => Unit): Unit = {
    test(testName) {
      withoutManagedCommitsDefaultTableProperties {
        f
      }
    }
  }

  /**
   * Runs the function `f` with managed commits default properties unset.
   * Any table created in function `f`` won't have managed commits enabled by default.
   */
  def withoutManagedCommitsDefaultTableProperties(f: => Unit): Unit = {
    val commitOwnerKey = MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey
    val oldCommitOwnerValue = spark.conf.getOption(commitOwnerKey)
    spark.conf.unset(commitOwnerKey)
    try { f } finally {
      oldCommitOwnerValue.foreach {
        spark.conf.set(commitOwnerKey, _)
      }
    }
  }

  /** Run the test with different backfill batch sizes: 1, 2, 10 */
  def testWithDifferentBackfillInterval(testName: String)(f: Int => Unit): Unit = {
    Seq(1, 2, 10).foreach { backfillBatchSize =>
      test(s"$testName [Backfill batch size: $backfillBatchSize]") {
        CommitOwnerProvider.clearNonDefaultBuilders()
        CommitOwnerProvider.registerBuilder(TrackingInMemoryCommitOwnerBuilder(backfillBatchSize))
        CommitOwnerProvider.registerBuilder(InMemoryCommitOwnerBuilder(backfillBatchSize))
        f(backfillBatchSize)
      }
    }
  }

  /** Run the test with:
   * 1. Without managed-commits
   * 2. With managed-commits with different backfill batch sizes
   */
  def testWithDifferentBackfillIntervalOptional(testName: String)(f: Option[Int] => Unit): Unit = {
    test(s"$testName [Backfill batch size: None]") {
      f(None)
    }
    val managedCommitOwnerConf = Map("randomConf" -> "randomConfValue")
    val managedCommitOwnerJson = JsonUtils.toJson(managedCommitOwnerConf)
    withSQLConf(
        DeltaConfigs.MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey -> "in-memory",
        DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.defaultTablePropertyKey -> managedCommitOwnerJson) {
      testWithDifferentBackfillInterval(testName) { backfillBatchSize =>
        f(Some(backfillBatchSize))
      }
    }
  }

  def getUpdatedActionsForZerothCommit(
      commitInfo: CommitInfo,
      oldMetadata: Metadata = Metadata()): UpdatedActions = {
    val newMetadataConfiguration =
      oldMetadata.configuration +
        (DeltaConfigs.MANAGED_COMMIT_OWNER_NAME.key -> "tracking-in-memory")
    val newMetadata = oldMetadata.copy(configuration = newMetadataConfiguration)
    UpdatedActions(commitInfo, newMetadata, Protocol(), oldMetadata, Protocol())
  }

  def getUpdatedActionsForNonZerothCommit(commitInfo: CommitInfo): UpdatedActions = {
    val updatedActions = getUpdatedActionsForZerothCommit(commitInfo)
    updatedActions.copy(oldMetadata = updatedActions.newMetadata)
  }
}

case class TrackingInMemoryCommitOwnerBuilder(
    batchSize: Long,
    defaultCommitOwnerClientOpt: Option[CommitOwnerClient] = None) extends CommitOwnerBuilder {
  lazy val trackingInMemoryCommitOwnerClient =
    defaultCommitOwnerClientOpt.getOrElse {
      new TrackingCommitOwnerClient(new PredictableUuidInMemoryCommitOwnerClient(batchSize))
    }

  override def name: String = "tracking-in-memory"
  override def build(conf: Map[String, String]): CommitOwnerClient = {
    trackingInMemoryCommitOwnerClient
  }
}

class PredictableUuidInMemoryCommitOwnerClient(batchSize: Long)
  extends InMemoryCommitOwner(batchSize) {

  var nextUuidSuffix = 1L
  override def generateUUID(): String = {
    nextUuidSuffix += 1
    s"uuid-${nextUuidSuffix - 1}"
  }
}

class TrackingCommitOwnerClient(delegatingCommitOwnerClient: InMemoryCommitOwner)
  extends CommitOwnerClient {

  var numCommitsCalled: Int = 0
  var numGetCommitsCalled: Int = 0
  var numBackfillToVersionCalled: Int = 0
  var numRegisterTableCalled: Int = 0
  var insideOperation: Boolean = false

  def recordOperation[T](op: String)(f: => T): T = synchronized {
    val oldInsideOperation = insideOperation
    try {
      if (!insideOperation) {
        op match {
          case "commit" => numCommitsCalled += 1
          case "getCommits" => numGetCommitsCalled += 1
          case "backfillToVersion" => numBackfillToVersionCalled += 1
          case "registerTable" => numRegisterTableCalled += 1
          case _ => ()
        }
      }
      insideOperation = true
      f
    } finally {
      insideOperation = oldInsideOperation
    }
  }

  override def commit(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = recordOperation("commit") {
    delegatingCommitOwnerClient.commit(
      logStore, hadoopConf, logPath, managedCommitTableConf, commitVersion, actions, updatedActions)
  }

  override def getCommits(
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      startVersion: Long,
      endVersion: Option[Long] = None): GetCommitsResponse = recordOperation("getCommits") {
    delegatingCommitOwnerClient.getCommits(
      logPath, managedCommitTableConf, startVersion, endVersion)
  }

  override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      managedCommitTableConf: Map[String, String],
      startVersion: Long,
      endVersion: Option[Long]): Unit = recordOperation("backfillToVersion") {
    delegatingCommitOwnerClient.backfillToVersion(
      logStore, hadoopConf, logPath, managedCommitTableConf, startVersion, endVersion)
  }

  override def semanticEquals(other: CommitOwnerClient): Boolean = this == other

  def reset(): Unit = {
    numCommitsCalled = 0
    numGetCommitsCalled = 0
    numBackfillToVersionCalled = 0
  }

  override def registerTable(
      logPath: Path,
      currentVersion: Long,
      currentMetadata: Metadata,
      currentProtocol: Protocol): Map[String, String] = recordOperation("registerTable") {
    delegatingCommitOwnerClient.registerTable(
      logPath, currentVersion, currentMetadata, currentProtocol)
  }
}

/**
 * A helper class which enables managed-commit for the test suite based on the given
 * `managedCommitBackfillBatchSize` conf.
 */
trait ManagedCommitBaseSuite extends SparkFunSuite with SharedSparkSession {

  // If this config is not overridden, managed commits are disabled.
  def managedCommitBackfillBatchSize: Option[Int] = None

  final def managedCommitsEnabledInTests: Boolean = managedCommitBackfillBatchSize.nonEmpty

  override protected def sparkConf: SparkConf = {
    if (managedCommitBackfillBatchSize.nonEmpty) {
      val managedCommitOwnerConf = Map("randomConf" -> "randomConfValue")
      val managedCommitOwnerJson = JsonUtils.toJson(managedCommitOwnerConf)
      super.sparkConf
        .set(DeltaConfigs.MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey, "tracking-in-memory")
        .set(DeltaConfigs.MANAGED_COMMIT_OWNER_CONF.defaultTablePropertyKey, managedCommitOwnerJson)
    } else {
      super.sparkConf
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    CommitOwnerProvider.clearNonDefaultBuilders()
    managedCommitBackfillBatchSize.foreach { batchSize =>
      CommitOwnerProvider.registerBuilder(TrackingInMemoryCommitOwnerBuilder(batchSize))
    }
  }

  protected def isICTEnabledForNewTables: Boolean = {
    spark.conf.getOption(DeltaConfigs.MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey).nonEmpty ||
      spark.conf.getOption(
        DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey).contains("true")
  }
}
