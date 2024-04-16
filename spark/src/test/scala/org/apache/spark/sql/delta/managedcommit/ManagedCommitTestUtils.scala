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

import org.apache.spark.sql.delta.{DeltaConfigs, DeltaTestUtilsBase}
import org.apache.spark.sql.delta.DeltaConfigs.MANAGED_COMMIT_OWNER_NAME
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.test.SharedSparkSession

trait ManagedCommitTestUtils
  extends DeltaTestUtilsBase { self: SparkFunSuite with SharedSparkSession =>

  def testWithoutManagedCommits(testName: String)(f: => Unit): Unit = {
    test(testName) {
      val oldCommitOwnerValue = spark.conf.get(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey)
      try {
        spark.conf.unset(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey)
        f
      } finally {
        spark.conf.set(MANAGED_COMMIT_OWNER_NAME.defaultTablePropertyKey, oldCommitOwnerValue)
      }
    }
  }

  /** Run the test with different backfill batch sizes: 1, 2, 10 */
  def testWithDifferentBackfillInterval(testName: String)(f: Int => Unit): Unit = {
    Seq(1, 2, 10).foreach { backfillBatchSize =>
      test(s"$testName [Backfill batch size: $backfillBatchSize]") {
        CommitStoreProvider.clearNonDefaultBuilders()
        CommitStoreProvider.registerBuilder(TrackingInMemoryCommitStoreBuilder(backfillBatchSize))
        CommitStoreProvider.registerBuilder(InMemoryCommitStoreBuilder(backfillBatchSize))
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
}

case class TrackingInMemoryCommitStoreBuilder(
    batchSize: Long,
    defaultCommitStoreOpt: Option[CommitStore] = None) extends CommitStoreBuilder {
  private lazy val trackingInMemoryCommitStore =
    defaultCommitStoreOpt.getOrElse {
      new TrackingCommitStore(new PredictableUuidInMemoryCommitStore(batchSize))
    }

  override def name: String = "tracking-in-memory"
  override def build(conf: Map[String, String]): CommitStore = trackingInMemoryCommitStore
}

class PredictableUuidInMemoryCommitStore(batchSize: Long) extends InMemoryCommitStore(batchSize) {
  var nextUuidSuffix = 0L
  override def generateUUID(): String = {
    nextUuidSuffix += 1
    s"uuid-${nextUuidSuffix - 1}"
  }
}

class TrackingCommitStore(delegatingCommitStore: InMemoryCommitStore) extends CommitStore {

  var numCommitsCalled: Int = 0
  var numGetCommitsCalled: Int = 0
  var insideOperation: Boolean = false

  def recordOperation[T](op: String)(f: => T): T = synchronized {
    val oldInsideOperation = insideOperation
    try {
      if (!insideOperation) {
        if (op == "commit") {
          numCommitsCalled += 1
        } else if (op == "getCommits") {
          numGetCommitsCalled += 1
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
      commitVersion: Long,
      actions: Iterator[String],
      updatedActions: UpdatedActions): CommitResponse = recordOperation("commit") {
    delegatingCommitStore
      .commit(logStore, hadoopConf, logPath, commitVersion, actions, updatedActions)
  }

  override def getCommits(
      logPath: Path,
      startVersion: Long,
      endVersion: Option[Long] = None): GetCommitsResponse = recordOperation("getCommits") {
    delegatingCommitStore.getCommits(logPath, startVersion, endVersion)
  }

  override def backfillToVersion(
      logStore: LogStore,
      hadoopConf: Configuration,
      logPath: Path,
      startVersion: Long,
      endVersion: Option[Long]): Unit = {
    delegatingCommitStore.backfillToVersion(logStore, hadoopConf, logPath, startVersion, endVersion)
  }

  override def semanticEquals(other: CommitStore): Boolean = this == other

  def registerTable(
      logPath: Path,
      maxCommitVersion: Long): Unit = {
    delegatingCommitStore.registerTable(logPath, maxCommitVersion)
  }

  def reset(): Unit = {
    numCommitsCalled = 0
    numGetCommitsCalled = 0
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
    CommitStoreProvider.clearNonDefaultBuilders()
    managedCommitBackfillBatchSize.foreach { batchSize =>
      CommitStoreProvider.registerBuilder(TrackingInMemoryCommitStoreBuilder(batchSize))
    }
  }
}
