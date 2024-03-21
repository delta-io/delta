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

import scala.collection.mutable

import org.apache.spark.sql.delta.DeltaConfigs.MANAGED_COMMIT_OWNER_CONF
import org.apache.spark.sql.delta.DeltaTestUtilsBase
import org.apache.spark.sql.delta.storage.LogStore
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

trait ManagedCommitTestUtils
  extends DeltaTestUtilsBase { self: SparkFunSuite with SharedSparkSession =>

  def testWithDifferentBackfillInterval(testName: String)(f: Int => Unit): Unit = {
    Seq(0, 2, 10).foreach { backfillBatchSize =>
      test(s"$testName [Backfill batch size: $backfillBatchSize]") {
        CommitStoreProvider.clearNonDefaultBuilders()
        CommitStoreProvider.registerBuilder(TrackingInMemoryCommitStoreBuilder(backfillBatchSize))
        CommitStoreProvider.registerBuilder(InMemoryCommitStoreBuilder(backfillBatchSize))
        f(backfillBatchSize)
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
