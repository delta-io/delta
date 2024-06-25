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
package io.delta.kernel.defaults.internal.coordinatedcommits

import java.{lang, util}
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.actions.{CommitInfo, Metadata, Protocol}
import io.delta.kernel.internal.TableConfig
import io.delta.storage.commit.{Commit, CommitCoordinatorClient, CommitResponse, GetCommitsResponse, UpdatedActions}
import io.delta.storage.LogStore
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util.concurrent.atomic.AtomicInteger
import java.util.Collections
import scala.collection.JavaConverters._

trait CoordinatedCommitsTestUtils {

  val hadoopConf = new Configuration()
  def commit(
    logPath: Path,
    tableConf: util.Map[String, String],
    version: Long,
    timestamp: Long,
    commit: util.List[String],
    commitCoordinatorClient: CommitCoordinatorClient): Commit = {
    val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)
    val updatedCommitInfo = CommitInfo.empty().withTimestamp(timestamp)
    val updatedActions = if (version == 0) {
      getUpdatedActionsForZerothCommit(updatedCommitInfo)
    } else {
      getUpdatedActionsForNonZerothCommit(updatedCommitInfo)
    }
    commitCoordinatorClient.commit(
      logStore,
      hadoopConf,
      logPath,
      tableConf,
      version,
      commit.iterator(),
      updatedActions).getCommit
  }

  def writeCommitZero(engine: Engine, logPath: Path, commit: util.List[String]): Unit = {
    createLogPath(engine, logPath)
    val logStore = LogStoreProvider.getLogStore(hadoopConf, logPath.toUri.getScheme)
    logStore.write(
      CoordinatedCommitsUtils.getHadoopDeltaFile(logPath, 0),
      commit.iterator(),
      true,
      hadoopConf)
  }

  def createLogPath(engine: Engine, logPath: Path): Unit = {
    // New table, create a delta log directory
    if (!engine.getFileSystemClient.mkdirs(logPath.toString)) {
      throw new RuntimeException("Failed to create delta log directory: " + logPath)
    }
  }

  def getUpdatedActionsForZerothCommit(
    commitInfo: CommitInfo,
    oldMetadata: Metadata = Metadata.empty()): UpdatedActions = {
    val newMetadataConfiguration =
      Map(TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
    val newMetadata = oldMetadata.withNewConfiguration(newMetadataConfiguration.asJava)
    new UpdatedActions(
      CoordinatedCommitsUtils.convertCommitInfoToAbstractCommitInfo(commitInfo),
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(newMetadata),
      CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(Protocol.empty()),
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(oldMetadata),
      CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(Protocol.empty()))
  }

  def getUpdatedActionsForNonZerothCommit(commitInfo: CommitInfo): UpdatedActions = {
    val updatedActions = getUpdatedActionsForZerothCommit(commitInfo)
    new UpdatedActions(
      updatedActions.getCommitInfo,
      updatedActions.getNewMetadata,
      updatedActions.getNewProtocol,
      updatedActions.getNewMetadata, // oldMetadata is replaced with newMetadata
      updatedActions.getOldProtocol)
  }
}

case class TrackingInMemoryCommitCoordinatorBuilder(
  batchSize: Long,
  defaultCommitCoordinatorClientOpt: Option[CommitCoordinatorClient] = None)
  extends CommitCoordinatorBuilder {
  lazy val trackingInMemoryCommitCoordinatorClient =
    defaultCommitCoordinatorClientOpt.getOrElse {
      new TrackingCommitCoordinatorClient(
        new InMemoryCommitCoordinator(batchSize))
    }

  override def getName: String = "tracking-in-memory"
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    trackingInMemoryCommitCoordinatorClient
  }
}

object TrackingCommitCoordinatorClient {
  private val insideOperation = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
}

class TrackingCommitCoordinatorClient(delegatingCommitCoordinatorClient: InMemoryCommitCoordinator)
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
    logPath: Path,
    coordinatedCommitsTableConf: util.Map[String, String],
    commitVersion: Long,
    actions: util.Iterator[String],
    updatedActions: UpdatedActions): CommitResponse = recordOperation("commit") {
    delegatingCommitCoordinatorClient.commit(
      logStore,
      hadoopConf,
      logPath,
      coordinatedCommitsTableConf,
      commitVersion,
      actions,
      updatedActions)
  }

  override def getCommits(
    logPath: Path,
    coordinatedCommitsTableConf: util.Map[String, String],
    startVersion: lang.Long,
    endVersion: lang.Long = null): GetCommitsResponse = recordOperation("getCommits") {
    delegatingCommitCoordinatorClient.getCommits(
      logPath, coordinatedCommitsTableConf, startVersion, endVersion)
  }

  override def backfillToVersion(
    logStore: LogStore,
    hadoopConf: Configuration,
    logPath: Path,
    coordinatedCommitsTableConf: util.Map[String, String],
    version: Long,
    lastKnownBackfilledVersion: lang.Long): Unit = recordOperation("backfillToVersion") {
    delegatingCommitCoordinatorClient.backfillToVersion(
      logStore,
      hadoopConf,
      logPath,
      coordinatedCommitsTableConf,
      version,
      lastKnownBackfilledVersion)
  }

  override def semanticEquals(other: CommitCoordinatorClient): lang.Boolean = this == other

  def reset(): Unit = {
    numCommitsCalled.set(0)
    numGetCommitsCalled.set(0)
    numBackfillToVersionCalled.set(0)
  }

  override def registerTable(
    logPath: Path,
    currentVersion: Long,
    currentMetadata: AbstractMetadata,
    currentProtocol: AbstractProtocol):
  util.Map[String, String] = recordOperation("registerTable") {
    delegatingCommitCoordinatorClient.registerTable(
      logPath, currentVersion, currentMetadata, currentProtocol)
  }
}
