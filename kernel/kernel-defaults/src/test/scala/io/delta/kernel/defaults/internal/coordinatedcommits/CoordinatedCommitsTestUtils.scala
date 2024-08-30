/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import io.delta.kernel.data.Row

import java.{lang, util}
import io.delta.storage.commit.{CommitCoordinatorClient, InMemoryCommitCoordinator, Commit => StorageCommit, CommitResponse => StorageCommitResponse, GetCommitsResponse => StorageGetCommitsResponse, TableDescriptor, TableIdentifier, UpdatedActions => StorageUpdatedActions}
import io.delta.kernel.defaults.internal.logstore.LogStoreProvider
import io.delta.kernel.engine.{CommitCoordinatorClientHandler, Engine}
import io.delta.kernel.internal.actions.{CommitInfo, Format, Metadata, Protocol}
import io.delta.kernel.internal.TableConfig
import io.delta.kernel.internal.util.{CoordinatedCommitsUtils, FileNames, VectorUtils}
import io.delta.kernel.internal.util.VectorUtils.{stringArrayValue, stringVector}
import io.delta.kernel.utils.CloseableIterator
import io.delta.kernel.engine.coordinatedcommits.{Commit, CommitResponse, GetCommitsResponse, UpdatedActions}
import io.delta.kernel.types.{LongType, StringType, StructType}
import io.delta.storage.LogStore
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import java.util.{Collections, Optional}
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.JavaConverters._

trait CoordinatedCommitsTestUtils {

  val hadoopConf = new Configuration()
  def getEmptyMetadata: Metadata = {
    new Metadata(
      util.UUID.randomUUID().toString,
      Optional.empty(),
      Optional.empty(),
      new Format(),
      "",
      null,
      stringArrayValue(Collections.emptyList()),
      Optional.empty(),
      VectorUtils.stringStringMapValue(Collections.emptyMap())
    )
  }

  def getProtocol(minReaderVersion: Int, minWriterVersion: Int): Protocol = {
    new Protocol(
      minReaderVersion, minWriterVersion, Collections.emptyList(), Collections.emptyList())
  }

  def getCommitInfo(newTimestamp: Long): CommitInfo = {
    new CommitInfo(
      Optional.of(newTimestamp),
      -1,
      null,
      null,
      Collections.emptyMap(),
      true,
      null)
  }

  def commit(
    logPath: String,
    tableConf: util.Map[String, String],
    version: Long,
    timestamp: Long,
    commit: CloseableIterator[Row],
    commitCoordinatorClientHandler: CommitCoordinatorClientHandler): Commit = {
    val updatedCommitInfo = getCommitInfo(timestamp)
    val updatedActions = if (version == 0) {
      getUpdatedActionsForZerothCommit(updatedCommitInfo)
    } else {
      getUpdatedActionsForNonZerothCommit(updatedCommitInfo)
    }
    commitCoordinatorClientHandler.commit(
      logPath,
      tableConf,
      version,
      commit,
      updatedActions).getCommit
  }

  def getHadoopDeltaFile(logPath: Path, version: Long): Path = {
    new Path(FileNames.deltaFile(new io.delta.kernel.internal.fs.Path(logPath.toString), version))
  }

  def writeConvertToCCCommit(
    engine: Engine, logPath: Path, commit: CloseableIterator[Row], version: Long): Unit = {
    createLogPath(engine, logPath)
    engine.getJsonHandler.writeJsonFileAtomically(
      getHadoopDeltaFile(logPath, version).toString,
      commit,
      true)
  }

  def createLogPath(engine: Engine, logPath: Path): Unit = {
    // New table, create a delta log directory
    if (!engine.getFileSystemClient.mkdirs(logPath.toString)) {
      throw new RuntimeException("Failed to create delta log directory: " + logPath)
    }
  }

  def getUpdatedActionsForZerothCommit(
    commitInfo: CommitInfo,
    oldMetadata: Metadata = getEmptyMetadata): UpdatedActions = {
    val newMetadataConfiguration =
      Map(TableConfig.COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "in-memory",
        TableConfig.COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
    val newMetadata = oldMetadata.withNewConfiguration(newMetadataConfiguration.asJava)
    new UpdatedActions(
      CoordinatedCommitsUtils.convertCommitInfoToAbstractCommitInfo(commitInfo),
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(newMetadata),
      CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(getProtocol(3, 7)),
      CoordinatedCommitsUtils.convertMetadataToAbstractMetadata(oldMetadata),
      CoordinatedCommitsUtils.convertProtocolToAbstractProtocol(getProtocol(3, 7)))
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

  def getVersionTimestampSchema: StructType = {
    new StructType()
      .add("version", LongType.LONG)
      .add("timestamp", LongType.LONG)
  }

  def getCommitRows(engine: Engine, version: Long, timestamp: Long): CloseableIterator[Row] = {
    val input = Seq(
      s"""{
        | "version":$version,
        | "timestamp":$timestamp
        |}
        |""".stripMargin.linesIterator.mkString)

    engine.getJsonHandler.parseJson(
      stringVector(input.asJava), getVersionTimestampSchema, Optional.empty()).getRows
  }
}

case class TrackingInMemoryCommitCoordinatorBuilder(hadoopConf: Configuration)
  extends CommitCoordinatorBuilder(hadoopConf) {
  override def getName: String = "tracking-in-memory"
  override def build(conf: util.Map[String, String]): CommitCoordinatorClient = {
    new TrackingCommitCoordinatorClient(
      new InMemoryCommitCoordinatorBuilder(hadoopConf).build(conf)
        .asInstanceOf[InMemoryCommitCoordinator])
  }
}

object TrackingCommitCoordinatorClient {
  val numCommitsCalled = new AtomicInteger(0)
  val numGetCommitsCalled = new AtomicInteger(0)
  val numBackfillToVersionCalled = new AtomicInteger(0)
  val numRegisterTableCalled = new AtomicInteger(0)

  private val insideOperation = new ThreadLocal[Boolean] {
    override def initialValue(): Boolean = false
  }
}

class TrackingCommitCoordinatorClient(delegatingCommitCoordinatorClient: InMemoryCommitCoordinator)
  extends CommitCoordinatorClient {

  def recordOperation[T](op: String)(f: => T): T = {
    val oldInsideOperation = TrackingCommitCoordinatorClient.insideOperation.get()
    try {
      if (!TrackingCommitCoordinatorClient.insideOperation.get()) {
        op match {
          case "commit" => TrackingCommitCoordinatorClient.numCommitsCalled.incrementAndGet()
          case "getCommits" => TrackingCommitCoordinatorClient.numGetCommitsCalled.incrementAndGet()
          case "backfillToVersion" =>
            TrackingCommitCoordinatorClient.numBackfillToVersionCalled.incrementAndGet()
          case "registerTable" =>
            TrackingCommitCoordinatorClient.numRegisterTableCalled.incrementAndGet()
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
    actions: util.Iterator[String],
    updatedActions: StorageUpdatedActions): StorageCommitResponse = recordOperation("commit") {
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
    startVersion: lang.Long,
    endVersion: lang.Long = null): StorageGetCommitsResponse = recordOperation("getCommits") {
    delegatingCommitCoordinatorClient.getCommits(tableDesc, startVersion, endVersion)
  }

  override def backfillToVersion(
    logStore: LogStore,
    hadoopConf: Configuration,
    tableDesc: TableDescriptor,
    version: Long,
    lastKnownBackfilledVersion: lang.Long): Unit = recordOperation("backfillToVersion") {
    delegatingCommitCoordinatorClient.backfillToVersion(
      logStore, hadoopConf, tableDesc, version, lastKnownBackfilledVersion)
  }

  override def semanticEquals(other: CommitCoordinatorClient): Boolean = this == other

  def reset(): Unit = {
    TrackingCommitCoordinatorClient.numCommitsCalled.set(0)
    TrackingCommitCoordinatorClient.numGetCommitsCalled.set(0)
    TrackingCommitCoordinatorClient.numBackfillToVersionCalled.set(0)
  }

  override def registerTable(
    logPath: Path,
    tableIdentifier: Optional[TableIdentifier],
    currentVersion: Long,
    currentMetadata: AbstractMetadata,
    currentProtocol: AbstractProtocol):
  util.Map[String, String] = recordOperation("registerTable") {
    delegatingCommitCoordinatorClient.registerTable(
      logPath, tableIdentifier, currentVersion, currentMetadata, currentProtocol)
  }
}
