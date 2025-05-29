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

import java.net.URI
import java.util.{Optional, UUID}

import scala.collection.JavaConverters._

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaConfigs.{COORDINATED_COMMITS_COORDINATOR_CONF, COORDINATED_COMMITS_COORDINATOR_NAME, COORDINATED_COMMITS_TABLE_CONF}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{Metadata, Protocol}
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.{FileNames, JsonUtils}
import io.delta.storage.commit.{
  CoordinatedCommitsUtils => JCoordinatedCommitsUtils,
  GetCommitsResponse => JGetCommitsResponse
}
import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCCommitCoordinatorClient}
import org.apache.hadoop.fs.Path
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.Mockito.{mock, when}
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.types.{IntegerType, StringType, StructField}

trait UCCommitCoordinatorClientSuiteBase extends CommitCoordinatorClientImplSuiteBase
  {
  /**
   * A unique table ID for each test.
   */
  protected var tableUUID = UUID.randomUUID()

  /**
   * A unique metastore ID for each test.
   */
  protected var metastoreId = UUID.randomUUID()

  protected var ucClient: UCClient = _

  @Mock
  protected val mockFactory: UCClientFactory = mock(classOf[UCClientFactory])

  protected var ucCommitCoordinator: InMemoryUCCommitCoordinator = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tableUUID = UUID.randomUUID()
    UCCommitCoordinatorClient.BACKFILL_LISTING_OFFSET = 100
    metastoreId = UUID.randomUUID()
    DeltaLog.clearCache()
    Mockito.reset(mockFactory)
    CommitCoordinatorProvider.clearAllBuilders()
    UCCommitCoordinatorBuilder.ucClientFactory = mockFactory
    UCCommitCoordinatorBuilder.clearCache()
    CommitCoordinatorProvider.registerBuilder(UCCommitCoordinatorBuilder)
    ucCommitCoordinator = new InMemoryUCCommitCoordinator()
    ucClient = new InMemoryUCClient(metastoreId.toString, ucCommitCoordinator)
    when(mockFactory.createUCClient(anyString(), anyString())).thenReturn(ucClient)
  }
  override protected def createTableCommitCoordinatorClient(
      deltaLog: DeltaLog): TableCommitCoordinatorClient = {
    var commitCoordinatorClient = UCCommitCoordinatorBuilder
      .build(spark, Map(UCCommitCoordinatorClient.UC_METASTORE_ID_KEY -> metastoreId.toString))
      .asInstanceOf[UCCommitCoordinatorClient]
    commitCoordinatorClient = new UCCommitCoordinatorClient(
      commitCoordinatorClient.conf,
      commitCoordinatorClient.ucClient) with DeltaLogging {
      override def recordDeltaEvent(opType: String, data: Any, path: Path): Unit = {
        data match {
          case ref: AnyRef => recordDeltaEvent(null, opType = opType, data = ref, path = Some(path))
          case _ => super.recordDeltaEvent(opType, data, path)
        }
      }
    }
    // Initialize table ID for the calling test
    // tableUUID = UUID.randomUUID().toString
    commitCoordinatorClient.registerTable(
      deltaLog.logPath, Optional.empty(), -1L, initMetadata(), Protocol(1, 1))
    TableCommitCoordinatorClient(
      commitCoordinatorClient,
      deltaLog,
      Map(UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableUUID.toString)
    )
  }

  override protected def registerBackfillOp(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      deltaLog: DeltaLog,
      version: Long): Unit = {
    ucClient.commit(
      tableUUID.toString,
      JCoordinatedCommitsUtils.getTablePath(deltaLog.logPath).toUri,
      Optional.empty(),
      Optional.of(version),
      false,
      Optional.empty(),
      Optional.empty())
  }

  override protected def validateBackfillStrategy(
      tableCommitCoordinatorClient: TableCommitCoordinatorClient,
      logPath: Path,
      version: Long): Unit = {
    val response = tableCommitCoordinatorClient.getCommits()
    assert(response.getCommits.size == 1)
    assert(response.getCommits.asScala.head.getVersion == version)
    assert(response.getLatestTableVersion == version)
  }

  protected def validateGetCommitsResult(
      response: JGetCommitsResponse,
      startVersion: Option[Long],
      endVersion: Option[Long],
      maxVersion: Long): Unit = {
    val expectedVersions = endVersion.map { _ => Seq.empty }.getOrElse(Seq(maxVersion))
    assert(response.getCommits.asScala.map(_.getVersion) == expectedVersions)
    assert(response.getLatestTableVersion == maxVersion)
  }

  override protected def initMetadata(): Metadata = {
    // Ensure that the metadata that is passed to registerTable has the
    // correct table conf set.
    Metadata(configuration = Map(
      COORDINATED_COMMITS_TABLE_CONF.key ->
        JsonUtils.toJson(Map(UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> tableUUID.toString)),
      COORDINATED_COMMITS_COORDINATOR_NAME.key -> UCCommitCoordinatorBuilder.getName,
      COORDINATED_COMMITS_COORDINATOR_CONF.key ->
        JsonUtils.toJson(
          Map(UCCommitCoordinatorClient.UC_METASTORE_ID_KEY -> metastoreId.toString))))
  }

  protected def waitForBackfill(
      version: Long,
      tableCommitCoordinatorClient: TableCommitCoordinatorClient): Unit = {
    eventually(timeout(10.seconds)) {
      val logPath = tableCommitCoordinatorClient.logPath
      val log = DeltaLog.forTable(spark, JCoordinatedCommitsUtils.getTablePath(logPath))
      val fs = logPath.getFileSystem(log.newDeltaHadoopConf())
      assert(fs.exists(FileNames.unsafeDeltaFile(logPath, version)))
    }
  }
}
