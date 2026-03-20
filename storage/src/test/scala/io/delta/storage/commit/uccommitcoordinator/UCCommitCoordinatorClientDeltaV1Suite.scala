/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.commit.uccommitcoordinator

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.storage.commit.Commit
import io.delta.storage.commit.{GetCommitsResponse, TableDescriptor, TableIdentifier}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import io.delta.storage.commit.uniform.UniformMetadata
import org.apache.hadoop.fs.{FileStatus, Path}
import org.scalatest.funsuite.AnyFunSuite

class UCCommitCoordinatorClientDeltaV1Suite extends AnyFunSuite {

  private class RecordingUCClient extends UCClient {
    var commitCalls: Int = 0

    override def getMetastoreId(): String = "metastore-id"

    override def commit(
        tableId: String,
        tableUri: java.net.URI,
        commit: Optional[Commit],
        lastKnownBackfilledVersion: Optional[java.lang.Long],
        disown: Boolean,
        newMetadata: Optional[AbstractMetadata],
        newProtocol: Optional[AbstractProtocol],
        uniform: Optional[UniformMetadata]): Unit = {
      commitCalls += 1
    }

    override def getCommits(
        tableId: String,
        tableUri: java.net.URI,
        startVersion: Optional[java.lang.Long],
        endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
      new GetCommitsResponse(Collections.emptyList(), -1L)
    }

    override def close(): Unit = {}
  }

  private class RecordingDeltaV1Client extends DeltaV1ManagedTableClient(
        "http://localhost:8080", Map("type" -> "static", "token" -> "token").asJava) {
    var loadResponse: DeltaV1ManagedTableClient.LoadTableResponse = _
    var updateResponse: DeltaV1ManagedTableClient.LoadTableResponse =
      new DeltaV1ManagedTableClient.LoadTableResponse()
    var lastLoadArgs: (String, String, String, java.lang.Long, java.lang.Long) = _
    var lastUpdateArgs:
      (String, String, String, DeltaV1ManagedTableClient.UpdateTableRequest) = _

    override def loadTable(
        catalog: String,
        schema: String,
        table: String,
        startVersion: java.lang.Long,
        endVersion: java.lang.Long): DeltaV1ManagedTableClient.LoadTableResponse = {
      lastLoadArgs = (catalog, schema, table, startVersion, endVersion)
      loadResponse
    }

    override def updateTable(
        catalog: String,
        schema: String,
        table: String,
        request: DeltaV1ManagedTableClient.UpdateTableRequest)
        : DeltaV1ManagedTableClient.LoadTableResponse = {
      lastUpdateArgs = (catalog, schema, table, request)
      updateResponse
    }
  }

  private class TestClient(ucClient: UCClient, deltaV1ManagedTableClient: DeltaV1ManagedTableClient)
      extends UCCommitCoordinatorClient(
        Collections.emptyMap[String, String](),
        ucClient,
        Optional.of("http://localhost:8080"),
        Map("type" -> "static", "token" -> "token").asJava) {

    override protected def deltaV1Client(
        tableDesc: TableDescriptor): Optional[DeltaV1ManagedTableClient] = {
      Optional.of(deltaV1ManagedTableClient)
    }

    def commitToUCTest(
        tableDesc: TableDescriptor,
        commitFile: Optional[FileStatus],
        commitVersion: Optional[java.lang.Long],
        commitTimestamp: Optional[java.lang.Long],
        lastKnownBackfilledVersion: Optional[java.lang.Long],
        newMetadata: Optional[AbstractMetadata],
        newProtocol: Optional[AbstractProtocol]): Unit = {
      super.commitToUC(
        tableDesc,
        tableDesc.getLogPath,
        commitFile,
        commitVersion,
        commitTimestamp,
        lastKnownBackfilledVersion,
        false,
        newMetadata,
        newProtocol)
    }

    def getCommitsFromUCTest(
        tableDesc: TableDescriptor,
        startVersion: Optional[java.lang.Long],
        endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
      super.getCommitsFromUCImpl(tableDesc, startVersion, endVersion)
    }
  }

  private def createMetadata(
      description: String,
      configuration: Map[String, String]): AbstractMetadata = new AbstractMetadata {
    override def getId: String = "id"
    override def getName: String = "name"
    override def getDescription: String = description
    override def getProvider: String = "delta"
    override def getFormatOptions: java.util.Map[String, String] = Collections.emptyMap()
    override def getSchemaString: String = """{"type":"struct","fields":[]}"""
    override def getPartitionColumns: java.util.List[String] = Collections.emptyList()
    override def getConfiguration: java.util.Map[String, String] = configuration.asJava
    override def getCreatedTime: java.lang.Long = 0L
  }

  private def createTableDescriptor(tableConf: Map[String, String]): TableDescriptor = {
    new TableDescriptor(
      new Path("file:///tmp/table/_delta_log"),
      Optional.of(new TableIdentifier(Array("main", "db"), "tbl")),
      tableConf.asJava)
  }

  test("commitToUC uses delta v1 managed-table path for metadata and commit updates") {
    val ucClient = new RecordingUCClient
    val deltaV1Client = new RecordingDeltaV1Client
    val currentState = new DeltaV1ManagedTableClient.LoadTableResponse()
    currentState.setEtag("etag-1")
    deltaV1Client.loadResponse = currentState

    val client = new TestClient(ucClient, deltaV1Client)
    val tableDesc =
      createTableDescriptor(
        Map(
          UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> "table-id",
          "unchanged" -> "same",
          "updated" -> "old",
          "removed" -> "gone"))
    val commitFile = new FileStatus(
      128L,
      false,
      1,
      4096L,
      123456789L,
      new Path("file:///tmp/table/_delta_log/_staged_commits/00000000000000000001.uuid.json"))
    val metadata =
      createMetadata(
        description = "updated comment",
        configuration = Map("unchanged" -> "same", "updated" -> "new", "added" -> "value"))

    client.commitToUCTest(
      tableDesc,
      Optional.of(commitFile),
      Optional.of(java.lang.Long.valueOf(1L)),
      Optional.of(java.lang.Long.valueOf(123456789L)),
      Optional.of(java.lang.Long.valueOf(0L)),
      Optional.of(metadata),
      Optional.empty())

    assert(deltaV1Client.lastLoadArgs === ("main", "db", "tbl", 0L, null))
    assert(deltaV1Client.lastUpdateArgs._1 === "main")
    assert(deltaV1Client.lastUpdateArgs._2 === "db")
    assert(deltaV1Client.lastUpdateArgs._3 === "tbl")
    assert(ucClient.commitCalls === 0)

    val request = deltaV1Client.lastUpdateArgs._4
    assert(request.getAssertTableId === "table-id")
    assert(request.getAssertEtag === "etag-1")
    assert(request.getLatestBackfilledVersion === 0L)
    assert(request.getSetProperties.asScala === Map("updated" -> "new", "added" -> "value"))
    assert(request.getRemoveProperties.asScala === Seq("removed"))
    assert(request.getComment === "updated comment")
    assert(request.getCommitInfo.getVersion === 1L)
    assert(request.getCommitInfo.getTimestamp === 123456789L)
    assert(request.getCommitInfo.getFileName === "00000000000000000001.uuid.json")
    assert(request.getCommitInfo.getFileSize === 128L)
    assert(request.getCommitInfo.getFileModificationTimestamp === 123456789L)
  }

  test("getCommitsFromUCImpl maps delta v1 commit response to storage commits") {
    val ucClient = new RecordingUCClient
    val deltaV1Client = new RecordingDeltaV1Client
    val loadResponse = new DeltaV1ManagedTableClient.LoadTableResponse()
    loadResponse.setLatestTableVersion(9L)
    loadResponse.setCommits(
      List(
        new DeltaV1ManagedTableClient.CommitInfo()
          .setVersion(7L)
          .setTimestamp(1000L)
          .setFileName("00000000000000000007.uuid.json")
          .setFileSize(256L)
          .setFileModificationTimestamp(1001L)).asJava)
    deltaV1Client.loadResponse = loadResponse

    val client = new TestClient(ucClient, deltaV1Client)
    val response = client.getCommitsFromUCTest(
      createTableDescriptor(Map(UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> "table-id")),
      Optional.of(java.lang.Long.valueOf(7L)),
      Optional.empty())

    assert(deltaV1Client.lastLoadArgs === ("main", "db", "tbl", 7L, null))
    assert(response.getLatestTableVersion === 9L)
    assert(response.getCommits.size() === 1)
    assert(response.getCommits.get(0).getVersion === 7L)
    assert(response.getCommits.get(0).getCommitTimestamp === 1000L)
    assert(response.getCommits.get(0).getFileStatus.getLen === 256L)
    assert(response.getCommits.get(0).getFileStatus.getModificationTime === 1001L)
    assert(response.getCommits.get(0).getFileStatus.getPath.getName === "00000000000000000007.uuid.json")
  }
}
