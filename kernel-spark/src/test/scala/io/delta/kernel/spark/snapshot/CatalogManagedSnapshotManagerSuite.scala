/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.spark.snapshot

import java.net.URI
import java.nio.file.Paths
import java.util.Optional

import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.spark.exception.VersionNotFoundException
import io.delta.storage.commit.{Commit, GetCommitsResponse}
import io.delta.storage.commit.uccommitcoordinator.{UCClient, UCCommitCoordinatorClient}
import io.delta.storage.commit.actions.{AbstractMetadata, AbstractProtocol}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.StructType
import org.scalatest.funsuite.AnyFunSuite

import scala.jdk.CollectionConverters._

class CatalogManagedSnapshotManagerSuite extends AnyFunSuite {

  private val TableId = "ucTableId"
  private val CatalogOwnedPreviewKey = "delta.feature.catalogOwned-preview"
  private val TablePath =
    Paths
      .get("..", "kernel", "kernel-defaults", "src", "test", "resources", "catalog-owned-preview")
      .toAbsolutePath
      .toString

  private val StagedCommitFiles = Seq(
    "_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json",
    "_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json"
  )

  private val LatestVersion = 2L
  private val V2Timestamp = 1749830881799L

  private val HadoopConf = new Configuration()

  private def loadCommitFileStatus(relativePath: String): FileStatus = {
    val fs = FileSystem.get(HadoopConf)
    fs.getFileStatus(new Path(s"$TablePath/$relativePath"))
  }

  private val CatalogTableDescriptor: CatalogTable = {
    val storage =
      CatalogStorageFormat.empty.copy(
        properties =
          Map(CatalogOwnedPreviewKey -> "supported", UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> TableId))

    CatalogTable(
      identifier = TableIdentifier("tbl", Some("ns")),
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = new StructType(),
      provider = Some("delta"))
  }

  private def buildCommits(): Seq[Commit] = {
    StagedCommitFiles.map { relativePath =>
      val status = loadCommitFileStatus(relativePath)
      val version = FileNames.deltaVersion(status.getPath.toString)
      new Commit(version, status, status.getModificationTime)
    }
  }

  private def newManager(): CatalogManagedSnapshotManager = {
    val ucClient = new StubUCClient(buildCommits(), LatestVersion)
    new CatalogManagedSnapshotManager(TablePath, CatalogTableDescriptor, ucClient, HadoopConf)
  }

  test("loadLatestSnapshot returns the latest UC version") {
    val manager = newManager()
    val snapshot = manager.loadLatestSnapshot()
    assert(snapshot.getVersion === LatestVersion)
  }

  test("getActiveCommitAtTime honours Unity Catalog staged commits") {
    val manager = newManager()
    manager.loadLatestSnapshot()
    val commit =
      manager.getActiveCommitAtTime(
        V2Timestamp,
        /* canReturnLastCommit = */ true,
        /* mustBeRecreatable = */ true,
        /* canReturnEarliestCommit = */ true)
    assert(commit.getVersion === LatestVersion)
  }

  test("checkVersionExists enforces UC ratified version bounds") {
    val manager = newManager()
    manager.loadLatestSnapshot()
    manager.checkVersionExists(LatestVersion, true, false)

    val ex =
      intercept[VersionNotFoundException] {
        manager.checkVersionExists(LatestVersion + 1, true, false)
      }
    assert(ex.getLatest === LatestVersion)
  }

  test("getTableChanges delegates to Unity Catalog commit range") {
    val manager = newManager()
    val engine = DefaultEngine.create(HadoopConf)
    val commitRange =
      manager.getTableChanges(engine, 1L, Optional.of(Long.box(LatestVersion)))

    assert(commitRange.getStartVersion === 1L)
    assert(commitRange.getEndVersion === LatestVersion)
  }

  private final class StubUCClient(commits: Seq[Commit], latestVersion: Long) extends UCClient {

    private val commitList = commits.sortBy(_.getVersion)

    override def getMetastoreId(): String = "test-metastore"

    override def commit(
        tableId: String,
        tableUri: URI,
        commit: Optional[Commit],
        lastKnownBackfilledVersion: Optional[java.lang.Long],
        disown: Boolean,
        newMetadata: Optional[AbstractMetadata],
        newProtocol: Optional[AbstractProtocol]
    ): Unit = throw new UnsupportedOperationException("commit is not supported in tests")

    override def getCommits(
        tableId: String,
        tableUri: URI,
        startVersion: Optional[java.lang.Long],
        endVersion: Optional[java.lang.Long]): GetCommitsResponse = {
      require(tableId == TableId, s"Unexpected table id: $tableId")

      val start = if (startVersion.isPresent) startVersion.get().longValue() else Long.MinValue
      val end = if (endVersion.isPresent) endVersion.get().longValue() else latestVersion

      val filtered =
        commitList.filter(commit => commit.getVersion >= start && commit.getVersion <= end)

      new GetCommitsResponse(filtered.asJava, latestVersion)
    }

    override def close(): Unit = {}
  }
}

