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

package io.delta.unity

import java.lang.{Long => JLong}
import java.net.URI
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.commit.PublishMetadata
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{TestUtils, WriteUtils}
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.files.ParsedCatalogCommitData
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.test.ActionUtils
import io.delta.kernel.utils.CloseableIterator
import io.delta.storage.commit.{Commit, GetCommitsResponse}
import io.delta.unity.InMemoryUCClient.TableData

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus => HadoopFileStatus, FileSystem, Path}

trait UCCatalogManagedTestUtils extends TestUtils with ActionUtils with WriteUtils {
  val fakeURI = new URI("s3://bucket/table")
  val baseTestTablePath = "/path/to/table"
  val baseTestLogPath = "/path/to/table/_delta_log"
  val emptyLongOpt = Optional.empty[java.lang.Long]()

  /** Helper method with reasonable defaults */
  def loadSnapshot(
      ucCatalogManagedClient: UCCatalogManagedClient,
      engine: Engine = defaultEngine,
      ucTableId: String = "ucTableId",
      tablePath: String = "tablePath",
      versionToLoad: Optional[java.lang.Long] = emptyLongOpt,
      timestampToLoad: Optional[java.lang.Long] = emptyLongOpt): SnapshotImpl = {
    ucCatalogManagedClient.loadSnapshot(
      engine,
      ucTableId,
      tablePath,
      versionToLoad,
      timestampToLoad).asInstanceOf[SnapshotImpl]
  }

  def hadoopCommitFileStatus(version: Long): HadoopFileStatus = {
    val filePath = FileNames.stagedCommitFile(baseTestLogPath, version)

    new HadoopFileStatus(
      version, /* length */
      false, /* isDir */
      version.toInt, /* blockReplication */
      version, /* blockSize */
      version, /* modificationTime */
      new Path(filePath))
  }

  def createCommit(version: Long): Commit = {
    new Commit(version, hadoopCommitFileStatus(version), version) // version, fileStatus, timestamp
  }

  /** Creates an InMemoryUCClient with the given tableId and commits for the specified versions. */
  def getInMemoryUCClientWithCommitsForTableId(
      tableId: String,
      versions: Seq[Long]): InMemoryUCClient = {
    val client = new InMemoryUCClient("ucMetastoreId")
    versions.foreach { v =>
      client.commitWithDefaults(tableId, fakeURI, Optional.of(createCommit(v)))
    }
    client
  }

  def createPublishMetadata(
      snapshotVersion: Long,
      logPath: String,
      catalogCommits: List[ParsedCatalogCommitData]): PublishMetadata = {
    new PublishMetadata(snapshotVersion, logPath, catalogCommits.asJava)
  }

  def getSingleElementRowIter(elem: String): CloseableIterator[Row] = {
    import io.delta.kernel.defaults.integration.DataBuilderUtils
    import io.delta.kernel.types.{StringType, StructField, StructType}

    val schema = new StructType().add(new StructField("testColumn", StringType.STRING, true))
    val simpleRow = DataBuilderUtils.row(schema, elem)
    singletonCloseableIterator(simpleRow)
  }

  /** Version TS for the test table used in [[withUCClientAndTestTable]] */
  val v0Ts = 1749830855993L // published commit
  val v1Ts = 1749830871085L // ratified staged commit
  val v2Ts = 1749830881799L // ratified staged commit

  /**
   * @param textFx test function to run that takes input (ucClient, tablePath, maxRatifiedVersion)
   */
  def withUCClientAndTestTable(
      textFx: (InMemoryUCClientWithMetrics, String, Long) => Unit): Unit = {
    val maxRatifiedVersion = 2L
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val ucClient = new InMemoryUCClientWithMetrics("ucMetastoreId")
    val fs = FileSystem.get(new Configuration())
    val catalogCommits = Seq(
      // scalastyle:off line.size.limit
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000001.4cb9708e-b478-44de-b203-53f9ba9b2876.json"),
      getTestResourceFilePath("catalog-owned-preview/_delta_log/_staged_commits/00000000000000000002.5b9bba4a-0085-430d-a65e-b0d38c1afbe9.json"))
      // scalastyle:on line.size.limit
      .map { path => fs.getFileStatus(new Path(path)) }
      .map { fileStatus =>
        new Commit(
          FileNames.deltaVersion(fileStatus.getPath.toString),
          fileStatus,
          fileStatus.getModificationTime)
      }
    val tableData = new TableData(maxRatifiedVersion, ArrayBuffer(catalogCommits: _*))
    ucClient.createTableIfNotExistsOrThrow("ucTableId", tableData)
    textFx(ucClient, tablePath, maxRatifiedVersion)
  }

  /** Wrapper class around InMemoryUCClient that tracks number of getCommit calls made */
  class InMemoryUCClientWithMetrics(ucMetastoreId: String) extends InMemoryUCClient(ucMetastoreId) {
    private var numGetCommitsCalls: Long = 0

    override def getCommits(
        tableId: String,
        tableUri: URI,
        startVersion: Optional[JLong],
        endVersion: Optional[JLong]): GetCommitsResponse = {
      numGetCommitsCalls += 1
      super.getCommits(tableId, tableUri, startVersion, endVersion)
    }

    def getNumGetCommitCalls: Long = numGetCommitsCalls
  }

  // TODO: [delta-io/delta#5118] If UC changes CREATE semantics, update logic here.
  /**
   * When a new UC table is created, it will have Delta version 0 but the max ratified verison in
   * UC is -1. This is a special edge case.
   */
  def createUCCatalogManagedClientForTableWithMaxRatifiedVersionNegativeOne(
      ucTableId: String = "ucTableId"): UCCatalogManagedClient = {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val tableData = new TableData(-1, ArrayBuffer[Commit]())
    ucClient.createTableIfNotExistsOrThrow(ucTableId, tableData)
    new UCCatalogManagedClient(ucClient)
  }
}
