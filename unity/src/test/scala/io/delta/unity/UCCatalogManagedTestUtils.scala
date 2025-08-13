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

import java.net.URI
import java.util.Optional

import io.delta.kernel.commit.CommitMetadata
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.test.ActionUtils
import io.delta.storage.commit.Commit

import org.apache.hadoop.fs.{FileStatus => HadoopFileStatus, Path}

trait UCCatalogManagedTestUtils extends TestUtils with ActionUtils {
  val fakeURI = new URI("s3://bucket/table")
  val baseTestTablePath = "/path/to/table"
  val baseTestLogPath = "/path/to/table/_delta_log"

  def createCommitMetadata(
      version: Long = 1L,
      logPath: String = baseTestLogPath,
      readProtocolOpt: Optional[Protocol] = Optional.empty(),
      readMetadataOpt: Optional[Metadata] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty()): CommitMetadata = new CommitMetadata(
    version,
    logPath,
    testCommitInfo(),
    readProtocolOpt,
    readMetadataOpt,
    newProtocolOpt,
    newMetadataOpt)

  def catalogManagedWriteCommitMetadata(
      logPath: String = baseTestLogPath): CommitMetadata = createCommitMetadata(
    logPath = logPath,
    readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
    readMetadataOpt = Optional.of(basicPartitionedMetadata))

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
}
