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

import scala.collection.JavaConverters._

import io.delta.kernel.commit.PublishMetadata
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.{TestUtils, WriteUtils}
import io.delta.kernel.internal.files.ParsedCatalogCommitData
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.test.ActionUtils
import io.delta.kernel.utils.CloseableIterator
import io.delta.storage.commit.Commit

import org.apache.hadoop.fs.{FileStatus => HadoopFileStatus, Path}

trait UCCatalogManagedTestUtils extends TestUtils with ActionUtils with WriteUtils {
  val fakeURI = new URI("s3://bucket/table")
  val baseTestTablePath = "/path/to/table"
  val baseTestLogPath = "/path/to/table/_delta_log"

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
}
