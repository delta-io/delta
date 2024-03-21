/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel

import java.io.ByteArrayInputStream

import scala.collection.JavaConverters._

import io.delta.kernel.client.{ExpressionHandler, FileReadRequest, FileSystemClient, JsonHandler, ParquetHandler, TableClient}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

trait MockFileSystemClientUtils {

  val dataPath = new Path("/fake/path/to/table/")
  val logPath = new Path(dataPath, "_delta_log")

  /** Delta file statuses where the timestamp = 10*version */
  def deltaFileStatuses(deltaVersions: Seq[Long]): Seq[FileStatus] = {
    assert(deltaVersions.size == deltaVersions.toSet.size)
    deltaVersions.map(v => FileStatus.of(FileNames.deltaFile(logPath, v), v, v*10))
  }

  /** Checkpoint file statuses where the timestamp = 10*version */
  def singularCheckpointFileStatuses(checkpointVersions: Seq[Long]): Seq[FileStatus] = {
    assert(checkpointVersions.size == checkpointVersions.toSet.size)
    checkpointVersions.map(v =>
      FileStatus.of(FileNames.checkpointFileSingular(logPath, v).toString, v, v*10)
    )
  }

  /** Checkpoint file statuses where the timestamp = 10*version */
  def multiCheckpointFileStatuses(
    checkpointVersions: Seq[Long], numParts: Int): Seq[FileStatus] = {
    assert(checkpointVersions.size == checkpointVersions.toSet.size)
    checkpointVersions.flatMap(v =>
      FileNames.checkpointFileWithParts(logPath, v, numParts).asScala
        .map(p => FileStatus.of(p.toString, v, v*10))
    )
  }

  /**
   * Create input function for createMockTableClient to implement listFrom from a list of
   * file statuses.
   * */
  def listFromFileList(files: Seq[FileStatus])(filePath: String): Seq[FileStatus] = {
    files.filter(_.getPath.compareTo(filePath) >= 0).sortBy(_.getPath)
  }

  /**
   * Create a mock {@link TableClient} to test log segment generation.
   */
  def createMockTableClient(listFromPath: String => Seq[FileStatus]): TableClient = {
    new TableClient {
      override def getExpressionHandler: ExpressionHandler = {
        throw new UnsupportedOperationException("not supported for SnapshotManagerSuite tests")
      }

      override def getJsonHandler: JsonHandler = {
        throw new UnsupportedOperationException("not supported for SnapshotManagerSuite tests")
      }

      override def getFileSystemClient: FileSystemClient =
        createMockFileSystemClient(listFromPath)

      override def getParquetHandler: ParquetHandler = {
        throw new UnsupportedOperationException("not supported for SnapshotManagerSuite tests")
      }
    }
  }

  private def createMockFileSystemClient(
    listFromPath: String => Seq[FileStatus]): FileSystemClient = {

    new FileSystemClient {

      override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
        toCloseableIterator(listFromPath(filePath).iterator.asJava)
      }

      override def resolvePath(path: String): String = {
        throw new UnsupportedOperationException("not supported for SnapshotManagerSuite tests")
      }

      override def readFiles(
        readRequests: CloseableIterator[FileReadRequest]
      ): CloseableIterator[ByteArrayInputStream] = {
        throw new UnsupportedOperationException("not supported for SnapshotManagerSuite tests")
      }
    }
  }
}
