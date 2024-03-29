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

import java.io.{ByteArrayInputStream, IOException}
import java.util.{Optional, UUID}

import scala.collection.JavaConverters._

import io.delta.kernel.client.{ExpressionHandler, FileReadRequest, FileSystemClient, JsonHandler, ParquetHandler, TableClient}
import io.delta.kernel.data.{ColumnVector, ColumnarBatch, FilteredColumnarBatch}
import io.delta.kernel.expressions.{Column, Predicate}
import io.delta.kernel.internal.checkpoints.SidecarFile
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, Utils}
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus, FileStatus}

trait MockFileSystemClientUtils {

  val dataPath = new Path("/fake/path/to/table/")
  val logPath = new Path(dataPath, "_delta_log")

  def mockTableClient(jsonHandler: JsonHandler): TableClient = {
    new TableClient() {
      override def getExpressionHandler: ExpressionHandler =
        throw new UnsupportedOperationException("not supported for in this test suite")

      override def getJsonHandler: JsonHandler = jsonHandler

      override def getFileSystemClient: FileSystemClient =
        throw new UnsupportedOperationException("not supported for in this test suite")

      override def getParquetHandler: ParquetHandler =
        throw new UnsupportedOperationException("not supported for in this test suite")
    }
  }

  def mockTableClient(jsonHandler: JsonHandler, parquetHandler: ParquetHandler): TableClient = {
    new TableClient() {
      override def getExpressionHandler: ExpressionHandler =
        throw new UnsupportedOperationException("not supported for in this test suite")

      override def getJsonHandler: JsonHandler = jsonHandler

      override def getFileSystemClient: FileSystemClient =
        throw new UnsupportedOperationException("not supported for in this test suite")

      override def getParquetHandler: ParquetHandler = parquetHandler
    }
  }

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

  /** Checkpoint file status for V2 checkpoint manifest */
  def v2CheckpointFileStatuses(
      checkpointVersions: Seq[(Long, Int)],
      fileType: String): Seq[(FileStatus, Seq[FileStatus])] = {
    assert(checkpointVersions.size == checkpointVersions.toSet.size)
    checkpointVersions.map { case(v, numSidecars) =>
      val checkpointManifest = FileStatus.of(
        FileNames.v2CheckpointManifestFile(
          logPath, v, UUID.randomUUID().toString, fileType).toString,
        v, v * 10)
      val sidecars = (0 until numSidecars).map { _ =>
        FileStatus.of(
          FileNames.v2CheckpointSidecarFile(logPath, UUID.randomUUID().toString).toString,
          v, v * 10)
      }
      (checkpointManifest, sidecars)
    }
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

  def createMockTableClient(
      listFromPath: String => Seq[FileStatus],
      parquetHandler: ParquetHandler): TableClient = {
    new TableClient {
      override def getExpressionHandler: ExpressionHandler = {
        throw new UnsupportedOperationException("not supported for SnapshotManagerSuite tests")
      }

      override def getJsonHandler: JsonHandler = {
        throw new UnsupportedOperationException("not supported for SnapshotManagerSuite tests")
      }

      override def getFileSystemClient: FileSystemClient =
        createMockFileSystemClient(listFromPath)

      override def getParquetHandler: ParquetHandler = parquetHandler
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

  class TestSidecarParquetHandler(sidecars: Seq[FileStatus]) extends ParquetHandler {
    import io.delta.kernel.internal.checkpoints.CheckpointerSuite._

    override def writeParquetFiles(
      directoryPath: String,
      dataIter: CloseableIterator[FilteredColumnarBatch],
      maxFileSize: Long,
      statsColumns: java.util.List[Column]): CloseableIterator[DataFileStatus] = {
      throw new UnsupportedOperationException("not supported for in this test suite")
    }

    override def readParquetFiles(
      fileIter: CloseableIterator[FileStatus],
      physicalSchema: StructType,
      predicate: Optional[Predicate]): CloseableIterator[ColumnarBatch] = {
      val file = fileIter.next()
      val path = new Path(file.getPath)

      Utils.singletonCloseableIterator(
        path.getParent match {
          case logPath => new ColumnarBatch {
            override def getSchema: StructType = SidecarFile.READ_SCHEMA

            override def getColumnVector(ordinal: Int): ColumnVector = {
              ordinal match {
                case 0 => stringVector(sidecars.map(_.getPath): _*) // path
                case 1 => longVector(sidecars.map(_.getSize): _*) // size
                case 2 => longVector(sidecars.map(_.getModificationTime): _*); // modification time
              }
            }

            override def getSize: Int = 2
          }
          case _ => throw new IOException("Unknown table")
        })
    }
  }
}
