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
package io.delta.kernel.test

import java.util.UUID

import io.delta.kernel.engine._
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.{CloseableIterator, FileStatus}

import scala.collection.JavaConverters._

/**
 * This is an extension to [[BaseMockFileSystemClient]] containing specific mock implementations
 * [[FileSystemClient]] which are shared across multiple test suite.
 *
 * [[MockListFromFileSystemClient]] - mocks the `listFrom` API within [[FileSystemClient]].
 */
trait MockFileSystemClientUtils extends MockEngineUtils {

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
   * Checkpoint file status for a top-level V2 checkpoint file.
   *
   * @param checkpointVersions List of checkpoint versions, given as Seq(version, whether to use
   *                           UUID naming scheme, number of sidecars).
   * Returns top-level checkpoint file and sidecar files for each checkpoint version.
   */
  def v2CheckpointFileStatuses(
      checkpointVersions: Seq[(Long, Boolean, Int)],
      fileType: String): Seq[(FileStatus, Seq[FileStatus])] = {
    checkpointVersions.map { case (v, useUUID, numSidecars) =>
      val topLevelFile = if (useUUID) {
        FileStatus.of(FileNames.topLevelV2CheckpointFile(
          logPath, v, UUID.randomUUID().toString, fileType).toString, v, v * 10)
      } else {
        FileStatus.of(FileNames.checkpointFileSingular(logPath, v).toString, v, v * 10)
      }
      val sidecars = (0 until numSidecars).map { _ =>
        FileStatus.of(
          FileNames.v2CheckpointSidecarFile(logPath, UUID.randomUUID().toString).toString,
          v, v * 10)
      }
      (topLevelFile, sidecars)
    }
  }

  /* Create input function for createMockEngine to implement listFrom from a list of
   * file statuses.
   */
  def listFromProvider(files: Seq[FileStatus])(filePath: String): Seq[FileStatus] = {
    files.filter(_.getPath.compareTo(filePath) >= 0).sortBy(_.getPath)
  }

  /**
   * Create a mock [[Engine]] to mock the [[FileSystemClient.listFrom]] calls using
   * the given contents. The contents are filtered depending upon the list from path prefix.
   */
  def createMockFSListFromEngine(
      contents: Seq[FileStatus],
      parquetHandler: ParquetHandler,
      jsonHandler: JsonHandler): Engine = {
    mockEngine(fileSystemClient =
      new MockListFromFileSystemClient(listFromProvider(contents)),
      parquetHandler = parquetHandler,
      jsonHandler = jsonHandler)
  }

  /**
   * Create a mock [[Engine]] to mock the [[FileSystemClient.listFrom]] calls using
   * the given contents. The contents are filtered depending upon the list from path prefix.
   */
  def createMockFSListFromEngine(contents: Seq[FileStatus]): Engine = {
    mockEngine(fileSystemClient =
      new MockListFromFileSystemClient(listFromProvider(contents)))
  }

  /**
   * Create a mock [[Engine]] to mock the [[FileSystemClient.listFrom]] calls using
   * [[MockListFromFileSystemClient]].
   */
  def createMockFSListFromEngine(listFromProvider: String => Seq[FileStatus]): Engine = {
    mockEngine(fileSystemClient = new MockListFromFileSystemClient(listFromProvider))
  }
}

/**
 * A mock [[FileSystemClient]] that answers `listFrom` calls from a given content provider.
 *
 * It also maintains metrics on number of times `listFrom` is called and arguments for each call.
 */
class MockListFromFileSystemClient(listFromProvider: String => Seq[FileStatus])
    extends BaseMockFileSystemClient {
  private var listFromCalls: Seq[String] = Seq.empty

  override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
    listFromCalls = listFromCalls :+ filePath
    toCloseableIterator(listFromProvider(filePath).iterator.asJava)
  }

  def getListFromCalls: Seq[String] = listFromCalls
}

/**
 * A mock [[FileSystemClient]] that answers `listFrom` calls from a given content provider and
 * implements the identity function for `resolvePath` calls.
 *
 * It also maintains metrics on number of times `listFrom` is called and arguments for each call.
 */
class MockListFromResolvePathFileSystemClient(listFromProvider: String => Seq[FileStatus])
  extends BaseMockFileSystemClient {
  private var listFromCalls: Seq[String] = Seq.empty

  override def listFrom(filePath: String): CloseableIterator[FileStatus] = {
    listFromCalls = listFromCalls :+ filePath
    toCloseableIterator(listFromProvider(filePath).iterator.asJava)
  }

  override def resolvePath(path: String): String = path

  def getListFromCalls: Seq[String] = listFromCalls
}
