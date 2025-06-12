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

package io.delta.kernel.defaults.catalogManaged

import java.io.IOException
import java.util.concurrent.ConcurrentHashMap

import io.delta.kernel.{ResolvedTable, TableManager}
import io.delta.kernel.catalogmanaged.CatalogManagedUtils
import io.delta.kernel.commit.CommitPayload
import io.delta.kernel.utils.FileStatus

trait InMemoryCatalogManagedTestClient extends AbstractCatalogManagedTestClient {

  ///////////////////////
  // In-Memory Catalog //
  ///////////////////////

  // TODO: include Protocol and Metadata
  case class CommitData(fileStatusOpt: Option[FileStatus])

  class TableData {
    var maxRatifiedVersion = -1L
    var commits = new java.util.HashMap[Long, CommitData]()

    def commit(commitVersion: Long, commitData: CommitData): Unit = {
      val expectedCommitVersion = maxRatifiedVersion + 1

      if (commitVersion != expectedCommitVersion) {
        // TODO: Implement all the right CommitException and Commit return types
        throw new RuntimeException("Commit failed")
      }

      commits.put(commitVersion, commitData)
      maxRatifiedVersion = commitVersion
    }
  }

  /** Map from LOG_PATH to TABLE_DATA */
  private val tables = new ConcurrentHashMap[String, TableData]()

  /////////////////////////
  // API Implementations //
  /////////////////////////

  override def forceCommit(payload: CommitPayload): Unit = {
    val targetPath = CatalogManagedUtils.getStagedCommitFilePath(
      payload.getLogPath,
      payload.getCommitVersion)

    engine.getJsonHandler.writeJsonFileAtomically(targetPath, payload.getFinalizedActions, false)

    val tableData = tables.computeIfAbsent(payload.getLogPath, _ => new TableData())
    val fileStatus = FileStatus.of(targetPath) // TODO figure out the right way to do this
    val commitData = CommitData(Some(fileStatus))

    tableData.commit(payload.getCommitVersion, commitData)
  }

  override def publish(logPath: String, version: Long): Unit = {
    if (tables.containsKey(logPath)) {
      throw new RuntimeException(s"Table entry for logPath $logPath does not exist")
    }

    val tableData = tables.get(logPath)

    if (!tableData.commits.containsKey(version)) {
      throw new RuntimeException(
        s"Table entry for logPath $logPath does not contain version $version")
    }

    val ratifiedCommitToPublish = tableData.commits.get(version)

    val publishedPath = CatalogManagedUtils.getPublishedDeltaFilePath(logPath, version)

    copyFile(ratifiedCommitToPublish.fileStatusOpt.get.getPath, publishedPath)

    tableData.commits.remove(version)
  }

  override def loadTable(
      path: String,
      versionToLoadOpt: Option[Long],
      timestampToLoadOpt: Option[Long],
      injectProtocolMetadata: Boolean): ResolvedTable = {
    var builder = TableManager.loadTable(path)

    if (versionToLoadOpt.isDefined) {
      builder = builder.atVersion(versionToLoadOpt.get)
    }

    // TODO: P & M

    builder.build(engine)
  }

  private def copyFile(srcPath: String, destPath: String): Unit = {
    import org.apache.commons.io.FileUtils
    import java.io.File
    try {
      val srcFile = new File(srcPath)
      val destFile = new File(destPath)
      FileUtils.copyFile(srcFile, destFile)
    } catch {
      case ex: Exception =>
        throw new IOException(s"Error copying file from $srcPath to $destPath", ex)
    }
  }
}
