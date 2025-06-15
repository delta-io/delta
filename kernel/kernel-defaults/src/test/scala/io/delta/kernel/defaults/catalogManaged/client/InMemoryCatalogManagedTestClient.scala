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

package io.delta.kernel.defaults.catalogManaged.client

import java.io.IOException
import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._

import io.delta.kernel.{ResolvedTable, TableManager}
import io.delta.kernel.catalogmanaged.CatalogManagedUtils
import io.delta.kernel.commit.CommitPayload
import io.delta.kernel.internal.DeltaErrors.wrapEngineExceptionThrowsIO
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.utils.FileStatus

import org.slf4j.LoggerFactory

trait InMemoryCatalogManagedTestClient extends AbstractCatalogManagedTestClient {
  private val logger = LoggerFactory.getLogger(classOf[InMemoryCatalogManagedTestClient])

  implicit class OptionalOps[T](opt: java.util.Optional[T]) {
    def toOption: Option[T] = if (opt.isPresent) Some(opt.get) else None
  }

  ///////////////////////
  // In-Memory Catalog //
  ///////////////////////

  case class CommitData(
      fileStatusOpt: Option[FileStatus],
      newProtocolOpt: Option[Protocol] = None,
      newMetadataOpt: Option[Metadata] = None)

  class TableData {
    var maxRatifiedVersion = -1L
    var latestProtocol: Protocol = null
    var latestMetadata: Metadata = null
    val commits = new java.util.HashMap[Long, CommitData]()

    def commit(commitVersion: Long, commitData: CommitData): Unit = {
      val expectedCommitVersion = maxRatifiedVersion + 1

      if (commitVersion != expectedCommitVersion) {
        // TODO: Implement all the right CommitException and Commit return types
        throw new RuntimeException(s"Commit failed. commitVersion $commitVersion did not equal " +
          s"expected next commit version $expectedCommitVersion")
      }

      commits.put(commitVersion, commitData)
      maxRatifiedVersion = commitVersion
      commitData.newProtocolOpt.foreach { p => latestProtocol = p }
      commitData.newMetadataOpt.foreach { m => latestMetadata = m }
    }

    override def toString: String = {
      val commitsStr = commits.asScala.toSeq
        .sortBy(_._1)
        .map { case (version, commitData) =>
          val fileInfo = commitData.fileStatusOpt.map(_.getPath)
          s"\t\tv$version: $fileInfo"
        }
        .mkString("\n")

      s"TableData\n\tmaxRatifiedVersion: $maxRatifiedVersion\n\tcommits:\n$commitsStr"
    }
  }

  /** Map from LOG_PATH to TABLE_DATA */
  val tables = new ConcurrentHashMap[String, TableData]()

  /////////////////////////
  // API Implementations //
  /////////////////////////

  override def forceCommit(payload: CommitPayload, commitType: CommitType): Unit = {
    logger.info(s"payload: $payload")

    assert(commitType == CommitType.STAGED, "Only CommitType.STAGED is supported.")

    if (payload.getCommitVersion == 0) {
      ensureLogDirAndStagedCommitsDirExists(payload.getLogPath)
    }

    val targetPath = CatalogManagedUtils.getStagedCommitFilePath(
      payload.getLogPath,
      payload.getCommitVersion)

    engine.getJsonHandler.writeJsonFileAtomically(targetPath, payload.getFinalizedActions, false)

    val tableData = tables.computeIfAbsent(payload.getLogPath, _ => new TableData())
    val fileStatus = FileStatus.of(targetPath) // TODO figure out the right way to do this
    val commitData = CommitData(
      Some(fileStatus),
      payload.getNewProtocolOpt.toOption,
      payload.getNewMetadataOpt.toOption)

    tableData.commit(payload.getCommitVersion, commitData)

    logger.info(s"Final state: ${toString()}")
  }

  override def publish(logPath: String, version: Long): Unit = {
    logger.info(s"publish: logPath=$logPath, version=$version")

    if (!tables.containsKey(logPath)) {
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

    logger.info(s"Final state: ${toString()}")
  }

  override def loadTable(
      path: String,
      versionToLoadOpt: Option[Long],
      timestampToLoadOpt: Option[Long] = None,
      injectProtocolMetadata: Boolean = true): ResolvedTable = {
    val logPath = s"$path/_delta_log"

    if (!tables.containsKey(logPath)) {
      throw new RuntimeException(s"Table entry for logPath $logPath does not exist")
    }

    val tableData = tables.get(logPath)

    var builder = TableManager.loadTable(path)

    if (versionToLoadOpt.isDefined) {
      builder = builder.atVersion(versionToLoadOpt.get)
    }

    if (injectProtocolMetadata) {
      builder = builder.withProtocolAndMetadata(tableData.latestProtocol, tableData.latestMetadata)
    }

    val parsedLogData = tableData
      .commits
      .asScala
      .filter(_._2.fileStatusOpt.isDefined)
      .map { case (_, commitData) => ParsedLogData.forFileStatus(commitData.fileStatusOpt.get) }
      .toList
      .asJava

    builder = builder.withLogData(parsedLogData)

    builder.build(engine)
  }

  override def toString: String = {
    val tableEntries = tables.asScala.toSeq.sortBy(_._1).map { case (logPath, tableData) =>
      s"\t$logPath: $tableData"
    }.mkString("\n")

    s"InMemoryCatalogManagedTestClient {\n$tableEntries\n}"
  }

  private def ensureLogDirAndStagedCommitsDirExists(logPath: String): Unit = {
    val stagedCommitsPath = CatalogManagedUtils.getStagedCommitsDirPath(logPath)

    // `mkdirs` will "Create a directory at the given path including parent directories".
    if (
      !wrapEngineExceptionThrowsIO(
        () => engine.getFileSystemClient.mkdirs(stagedCommitsPath),
        s"Creating directories for path $stagedCommitsPath")
    ) {
      throw new RuntimeException("Failed to create directory: $stagedCommitsPath")
    }
  }

  private def copyFile(srcPath: String, dstPath: String): Unit = {
    import java.io.File

    import org.apache.commons.io.FileUtils
    try {
      val srcFile =
        if (srcPath.startsWith("file:")) Paths.get(new URI(srcPath)).toFile else new File(srcPath)
      val dstFile =
        if (dstPath.startsWith("file:")) Paths.get(new URI(dstPath)).toFile else new File(dstPath)
      FileUtils.copyFile(srcFile, dstFile)
    } catch {
      case ex: Exception =>
        throw new IOException(s"Error copying file from $srcPath to $dstPath", ex)
    }
  }
}
