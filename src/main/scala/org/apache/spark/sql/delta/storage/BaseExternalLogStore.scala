/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.storage

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileSystemException

import com.google.common.io.CountingOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames

abstract class BaseExternalLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends HadoopFileSystemLogStore(sparkConf, hadoopConf)
    with DeltaLogging {

  /**
   * Delete file from filesystem.
   * @param fs reference to [[FileSystem]]
   * @param path path to delete
   * @return Boolean true if delete is successful else false
   */
  private def deleteFile(fs: FileSystem, path: Path): Boolean = {
    logDebug(s"delete file: $path")
    fs.delete(path, false)
  }

  private def copyFile(fs: FileSystem, src: Path, dst: Path) {
    logDebug(s"copy file: $src -> $dst")
    val input_stream = fs.open(src)
    val output_stream = fs.create(dst, true)
    try {
      IOUtils.copy(input_stream, output_stream)
    } finally {
      output_stream.close()
      input_stream.close()
    }
  }

  /**
   * Check if the path is an initial version of a Delta log.
   */
  private def isInitialVersion(path: Path): Boolean = {
    FileNames.isDeltaFile(path) && FileNames.deltaVersion(path) == 0L
  }

  /**
   * Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
   * In case both iterators have [[FileStatus]]s for the same file path, keep the one from
   * `iterWithPrecedence` and discard that from `iter`.
   */
  private def mergeFileIterators(
    iter: Iterator[FileStatus],
    iterWithPrecedence: Iterator[FileStatus]
  ): Iterator[FileStatus] = {
    val result = (iter.map(f => (f.getPath, f)).toMap
      ++ iterWithPrecedence.map(f => (f.getPath, f))).values.toSeq
      .sortBy(_.getPath.getName)
    result.iterator
  }

  private def resolved(path: Path): (FileSystem, Path) = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    (fs, resolvedPath)
  }

  private def stripUserInfo(path: Path): Path = {
    val uri = path.toUri
    val newUri = new URI(
      uri.getScheme,
      null,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment
    )
    new Path(newUri)
  }

  /**
   * Method for assuring consistency on filesystem according to the external cache.
   * Method try to rewrite TransactionLog from temporary path if it not exists.
   * Method returns completed [[LogEntryMetadata]]
   */

   private def tryFixTransactionLog(fs: FileSystem, entry: LogEntryMetadata): LogEntryMetadata = {
    logDebug(s"Try to fix: ${entry.path}")
    val tempPath = entry.tempPath.get
    try {
      copyFile(fs, tempPath, entry.path)
      val completedEntry = entry.complete()
      writeCache(fs, completedEntry, overwrite = true)
      completedEntry
    } catch {
      case e: FileNotFoundException =>
        logWarning(s"ignoring $e while fixing (already done?)")
        entry
    }
  }

  /**
   * Returns path stripped user info.
   */
  protected def getPathKey(resolvedPath: Path): Path = {
    stripUserInfo(resolvedPath)
  }

  /**
   * Generate temporary path for TransactionLog.
   */
  protected def getTemporaryPath(path: Path): Path = {
    val uuid = java.util.UUID.randomUUID().toString
    new Path(s"${path.getParent}/.${path.getName}.$uuid.temp")
  }

  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory, which merges
   * the file system list and the db list
   */
  protected def listFrom(
    fs: FileSystem,
    resolvedPath: Path,
    useCache: Boolean = true): Iterator[FileStatus] = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }

    val listedFromFs =
      fs.listStatus(parentPath)
        .filter(path => !path.getPath.getName.endsWith(".temp"))
        .filter(path => path.getPath.getName >= resolvedPath.getName)

    if (!useCache) {
      return listedFromFs.iterator
    }

    val listedFromDB = listFromCache(fs, resolvedPath)
      .toList
      .map(entry => if (!entry.isComplete) tryFixTransactionLog(fs, entry) else entry)
      .map(entry => entry.asFileStatus(fs))

    // for debug
    listedFromFs.iterator
      .map(entry => s"fs item: ${entry.getPath}")
      .foreach(x => logDebug(x))

    listedFromDB.iterator
      .map(entry => s"db item: ${entry.getPath}")
      .foreach(x => logDebug(x))
    // end debug

    mergeFileIterators(listedFromDB.iterator, listedFromFs.iterator)
  }

  /**
   * Write file with actions under a specific path.
   */
  protected def writeActions(fs: FileSystem,
    path: Path,
    actions: Iterator[String]): Long = {
    logDebug(s"writeActions to: $path")
    val stream = new CountingOutputStream(fs.create(path, true))
    actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
    stream.close()
    stream.getCount
  }


  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory.
   */
  override def listFrom(path: Path): Iterator[FileStatus] = {
    logDebug(s"listFrom path: ${path}")
    val (fs, resolvedPath) = resolved(path)
    listFrom(fs, resolvedPath)
  }

  override def write(path: Path,
    actions: Iterator[String],
    overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath) = resolved(path)

    logDebug(s"write path: ${path}, ${overwrite}")

    val tempPath = getTemporaryPath(resolvedPath)
    val actionsSeq = actions.toSeq
    val fileLength = writeActions(fs, tempPath, actionsSeq.iterator)

    val logEntryMetadata =
      LogEntryMetadata(resolvedPath, fileLength, Some(tempPath))

    try {
      writeCache(fs, logEntryMetadata, overwrite)
    } catch {
      case e: Throwable =>
        logError(s"${e.getClass.getName}: $e")
        throw e
    }
    try {
      writeActions(fs, resolvedPath, actionsSeq.iterator)
      writeCache(fs, logEntryMetadata.complete(), overwrite = true)
    } catch {
      case e: Throwable =>
        logWarning(s"${e.getClass.getName}: ignoring recoverable error: $e")
    }
  }

  /*
   * Write cache in exclusive way.
   * Method should throw @java.nio.file.FileAlreadyExistsException if path exists in cache.
   */
  protected def writeCache(
    fs: FileSystem,
    logEntry: LogEntryMetadata,
    overwrite: Boolean = false): Unit

  protected def cleanCache(p: LogEntryMetadata => Boolean)

  protected def listFromCache(fs: FileSystem, resolvedPath: Path): Iterator[LogEntryMetadata]

  override def isPartialWriteVisible(path: Path): Boolean = false
}

class CachedFileStatus(
  length: Long,
  isdir: Boolean,
  block_replication: Int,
  blocksize: Long,
  modification_time: Long,
  path: Path
) extends FileStatus(length, isdir, block_replication, blocksize, modification_time, path)

/**
 * The file metadata to be stored in the external db
 */
case class LogEntryMetadata(path: Path,
  length: Long,
  tempPath: Option[Path] = None,
  isComplete: Boolean = false,
  modificationTime: Long = System.currentTimeMillis()
) {
  def complete(): LogEntryMetadata = {
    LogEntryMetadata(this.path, this.length, this.tempPath, true)
  }

  def asFileStatus(fs: FileSystem): FileStatus = {
    new CachedFileStatus(
      length,
      false,
      1,
      fs.getDefaultBlockSize(path),
      modificationTime,
      path
    )
  }
}
