/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.storage

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8

import scala.util.control.NonFatal

import com.google.common.io.CountingOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.storage.HadoopFileSystemLogStore
import org.apache.spark.sql.delta.util.FileNames

abstract class BaseExternalLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends HadoopFileSystemLogStore(sparkConf, hadoopConf) with DeltaLogging {

  import BaseExternalLogStore._

  /**
   * Delete file from filesystem.
   *
   * @param fs   reference to [[FileSystem]]
   * @param path path to delete
   * @return Boolean true if delete is successful else false
   */
  private def deleteFile(fs: FileSystem, path: Path): Boolean = {
    logDebug(s"delete file: $path")
    fs.delete(path, false)
  }

  /**
   * Copies file within filesystem.
   *
   * Ensures that the `src` file is either entirely copied to the `dst` file, or not at all.
   *
   * @param fs  reference to [[FileSystem]]
   * @param src path to source file
   * @param dst path to destination file
   */
  private def copyFile(fs: FileSystem, src: Path, dst: Path) {
    logDebug(s"copy file: $src -> $dst")
    val input_stream = fs.open(src)
    val output_stream = fs.create(dst, true)
    try {
      IOUtils.copy(input_stream, output_stream)
      output_stream.close()
    } finally {
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
    val result = (
        iter.map(f => (f.getPath, f)).toMap ++
        iterWithPrecedence.map(f => (f.getPath, f))
      )
      .values
      .toSeq
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
   * Best-effort at assuring consistency on file system according to the external cache.
   * Tries to rewrite TransactionLog entry from temporary path if it does not exists.
   * Will retry at most 3 times.
   *
   * @returns the correct FileStatus to read the entry's data from
   */
  private def tryFixTransactionLog(fs: FileSystem, entry: LogEntryMetadata): FileStatus = {
    logDebug(s"Try to fix: ${entry.path}")
    val completedEntry = entry.complete()

    for (i <- 0 until 3) {
      try {
        if (!fs.exists(entry.path)) {
          copyFile(fs, entry.tempPath, entry.path)
        }
        writeCache(fs, completedEntry, overwrite = true)
        return completedEntry.asFileStatus(fs)
      } catch {
        case NonFatal(e) =>
          logWarning(s"Ignoring $e while fixing. Iteration $i of 3.")
      }
    }

    entry.tempPathAsFileStatus(fs)
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
    new Path(s"${path.getParent}/.temp/${path.getName}.$uuid")
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
      .map { entry =>
        if (!entry.isComplete) tryFixTransactionLog(fs, entry) else entry.asFileStatus(fs)
      }

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

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath) = resolved(path)

    // 1. If N.json already exists in the _delta_log, exit early
    if (fs.exists(resolvedPath)) {
      logDebug(s"$resolvedPath already exists. Returning early.")
    }

    // 2. Else, ensure N-1.json exists in the _delta_log
    val prevVersionPath = getPreviousVersionPath(resolvedPath)
    if (!prevVersionPath.exists(fs.exists)) {
      val prevVersionEntry = lookup(fs, prevVersionPath.get)

      if (prevVersionEntry.isEmpty) {
        logError(s"While trying to write $path, the preceding _delta_log entry " +
          s"${prevVersionPath.get} was not found and neither was the preceding external entry. " +
          s"This means that file ${prevVersionPath.get} has been lost.")
      }

      logDebug(s"Previous file $prevVersionPath doesn't exist in the _delta_log. Fixing now.")
      tryFixTransactionLog(fs, prevVersionEntry.get)
    }

    logDebug(s"Writing file: $path, $overwrite")

    val tempPath = getTemporaryPath(resolvedPath)
    val fileLength = writeActions(fs, tempPath, actions)

    val logEntryMetadata = LogEntryMetadata(resolvedPath, fileLength, tempPath, isComplete = false)

    try {
      writeCache(fs, logEntryMetadata, overwrite)
    } catch {
      case e: Throwable =>
        logError(s"${e.getClass.getName}: $e")
        throw e
    }

    try {
      copyFile(fs, tempPath, resolvedPath)
      writeCache(fs, logEntryMetadata.complete(), overwrite = true)
    } catch {
      case e: Throwable =>
        logWarning(s"${e.getClass.getName}: ignoring recoverable error: $e")
    }

    // TODO: delete old DBB entries and temp files
  }

  /*
   * Write cache in exclusive way.
   * Method should throw @java.nio.file.FileAlreadyExistsException if path exists in cache.
   */
  protected def writeCache(
      fs: FileSystem,
      logEntry: LogEntryMetadata,
      overwrite: Boolean = false): Unit

  protected def listFromCache(fs: FileSystem, resolvedPath: Path): Iterator[LogEntryMetadata]

  protected def lookup(fs: FileSystem, resolvedPath: Path): Option[LogEntryMetadata]

  override def isPartialWriteVisible(path: Path): Boolean = false
}

object BaseExternalLogStore {

  /**
   * Returns the path for the previous version N-1 given the input version N, if such a path exists
   */
  def getPreviousVersionPath(currVersionPath: Path): Option[Path] = {
    val currVersion = FileNames.deltaVersion(currVersionPath)

    if (currVersion > 0) {
      val prevVersion = currVersion - 1
      val parentPath = currVersionPath.getParent
      Some(FileNames.deltaFile(parentPath, prevVersion))
    } else {
      None
    }
  }
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
case class LogEntryMetadata(
    path: Path,
    length: Long,
    tempPath: Path,
    isComplete: Boolean,
    modificationTime: Long = System.currentTimeMillis()) {

  def complete(): LogEntryMetadata = {
    LogEntryMetadata(this.path, this.length, this.tempPath, isComplete = true)
  }

  def tempPathAsFileStatus(fs: FileSystem): FileStatus = {
    new CachedFileStatus(
      length,
      false,
      1,
      fs.getDefaultBlockSize(tempPath),
      modificationTime,
      tempPath
    )
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
