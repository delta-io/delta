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
import java.nio.file

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

  private val MILLIS_IN_DAY = 86400000

  ///////////////////////////////////////////////////////////////////////////
  // Public API Overrides
  ///////////////////////////////////////////////////////////////////////////

  override def isPartialWriteVisible(path: Path): Boolean = false

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
    if (fs.exists(resolvedPath) && !overwrite) {
      throw new java.nio.file.FileAlreadyExistsException(resolvedPath.toString)
    }

    // 2. Ensure N-1.json exists in the _delta_log
    val prevVersionPathOpt = getPreviousVersionPath(resolvedPath)
    if (prevVersionPathOpt.isDefined && !fs.exists(prevVersionPathOpt.get)) {
      val prevVersionPath = prevVersionPathOpt.get
      val prevVersionEntryOpt = lookupInCache(fs, prevVersionPath)

      if (prevVersionEntryOpt.isEmpty) {
        logWarning(s"While trying to write $path, the preceding _delta_log entry " +
          s"$prevVersionPath was not found and neither was the preceding external entry. " +
          s"This means that file $prevVersionPath has been lost.")
      } else {
        if (prevVersionEntryOpt.get.isComplete) {
          logWarning(s"External entry for file $prevVersionPath is marked as complete, " +
            s"but file does not exist in the _delta_log.")
        }

        logDebug(s"Previous file $prevVersionPath doesn't exist in the _delta_log. Fixing now.")
        tryFixTransactionLog(fs, prevVersionEntryOpt.get, ensureTargetFileExists = true)
      }
    }

    logDebug(s"Writing file: $path, $overwrite")

    // 3. Create temp file T(N)
    val tempPath = getTemporaryPath(resolvedPath)
    val fileLength = writeActions(fs, tempPath, actions)

    // 4. Commit to external cache entry E(N, complete = false)
    val logEntryMetadata =
      LogEntryMetadata(resolvedPath, fileLength, tempPath, overwrite, isComplete = false)

    try {
      writeCache(fs, logEntryMetadata, overwrite)
    } catch {
      case e: Throwable =>
        logError(s"${e.getClass.getName}: $e")
        throw e
    }

    // At this point, the commit is recoverable and any future errors are not passed to the user

    try {
      // 5. Copy T(N) into target N.json
      copyFile(fs, tempPath, resolvedPath)

      // 6. Commit to external cache entry E(N, complete = true)
      writeCache(fs, logEntryMetadata.complete(), overwrite = true)
    } catch {
      case e: Throwable =>
        logWarning(s"${e.getClass.getName}: ignoring recoverable error: $e")
    }

    // 7. Delete all external cache E(*, complete = false) entries older than 1 day
    val tempFilesToDelete = deleteFromCacheAllOlderThan(
      fs, resolvedPath.getParent, System.currentTimeMillis() - MILLIS_IN_DAY)

    // 8. Delete the temp file T(*) for each deleted external entry E(*, true) older than 1 day
    tempFilesToDelete.foreach(fs.delete(_, false))
  }

  ///////////////////////////////////////////////////////////////////////////
  // Protected Methods
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Write cache in exclusive way.
   *
   * @throws java.nio.file.FileAlreadyExistsException if path exists in cache and `overwrite` is
   *                                                  false
   */
  protected def writeCache(
      fs: FileSystem,
      logEntry: LogEntryMetadata,
      overwrite: Boolean = false): Unit

  /**
   * List the paths in the external cache that are lexicographically greater or equal to
   * (UTF-8 sorting) the given `resolvedPath`. The result should also be sorted by the file name.
   */
  protected def listFromCache(fs: FileSystem, resolvedPath: Path): Iterator[LogEntryMetadata]

  /**
   * Returns the LogEntryMetadata from the external cache with the matching resolvedPath if it
   * exists, else None
   */
  protected def lookupInCache(fs: FileSystem, resolvedPath: Path): Option[LogEntryMetadata]

  /**
   * Deletes all incomplete external cache LogEntryMetadata entries with modification times greater
   * than or equal to the given `expirationTime`, and returns their temp paths.
   */
  protected def deleteFromCacheAllOlderThan(
      fs: FileSystem,
      parentPath: Path,
      expirationTime: Long): Iterator[Path]

  ///////////////////////////////////////////////////////////////////////////
  // Private Helper Methods
  ///////////////////////////////////////////////////////////////////////////

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
      .toList // TODO remove when productionizing
      .filter { entry => !entry.isComplete }
      // ensureTargetFileExists = false as we can just read the temp file T(N) instead
      .map { entry => tryFixTransactionLog(fs, entry, ensureTargetFileExists = false) }

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
   * Returns the path for the previous version N-1 given the input version N, if such a path exists
   */
  def getPreviousVersionPath(path: Path): Option[Path] = {
    if (!FileNames.isDeltaFile(path)) return None

    val currVersion = FileNames.deltaVersion(path)

    if (currVersion > 0) {
      val prevVersion = currVersion - 1
      val parentPath = path.getParent
      Some(FileNames.deltaFile(parentPath, prevVersion))
    } else {
      None
    }
  }

  /**
   * Write file with actions under a specific path.
   */
  private def writeActions(fs: FileSystem,
      path: Path,
      actions: Iterator[String]): Long = {
    logDebug(s"writeActions to: $path")
    val stream = new CountingOutputStream(fs.create(path, true))
    actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
    stream.close()
    stream.getCount
  }

  /**
   * Copies file within filesystem with overwrite = true.
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
   * Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
   * In case both iterators have [[FileStatus]]s for the same file path, keep the one from
   * `iterWithPrecedence` and discard that from `iter`.
   */
  private def mergeFileIterators(
      iter: Iterator[FileStatus],
      iterWithPrecedence: Iterator[FileStatus]): Iterator[FileStatus] = {
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
   * Best-effort at ensuring consistency on file system according to the external cache. In case of
   * failure, will retry at most 3 times.
   *
   * Tries (in most cases) to copy the file in the LogEntryMetadata's `tempPath` to the target
   * LogEntryMetadata's `path` and then commit to the external cache.
   *
   * The specific cases when the `tempPath` will be copied into `path` are outlined here:
   * - Let O=entry.overwrite, and let T=target `path` file already exists
   * - O=false & T=false => perform copy
   * - O=false & T=true  => don't perform copy
   * - O=true  & T=false => perform copy
   * - O=true  & T=true  => perform copy
   *
   * @param ensureTargetFileExists whether this function should still successfully return even if,
   *                               after trying to copy, either
   *                               a) `entry.overwrite` is false and the target `path` file does not
   *                                   exist, or
   *                               b) `entry.overwrite` is true but the target `path` file was not
   *                                   successfully overwritten
   * @param commitFailureAcceptable whether this function should still successfully return even if
   *                                the external commit didn't succeed
   * @return the correct FileStatus from which to read the entry's data. If the copy was successful
   *         or the target `path` file already exists, this will be the entry's `path` (as a
   *         FileStatus). Else, returns the entry's `tempPath` (as a FileStatus).
   * @throws RuntimeException if `commitFailureAcceptable` is true and after 3 failed attempts,
   *                          external entry E(N, complete=true) was not committed and
   * @throws RuntimeException if `ensureTargetFileExists` is true and after 3 failed attempts either
   *                          a) `entry.overwrite` is false and the target `path` file does not
   *                              exist
   *                          b) `entry.overwrite` is true and the target `path` file was not
   *                              successfully overwritten
   */
  private def tryFixTransactionLog(
      fs: FileSystem,
      entry: LogEntryMetadata,
      ensureTargetFileExists: Boolean,
      commitFailureAcceptable: Boolean = true): FileStatus = {
    logDebug(s"Try to fix: ${entry.path}")
    val completedEntry = entry.complete()
    val performCopy = entry.overwrite || !fs.exists(entry.path)
    var copySuccess = !performCopy

    for (i <- 0 until 3) {
      try {
        // Both copyFile and writeCache have overwrite=true, so we don't expect any
        // FileAlreadyExistsExceptions from those calls

        if (performCopy && !copySuccess) {
          copyFile(fs, entry.tempPath, entry.path)
          copySuccess = true
        }

        writeCache(fs, completedEntry, overwrite = true)

        return completedEntry.asFileStatus(fs)
      } catch {
        case NonFatal(e) =>
          logWarning(s"${e.getClass.getName}: Ignoring error while fixing. Iteration $i of 3. $e")
      }
    }

    if (!commitFailureAcceptable) {
      throw new RuntimeException(
        s"External commit failure while trying to fix transaction log for path ${entry.path}")
    } else if (ensureTargetFileExists && !copySuccess) {
      throw new RuntimeException(
        s"Copy failure while trying to fix the transaction log for path ${entry.path}")
    } else if (copySuccess) {
      completedEntry.asFileStatus(fs)
    } else {
      entry.tempPathAsFileStatus(fs)
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
    new Path(s"${path.getParent}/.temp/${path.getName}.$uuid")
  }
}

class CachedFileStatus(
    length: Long,
    isdir: Boolean,
    block_replication: Int,
    block_size: Long,
    modification_time: Long,
    path: Path
) extends FileStatus(length, isdir, block_replication, block_size, modification_time, path)

/**
 * The file metadata to be stored in the external db
 */
case class LogEntryMetadata(
    path: Path,
    length: Long,
    tempPath: Path,
    overwrite: Boolean,
    isComplete: Boolean,
    modificationTime: Long = System.currentTimeMillis()) {

  def complete(): LogEntryMetadata = {
    LogEntryMetadata(this.path, this.length, this.tempPath, this.overwrite, isComplete = true)
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
