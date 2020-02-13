/*
 * Copyright 2019 Databricks, Inc.
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
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.util.FileNames
import com.google.common.cache.CacheBuilder
import com.google.common.io.CountingOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.http.client.utils.URLEncodedUtils

import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.metering.DeltaLogging
import java.sql.{DriverManager, Connection, ResultSet, SQLException}


abstract class BaseExternalLogStore (
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends HadoopFileSystemLogStore(sparkConf, hadoopConf)
    with DeltaLogging {

  protected def resolved(path: Path): (FileSystem, Path) = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    (fs, resolvedPath)
  }

  protected def getPathKey(resolvedPath: Path): Path = {
    stripUserInfo(resolvedPath)
  }

  protected def stripUserInfo(path: Path): Path = {
    val uri = path.toUri
    val newUri = new URI(
      uri.getScheme,
      null,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment)
    new Path(newUri)
  }

  /**
   * Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
   * In case both iterators have [[FileStatus]]s for the same file path, keep the one from
   * `iterWithPrecedence` and discard that from `iter`.
   */
  protected def mergeFileIterators(
      iter: Iterator[FileStatus],
      iterWithPrecedence: Iterator[FileStatus]): Iterator[FileStatus] = {
    val result = (
      iter.map(f => (f.getPath, f)).toMap
      ++ iterWithPrecedence.map(f => (f.getPath, f)))
      .values
      .toSeq
      .sortBy(_.getPath.getName)
    result.iterator
  }

  protected def writeActions(fs: FileSystem, path: Path, actions: Iterator[String]): Long = {
    logDebug(s"writeActions to: $path")
    val stream = new CountingOutputStream(fs.create(path, true))
    actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
    stream.close()
    stream.getCount()
  }

  protected def delete_file(fs: FileSystem, path: Path) = {
    logDebug(s"delete file: $path")
    fs.delete(path, false)
  }


  /**
   * List files starting from `resolvedPath` (inclusive) in the same directory.
   */

  override def listFrom(path: Path): Iterator[FileStatus] = {
    logDebug(s"listFrom path: ${path}")
    val (fs, resolvedPath) = resolved(path)
    listFrom(fs, resolvedPath)
  }

  /**
   * List fitles starting from `resolvedPath` (inclusive) in the same directory, which merges
   * the file system list and the db list
   */

  def listFrom(fs: FileSystem, resolvedPath: Path): Iterator[FileStatus] = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }

    val listedFromFs =
      fs.listStatus(parentPath).filter( path => (path.getPath.getName >= resolvedPath.getName) )
    listedFromFs.iterator.map(entry => s"fs item: ${entry.getPath}").foreach(x => logDebug(x))

    val listedFromDB = listFromInternal(fs, resolvedPath).toList
    listedFromDB.iterator.map(entry => s"db item: ${entry.getPath}").foreach(x => logDebug(x))

    mergeFileIterators(listedFromFs.iterator, listedFromDB.iterator)
  }

  def listFromInternal(fs: FileSystem, resolvedPath: Path): Iterator[FileStatus] = {
    val parentPath = resolvedPath.getParent
    fixTransactionLog(fs, parentPath, resolvedPath)
    return listLogEntriesFrom(fs, parentPath, resolvedPath)
      .filter(_.isComplete)
      .map(item => item.asFileStatus(fs))
  }

  private def fixTransactionLog(
      fs: FileSystem,
      parentPath: Path,
      resolvedPath: Path
  ) = {
    listLogEntriesFrom(fs, parentPath, resolvedPath)
    .filter(!_.isComplete)
    .foreach(item => {
        assert(item.tempPath.isDefined, "tempPath must be present for incomplete entries")
        logDebug(s"fixing $item")
        // TODO copy streams instead of copying line by line
        val tempPath = item.tempPath.get
        try {
          val length = writeActions(fs, item.path, read(tempPath).iterator)
          putLogEntry(LogEntry(item.path, None, length, System.currentTimeMillis(), true), true)
          logDebug(s"delete ${item.tempPath}")
          delete_file(fs, tempPath)
        } catch {
          case e: FileNotFoundException =>
            logWarning(s"ignoring $e while fixing (already done?)")
        }
    })
  }

  protected def putLogEntry(
    logEntry: LogEntry, overwrite: Boolean): Unit

  protected def listLogEntriesFrom(
    fs: FileSystem,
    parentPath: Path,
    fromPath: Path
  ): Iterator[LogEntry]

  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val (fs, resolvedPath) = resolved(path)
    val parentPath = resolvedPath.getParent
    val lockedPath = getPathKey(resolvedPath)
    var stream: FSDataOutputStream = null
    logDebug(s"write path: ${path}, ${overwrite}")

    val uuid = java.util.UUID.randomUUID().toString
    val temp_path = new Path(s"$parentPath/temp/${path.getName}.$uuid")

    val actions_seq = actions.toSeq

    val length = writeActions(fs, temp_path, actions_seq.iterator)

    try {
      putLogEntry(
        LogEntry(resolvedPath, Some(temp_path), length, System.currentTimeMillis(), false),
        overwrite
      )
    } catch {
      case e: Throwable =>
        logError(s"${e.getClass.getName}: $e")
        delete_file(fs, temp_path)
        throw e
    }
    try {
      val actions_length = writeActions(fs, resolvedPath, actions_seq.iterator)
      putLogEntry(
        LogEntry(resolvedPath, None, actions_length, System.currentTimeMillis(), true),
        true
      )
      delete_file(fs, temp_path)
    } catch {
      case e: Throwable => logWarning(s"${e.getClass.getName}: ignoring recoverable error: $e")
    }
  }

  override def isPartialWriteVisible(path: Path): Boolean = false

  override def invalidateCache(): Unit = {
  }
}

/**
 * The file metadata to be stored in the external db
 */

case class LogEntry(
  path: Path,
  tempPath: Option[Path],
  length: Long,
  modificationTime: Long,
  isComplete: Boolean
) {
  def asFileStatus(fs: FileSystem): FileStatus = {
    new FileStatus(
      length,
      false,
      1,
      fs.getDefaultBlockSize(path),
      modificationTime,
      path
    )
  }
}
