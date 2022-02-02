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
import java.nio.file.FileSystemException

import com.google.common.io.CountingOutputStream
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.spark.sql.delta.util.FileNames
import org.apache.spark.sql.delta.storage.HadoopFileSystemLogStore

abstract class BaseExternalLogStore(
    sparkConf: SparkConf,
    hadoopConf: Configuration
) extends HadoopFileSystemLogStore(sparkConf, hadoopConf)
    with DeltaLogging {

  /**
   * Copies file within filesystem
   * @param fs reference to [[FileSystem]]
   * @param src path to source file
   * @param dst path to destination file
   */
  private def copyFile(fs: FileSystem, src: Path, dst: Path): Unit = {
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

  private def resolvePath(
      path: Path,
      hadoopConf: Configuration
  ): (FileSystem, Path) = {
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
   * Method tries to rewrite TransactionLog entry from temporary path if it does not exists.
   * Method returns completed [[LogEntry]]
   */

  private def fixDeltaLog(fs: FileSystem, entry: LogEntry): Unit = {
    if (entry.complete) {
      return
    }
    var retry = 0;
    var copied = false;

    while (true) {
      logDebug(
        s"${if (retry > 0) "re" else ""}trying to fix: ${entry.fileName}"
      )
      try {
        if (!copied && !fs.exists(entry.absoluteJsonPath)) {
          onFixDeltaLogCopyTempFile()
          copyFile(fs, entry.absoluteTempPath, entry.absoluteJsonPath());
          copied = true;
        }
        onFixDeltaLogPutDbEntry();
        putDbEntry(entry.asComplete(), overwrite = true);
        logInfo(s"fixed ${entry.fileName}")
        return
      } catch {
        case e: Throwable =>
          logInfo(s"${e.getClass.getName}: $e")
          if (retry >= 3) throw e
      }
      retry += 1;
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
  protected def createTemporaryPath(path: Path): String = {
    val uuid = java.util.UUID.randomUUID().toString
    s".tmp/${path.getName}.$uuid" // Q: remove path.getName part?
  }
  override def listFrom(path: Path): Iterator[FileStatus] = {
    listFrom(path, getHadoopConfiguration)
  }

  override def listFrom(
      path: Path,
      hadoopConf: Configuration
  ): Iterator[FileStatus] = {
    logDebug(s"listFrom path: ${path}")
    val (fs, resolvedPath) = resolvePath(path, hadoopConf)
    var entry = getLatestDbEntry(resolvedPath.getParent)
    entry.foreach(fixDeltaLog(fs, _))

    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }

    super.listFrom(path, hadoopConf)
  }

  /**
   * Write file with actions under a specific path.
   */
  protected def writeActions(
      fs: FileSystem,
      path: Path,
      actions: Iterator[String]
  ): Unit = {
    logDebug(s"writeActions to: $path")
    val stream = fs.create(path, true)
    actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
    stream.close()
  }

  def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean = false
  ): Unit = {
    write(path, actions, overwrite, getHadoopConfiguration)
  }

  override def write(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration
  ): Unit = {
    val (fs, resolvedPath) = resolvePath(path, hadoopConf)

    if (overwrite) {
      return writeActions(fs, path, actions);
    };

    if (FileNames.isDeltaFile(path)) {
      val parentPath = resolvedPath.getParent
      val version = FileNames.deltaVersion(path)

      if (version > 0) {
        val prevVersion = version - 1
        val prevPath = FileNames.deltaFile(parentPath, prevVersion)
        getDbEntry(prevPath) match {
          case Some(entry) => fixDeltaLog(fs, entry)
          case None =>
            if (!fs.exists(prevPath)) {
              throw new java.nio.file.FileSystemException(
                s"previous commit ${prevPath} doesn't exist"
              )
            }
          // previous commit exists in fs but not in dynamodb
        }
      } else {
        getDbEntry(path) match {
          case Some(entry) =>
            if (entry.complete && !fs.exists(path)) {
              throw new java.nio.file.FileSystemException(
                s"Old entries for ${parentPath} still exist in the database"
              )
            }
          case None => ;
        }
      }

    }

    val tempPath = createTemporaryPath(resolvedPath)

    val entry =
      LogEntry(
        resolvedPath.getParent,
        resolvedPath.getName,
        tempPath,
        false,
        None
      )

    logDebug(s"write: writing to temporary file ${tempPath}")
    writeActions(fs, entry.absoluteTempPath(), actions);

    // commit to dynamodb E(N, false) and exclude commitTime
    logDebug(s"write incomplete entry to dynamodb ${entry}")
    putDbEntry(entry, overwrite = false)

    try {
      onWriteCopyTempFile();
      copyFile(fs, entry.absoluteTempPath(), resolvedPath)
      // Commit (with overwrite=true) to DynamoDB entry E(N, true)
      // with commitTime attribute in epoch seconds
      onWritePutDbEntry();
      putDbEntry(entry.asComplete(), overwrite = true)
    } catch {
      case e: Throwable =>
        logInfo(s"${e.getClass.getName}: ignoring recoverable error: $e")
    }
  }

  protected def onWriteCopyTempFile(): Unit = {}
  protected def onWritePutDbEntry(): Unit = {}
  protected def onFixDeltaLogCopyTempFile(): Unit = {}
  protected def onFixDeltaLogPutDbEntry(): Unit = {}

  /*
   * Write to db in exclusive way.
   * Method should throw @java.nio.file.FileAlreadyExistsException if path exists in cache.
   */
  protected def putDbEntry(
      logEntry: LogEntry,
      overwrite: Boolean = false
  ): Unit

  protected def getDbEntry(
      absoluteJsonPath: Path
  ): Option[LogEntry]

  protected def getLatestDbEntry(
      tablePath: Path
  ): Option[LogEntry]

  override def isPartialWriteVisible(path: Path): Boolean = false
}

/**
 * The file metadata to be stored in the external db
 */
case class LogEntry(
    tablePath: Path, // Q: should it be path to delta_log or parent one?
    fileName: String,
    tempPath: String,
    complete: Boolean,
    commitTime: Option[Long]
) {
  def asComplete(): LogEntry = {
    LogEntry(
      tablePath,
      fileName,
      tempPath,
      true,
      Some(System.currentTimeMillis() / 1000)
    )
  }
  def absoluteJsonPath(): Path = new Path(tablePath, fileName)
  def absoluteTempPath(): Path = new Path(tablePath, tempPath)
}
