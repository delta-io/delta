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

/**
* Used so that we can use an external store child implementation
* to provide the mutual exclusion that the cloud store,
* e.g. s3, is missing.
*/
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
   * Method returns completed [[ExternalCommitEntry]]
   */

  private def fixDeltaLog(fs: FileSystem, entry: ExternalCommitEntry): Unit = {
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
          fixDeltaLogCopyTempFile(fs, entry.absoluteTempPath, entry.absoluteJsonPath);
          copied = true;
        }
        fixDeltaLogPutCompleteDbEntry(entry);
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
   * The following four methods are extracted for testing purposes
   * so we can more easily inject errors and test for failures.
   */

  protected def writeCopyTempFile(fs: FileSystem, src: Path, dst: Path) = {
    copyFile(fs, src, dst)
  }

  protected def writePutCompleteDbEntry(entry: ExternalCommitEntry): Unit = {
    putExternalEntry(entry.asComplete(), overwrite = true)
  }

  protected def fixDeltaLogCopyTempFile(fs: FileSystem, src: Path, dst: Path): Unit = {
    copyFile(fs, src, dst);
  }

  protected def fixDeltaLogPutCompleteDbEntry(entry: ExternalCommitEntry): Unit = {
    putExternalEntry(entry.asComplete(), overwrite = true);
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
    s".tmp/${path.getName}.$uuid"
  }

  protected def getTablePath(path: Path): Path = {
    var tablePath = path.getParent;
    if (tablePath.getName == "_delta_log") {
      // we do this conditionally as tests in LogStoreSuiet do not
      // create _delta_log directory
      tablePath = tablePath.getParent
    }
    return tablePath
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
    val tablePath = getTablePath(resolvedPath)
    var entry = getLatestExternalEntry(tablePath)
    entry.foreach(fixDeltaLog(fs, _))

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

    val tablePath = getTablePath(resolvedPath)

    if (FileNames.isDeltaFile(path)) {
      val version = FileNames.deltaVersion(path)

      if (version > 0) {
        val prevVersion = version - 1
        val prevPath = FileNames.deltaFile(tablePath, prevVersion)
        getExternalEntry(tablePath, prevPath) match {
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
        getExternalEntry(tablePath, path) match {
          case Some(entry) =>
            if (entry.complete && !fs.exists(path)) {
              throw new java.nio.file.FileSystemException(
                s"Old entries for ${tablePath} still exist in the database"
              )
            }
          case None => ;
        }
      }

    }

    val tempPath = createTemporaryPath(resolvedPath)

    val entry =
      ExternalCommitEntry(
        tablePath,
        resolvedPath.getName,
        tempPath,
        false,
        None
      )

    logDebug(s"write: writing to temporary file ${tempPath}")
    writeActions(fs, entry.absoluteTempPath(), actions);

    // commit to dynamodb E(N, false) and exclude commitTime
    logDebug(s"write incomplete entry to dynamodb ${entry}")
    putExternalEntry(entry, overwrite = false)

    try {
      writeCopyTempFile(fs, entry.absoluteTempPath(), resolvedPath)
      // Commit (with overwrite=true) to DynamoDB entry E(N, true)
      // with commitTime attribute in epoch seconds
      writePutCompleteDbEntry(entry)
    } catch {
      case e: Throwable =>
        logInfo(s"${e.getClass.getName}: ignoring recoverable error: $e")
    }
  }

  /*
   * Write to db in exclusive way.
   * Method should throw @java.nio.file.FileAlreadyExistsException if path exists in cache.
   */
  protected def putExternalEntry(
      ExternalCommitEntry: ExternalCommitEntry,
      overwrite: Boolean = false
  ): Unit

  protected def getExternalEntry(
      tablePath: Path,
      jsonPath: Path
  ): Option[ExternalCommitEntry]

  protected def getLatestExternalEntry(
      tablePath: Path
  ): Option[ExternalCommitEntry]

  override def isPartialWriteVisible(path: Path): Boolean = false
}

/**
 * The file metadata to be stored in the external db
 * @param tablePath path to the root of the table (path/to/my/table)
 * @param tempPath relative path to the `tablePath`
 * @param commitTime in epoch seconds
 */
case class ExternalCommitEntry(
    tablePath: Path,
    fileName: String,
    tempPath: String,
    complete: Boolean,
    commitTime: Option[Long]
) {
  def asComplete(): ExternalCommitEntry = {
    ExternalCommitEntry(
      tablePath,
      fileName,
      tempPath,
      true,
      Some(System.currentTimeMillis() / 1000)
    )
  }
  def absoluteJsonPath(): Path = new Path(new Path(tablePath, "_delta_log"), fileName)
  def absoluteTempPath(): Path = new Path(tablePath, tempPath)
}
