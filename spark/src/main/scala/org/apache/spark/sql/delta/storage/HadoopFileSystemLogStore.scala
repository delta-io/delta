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

package org.apache.spark.sql.delta.storage

import java.io.{BufferedReader, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaErrors
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, FSDataInputStream, Path}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * Default implementation of [[LogStore]] for Hadoop [[FileSystem]] implementations.
 */
abstract class HadoopFileSystemLogStore(
    sparkConf: SparkConf,
    hadoopConf: Configuration) extends LogStore {

  def this(sc: SparkContext) = this(sc.getConf, sc.hadoopConfiguration)

  protected def getHadoopConfiguration: Configuration = {
    // scalastyle:off deltahadoopconfiguration
    SparkSession.getActiveSession.map(_.sessionState.newHadoopConf()).getOrElse(hadoopConf)
    // scalastyle:on deltahadoopconfiguration
  }

  override def read(path: Path): Seq[String] = {
    read(path, getHadoopConfiguration)
  }

  override def read(path: Path, hadoopConf: Configuration): Seq[String] = {
    readStream(open(path, hadoopConf))
  }

  override def readAsIterator(path: Path): ClosableIterator[String] = {
    readAsIterator(path, getHadoopConfiguration)
  }

  override def readAsIterator(path: Path, hadoopConf: Configuration): ClosableIterator[String] =
    readStreamAsIterator(open(path, hadoopConf))

  private def open(path: Path, hadoopConf: Configuration): FSDataInputStream =
    path.getFileSystem(hadoopConf).open(path)

  private def readStream(stream: FSDataInputStream): Seq[String] = {
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).asScala.map(_.trim).toSeq
    } finally {
      stream.close()
    }
  }

  private def readStreamAsIterator(stream: FSDataInputStream): ClosableIterator[String] = {
    val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
    new LineClosableIterator(reader)
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    listFrom(path, getHadoopConfiguration)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): Iterator[FileStatus] = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) {
      throw DeltaErrors.fileOrDirectoryNotFoundException(s"${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator
  }

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    resolvePathOnPhysicalStorage(path, getHadoopConfiguration)
  }

  override def resolvePathOnPhysicalStorage(path: Path, hadoopConf: Configuration): Path = {
    path.getFileSystem(hadoopConf).makeQualified(path)
  }

  /**
   * An internal write implementation that uses FileSystem.rename().
   *
   * This implementation should only be used for the underlying file systems that support atomic
   * renames, e.g., Azure is OK but HDFS is not.
   */
  @deprecated("call the method that asks for a Hadoop Configuration object instead")
  protected def writeWithRename(
      path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    writeWithRename(path, actions, overwrite, getHadoopConfiguration)
  }

  /**
   * An internal write implementation that uses FileSystem.rename().
   *
   * This implementation should only be used for the underlying file systems that support atomic
   * renames, e.g., Azure is OK but HDFS is not.
   */
  protected def writeWithRename(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    val fs = path.getFileSystem(hadoopConf)

    if (!fs.exists(path.getParent)) {
      throw DeltaErrors.fileOrDirectoryNotFoundException(s"${path.getParent}")
    }
    if (overwrite) {
      val stream = fs.create(path, true)
      try {
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      } finally {
        stream.close()
      }
    } else {
      if (fs.exists(path)) {
        throw DeltaErrors.fileAlreadyExists(path.toString)
      }
      val tempPath = createTempPath(path)
      var streamClosed = false // This flag is to avoid double close
      var renameDone = false // This flag is to save the delete operation in most of cases.
      val stream = fs.create(tempPath)
      try {
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
        stream.close()
        streamClosed = true
        try {
          if (fs.rename(tempPath, path)) {
            renameDone = true
          } else {
            if (fs.exists(path)) {
              throw DeltaErrors.fileAlreadyExists(path.toString)
            } else {
              throw DeltaErrors.cannotRenamePath(tempPath.toString, path.toString)
            }
          }
        } catch {
          case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
            throw DeltaErrors.fileAlreadyExists(path.toString)
        }
      } finally {
        if (!streamClosed) {
          stream.close()
        }
        if (!renameDone) {
          fs.delete(tempPath, false)
        }
      }
    }
  }

  protected def createTempPath(path: Path): Path = {
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}.tmp")
  }

  override def invalidateCache(): Unit = {}
}
