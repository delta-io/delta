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

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, RawLocalFileSystem}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
 * Used to read and write individual deltas to/from the distributed storage system.
 * Internally, the correctness of the optimistic concurrency control relies
 * on mutual exclusion for writing a given log delta.
 */
trait LogStore {

  final def read(path: String): Seq[String] = read(new Path(path))

  def read(path: Path): Seq[String]

  final def write(path: String, actions: Iterator[String]): Unit = write(new Path(path), actions)

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit

  final def listFrom(path: String): Iterator[FileStatus] = listFrom(new Path(path))

  def listFrom(path: Path): Iterator[FileStatus]

  def invalidateCache(): Unit

  def resolvePathOnPhysicalStorage(path: Path): Path = {
    throw new UnsupportedOperationException(
      s"Resolving path for generating Hive manifest is not supported with ${this.getClass}")
  }
}

object LogStore extends Logging {
  def apply(sc: SparkContext): LogStore = {
    apply(sc.getConf, sc.hadoopConfiguration)
  }

  def apply(sparkConf: SparkConf, hadoopConf: Configuration): LogStore = {
    val logStoreClass = Utils.classForName(sparkConf.get(
      "spark.databricks.tahoe.logStore.class",
      classOf[HDFSLogStore].getName))
    logInfo("LogStore class: " + logStoreClass)
    logStoreClass.getConstructor(classOf[SparkConf], classOf[Configuration])
      .newInstance(sparkConf, hadoopConf).asInstanceOf[LogStore]
  }
}

trait LogStoreProvider {
  def createLogStore(spark: SparkSession): LogStore = {
    LogStore(spark.sparkContext)
  }
}

/**
 * This is a LogStore implementation for HDFS or HDFS like file system (such as Azure blob storage).
 * The file system must support file atomic rename. However, overwriting a file is not atomic and
 * the caller must handle partial files.
 */
class HDFSLogStore(sparkConf: SparkConf, hadoopConf: Configuration) extends LogStore {

  def this(sc: SparkContext) = this(sc.getConf, sc.hadoopConfiguration)

  protected def getHadoopConfiguration: Configuration = {
    SparkSession.getActiveSession.map(_.sessionState.newHadoopConf()).getOrElse(hadoopConf)
  }

  override def read(path: Path): Seq[String] = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val stream = fs.open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).asScala.map(_.trim)
    } finally {
      stream.close()
    }
  }

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    if (fs.isInstanceOf[RawLocalFileSystem]) {
      // We need to add `synchronized` for RawLocalFileSystem as its rename will not throw an
      // exception when the target file exists. Hence we must make sure `exists + rename` in
      // `writeInternal` for RawLocalFileSystem is atomic in our tests.
      synchronized {
        writeInternal(fs, path, actions, overwrite)
      }
    } else {
      // rename is atomic and also will fail when the target file exists. Not need to add the extra
      // `synchronized`.
      writeInternal(fs, path, actions, overwrite)
    }
  }

  private def writeInternal(
      fs: FileSystem,
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean): Unit = {
    // mimic S3 behavior
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    if (overwrite) {
      val stream = fs.create(path, true)
      try {
        actions.map(_ + "\n").map(_.getBytes("utf-8")).foreach(stream.write)
      } finally {
        stream.close()
      }
    } else {
      if (fs.exists(path)) {
        throw new FileAlreadyExistsException(path.toString)
      }
      val tempPath = createTempPath(path)
      var streamClosed = false // This flag is to avoid double close
      var renameDone = false // This flag is to save the delete operation in most of cases.
      val stream = fs.create(tempPath)
      try {
        actions.map(_ + "\n").map(_.getBytes("utf-8")).foreach(stream.write)
        stream.close()
        streamClosed = true
        try {
          if (fs.rename(tempPath, path)) {
            renameDone = true
          } else {
            if (fs.exists(path)) {
              throw new FileAlreadyExistsException(path.toString)
            } else {
              throw new IllegalStateException(s"Cannot rename $tempPath to $path")
            }
          }
        } catch {
          case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
            throw new FileAlreadyExistsException(path.toString)
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

  private def createTempPath(path: Path): Path = {
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}.tmp")
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    // mimic S3 behavior
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator
  }

  override def invalidateCache(): Unit = {}

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    path.getFileSystem(getHadoopConfiguration).makeQualified(path)
  }
}
