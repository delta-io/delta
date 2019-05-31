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

import java.io._
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.{EnumSet, UUID}

import scala.collection.JavaConverters._

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.CreateFlag.CREATE
import org.apache.hadoop.fs.Options.{ChecksumOpt, CreateOpts}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

/**
 * The [[LogStore]] implementation for HDFS, which uses Hadoop [[FileContext]] API's to
 * provide the necessary atomic and durability guarantees:
 *
 * 1. Atomic visibility of files: `FileContext.rename` is used write files which is atomic for HDFS.
 *
 * 2. Consistent file listing: HDFS file listing is consistent.
 */
class HDFSLogStore(sparkConf: SparkConf, defaultHadoopConf: Configuration) extends LogStore {

  def this(sc: SparkContext) = this(sc.getConf, sc.hadoopConfiguration)

  protected def getActiveHadoopConf: Configuration = {
    SparkSession.getActiveSession.map(_.sessionState.newHadoopConf()).getOrElse(defaultHadoopConf)
  }

  protected def getFileContext(path: Path): FileContext = {
    FileContext.getFileContext(path.toUri, getActiveHadoopConf)
  }

  override def read(path: Path): Seq[String] = {
    val stream = getFileContext(path).open(path)
    try {
      val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
      IOUtils.readLines(reader).asScala.map(_.trim)
    } finally {
      stream.close()
    }
  }

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val isLocalFs = path.getFileSystem(getActiveHadoopConf).isInstanceOf[RawLocalFileSystem]
    if (isLocalFs) {
      // We need to add `synchronized` for RawLocalFileSystem as its rename will not throw an
      // exception when the target file exists. Hence we must make sure `exists + rename` in
      // `writeInternal` for RawLocalFileSystem is atomic in our tests.
      synchronized {
        writeInternal(path, actions, overwrite)
      }
    } else {
      // rename is atomic and also will fail when the target file exists. Not need to add the extra
      // `synchronized`.
      writeInternal(path, actions, overwrite)
    }
  }

  private def writeInternal(path: Path, actions: Iterator[String], overwrite: Boolean): Unit = {
    val fc = getFileContext(path)

    if (!overwrite && fc.util.exists(path)) {
      // This is needed for the tests to throw error with local file system
      throw new FileAlreadyExistsException(path.toString)
    }

    val tempPath = createTempPath(path)
    var streamClosed = false // This flag is to avoid double close
    var renameDone = false // This flag is to save the delete operation in most of cases.
    val stream = fc.create(
      tempPath, EnumSet.of(CREATE), CreateOpts.checksumParam(ChecksumOpt.createDisabled()))

    try {
      actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      stream.close()
      streamClosed = true
      try {
        val renameOpt = if (overwrite) Options.Rename.OVERWRITE else Options.Rename.NONE
        fc.rename(tempPath, path, renameOpt)
        renameDone = true
      } catch {
        case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
          throw new FileAlreadyExistsException(path.toString)
      }
    } finally {
      if (!streamClosed) {
        stream.close()
      }
      if (!renameDone) {
        fc.delete(tempPath, false)
      }
    }
  }

  private def createTempPath(path: Path): Path = {
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}.tmp")
  }

  override def listFrom(path: Path): Iterator[FileStatus] = {
    val fc = getFileContext(path)
    if (!fc.util.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fc.util.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator
  }

  override def invalidateCache(): Unit = {}

  override def resolvePathOnPhysicalStorage(path: Path): Path = {
    getFileContext(path).makeQualified(path)
  }

  override def isPartialWriteVisible(path: Path): Boolean = true
}
