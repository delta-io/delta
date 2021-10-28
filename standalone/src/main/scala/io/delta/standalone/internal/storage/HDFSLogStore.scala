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

package io.delta.standalone.internal.storage

import java.io.IOException
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.EnumSet

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Options, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.CreateFlag.CREATE
import org.apache.hadoop.fs.Options.{ChecksumOpt, CreateOpts}

import io.delta.standalone.internal.exception.DeltaErrors
import io.delta.standalone.internal.logging.Logging

/**
 * The [[LogStore]] implementation for HDFS, which uses Hadoop [[FileContext]] APIs to
 * provide the necessary atomic and durability guarantees:
 *
 * 1. Atomic visibility of files: `FileContext.rename` is used write files which is atomic for HDFS.
 *
 * 2. Consistent file listing: HDFS file listing is consistent.
 */
private[internal] class HDFSLogStore(override val initHadoopConf: Configuration)
  extends HadoopFileSystemLogStore(initHadoopConf) with Logging {

  val noAbstractFileSystemExceptionMessage = "No AbstractFileSystem"

  override def write(
      path: Path,
      actions: java.util.Iterator[String],
      overwrite: java.lang.Boolean,
      hadoopConf: Configuration): Unit = {
    val isLocalFs = path.getFileSystem(hadoopConf).isInstanceOf[RawLocalFileSystem]
    if (isLocalFs) {
      // We need to add `synchronized` for RawLocalFileSystem as its rename will not throw an
      // exception when the target file exists. Hence we must make sure `exists + rename` in
      // `writeInternal` for RawLocalFileSystem is atomic in our tests.
      synchronized {
        writeInternal(path, actions.asScala, overwrite, hadoopConf)
      }
    } else {
      // rename is atomic and also will fail when the target file exists. Not need to add the extra
      // `synchronized`.
      writeInternal(path, actions.asScala, overwrite, hadoopConf)
    }
  }

  override def isPartialWriteVisible(
      path: Path, hadoopConf: Configuration): java.lang.Boolean = true

  private def writeInternal(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    val fc: FileContext = try {
      getFileContext(path, hadoopConf)
    } catch {
      case e: IOException if e.getMessage.contains(noAbstractFileSystemExceptionMessage) =>
        val newException = DeltaErrors.incorrectLogStoreImplementationException(e)
        logError(newException.getMessage, newException.getCause)
        throw newException
    }
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
        // TODO: this is a workaround of HADOOP-16255 - remove this when HADOOP-16255 is resolved
        tryRemoveCrcFile(fc, tempPath)
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

  private def getFileContext(path: Path, hadoopConf: Configuration): FileContext = {
    FileContext.getFileContext(path.toUri, hadoopConf)
  }

  private def tryRemoveCrcFile(fc: FileContext, path: Path): Unit = {
    try {
      val checksumFile = new Path(path.getParent, s".${path.getName}.crc")
      if (fc.util.exists(checksumFile)) {
        // checksum file exists, deleting it
        fc.delete(checksumFile, true)
      }
    } catch {
      case NonFatal(_) => // ignore, we are removing crc file as "best-effort"
    }
  }

}
