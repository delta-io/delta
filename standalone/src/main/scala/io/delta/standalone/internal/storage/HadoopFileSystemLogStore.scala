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

package io.delta.standalone.internal.storage

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import scala.collection.JavaConverters._

import io.delta.standalone.data.CloseableIterator
import io.delta.standalone.storage.LogStore

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}

/**
 * Default implementation of [[LogStore]] for Hadoop [[FileSystem]] implementations.
 */
abstract class HadoopFileSystemLogStore(override val initHadoopConf: Configuration)
  extends LogStore(initHadoopConf) {

  override def read(path: Path, hadoopConf: Configuration): CloseableIterator[String] = {
    val fs = path.getFileSystem(hadoopConf)
    val stream = fs.open(path)
    val reader = new BufferedReader(new InputStreamReader(stream, UTF_8))
    new LineCloseableIterator(reader)
  }

  override def listFrom(path: Path, hadoopConf: Configuration): java.util.Iterator[FileStatus] = {
    val fs = path.getFileSystem(hadoopConf)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }
    val files = fs.listStatus(path.getParent)
    files.filter(_.getPath.getName >= path.getName).sortBy(_.getPath.getName).iterator.asJava
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
  protected def writeWithRename(
      path: Path,
      actions: Iterator[String],
      overwrite: Boolean,
      hadoopConf: Configuration): Unit = {
    val fs = path.getFileSystem(hadoopConf)

    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
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
        throw new FileAlreadyExistsException(path.toString)
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
              throw new FileAlreadyExistsException(path.toString)
            } else {
              throw new IllegalStateException(s"Cannot rename $tempPath to $path")
            }
          }
        } catch {
          case _: org.apache.hadoop.fs.FileAlreadyExistsException =>
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

  protected def createTempPath(path: Path): Path = {
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}.tmp")
  }
}
