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
import java.nio.file.FileAlreadyExistsException
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import org.apache.spark.SparkConf

/**
 * LogStore implementation for Azure.
 *
 * We assume the following from Azure's [[FileSystem]] implementations:
 * - Rename without overwrite is atomic.
 * - List-after-write is consistent.
 *
 * Regarding file creation, this implementation:
 * - Uses atomic rename when overwrite is false; if the destination file exists or the rename
 *   fails, throws an exception.
 * - Uses create-with-overwrite when overwrite is true. This does not make the file atomically
 *   visible and therefore the caller must handle partial files.
 */
class AzureLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
  extends FileSystemLogStore(sparkConf, hadoopConf) {

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val fs = path.getFileSystem(getHadoopConfiguration)

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

  override def invalidateCache(): Unit = {}

  protected def createTempPath(path: Path): Path = {
    new Path(path.getParent, s".${path.getName}.${UUID.randomUUID}.tmp")
  }
}
