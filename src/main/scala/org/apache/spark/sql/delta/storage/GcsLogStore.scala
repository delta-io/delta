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

package org.apache.spark.sql.delta.storage

import java.io.{IOException, _}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

/**
 * The [[LogStore]] implementation for Gcs, which uses gcs-connector to
 * provide the necessary atomic and durability guarantees:
 *
 * 1. Atomic Visibility: Read/read-after-metadata-update/delete are strongly
 * consistent for gcs.
 *
 * 2. Consistent Listing: Gcs guarantees strong consistency for both object and
 * bucket listing operations.
 *
 * 3. Mutual Exclusion: Preconditions are used to handle race conditions.
 *
 * Refer https://cloud.google.com/storage/docs/consistency for 1 and 2.
 *
 * Regarding file creation, this implementation:
 * - Throws [[FileAlreadyExistsException]] if file exists and overwrite is false.
 * - Opens a stream to write to gcs which can fail due to race conditions.
 */
class GcsLogStore(sparkConf: SparkConf, defaultHadoopConf: Configuration)
  extends HadoopFileSystemLogStore(sparkConf, defaultHadoopConf) with Logging {

  protected def getFileContext(path: Path): FileContext = {
    FileContext.getFileContext(path.toUri, getHadoopConfiguration)
  }

  val preconditionFailedExceptionMessage = "412 Precondition Failed"

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    if (!fs.exists(path.getParent)) {
      throw new FileNotFoundException(s"No such file or directory: ${path.getParent}")
    }

    if (fs.exists(path) && !overwrite) {
      throw new FileAlreadyExistsException(path.toString)
    }
    writeObject(path, fs, actions)
  }

  private def writeObject(path: Path, fs: FileSystem, actions: Iterator[String]): Unit = {
    val stream = fs.create(path, true)
    try {
      actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      stream.close()
    } catch {
      // Gcs uses preconditions that guarantee that object will be created iff it doesn't exist.
      // Reference: https://cloud.google.com/storage/docs/generations-preconditions
      case e: IOException if e.getMessage.contains(preconditionFailedExceptionMessage) =>
        val newException = new FileAlreadyExistsException(path.toString, null, e.getMessage)
        logError(newException.getMessage, newException.getCause)
        throw newException
    } finally {
      stream.close()
    }
  }

  override def invalidateCache(): Unit = {}

  override def isPartialWriteVisible(path: Path): Boolean = true
}
