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

package io.delta.storage

import java.io.{IOException, _}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.FileAlreadyExistsException

import org.apache.spark.sql.delta.storage.HadoopFileSystemLogStore
import com.google.common.base.Throwables
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

import org.apache.spark.SparkConf
import org.apache.spark.annotation.Unstable
import org.apache.spark.internal.Logging

/**
 * :: Unstable ::
 *
 * The [[LogStore]] implementation for GCS, which uses gcs-connector to
 * provide the necessary atomic and durability guarantees:
 *
 * 1. Atomic Visibility: Read/read-after-metadata-update/delete are strongly
 * consistent for GCS.
 *
 * 2. Consistent Listing: GCS guarantees strong consistency for both object and
 * bucket listing operations.
 * https://cloud.google.com/storage/docs/consistency
 *
 * 3. Mutual Exclusion: Preconditions are used to handle race conditions.
 *
 * Regarding file creation, this implementation:
 * - Opens a stream to write to GCS otherwise.
 * - Throws [[FileAlreadyExistsException]] if file exists and overwrite is false.
 * - Assumes file writing to be all-or-nothing, irrespective of overwrite option.
 *
 * @note This class is not meant for direct access but for configuration based on storage system.
 *       See https://docs.delta.io/latest/delta-storage.html for details.
 */
@Unstable
class GCSLogStore(sparkConf: SparkConf, defaultHadoopConf: Configuration)
  extends HadoopFileSystemLogStore(sparkConf, defaultHadoopConf) with Logging {

  val preconditionFailedExceptionMessage = "412 Precondition Failed"

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    val fs = path.getFileSystem(getHadoopConfiguration)

    // This is needed for the tests to throw error with local file system.
    if (fs.isInstanceOf[LocalFileSystem] && !overwrite && fs.exists(path)) {
      throw new FileAlreadyExistsException(path.toString)
    }

    try {
      // If overwrite=false and path already exists, gcs-connector will throw
      // org.apache.hadoop.fs.FileAlreadyExistsException after fs.create is invoked.
      // This should be mapped to java.nio.file.FileAlreadyExistsException.
      val stream = fs.create(path, overwrite)
      try {
        actions.map(_ + "\n").map(_.getBytes(UTF_8)).foreach(stream.write)
      } finally {
        stream.close()
      }
    } catch {
      case e: org.apache.hadoop.fs.FileAlreadyExistsException =>
        throw new FileAlreadyExistsException(path.toString).initCause(e)
      // GCS uses preconditions to handle race conditions for multiple writers.
      // If path gets created between fs.create and stream.close by an external
      // agent or race conditions. Then this block will execute.
      // Reference: https://cloud.google.com/storage/docs/generations-preconditions
      case e: IOException if isPreconditionFailure(e) =>
        if (!overwrite) {
          throw new FileAlreadyExistsException(path.toString).initCause(e)
        }
    }
  }

  private def isPreconditionFailure(x: Throwable): Boolean = {
    Throwables.getCausalChain(x)
      .stream()
      .filter(p => p != null)
      .filter(p => p.getMessage != null)
      .filter(p => p.getMessage.contains(preconditionFailedExceptionMessage))
      .findFirst
      .isPresent;
  }

  override def invalidateCache(): Unit = {}

  override def isPartialWriteVisible(path: Path): Boolean = false
}
