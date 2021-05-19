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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf

/**
 * Default [[LogStore]] implementation (should be used for testing only!).
 *
 * Production users should specify the appropriate [[[LogStore]] implementation in Spark properties.
 *
 * We assume the following from [[org.apache.hadoop.fs.FileSystem]] implementations:
 * - Rename without overwrite is atomic.
 * - List-after-write is consistent.
 *
 * Regarding file creation, this implementation:
 * - Uses atomic rename when overwrite is false; if the destination file exists or the rename
 *   fails, throws an exception.
 * - Uses create-with-overwrite when overwrite is true. This does not make the file atomically
 *   visible and therefore the caller must handle partial files.
 */
class LocalLogStore(sparkConf: SparkConf, hadoopConf: Configuration)
    extends HadoopFileSystemLogStore(sparkConf: SparkConf, hadoopConf: Configuration) {

  /**
   * This write implementation needs to wraps `writeWithRename` with `synchronized` as the rename()
   * for [[org.apache.hadoop.fs.RawLocalFileSystem]] doesn't throw an exception when the target file
   * exists. Hence we must make sure `exists + rename` in `writeWithRename` is atomic in our tests.
   */
  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    synchronized {
      writeWithRename(path, actions, overwrite)
    }
  }

  override def invalidateCache(): Unit = {}

  override def isPartialWriteVisible(path: Path): Boolean = true
}
