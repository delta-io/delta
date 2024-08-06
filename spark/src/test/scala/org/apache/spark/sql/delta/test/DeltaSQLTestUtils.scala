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

package org.apache.spark.sql.delta.test

import java.io.File

import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.util.Utils

trait DeltaSQLTestUtils extends SQLTestUtils {
  /**
   * Override the temp dir/path creation methods from [[SQLTestUtils]] to:
   * 1. Drop the call to `waitForTasksToFinish` which is a source of flakiness due to timeouts
   *    without clear benefits.
   * 2. Allow creating paths with special characters for better test coverage.
   */
  override protected def withTempDir(f: File => Unit): Unit = {
    withTempDir(prefix = "spark")(f)
  }

  override protected def withTempPaths(numPaths: Int)(f: Seq[File] => Unit): Unit = {
    withTempPaths(numPaths, prefix = "spark")(f)
  }

  override def withTempPath(f: File => Unit): Unit = {
    withTempPath(prefix = "spark")(f)
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  def withTempDir(prefix: String)(f: File => Unit): Unit = {
    val path = Utils.createTempDir(namePrefix = prefix)
    try f(path) finally Utils.deleteRecursively(path)
  }

  /**
   * Generates a temporary directory path without creating the actual directory, which is then
   * passed to `f` and will be deleted after `f` returns.
   */
  def withTempPath(prefix: String)(f: File => Unit): Unit = {
    val path = Utils.createTempDir(namePrefix = prefix)
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  /**
   * Generates the specified number of temporary directory paths without creating the actual
   * directories, which are then passed to `f` and will be deleted after `f` returns.
   */
  protected def withTempPaths(numPaths: Int, prefix: String)(f: Seq[File] => Unit): Unit = {
    val files =
      Seq.fill[File](numPaths)(Utils.createTempDir(namePrefix = prefix).getCanonicalFile)
    files.foreach(_.delete())
    try f(files) finally {
      files.foreach(Utils.deleteRecursively)
    }
  }
}
