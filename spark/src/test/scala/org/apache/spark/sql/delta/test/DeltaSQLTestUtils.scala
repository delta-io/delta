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
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * This method is copied over from [[SQLTestUtils]] of Apache Spark.
   */
  def withTempDir(prefix: String)(f: File => Unit): Unit = {
    val path = Utils.createTempDir(namePrefix = prefix)
    try f(path) finally Utils.deleteRecursively(path)
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   *
   * This method is copied over from [[SQLTestUtils]] of Apache Spark.
   */
  def withTempPath(prefix: String)(f: File => Unit): Unit = {
    withTempDir(prefix)(f)
  }
}
