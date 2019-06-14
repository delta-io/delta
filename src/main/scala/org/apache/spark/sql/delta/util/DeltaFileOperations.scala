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

package org.apache.spark.sql.delta.util

import java.net.URI

import scala.util.control.NonFatal

import org.apache.spark.sql.delta.metering.DeltaLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.TaskContext

/**
 * Some utility methods on files, directories, and paths.
 */
object DeltaFileOperations extends DeltaLogging {
  /**
   * Create an absolute path from `child` using the `basePath` if the child is a relative path.
   * Return `child` if it is an absolute path.
   *
   * @param basePath Base path to prepend to `child` if child is a relative path.
   *                 Note: It is assumed that the basePath do not have any escaped characters and
   *                 is directly readable by Hadoop APIs.
   * @param child    Child path to append to `basePath` if child is a relative path.
   *                 Note: t is assumed that the child is escaped, that is, all special chars that
   *                 need escaping by URI standards are already escaped.
   * @return Absolute path without escaped chars that is directly readable by Hadoop APIs.
   */
  def absolutePath(basePath: String, child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      val merged = new Path(basePath, p)
      // URI resolution strips the final `/` in `p` if it exists
      val mergedUri = merged.toUri.toString
      if (child.endsWith("/") && !mergedUri.endsWith("/")) {
        new Path(new URI(mergedUri + "/"))
      } else {
        merged
      }
    }
  }

  /**
   * Given a path `child`:
   *   1. Returns `child` if the path is already relative
   *   2. Tries relativizing `child` with respect to `basePath`
   *      a) If the `child` doesn't live within the same base path, returns `child` as is
   *      b) If `child` lives in a different FileSystem, throws an exception
   * Note that `child` may physically be pointing to a path within `basePath`, but may logically
   * belong to a different FileSystem, e.g. DBFS mount points and direct S3 paths.
   */
  def tryRelativizePath(fs: FileSystem, basePath: Path, child: Path): Path = {
    // Child Paths can be absolute and use a separate fs
    val childUri = child.toUri
    // We can map multiple schemes to the same `FileSystem` class, but `FileSystem.getScheme` is
    // usually just a hard-coded string. Hence, we need to use the scheme of the URI that we use to
    // create the FileSystem here.
    if (child.isAbsolute) {
      try {
        new Path(fs.makeQualified(basePath).toUri.relativize(fs.makeQualified(child).toUri))
      } catch {
        case e: IllegalArgumentException =>
          throw new IllegalStateException(
            s"""Failed to relativize the path ($child). This can happen when absolute paths make
               |it into the transaction log, which start with the scheme s3://, wasbs:// or adls://.
               |This is a bug that has existed before DBR 5.0. To fix this issue, please upgrade
               |your writer jobs to DBR 5.0 and please run:
               |%scala com.databricks.delta.Delta.fixAbsolutePathsInLog("$child")
             """.stripMargin)
      }
    } else {
      child
    }
  }

  /** Register a task failure listener to delete a temp file in our best effort. */
  def registerTempFileDeletionTaskFailureListener(
      conf: Configuration,
      tempPath: Path): Unit = {
    val tc = TaskContext.get
    if (tc == null) {
      throw new IllegalStateException("Not running on a Spark task thread")
    }
    tc.addTaskFailureListener { (_, _) =>
      // Best effort to delete the temp file
      try {
        tempPath.getFileSystem(conf).delete(tempPath, false /* = recursive */)
      } catch {
        case NonFatal(e) =>
          logError(s"Failed to delete $tempPath", e)
      }
      () // Make the compiler happy
    }
  }
}
