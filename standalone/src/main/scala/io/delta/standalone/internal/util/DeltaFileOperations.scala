/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

package io.delta.standalone.internal.util

import org.apache.hadoop.fs.{FileSystem, Path}

import io.delta.standalone.internal.logging.Logging

/**
 * Utility methods on files, directories, and paths.
 */
private[internal] object DeltaFileOperations extends Logging {

  /**
   * Given a path `child`:
   *   1. Returns `child` if the path is already relative
   *   2. Tries relativizing `child` with respect to `basePath`
   *      a) If the `child` doesn't live within the same base path, returns `child` as is
   *      b) If `child` lives in a different FileSystem, throws an exception
   * Note that `child` may physically be pointing to a path within `basePath`, but may logically
   * belong to a different FileSystem, e.g. DBFS mount points and direct S3 paths.
   */
  def tryRelativizePath(
      fs: FileSystem,
      basePath: Path,
      child: Path,
      ignoreError: Boolean = false): Path = {
    // We can map multiple schemes to the same `FileSystem` class, but `FileSystem.getScheme` is
    // usually just a hard-coded string. Hence, we need to use the scheme of the URI that we use to
    // create the FileSystem here.
    if (child.isAbsolute) {
      try {
        new Path(fs.makeQualified(basePath).toUri.relativize(fs.makeQualified(child).toUri))
      } catch {
        case _: IllegalArgumentException if ignoreError =>
          // ES-85571: when the file system failed to make the child path qualified,
          // it means the child path exists in a different file system
          // (a different authority or schema). This usually happens when the file is coming
          // from the across buckets or across cloud storage system shallow clone.
          // When ignoreError being set to true, not try to relativize this path,
          // ignore the error and just return `child` as is.
          child
        case e: IllegalArgumentException =>
          logError(s"Failed to relativize the path ($child) " +
            s"with the base path ($basePath) and the file system URI (${fs.getUri})", e)
          throw new IllegalStateException(
            s"""Failed to relativize the path ($child). This can happen when absolute paths make
               |it into the transaction log, which start with the scheme
               |s3://, wasbs:// or adls://. This is a bug that has existed before DBR 5.0.
               |To fix this issue, please upgrade your writer jobs to DBR 5.0 and please run:
               |%scala com.databricks.delta.Delta.fixAbsolutePathsInLog("$child").
               |
               |If this table was created with a shallow clone across file systems
               |(different buckets/containers) and this table is NOT USED IN PRODUCTION, you can
               |set the SQL configuration spark.databricks.delta.vacuum.relativize.ignoreError
               |to true. Using this SQL configuration could lead to accidental data loss,
               |therefore we do not recommend the use of this flag unless
               |this is a shallow clone for testing purposes.
             """.stripMargin)
      }
    } else {
      child
    }
  }
}
