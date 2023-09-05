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

package org.apache.spark.sql.delta.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * Bundling the `Path` with the `FileSystem` instance ensures
 * that we never pass the wrong file system with the path to a function
 * at compile time.
 */
case class PathWithFileSystem private (path: Path, fs: FileSystem) {

  /**
   * Extends the path with `s`
   *
   * The resulting path must be on the same filesystem.
   */
  def withSuffix(s: String): PathWithFileSystem = new PathWithFileSystem(new Path(path, s), fs)

  /**
   * Qualify `path`` using `fs`
   */
  def makeQualified(): PathWithFileSystem = {
    val qualifiedPath = fs.makeQualified(path)
    PathWithFileSystem(qualifiedPath, fs)
  }
}

object PathWithFileSystem {

  /**
   * Create a new `PathWithFileSystem` instance by calling `getFileSystem`
   * on `path` with the given `hadoopConf`.
   */
  def withConf(path: Path, hadoopConf: Configuration): PathWithFileSystem = {
    val fs = path.getFileSystem(hadoopConf)
    PathWithFileSystem(path, fs)
  }
}
