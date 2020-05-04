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

package org.apache.spark.sql.delta.util

import java.util.Objects

import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus, Path}

/** A serializable variant of HDFS's FileStatus. */
case class SerializableFileStatus(
    path: String,
    length: Long,
    isDir: Boolean,
    modificationTime: Long) {

  // Important note! This is very expensive to compute, but we don't want to cache it
  // as a `val` because Paths internally contain URIs and therefore consume lots of memory.
  def getPath: Path = new Path(path)
  def getLen: Long = length
  def getModificationTime: Long = modificationTime
  def isDirectory: Boolean = isDir

  def toFileStatus: FileStatus = {
    new LocatedFileStatus(
      new FileStatus(length, isDir, 0, 0, modificationTime, new Path(path)),
      null)
  }

  override def equals(obj: Any): Boolean = obj match {
    case other: SerializableFileStatus =>
      // We only compare the paths to stay consistent with FileStatus.equals.
      Objects.equals(path, other.path)
    case _ => false
  }

  override def hashCode(): Int = {
    // We only use the path to stay consistent with FileStatus.hashCode.
    Objects.hashCode(path)
  }
}

object SerializableFileStatus {
  def fromStatus(status: FileStatus): SerializableFileStatus = {
    SerializableFileStatus(
      Option(status.getPath).map(_.toString).orNull,
      status.getLen,
      status.isDirectory,
      status.getModificationTime)
  }

  val EMPTY: SerializableFileStatus = fromStatus(new FileStatus())
}
