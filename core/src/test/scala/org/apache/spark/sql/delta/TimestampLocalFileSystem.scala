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

package org.apache.spark.sql.delta

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{DelegateToFileSystem, Path, RawLocalFileSystem}
import org.apache.hadoop.fs.FileStatus

/**
 * This custom fs implementation is used for testing the msync calling in HDFSLogStore writes.
 * If `msync` is not called, `listStatus` will return stale results.
 */
class TimestampLocalFileSystem extends RawLocalFileSystem {

  private var uri: URI = _
  private var latestTimestamp: Long = 0

  override def getScheme: String = TimestampLocalFileSystem.scheme

  override def initialize(name: URI, conf: Configuration): Unit = {
    uri = URI.create(name.getScheme + ":///")
    super.initialize(name, conf)
  }

  override def getUri(): URI = if (uri == null) {
    // RawLocalFileSystem's constructor will call this one before `initialize` is called.
    // Just return the super's URI to avoid NPE.
    super.getUri
  } else {
    uri
  }

  override def listStatus(path: Path): Array[FileStatus] = {
    super.listStatus(path).filter(_.getModificationTime <= latestTimestamp)
  }

  override def msync(): Unit = {
    latestTimestamp = System.currentTimeMillis()
  }
}

class TimestampAbstractFileSystem(uri: URI, conf: Configuration)
    extends DelegateToFileSystem(
      uri,
      new TimestampLocalFileSystem,
      conf,
      TimestampLocalFileSystem.scheme,
      false)

/**
 * Singleton for BlockWritesLocalFileSystem used to initialize the file system countdown latch.
 */
object TimestampLocalFileSystem {
  val scheme = "ts"
}
