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
import java.util.concurrent.CountDownLatch

import org.apache.spark.sql.delta.BlockWritesLocalFileSystem.scheme
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{DelegateToFileSystem, FSDataOutputStream, Path, RawLocalFileSystem}
import org.apache.hadoop.util.Progressable

/**
 * This custom fs implementation is used for testing the execution multiple batches of Optimize.
 */
class BlockWritesLocalFileSystem extends RawLocalFileSystem {

  private var uri: URI = _

  override def getScheme: String = scheme

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

  override def create(
      f: Path,
      overwrite: Boolean,
      bufferSize: Int,
      replication: Short,
      blockSize: Long,
      progress: Progressable): FSDataOutputStream = {
    // called when data files and log files are written
    BlockWritesLocalFileSystem.blockLatch.countDown()
    BlockWritesLocalFileSystem.blockLatch.await()
    super.create(f, overwrite, bufferSize, replication, blockSize, progress)
  }
}

/**
 * An AbstractFileSystem implementation wrapper around [[BlockWritesLocalFileSystem]].
 */
class BlockWritesAbstractFileSystem(uri: URI, conf: Configuration)
    extends DelegateToFileSystem(
      uri,
      new BlockWritesLocalFileSystem,
      conf,
      BlockWritesLocalFileSystem.scheme,
      false)

/**
 * Singleton for BlockWritesLocalFileSystem used to initialize the file system countdown latch.
 */
object BlockWritesLocalFileSystem {
  val scheme = "block"

  /** latch that blocks writes */
  private var blockLatch: CountDownLatch = _

  /**
   * @param numWrites - writing is blocked until there are `numWrites` concurrent writes to
   *                  the file system.
   */
  def blockUntilConcurrentWrites(numWrites: Integer): Unit = {
    blockLatch = new CountDownLatch(numWrites)
  }
}
