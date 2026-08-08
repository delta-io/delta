/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package io.delta.storage.integration

import java.io.IOException
import java.net.{InetAddress, Socket}
import java.nio.file.FileAlreadyExistsException

import scala.jdk.CollectionConverters._

import io.delta.storage.S3LogStore
import org.apache.hadoop.fs.Path

/** Runs one conditional write in a dedicated JVM for the multi-driver integration test. */
object S3LogStoreIntegrationWriter {
  val ConflictExitCode = 10

  def main(args: Array[String]): Unit = {
    require(args.length == 3, "Expected path, action, and barrier port arguments.")
    val path = new Path(args(0))
    val action = args(1)
    val barrierPort = args(2).toInt
    val configuration = S3IntegrationTestUtils.configuration()
    val logStore = new S3LogStore(configuration)

    // Initialize this process's S3A client before declaring the writer ready. The parent releases
    // every child only after all clients reach this barrier, so JVM startup cannot serialize the
    // conditional requests.
    path.getFileSystem(configuration)
    val barrier = new Socket(InetAddress.getLoopbackAddress, barrierPort)
    try {
      if (barrier.getInputStream.read() < 0) {
        throw new IOException("The parent closed the writer barrier before releasing this process.")
      }
    } finally {
      barrier.close()
    }

    try {
      logStore.write(path, Iterator(action).asJava, false, configuration)
    } catch {
      case _: FileAlreadyExistsException => System.exit(ConflictExitCode)
    }
  }
}
