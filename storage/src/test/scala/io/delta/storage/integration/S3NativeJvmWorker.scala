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

import java.nio.file.FileAlreadyExistsException
import java.util.Collections

import io.delta.storage.S3LogStore
import org.apache.hadoop.fs.Path

/**
 * Runs one publication in a separate JVM with no shared locks or object-identity caches.
 * Exit 0 means success, 3 means a foreign conflict, and 2 means an unexpected failure.
 * Server-side gates let the parent stop this process at an exact HTTP publication boundary.
 */
object S3NativeJvmWorker {
  def main(args: Array[String]): Unit = {
    require(args.length == 5, "Usage: endpoint bucket key label payloadSize")
    val conf = S3NativeTestFixture.configuration(args(0))
    val result = try {
      val store = new S3LogStore(conf)
      val size = args(4).toInt
      val line = if (size == 0) args(3) else args(3) + ("x" * size)
      store.write(new Path(s"s3a://${args(1)}/${args(2)}"),
        Collections.singletonList(line).iterator(), false, conf)
      println("SUCCESS")
      0
    } catch {
      case e: FileAlreadyExistsException => println(s"CONFLICT: ${e.getMessage}"); 3
      case e: Throwable => e.printStackTrace(System.err); 2
    }
    System.exit(result)
  }
}
