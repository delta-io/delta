/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.storage.internal

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{Files, Path}

import scala.collection.JavaConverters._

import org.scalatest.funsuite.AnyFunSuite

class S3WriteReplayBufferSuite extends AnyFunSuite {

  private def listFiles(directory: Path): Seq[Path] = {
    val files = Files.list(directory)
    try {
      files.iterator().asScala.toSeq
    } finally {
      files.close()
    }
  }

  private def withTempDirectory(testCode: Path => Unit): Unit = {
    val directory = Files.createTempDirectory("delta-s3-replay-test")
    try {
      testCode(directory)
    } finally {
      listFiles(directory).foreach(Files.deleteIfExists)
      Files.deleteIfExists(directory)
    }
  }

  test("in-memory buffer replays exact bytes after seal") {
    withTempDirectory { directory =>
      val buffer = new S3WriteReplayBuffer(1024, directory)
      try {
        buffer.write("first\n".getBytes(UTF_8))
        buffer.write("second\n".getBytes(UTF_8))
        buffer.seal()

        val replayed = new ByteArrayOutputStream()
        buffer.replayTo(replayed)

        assert(replayed.toString(UTF_8.name()) == "first\nsecond\n")
        assert(listFiles(directory).isEmpty)
      } finally {
        buffer.close()
      }
    }
  }

  test("buffer spills with owner-only permissions and deletes spill on close") {
    withTempDirectory { directory =>
      val buffer = new S3WriteReplayBuffer(4, directory)
      buffer.write("12345".getBytes(UTF_8))

      val spillFiles = listFiles(directory)
      assert(spillFiles.size == 1)
      assert(Files.getPosixFilePermissions(spillFiles.head) ==
        Set(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE).asJava)

      buffer.seal()
      val replayed = new ByteArrayOutputStream()
      buffer.replayTo(replayed)
      assert(replayed.toString(UTF_8.name()) == "12345")

      buffer.close()
      assert(listFiles(directory).isEmpty)
    }
  }

  test("buffer rejects replay before seal and writes after seal") {
    withTempDirectory { directory =>
      val buffer = new S3WriteReplayBuffer(1024, directory)
      try {
        buffer.write("payload".getBytes(UTF_8))
        intercept[IllegalStateException] {
          buffer.replayTo(new ByteArrayOutputStream())
        }

        buffer.seal()
        intercept[IllegalStateException] {
          buffer.write("late".getBytes(UTF_8))
        }
      } finally {
        buffer.close()
      }
    }
  }

  test("close deletes an unsealed spill file") {
    withTempDirectory { directory =>
      val buffer = new S3WriteReplayBuffer(1, directory)
      buffer.write("payload".getBytes(UTF_8))
      assert(listFiles(directory).size == 1)

      buffer.close()

      assert(listFiles(directory).isEmpty)
    }
  }
}
