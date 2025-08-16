/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.kernel.internal.metrics

import org.scalatest.funsuite.AnyFunSuite

class FileCounterSuite extends AnyFunSuite {

  test("FileCount basic operations") {
    val fileCount = new FileCounter()

    // Verify initial values
    assert(fileCount.fileCount() === 0)
    assert(fileCount.sizeInBytes() === 0)

    // Increment by one file with sizeInBytesInBytes
    fileCount.increment(1024)
    assert(fileCount.fileCount() === 1)
    assert(fileCount.sizeInBytes() === 1024)

    // Increment by one more file with different sizeInBytesInBytes
    fileCount.increment(2048)
    assert(fileCount.fileCount() === 2)
    assert(fileCount.sizeInBytes() === 3072) // 1024 + 2048

    // Reset and verify values are back to zero
    fileCount.reset()
    assert(fileCount.fileCount() === 0)
    assert(fileCount.sizeInBytes() === 0)
  }

  test("FileCount increment with fileCount and sizeInBytesInBytes") {
    val fileCount = new FileCounter()

    // Increment with multiple files and sizeInBytesInBytes
    fileCount.increment(10)
    fileCount.increment(15)
    assert(fileCount.fileCount() === 2)
    assert(fileCount.sizeInBytes() === 25)

    // Increment with more files and sizeInBytesInBytes
    fileCount.increment(20)
    assert(fileCount.fileCount() === 3)
    assert(fileCount.sizeInBytes() === 45)
  }

  test("FileCount toString representation") {
    val fileCount = new FileCounter()
    fileCount.increment(5120)

    val stringRepresentation = fileCount.toString()
    assert(stringRepresentation === "FileCount(fileCount=1, bytes=5120)")
  }
}
