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
    assert(fileCount.count() === 0)
    assert(fileCount.size() === 0)

    // Increment by one file with size
    fileCount.increment(1024)
    assert(fileCount.count() === 1)
    assert(fileCount.size() === 1024)

    // Increment by one more file with different size
    fileCount.increment(2048)
    assert(fileCount.count() === 2)
    assert(fileCount.size() === 3072) // 1024 + 2048

    // Reset and verify values are back to zero
    fileCount.reset()
    assert(fileCount.count() === 0)
    assert(fileCount.size() === 0)
  }

  test("FileCount increment with count and size") {
    val fileCount = new FileCounter()

    // Increment with multiple files and size
    fileCount.increment(5, 10240)
    assert(fileCount.count() === 5)
    assert(fileCount.size() === 10240)

    // Increment with more files and size
    fileCount.increment(3, 6144)
    assert(fileCount.count() === 8) // 5 + 3
    assert(fileCount.size() === 16384) // 10240 + 6144
  }

  test("FileCount toString representation") {
    val fileCount = new FileCounter()
    fileCount.increment(3, 5120)

    val stringRepresentation = fileCount.toString()
    assert(stringRepresentation === "FileCount(count=3, size=5120)")
  }
}
