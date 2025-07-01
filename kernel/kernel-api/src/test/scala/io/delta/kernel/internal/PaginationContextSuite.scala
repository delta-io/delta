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
package io.delta.kernel.internal

import java.util.Optional

import io.delta.kernel.internal.replay.PaginationContext

import org.scalatest.funsuite.AnyFunSuite

class PaginationContextSuite extends AnyFunSuite {

  test("forFirstPage should create context with empty optionals and specified page size") {
    val pageSize = 100L
    val context = PaginationContext.forFirstPage(pageSize)

    assert(!context.getLastReadLogFileName().isPresent)
    assert(!context.getLastReturnedRowIndex().isPresent)
    assert(!context.getLastReadSidecarFileIdx().isPresent)
    assert(context.getPageSize() === pageSize)
  }

  test("forPageWithPageToken should create context with provided values") {
    val lastReadLogFileName = "00000000000000000000.json"
    val lastReturnedRowIndex = 42L
    val lastReadSidecarFileIdx = Optional.of(java.lang.Long.valueOf(5L))
    val pageSize = 50L

    val context = PaginationContext.forPageWithPageToken(
      lastReadLogFileName,
      lastReturnedRowIndex,
      lastReadSidecarFileIdx,
      pageSize)

    assert(context.getLastReadLogFileName() === Optional.of(lastReadLogFileName))
    assert(context.getLastReturnedRowIndex() === Optional.of(lastReturnedRowIndex))
    assert(context.getLastReadSidecarFileIdx() === lastReadSidecarFileIdx)
    assert(context.getPageSize() === pageSize)
  }

  test("forPageWithPageToken should handle empty sidecar file index") {
    val lastReadLogFileName = "00000000000000000000.json"
    val lastReturnedRowIndex = 42L
    val lastReadSidecarFileIdx = Optional.empty[java.lang.Long]()
    val pageSize = 50L

    val context = PaginationContext.forPageWithPageToken(
      lastReadLogFileName,
      lastReturnedRowIndex,
      lastReadSidecarFileIdx,
      pageSize)

    assert(context.getLastReadLogFileName() === Optional.of(lastReadLogFileName))
    assert(context.getLastReturnedRowIndex() === Optional.of(lastReturnedRowIndex))
    assert(!context.getLastReadSidecarFileIdx().isPresent)
    assert(context.getPageSize() === pageSize)
  }

  test("should throw exception for zero page size") {
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forFirstPage(0L)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

  test("should throw exception for negative page size") {
    val negativePageSize = -10L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forFirstPage(negativePageSize)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

  test("should throw exception for negative page size with page token") {
    val lastReadLogFileName = "00000000000000000000.json"
    val lastReturnedRowIndex = 42L
    val lastReadSidecarFileIdx = Optional.empty[java.lang.Long]()
    val negativePageSize = -5L

    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        lastReadLogFileName,
        lastReturnedRowIndex,
        lastReadSidecarFileIdx,
        negativePageSize)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

}
