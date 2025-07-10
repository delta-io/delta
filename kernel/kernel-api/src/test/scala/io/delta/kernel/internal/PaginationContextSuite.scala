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

import io.delta.kernel.Meta
import io.delta.kernel.internal.replay.{PageToken, PaginationContext}

import org.scalatest.funsuite.AnyFunSuite

class PaginationContextSuite extends AnyFunSuite {

  private val TEST_FILE_NAME = "test_file.json"
  private val TEST_ROW_INDEX = 42L
  private val TEST_SIDECAR_INDEX = Optional.of(java.lang.Long.valueOf(5L))
  private val TEST_INVALID_KERNEL_VERSION = "300.0.0"
  private val TEST_VALID_KERNEL_VERSION = Meta.KERNEL_VERSION
  private val TEST_TABLE_PATH = "/path/to/table"
  private val TEST_WRONG_TABLE_PATH = "/wrong/path/to/table"
  private val TEST_TABLE_VERSION = 5L
  private val TEST_WRONG_TABLE_VERSION = 5000L
  private val TEST_PREDICATE_HASH = 123L
  private val TEST_LOG_SEGMENT_HASH = 456L

  private val validPageToken = new PageToken(
    TEST_FILE_NAME,
    TEST_ROW_INDEX,
    TEST_SIDECAR_INDEX,
    TEST_VALID_KERNEL_VERSION,
    TEST_TABLE_PATH,
    TEST_TABLE_VERSION,
    TEST_PREDICATE_HASH,
    TEST_LOG_SEGMENT_HASH)

  private val invalidKernelVersionPageToken = new PageToken(
    TEST_FILE_NAME,
    TEST_ROW_INDEX,
    TEST_SIDECAR_INDEX,
    TEST_INVALID_KERNEL_VERSION,
    TEST_TABLE_PATH,
    TEST_TABLE_VERSION,
    TEST_PREDICATE_HASH,
    TEST_LOG_SEGMENT_HASH)

  test("forFirstPage should create context with empty optionals and specified page size") {
    val pageSize = 100L
    val context = PaginationContext.forFirstPage(TEST_TABLE_PATH, TEST_TABLE_VERSION, pageSize)

    assert(!context.getLastReadLogFileName().isPresent)
    assert(!context.getLastReturnedRowIndex().isPresent)
    assert(!context.getLastReadSidecarFileIdx().isPresent)
    assert(context.getPageSize() === pageSize)
  }

  test("forPageWithPageToken should create context with provided values") {
    val pageSize = 50L
    val context = PaginationContext.forPageWithPageToken(
      TEST_TABLE_PATH,
      TEST_TABLE_VERSION,
      pageSize,
      validPageToken)

    assert(context.getLastReadLogFileName() === Optional.of(TEST_FILE_NAME))
    assert(context.getLastReturnedRowIndex() === Optional.of(TEST_ROW_INDEX))
    assert(context.getLastReadSidecarFileIdx() === TEST_SIDECAR_INDEX)
    assert(context.getPageSize() === pageSize)
  }

  test("forPageWithPageToken should throw exception when page token is null") {
    val pageSize = 50L

    val e = intercept[NullPointerException] {
      PaginationContext.forPageWithPageToken(TEST_TABLE_PATH, TEST_TABLE_VERSION, pageSize, null)
    }
    assert(e.getMessage === "page token is null")
  }

  test("should throw exception for zero page size") {
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forFirstPage(TEST_TABLE_PATH, TEST_TABLE_VERSION, 0L)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

  test("should throw exception for negative page size") {
    val negativePageSize = -10L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forFirstPage(TEST_TABLE_PATH, TEST_TABLE_VERSION, negativePageSize)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

  test("should throw exception for negative page size with page token") {
    val negativePageSize = -5L

    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        negativePageSize,
        validPageToken)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

  test("should throw exception when the requested kernel version doesn't " +
    "match the value in page token") {
    val pageSize = 50L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        pageSize,
        invalidKernelVersionPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: kernel version"))
  }

  test("should throw exception for when the requested table path doesn't " +
    "match the value in page token") {
    val pageSize = 50L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_WRONG_TABLE_PATH,
        TEST_TABLE_VERSION,
        pageSize,
        validPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: table path"))
  }

  test("should throw exception for when the requested table version doesn't " +
    "match the value in page token") {
    val pageSize = 50L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_WRONG_TABLE_VERSION,
        pageSize,
        validPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: table version"))
  }
}
