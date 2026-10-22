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

import java.util.{Collections, Optional}

import scala.collection.JavaConverters._

import io.delta.kernel.Meta
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.replay.{PageToken, PaginationContext}
import io.delta.kernel.internal.snapshot.LogSegment
import io.delta.kernel.test.MockFileSystemClientUtils

import org.scalatest.funsuite.AnyFunSuite

class PaginationContextSuite extends AnyFunSuite with MockFileSystemClientUtils {

  private val TEST_FILE_NAME = "test_file.json"
  private val TEST_ROW_INDEX = 42L
  private val TEST_SIDECAR_INDEX = Optional.of(java.lang.Long.valueOf(5L))
  private val TEST_INVALID_KERNEL_VERSION = "300.0.0"
  private val TEST_VALID_KERNEL_VERSION = Meta.KERNEL_VERSION
  private val TEST_TABLE_PATH = "/path/to/table"
  private val TEST_WRONG_TABLE_PATH = "/wrong/path/to/table"
  private val TEST_TABLE_VERSION = 5L
  private val TEST_WRONG_TABLE_VERSION = 5000L
  private val TEST_PREDICATE_HASH = 123
  private val TEST_WRONG_PREDICATE_HASH = 321
  private val TEST_LOG_SEGMENT_HASH = 456
  private val TEST_WRONG_LOG_SEGMENT_HASH = 654

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
    val context = PaginationContext.forFirstPage(
      TEST_TABLE_PATH,
      TEST_TABLE_VERSION,
      TEST_LOG_SEGMENT_HASH,
      TEST_PREDICATE_HASH,
      pageSize)

    assert(!context.getLastReadLogFilePath().isPresent)
    assert(!context.getLastReturnedRowIndex().isPresent)
    assert(!context.getLastReadSidecarFileIdx().isPresent)
    assert(context.getPageSize() === pageSize)
  }

  test("forPageWithPageToken should create context with provided values") {
    val pageSize = 50L
    val context = PaginationContext.forPageWithPageToken(
      TEST_TABLE_PATH,
      TEST_TABLE_VERSION,
      TEST_LOG_SEGMENT_HASH,
      TEST_PREDICATE_HASH,
      pageSize,
      validPageToken)

    assert(context.getLastReadLogFilePath() === Optional.of(TEST_FILE_NAME))
    assert(context.getLastReturnedRowIndex() === Optional.of(TEST_ROW_INDEX))
    assert(context.getLastReadSidecarFileIdx() === TEST_SIDECAR_INDEX)
    assert(context.getPageSize() === pageSize)
  }

  test("forPageWithPageToken should throw exception when page token is null") {
    val pageSize = 50L

    val e = intercept[NullPointerException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        TEST_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
        pageSize,
        null /* page token */ )
    }
    assert(e.getMessage === "page token is null")
  }

  test("should throw exception for zero page size") {
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forFirstPage(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        TEST_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
        0L)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

  test("should throw exception for negative page size") {
    val negativePageSize = -10L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forFirstPage(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        TEST_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
        negativePageSize)
    }
    assert(e.getMessage === "Page size must be greater than zero!")
  }

  test("should throw exception for negative page size with page token") {
    val negativePageSize = -5L

    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        TEST_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
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
        TEST_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
        pageSize,
        invalidKernelVersionPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: token kernel version"))
  }

  test("should throw exception for when the requested table path doesn't " +
    "match the value in page token") {
    val pageSize = 50L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_WRONG_TABLE_PATH,
        TEST_TABLE_VERSION,
        TEST_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
        pageSize,
        validPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: token table path"))
  }

  test("should throw exception for when the requested table version doesn't " +
    "match the value in page token") {
    val pageSize = 50L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_WRONG_TABLE_VERSION,
        TEST_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
        pageSize,
        validPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: token table version"))
  }

  test("should throw exception for when the requested predicate doesn't " +
    "match the value in page token") {
    val pageSize = 50L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        TEST_LOG_SEGMENT_HASH,
        TEST_WRONG_PREDICATE_HASH,
        pageSize,
        validPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: token predicate"))
  }

  test("should throw exception for when the requested log segment doesn't " +
    "match the value in page token") {
    val pageSize = 50L
    val e = intercept[IllegalArgumentException] {
      PaginationContext.forPageWithPageToken(
        TEST_TABLE_PATH,
        TEST_TABLE_VERSION,
        TEST_WRONG_LOG_SEGMENT_HASH,
        TEST_PREDICATE_HASH,
        pageSize,
        validPageToken)
    }
    assert(e.getMessage.contains("Invalid page token: token log segment"))
  }

  test("pagination should work when staged commit becomes published - #4927") {
    // This test verifies the fix for issue #4927 where pagination fails when
    // a staged commit (9.uuid.json) is renamed to a published commit (9.json)
    // between pagination requests

    val pageSize = 100L
    val version = 9L

    // First page request: LogSegment contains staged commit
    val stagedCommit = stagedCommitFile(9)
    val logSegmentStaged = new LogSegment(
      logPath,
      version,
      Collections.singletonList(stagedCommit),
      Collections.emptyList(),
      Collections.emptyList(),
      stagedCommit,
      Optional.empty(),
      Optional.empty())

    val stagedHash = logSegmentStaged.hashCode()

    // Create page token from first page with staged commit hash
    val pageToken = new PageToken(
      "00000000000000000009.dc0f9f58-a1a0-46fd-971a-bd8b2e9dbb81.json",
      TEST_ROW_INDEX,
      TEST_SIDECAR_INDEX,
      TEST_VALID_KERNEL_VERSION,
      TEST_TABLE_PATH,
      version,
      TEST_PREDICATE_HASH,
      stagedHash)

    // Second page request: Same version but now with published commit
    val publishedCommit = deltaFileStatus(9)
    val logSegmentPublished = new LogSegment(
      logPath,
      version,
      Collections.singletonList(publishedCommit),
      Collections.emptyList(),
      Collections.emptyList(),
      publishedCommit,
      Optional.empty(),
      Optional.empty())

    val publishedHash = logSegmentPublished.hashCode()

    // The hashes should be equal because they represent the same logical version
    assert(
      stagedHash == publishedHash,
      "LogSegment hashes should be equal for staged and published commits of same version")

    // This should NOT throw an exception - the fix allows pagination to continue
    val secondPageContext = PaginationContext.forPageWithPageToken(
      TEST_TABLE_PATH,
      version,
      publishedHash,
      TEST_PREDICATE_HASH,
      pageSize,
      pageToken)

    assert(secondPageContext.getLastReturnedRowIndex() === Optional.of(TEST_ROW_INDEX))
  }

  test("pagination across multiple versions with mixed staged and published commits") {
    // Test pagination with a more complex scenario: multiple versions with
    // some staged and some published commits

    val pageSize = 100L
    val version = 9L

    // First page: Mix of published and staged commits (versions 7, 8, 9)
    val deltasPage1 = Seq(
      deltaFileStatus(7), // published
      stagedCommitFile(8), // staged
      deltaFileStatus(9) // published
    ).asJava

    val logSegment1 = new LogSegment(
      logPath,
      version,
      deltasPage1,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(9),
      Optional.empty(),
      Optional.empty())

    val hash1 = logSegment1.hashCode()

    val pageToken = new PageToken(
      "00000000000000000009.json",
      TEST_ROW_INDEX,
      TEST_SIDECAR_INDEX,
      TEST_VALID_KERNEL_VERSION,
      TEST_TABLE_PATH,
      version,
      TEST_PREDICATE_HASH,
      hash1)

    // Second page: Same versions but version 8 is now published
    val deltasPage2 = Seq(
      deltaFileStatus(7), // published (same)
      deltaFileStatus(8), // NOW published (was staged)
      deltaFileStatus(9) // published (same)
    ).asJava

    val logSegment2 = new LogSegment(
      logPath,
      version,
      deltasPage2,
      Collections.emptyList(),
      Collections.emptyList(),
      deltaFileStatus(9),
      Optional.empty(),
      Optional.empty())

    val hash2 = logSegment2.hashCode()

    // Hashes should match despite file name changes
    assert(hash1 == hash2, "Hashes should be equal when logical versions are the same")

    // Pagination should continue without error
    val secondPageContext = PaginationContext.forPageWithPageToken(
      TEST_TABLE_PATH,
      version,
      hash2,
      TEST_PREDICATE_HASH,
      pageSize,
      pageToken)

    assert(secondPageContext.getLastReturnedRowIndex() == Optional.of(TEST_ROW_INDEX))
  }
}
