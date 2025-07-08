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

import java.util
import java.util.{HashMap, Map}
import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.data.Row
import io.delta.kernel.internal.annotation.VisibleForTesting
import io.delta.kernel.internal.data.GenericRow
import io.delta.kernel.internal.replay.PageToken
import io.delta.kernel.test.MockFileSystemClientUtils
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class PageTokenSuite extends AnyFunSuite with MockFileSystemClientUtils {

  private val TEST_FILE_NAME = "/path/to/table/test_file.json"
  private val TEST_ROW_INDEX = 42L
  private val TEST_SIDECAR_INDEX = Optional.of(java.lang.Long.valueOf(5L))
  private val TEST_KERNEL_VERSION = "4.0.0"
  private val TEST_TABLE_PATH = "/path/to/table"
  private val TEST_TABLE_VERSION = 5L
  private val TEST_PREDICATE_HASH = 123L
  private val TEST_LOG_SEGMENT_HASH = 456L

  private val expectedPageToken = new PageToken(
    TEST_FILE_NAME,
    TEST_ROW_INDEX,
    TEST_SIDECAR_INDEX,
    TEST_KERNEL_VERSION,
    TEST_TABLE_PATH,
    TEST_TABLE_VERSION,
    TEST_PREDICATE_HASH,
    TEST_LOG_SEGMENT_HASH)

  private val rowData: Map[Integer, Object] = new HashMap()
  rowData.put(0, TEST_FILE_NAME)
  rowData.put(1, TEST_ROW_INDEX.asInstanceOf[Object])
  rowData.put(2, TEST_SIDECAR_INDEX.get())
  rowData.put(3, TEST_KERNEL_VERSION)
  rowData.put(4, TEST_TABLE_PATH)
  rowData.put(5, TEST_TABLE_VERSION.asInstanceOf[Object])
  rowData.put(6, TEST_PREDICATE_HASH.asInstanceOf[Object])
  rowData.put(7, TEST_LOG_SEGMENT_HASH.asInstanceOf[Object])

  val expectedRow = new GenericRow(PageToken.PAGE_TOKEN_SCHEMA, rowData)

  test("PageToken.fromRow with valid data") {
    val pageToken = PageToken.fromRow(expectedRow)
    assert(pageToken.equals(expectedPageToken))
  }

  test("PageToken.toRow with valid data") {
    val row = expectedPageToken.toRow
    assert(row.getSchema.equals(PageToken.PAGE_TOKEN_SCHEMA))

    assert(row.getString(0) == TEST_FILE_NAME)
    assert(row.getLong(1) == TEST_ROW_INDEX)
    assert(Optional.of(row.getLong(2)) == TEST_SIDECAR_INDEX)
    assert(row.getString(3) == TEST_KERNEL_VERSION)
    assert(row.getString(4) == TEST_TABLE_PATH)
    assert(row.getLong(5) == TEST_TABLE_VERSION)
    assert(row.getLong(6) == TEST_PREDICATE_HASH)
    assert(row.getLong(7) == TEST_LOG_SEGMENT_HASH)
  }

  test("E2E: PageToken round-trip: toRow -> fromRow") {
    val row = expectedPageToken.toRow
    val reconstructedPageToken = PageToken.fromRow(row)
    assert(reconstructedPageToken.equals(expectedPageToken))
  }

  test("PageToken.fromRow throws exception when input row schema has invalid field name") {
    val invalidSchema = new StructType()
      .add("wrongFieldName", StringType.STRING)
      .add("lastReturnedRowIndex", LongType.LONG)
      .add("lastReadSidecarFileIdx", LongType.LONG)
      .add("kernelVersion", StringType.STRING)
      .add("tablePath", StringType.STRING)
      .add("tableVersion", LongType.LONG)
      .add("predicateHash", LongType.LONG)
      .add("logSegmentHash", LongType.LONG)

    val invalidRowData: Map[Integer, Object] = new HashMap()
    invalidRowData.put(0, TEST_FILE_NAME)
    invalidRowData.put(1, TEST_ROW_INDEX.asInstanceOf[Object])
    invalidRowData.put(2, TEST_SIDECAR_INDEX)
    invalidRowData.put(3, TEST_KERNEL_VERSION)
    invalidRowData.put(4, TEST_TABLE_PATH)
    invalidRowData.put(5, TEST_TABLE_VERSION.asInstanceOf[Object])
    invalidRowData.put(6, TEST_PREDICATE_HASH.asInstanceOf[Object])
    invalidRowData.put(7, TEST_LOG_SEGMENT_HASH.asInstanceOf[Object])

    val row = new GenericRow(invalidSchema, invalidRowData)
    val exception = intercept[IllegalArgumentException] {
      PageToken.fromRow(row)
    }
    assert(exception.getMessage.contains(
      "Invalid Page Token: input row schema does not match expected PageToken schema"))
  }

  test("PageToken.fromRow throws exception when input row schema has wrong data type") {
    val invalidSchema = new StructType()
      .add("lastReadLogFilePath", StringType.STRING)
      .add("lastReturnedRowIndex", LongType.LONG)
      .add("lastReadSidecarFileIdx", StringType.STRING) // should be long type
      .add("kernelVersion", StringType.STRING)
      .add("tablePath", StringType.STRING)
      .add("tableVersion", LongType.LONG)
      .add("predicateHash", LongType.LONG)
      .add("logSegmentHash", LongType.LONG)

    val invalidRowData: Map[Integer, Object] = new HashMap()
    invalidRowData.put(0, TEST_FILE_NAME)
    invalidRowData.put(1, TEST_ROW_INDEX.asInstanceOf[Object])
    invalidRowData.put(2, TEST_SIDECAR_INDEX)
    invalidRowData.put(3, TEST_KERNEL_VERSION)
    invalidRowData.put(4, TEST_TABLE_PATH)
    invalidRowData.put(5, TEST_TABLE_VERSION.asInstanceOf[Object])
    invalidRowData.put(6, TEST_PREDICATE_HASH.asInstanceOf[Object])
    invalidRowData.put(7, TEST_LOG_SEGMENT_HASH.asInstanceOf[Object])

    val row = new GenericRow(invalidSchema, invalidRowData)
    val exception = intercept[IllegalArgumentException] {
      PageToken.fromRow(row)
    }
    assert(exception.getMessage.contains(
      "Invalid Page Token: input row schema does not match expected PageToken schema"))
  }

  test("PageToken.fromRow accepts the case sidecar field is null") {
    val nullSidecarData: Map[Integer, Object] = new HashMap(rowData)
    nullSidecarData.put(2, null)
    val nullSidecarRow = new GenericRow(PageToken.PAGE_TOKEN_SCHEMA, nullSidecarData)
    val pageToken = PageToken.fromRow(nullSidecarRow)
    assert(pageToken.getLastReadSidecarFileIdx == Optional.empty())
  }

  test("PageToken.fromRow throws exception when required field is null") {
    val invalidData: Map[Integer, Object] = new HashMap(rowData)
    invalidData.put(3, null)
    val invalidRow = new GenericRow(PageToken.PAGE_TOKEN_SCHEMA, invalidData)
    val exception = intercept[IllegalArgumentException] {
      PageToken.fromRow(invalidRow)
    }
    assert(exception.getMessage.contains(
      "Invalid Page Token: required field"))
  }
}
