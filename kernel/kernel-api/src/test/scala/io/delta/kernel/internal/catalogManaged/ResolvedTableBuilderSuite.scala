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

package io.delta.kernel.internal.catalogManaged

import java.util.Collections

import scala.collection.JavaConverters._

import io.delta.kernel.TableManager
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.files.ParsedLogData
import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType
import io.delta.kernel.internal.table.ResolvedTableInternal
import io.delta.kernel.test.{ActionUtils, MockFileSystemClientUtils, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class ResolvedTableBuilderSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with ActionUtils
    with VectorTestUtils {

  private val emptyMockEngine = createMockFSListFromEngine(Nil)
  private val metadata = testMetadata(new StructType().add("c1", IntegerType.INTEGER))

  ///////////////////////////////////////
  // builder validation tests -- START //
  ///////////////////////////////////////

  test("loadTable: null path throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadTable(null)
    }
  }

  test("atVersion: negative version throws IllegalArgumentException") {
    val builder = TableManager.loadTable(dataPath.toString).atVersion(-1)

    val exMsg = intercept[IllegalArgumentException] {
      builder.build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "version must be >= 0")
  }

  test("withLogData: null input throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadTable(dataPath.toString).withLogData(null)
    }
  }

  Seq(
    ParsedLogData.forInlineData(0, ParsedLogType.RATIFIED_INLINE_COMMIT, emptyColumnarBatch),
    ParsedLogData.forFileStatus(logCompactionStatus(0, 1))).foreach { parsedLogData =>
    val suffix = s"- type=${parsedLogData.`type`}"
    test(s"withLogData: non-RATIFIED_STAGED_COMMIT throws IllegalArgumentException $suffix") {
      val builder = TableManager
        .loadTable(dataPath.toString)
        .withLogData(Collections.singletonList(parsedLogData))

      val exMsg = intercept[IllegalArgumentException] {
        builder.build(emptyMockEngine)
      }.getMessage

      assert(exMsg.contains("Only RATIFIED_STAGED_COMMIT log data is supported"))
    }
  }

  test("withLogData: non-contiguous input throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadTable(dataPath.toString)
        .withLogData(parsedRatifiedStagedCommits(Seq(0, 2)).toList.asJava)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("Log data must be sorted and contiguous"))
  }

  test("withLogData: non-sorted input throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadTable(dataPath.toString)
        .withLogData(parsedRatifiedStagedCommits(Seq(2, 1, 0)).toList.asJava)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("Log data must be sorted and contiguous"))
  }

  test("withProtocolAndMetadata - null protocol throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadTable(dataPath.toString)
        .withProtocolAndMetadata(null, metadata)
    }

    assertThrows[NullPointerException] {
      TableManager.loadTable(dataPath.toString)
        .withProtocolAndMetadata(new Protocol(1, 2), null)
    }
  }

  test("withProtocolAndMetadata - invalid readerVersion throws KernelException") {
    val exMsg = intercept[KernelException] {
      TableManager.loadTable(dataPath.toString)
        .withProtocolAndMetadata(new Protocol(999, 2), metadata)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("Unsupported Delta protocol reader version"))
  }

  test("withProtocolAndMetadata - unknown reader feature throws KernelException") {
    val exMsg = intercept[KernelException] {
      TableManager.loadTable(dataPath.toString)
        .withProtocolAndMetadata(
          new Protocol(3, 7, Set("unknownReaderFeature").asJava, Collections.emptySet()),
          metadata)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("Unsupported Delta table feature"))
  }

  /////////////////////////////////////
  // builder validation tests -- END //
  /////////////////////////////////////

  test("if P & M are provided then LogSegment is not loaded") {
    val resolvedTable = TableManager
      .loadTable(dataPath.toString)
      .atVersion(13)
      .withProtocolAndMetadata(new Protocol(1, 2), metadata)
      .withLogData(Collections.emptyList())
      .build(emptyMockEngine)
      .asInstanceOf[ResolvedTableInternal]

    assert(!resolvedTable.getLazyLogSegment.isPresent)
  }

  // TODO: Mock JSON reading and then actually read the P & M
}
