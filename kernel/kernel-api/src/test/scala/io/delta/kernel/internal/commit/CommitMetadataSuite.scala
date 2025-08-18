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

package io.delta.kernel.internal.commit

import java.util.Optional

import io.delta.kernel.commit.CommitMetadata
import io.delta.kernel.commit.CommitMetadata.CommitType
import io.delta.kernel.internal.actions.{Metadata, Protocol}
import io.delta.kernel.test.{ActionUtils, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class CommitMetadataSuite extends AnyFunSuite
    with ActionUtils
    with VectorTestUtils {

  private val protocol12 = new Protocol(1, 2)
  private val logPath = "/fake/_delta_log"
  private val version = 5L

  test("constructor validates non-negative version") {
    val ex = intercept[IllegalArgumentException] {
      new CommitMetadata(
        -1L, // negative version
        logPath,
        testCommitInfo(),
        Optional.of(protocol12),
        Optional.of(basicPartitionedMetadata),
        Optional.empty(),
        Optional.empty())
    }
    assert(ex.getMessage.contains("version must be non-negative"))
  }

  test("constructor validates null parameters") {
    intercept[NullPointerException] {
      new CommitMetadata(
        version,
        null, // null logPath
        testCommitInfo(),
        Optional.of(protocol12),
        Optional.of(basicPartitionedMetadata),
        Optional.empty(),
        Optional.empty())
    }

    intercept[NullPointerException] {
      new CommitMetadata(
        version,
        logPath,
        null, // null commitInfo
        Optional.of(protocol12),
        Optional.of(basicPartitionedMetadata),
        Optional.empty(),
        Optional.empty())
    }
  }

  test("constructor validates readProtocol and readMetadata consistency") {
    // Both present is valid
    new CommitMetadata(
      version,
      logPath,
      testCommitInfo(),
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata),
      Optional.empty(),
      Optional.empty())

    // Both absent is valid if new ones are present
    new CommitMetadata(
      version,
      logPath,
      testCommitInfo(),
      Optional.empty(),
      Optional.empty(),
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata))

    // One present, one absent should fail
    intercept[IllegalArgumentException] {
      new CommitMetadata(
        version,
        logPath,
        testCommitInfo(),
        Optional.of(protocol12),
        Optional.empty(), // readMetadata absent but readProtocol present
        Optional.empty(),
        Optional.empty())
    }
  }

  test("constructor validates at least one protocol must be present") {
    intercept[IllegalArgumentException] {
      new CommitMetadata(
        version,
        logPath,
        testCommitInfo(),
        Optional.empty(), // no read protocol
        Optional.empty(),
        Optional.empty(), // no new protocol
        Optional.of(basicPartitionedMetadata))
    }
  }

  test("constructor validates at least one metadata must be present") {
    intercept[IllegalArgumentException] {
      new CommitMetadata(
        version,
        logPath,
        testCommitInfo(),
        Optional.empty(),
        Optional.empty(), // no read metadata
        Optional.of(protocol12),
        Optional.empty() // no new metadata
      )
    }
  }

  test("constructor validates ICT present if catalogManaged enabled") {
    val exMsg = intercept[IllegalArgumentException] {
      new CommitMetadata(
        version,
        logPath,
        testCommitInfo(ictEnabled = false), // ICT not enabled!
        Optional.empty(),
        Optional.empty(),
        Optional.of(protocolWithCatalogManagedSupport),
        Optional.of(basicPartitionedMetadata))
    }.getMessage

    assert(exMsg.contains("InCommitTimestamp must be present for commits to catalogManaged tables"))
  }

  test("getEffectiveProtocol returns new protocol when present") {
    val newProtocol = new Protocol(2, 3)
    val commitMetadata = new CommitMetadata(
      version,
      logPath,
      testCommitInfo(),
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata),
      Optional.of(newProtocol),
      Optional.empty())

    assert(commitMetadata.getEffectiveProtocol == newProtocol)
  }

  test("getEffectiveProtocol returns read protocol when new protocol absent") {
    val commitMetadata = new CommitMetadata(
      version,
      logPath,
      testCommitInfo(),
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata),
      Optional.empty(),
      Optional.empty())

    assert(commitMetadata.getEffectiveProtocol == protocol12)
  }

  test("getEffectiveMetadata returns new metadata when present") {
    val newMetadata = testMetadata(new StructType().add("newCol", IntegerType.INTEGER))
    val commitMetadata = new CommitMetadata(
      version,
      logPath,
      testCommitInfo(),
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata),
      Optional.empty(),
      Optional.of(newMetadata))

    assert(commitMetadata.getEffectiveMetadata == newMetadata)
  }

  test("getEffectiveMetadata returns read metadata when new metadata absent") {
    val commitMetadata = new CommitMetadata(
      version,
      logPath,
      testCommitInfo(),
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata),
      Optional.empty(),
      Optional.empty())

    assert(commitMetadata.getEffectiveMetadata == basicPartitionedMetadata)
  }

  // ========== CommitType Tests START ==========

  case class CommitTypeTestCase(
      readProtocolOpt: Optional[Protocol] = Optional.empty(),
      readMetadataOpt: Optional[Metadata] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty(),
      expectedCommitType: CommitType)

  private val commitTypeTestCases = Seq(
    CommitTypeTestCase(
      newProtocolOpt = Optional.of(protocol12),
      newMetadataOpt = Optional.of(basicPartitionedMetadata),
      expectedCommitType = CommitType.FILESYSTEM_CREATE),
    CommitTypeTestCase(
      newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      newMetadataOpt = Optional.of(basicPartitionedMetadata),
      expectedCommitType = CommitType.CATALOG_CREATE),
    CommitTypeTestCase(
      readProtocolOpt = Optional.of(protocol12),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      expectedCommitType = CommitType.FILESYSTEM_WRITE),
    CommitTypeTestCase(
      readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      expectedCommitType = CommitType.CATALOG_WRITE),
    CommitTypeTestCase(
      readProtocolOpt = Optional.of(protocol12),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      expectedCommitType = CommitType.FILESYSTEM_UPGRADE_TO_CATALOG),
    CommitTypeTestCase(
      readProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      readMetadataOpt = Optional.of(basicPartitionedMetadata),
      newProtocolOpt = Optional.of(protocol12),
      expectedCommitType = CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM))

  commitTypeTestCases.foreach { testCase =>
    test(s"getCommitType returns ${testCase.expectedCommitType}") {
      val commitMetadata = new CommitMetadata(
        version,
        logPath,
        testCommitInfo(),
        testCase.readProtocolOpt,
        testCase.readMetadataOpt,
        testCase.newProtocolOpt,
        testCase.newMetadataOpt)

      assert(commitMetadata.getCommitType == testCase.expectedCommitType)
    }
  }

  // ========== CommitType Tests END ==========
}
