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
import io.delta.kernel.internal.util.{Tuple2 => KernelTuple2}
import io.delta.kernel.test.{ActionUtils, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}

import org.scalatest.funsuite.AnyFunSuite

class CommitMetadataSuite extends AnyFunSuite
    with ActionUtils
    with VectorTestUtils {

  private val protocol12 = new Protocol(1, 2)
  private val logPath = "/fake/_delta_log"
  private val createVersion0 = 0
  private val updateVersionNonZero = 1

  test("constructor validates non-negative version") {
    val ex = intercept[IllegalArgumentException] {
      new CommitMetadata(
        -1L, // negative version
        logPath,
        testCommitInfo(),
        Optional.empty(), // readPandMOpt
        Optional.empty(), // newProtocolOpt
        Optional.empty()
      ) // newMetadataOpt
    }
    assert(ex.getMessage.contains("version must be non-negative"))
  }

  test("constructor validates null parameters") {
    intercept[NullPointerException] {
      new CommitMetadata(
        updateVersionNonZero,
        null, // null logPath
        testCommitInfo(),
        Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
        Optional.empty(), // newProtocolOpt
        Optional.empty()
      ) // newMetadataOpt
    }

    intercept[NullPointerException] {
      new CommitMetadata(
        updateVersionNonZero,
        logPath,
        null, // null commitInfo
        Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
        Optional.empty(), // newProtocolOpt
        Optional.empty()
      ) // newMetadataOpt
    }
  }

  test("constructor validates readProtocol and readMetadata consistency") {
    // Both present is valid
    new CommitMetadata(
      updateVersionNonZero,
      logPath,
      testCommitInfo(),
      Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      Optional.empty(),
      Optional.empty())

    // Both absent is valid if new ones are present
    new CommitMetadata(
      createVersion0,
      logPath,
      testCommitInfo(),
      Optional.empty(),
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata))
  }

  test("constructor validates at least one protocol must be present") {
    intercept[IllegalArgumentException] {
      new CommitMetadata(
        createVersion0,
        logPath,
        testCommitInfo(),
        Optional.empty(), // no read protocol or metadata
        Optional.empty(), // no new protocol
        Optional.of(basicPartitionedMetadata))
    }
  }

  test("constructor validates at least one metadata must be present") {
    intercept[IllegalArgumentException] {
      new CommitMetadata(
        createVersion0,
        logPath,
        testCommitInfo(),
        Optional.empty(), // protocol or metadata
        Optional.of(protocol12),
        Optional.empty() // no new metadata
      )
    }
  }

  test("constructor validates ICT present if catalogManaged enabled") {
    val exMsg = intercept[IllegalArgumentException] {
      new CommitMetadata(
        createVersion0,
        logPath,
        testCommitInfo(ictEnabled = false), // ICT not enabled!
        Optional.empty(),
        Optional.of(protocolWithCatalogManagedSupport),
        Optional.of(basicPartitionedMetadata))
    }.getMessage

    assert(exMsg.contains("InCommitTimestamp must be present for commits to catalogManaged tables"))
  }

  test("getEffectiveProtocol returns new protocol when present") {
    val newProtocol = new Protocol(2, 3)
    val commitMetadata = new CommitMetadata(
      updateVersionNonZero,
      logPath,
      testCommitInfo(),
      Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      Optional.of(newProtocol),
      Optional.empty())

    assert(commitMetadata.getEffectiveProtocol == newProtocol)
  }

  test("getEffectiveProtocol returns read protocol when new protocol absent") {
    val commitMetadata = new CommitMetadata(
      updateVersionNonZero,
      logPath,
      testCommitInfo(),
      Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      Optional.empty(),
      Optional.empty())

    assert(commitMetadata.getEffectiveProtocol == protocol12)
  }

  test("getEffectiveMetadata returns new metadata when present") {
    val newMetadata = testMetadata(new StructType().add("newCol", IntegerType.INTEGER))
    val commitMetadata = new CommitMetadata(
      updateVersionNonZero,
      logPath,
      testCommitInfo(),
      Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      Optional.empty(),
      Optional.of(newMetadata))

    assert(commitMetadata.getEffectiveMetadata == newMetadata)
  }

  test("getEffectiveMetadata returns read metadata when new metadata absent") {
    val commitMetadata = new CommitMetadata(
      updateVersionNonZero,
      logPath,
      testCommitInfo(),
      Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      Optional.empty(),
      Optional.empty())

    assert(commitMetadata.getEffectiveMetadata == basicPartitionedMetadata)
  }

  // ========== CommitType Tests START ==========

  case class CommitTypeTestCase(
      readPandMOpt: Optional[KernelTuple2[Protocol, Metadata]] = Optional.empty(),
      newProtocolOpt: Optional[Protocol] = Optional.empty(),
      newMetadataOpt: Optional[Metadata] = Optional.empty(),
      expectedCommitType: CommitType)

  private val commitTypeTestCases = Seq(
    CommitTypeTestCase(
      readPandMOpt = Optional.empty(), // No read state for table creation
      newProtocolOpt = Optional.of(protocol12),
      newMetadataOpt = Optional.of(basicPartitionedMetadata),
      expectedCommitType = CommitType.FILESYSTEM_CREATE),
    CommitTypeTestCase(
      readPandMOpt = Optional.empty(), // No read state for table creation
      newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      newMetadataOpt = Optional.of(basicPartitionedMetadata),
      expectedCommitType = CommitType.CATALOG_CREATE),
    CommitTypeTestCase(
      readPandMOpt = Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      expectedCommitType = CommitType.FILESYSTEM_WRITE),
    CommitTypeTestCase(
      readPandMOpt = Optional.of(
        new KernelTuple2(protocolWithCatalogManagedSupport, basicPartitionedMetadata)),
      expectedCommitType = CommitType.CATALOG_WRITE),
    CommitTypeTestCase(
      readPandMOpt = Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)),
      newProtocolOpt = Optional.of(protocolWithCatalogManagedSupport),
      expectedCommitType = CommitType.FILESYSTEM_UPGRADE_TO_CATALOG),
    CommitTypeTestCase(
      readPandMOpt = Optional.of(
        new KernelTuple2(protocolWithCatalogManagedSupport, basicPartitionedMetadata)),
      newProtocolOpt = Optional.of(protocol12),
      expectedCommitType = CommitType.CATALOG_DOWNGRADE_TO_FILESYSTEM))

  commitTypeTestCases.foreach { testCase =>
    test(s"getCommitType returns ${testCase.expectedCommitType}") {
      // version > 0 for writes, version 0 for create
      val version = if (testCase.readPandMOpt.isPresent) 1L else 0L

      val commitMetadata = new CommitMetadata(
        version,
        logPath,
        testCommitInfo(),
        testCase.readPandMOpt,
        testCase.newProtocolOpt,
        testCase.newMetadataOpt)

      assert(commitMetadata.getCommitType == testCase.expectedCommitType)
    }
  }

  // ========== CommitType Tests END ==========

  test("checkReadStateAbsentIfAndOnlyIfVersion0 - version 0 with absent readState should pass") {
    // This should pass: version 0 (table creation) with absent readPandMOpt
    new CommitMetadata(
      createVersion0,
      logPath,
      testCommitInfo(),
      Optional.empty(), // absent readPandMOpt
      Optional.of(protocol12),
      Optional.of(basicPartitionedMetadata))
  }

  test("checkReadStateAbsentIfAndOnlyIfVersion0 - version 0 with present readState should fail") {
    // This should fail: version 0 (table creation) with present readPandMOpt
    val exMsg = intercept[IllegalArgumentException] {
      new CommitMetadata(
        createVersion0,
        logPath,
        testCommitInfo(),
        Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)), // present readPandMOpt
        Optional.empty(),
        Optional.empty())
    }.getMessage
    assert(exMsg.contains("Table creation (version 0) requires absent readPandMOpt"))
  }

  test("checkReadStateAbsentIfAndOnlyIfVersion0 - version > 0 with present readState should pass") {
    // This should pass: version > 0 (existing table) with present readPandMOpt
    new CommitMetadata(
      updateVersionNonZero,
      logPath,
      testCommitInfo(),
      Optional.of(new KernelTuple2(protocol12, basicPartitionedMetadata)), // present readPandMOpt
      Optional.empty(),
      Optional.empty())
  }

  test("checkReadStateAbsentIfAndOnlyIfVersion0 - version > 0 with absent readState should fail") {
    // This should fail: version > 0 (existing table) with absent readPandMOpt
    val exMsg = intercept[IllegalArgumentException] {
      new CommitMetadata(
        updateVersionNonZero,
        logPath,
        testCommitInfo(),
        Optional.empty(), // absent readPandMOpt
        Optional.of(protocol12),
        Optional.of(basicPartitionedMetadata))
    }.getMessage
    assert(exMsg.contains("existing table writes (version > 0) require present readPandMOpt"))
  }
}
