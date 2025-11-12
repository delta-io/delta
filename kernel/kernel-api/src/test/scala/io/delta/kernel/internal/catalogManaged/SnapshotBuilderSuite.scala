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
import io.delta.kernel.commit.{CommitMetadata, CommitResponse, Committer}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, UnsupportedProtocolVersionException, UnsupportedTableFeatureException}
import io.delta.kernel.internal.actions.Protocol
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter
import io.delta.kernel.internal.files.{ParsedCatalogCommitData, ParsedLogData, ParsedPublishedDeltaData}
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.test.{ActionUtils, MockFileSystemClientUtils, MockSnapshotUtils, VectorTestUtils}
import io.delta.kernel.types.{IntegerType, StructType}
import io.delta.kernel.utils.CloseableIterator

import org.scalatest.funsuite.AnyFunSuite

class SnapshotBuilderSuite extends AnyFunSuite
    with MockFileSystemClientUtils
    with ActionUtils
    with VectorTestUtils
    with MockSnapshotUtils {

  private val emptyMockEngine = createMockFSListFromEngine(Nil)
  private val protocol = new Protocol(1, 2)
  private val metadata = testMetadata(new StructType().add("c1", IntegerType.INTEGER))
  private val mockSnapshotAtTimestamp0 =
    getMockSnapshot(dataPath, latestVersion = 0L, timestamp = 0L)

  ///////////////////////////////////////
  // Builder Validation Tests -- START //
  ///////////////////////////////////////

  test("loadTable: null path throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadSnapshot(null)
    }
  }

  // ===== Version Tests ===== //

  test("atVersion: negative version throws IllegalArgumentException") {
    val builder = TableManager.loadSnapshot(dataPath.toString).atVersion(-1)

    val exMsg = intercept[IllegalArgumentException] {
      builder.build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "version must be >= 0")
  }

  // ===== Timestamp Tests ===== //

  test("atTimestamp: null latestSnapshot throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadSnapshot(dataPath.toString).atTimestamp(1000L, null)
    }
  }

  test("atTimestamp: negative timestamp throws IllegalArgumentException") {
    val builder =
      TableManager.loadSnapshot(dataPath.toString).atTimestamp(-1L, mockSnapshotAtTimestamp0)

    val exMsg = intercept[IllegalArgumentException] {
      builder.build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "timestamp must be >= 0")
  }

  test("atTimestamp: timestamp greater than latest snapshot throws IllegalArgumentException") {
    val builder =
      TableManager.loadSnapshot(dataPath.toString).atTimestamp(99, mockSnapshotAtTimestamp0)

    val exMsg = intercept[KernelException] {
      builder.build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("The provided timestamp 99 ms"))
    assert(exMsg.contains("is after the latest available version"))
  }

  test("atTimestamp: timestamp and version both provided throws IllegalArgumentException") {
    val builder = TableManager.loadSnapshot(dataPath.toString)
      .atVersion(1)
      .atTimestamp(0L, mockSnapshotAtTimestamp0)

    val exMsg = intercept[IllegalArgumentException] {
      builder.build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "timestamp and version cannot be provided together")
  }

  test("atTimestamp: protocol and metadata with timestamp throws IllegalArgumentException") {
    val builder = TableManager.loadSnapshot(dataPath.toString)
      .atTimestamp(0L, mockSnapshotAtTimestamp0)
      .withProtocolAndMetadata(protocol, metadata)

    val exMsg = intercept[IllegalArgumentException] {
      builder.build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "protocol and metadata can only be provided if a version is provided")
  }

  // ===== Committer Tests ===== //

  test("withCommitter: null committer throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadSnapshot(dataPath.toString).withCommitter(null)
    }
  }

  test("when no committer is provided, the default committer is created") {
    val committer = TableManager.loadSnapshot(dataPath.toString)
      .asInstanceOf[SnapshotBuilderImpl]
      .atVersion(1)
      .withProtocolAndMetadata(protocol, metadata) // avoid trying to use engine to load log segment
      .build(emptyMockEngine)
      .getCommitter

    assert(committer.isInstanceOf[DefaultFileSystemManagedTableOnlyCommitter])
  }

  test("custom committer is correctly propagated") {
    class CustomCommitter extends Committer {
      override def commit(
          engine: Engine,
          finalizedActions: CloseableIterator[Row],
          commitMetadata: CommitMetadata): CommitResponse = {
        throw new UnsupportedOperationException("Not implemented")
      }
    }

    val committer = TableManager.loadSnapshot(dataPath.toString)
      .asInstanceOf[SnapshotBuilderImpl]
      .atVersion(1)
      .withCommitter(new CustomCommitter())
      .withProtocolAndMetadata(protocol, metadata) // avoid trying to use engine to load log segment
      .build(emptyMockEngine)
      .getCommitter

    assert(committer.isInstanceOf[CustomCommitter])
  }

  // ===== Protocol and Metadata Tests ===== //

  test("withProtocolAndMetadata: null protocol throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadSnapshot(dataPath.toString)
        .withProtocolAndMetadata(null, metadata)
    }

    assertThrows[NullPointerException] {
      TableManager.loadSnapshot(dataPath.toString)
        .withProtocolAndMetadata(protocol, null)
    }
  }

  test("withProtocolAndMetadata: only if version is provided") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .withProtocolAndMetadata(protocol, metadata)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "protocol and metadata can only be provided if a version is provided")
  }

  test("withProtocolAndMetadata: invalid readerVersion throws KernelException") {
    val ex = intercept[UnsupportedProtocolVersionException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(10)
        .withProtocolAndMetadata(new Protocol(999, 2), metadata)
        .build(emptyMockEngine)
    }

    assert(ex.getVersionType === UnsupportedProtocolVersionException.ProtocolVersionType.READER)
    assert(ex.getMessage.contains("Unsupported Delta protocol reader version"))
  }

  test("withProtocolAndMetadata: unknown reader feature throws KernelException") {
    val exMsg = intercept[UnsupportedTableFeatureException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(10)
        .withProtocolAndMetadata(
          new Protocol(3, 7, Set("unknownReaderFeature").asJava, Collections.emptySet()),
          metadata)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("Unsupported Delta table feature"))
  }

  // ===== LogData Tests ===== //

  test("withLogData: null input throws NullPointerException") {
    assertThrows[NullPointerException] {
      TableManager.loadSnapshot(dataPath.toString).withLogData(null)
    }
  }

  Seq(
    ParsedCatalogCommitData.forInlineData(1, emptyColumnarBatch),
    ParsedPublishedDeltaData.forFileStatus(deltaFileStatus(1, logPath)),
    ParsedLogData.forFileStatus(logCompactionStatus(0, 1))).foreach { parsedLogData =>
    val suffix = s"- type=${parsedLogData.getClass.getSimpleName}"
    test(s"withLogData: non-staged-ratified-commit throws IllegalArgumentException $suffix") {
      val builder = TableManager
        .loadSnapshot(dataPath.toString)
        .atVersion(1)
        .withLogData(Collections.singletonList(parsedLogData))

      val exMsg = intercept[IllegalArgumentException] {
        builder.build(emptyMockEngine)
      }.getMessage

      assert(exMsg.contains("Only staged ratified commits are supported"))
    }
  }

  test("withLogData: non-contiguous input throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(2)
        .withLogData(parsedRatifiedStagedCommits(Seq(0, 2)).toList.asJava)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("Log data must be sorted and contiguous"))
  }

  test("withLogData: non-sorted input throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(2)
        .withLogData(parsedRatifiedStagedCommits(Seq(2, 1, 0)).toList.asJava)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg.contains("Log data must be sorted and contiguous"))
  }

  /////////////////////////////////////
  // Builder Validation Tests -- END //
  /////////////////////////////////////

  test("if P & M are provided then LogSegment is not loaded") {
    val snapshot = TableManager
      .loadSnapshot(dataPath.toString)
      .asInstanceOf[SnapshotBuilderImpl]
      .atVersion(13)
      .withProtocolAndMetadata(protocol, metadata)
      .withLogData(Collections.emptyList())
      .build(emptyMockEngine)

    assert(!snapshot.getLazyLogSegment.isPresent)
  }

  // ===== MaxCatalogVersion Tests ===== //

  test("withMaxCatalogVersion: negative version throws IllegalArgumentException") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString).withMaxCatalogVersion(-1)
    }.getMessage

    assert(exMsg === "A valid version must be >= 0")
  }

  test("withMaxCatalogVersion: zero is valid") {
    // Should not throw
    TableManager.loadSnapshot(dataPath.toString)
      .atVersion(0)
      .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
      .withMaxCatalogVersion(0)
      .build(emptyMockEngine)
  }

  test("withMaxCatalogVersion: positive version is valid") {
    // Should not throw
    TableManager.loadSnapshot(dataPath.toString)
      .atVersion(10)
      .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
      .withMaxCatalogVersion(10)
      .build(emptyMockEngine)
  }

  test("withMaxCatalogVersion: version time-travel must be <= maxCatalogVersion") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(15)
        .withMaxCatalogVersion(10)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "Cannot time-travel to version 15 after the max catalog version 10")
  }

  test("withMaxCatalogVersion: version time-travel equal to maxCatalogVersion is valid") {
    // Should not throw
    TableManager.loadSnapshot(dataPath.toString)
      .atVersion(10)
      .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
      .withMaxCatalogVersion(10)
      .build(emptyMockEngine)
  }

  test("withMaxCatalogVersion: version time-travel less than maxCatalogVersion is valid") {
    // Should not throw
    TableManager.loadSnapshot(dataPath.toString)
      .atVersion(5)
      .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
      .withMaxCatalogVersion(10)
      .build(emptyMockEngine)
  }

  test("withMaxCatalogVersion: timestamp time-travel latestSnapshot must have version = " +
    "maxCatalogVersion") {
    val mockSnapshotAtVersion5 =
      getMockSnapshot(dataPath, latestVersion = 5L, timestamp = 1000L)

    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atTimestamp(0L, mockSnapshotAtVersion5)
        .withMaxCatalogVersion(10)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "The latestSnapshot provided for timestamp-based time-travel queries must " +
      "have version = maxCatalogVersion")
  }

  test(
    "withMaxCatalogVersion: timestamp time-travel with matching latestSnapshot version is valid") {
    val mockSnapshotAtVersion10 =
      getMockSnapshot(dataPath, latestVersion = 10L, timestamp = 1000L)

    // Input validation should not throw (but will throw later when trying to construct log segment)
    val exMsg = intercept[Exception] {
      TableManager.loadSnapshot(dataPath.toString)
        .atTimestamp(500L, mockSnapshotAtVersion10)
        .withMaxCatalogVersion(10)
        .build(emptyMockEngine)
    }.getMessage

    // Should fail on log segment loading, not on validation
    assert(!exMsg.contains("latestSnapshot provided for timestamp-based time-travel"))
  }

  test("withMaxCatalogVersion: without version, logData must end with maxCatalogVersion") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .withLogData(parsedRatifiedStagedCommits(Seq(0, 1, 2)).toList.asJava)
        .withMaxCatalogVersion(5)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "Provided catalog commits must end with max catalog version")
  }

  test("withMaxCatalogVersion: without version, logData ending with maxCatalogVersion is valid") {
    // Input validation should not throw (but will throw later when trying to construct log segment)
    val exMsg = intercept[Exception] {
      TableManager.loadSnapshot(dataPath.toString)
        .withLogData(parsedRatifiedStagedCommits(Seq(0, 1, 2, 3, 4, 5)).toList.asJava)
        .withMaxCatalogVersion(5)
        .build(emptyMockEngine)
    }.getMessage

    // Should fail on log segment loading, not on validation
    assert(!exMsg.contains("Provided catalog commits must end with max catalog version"))
  }

  test("withMaxCatalogVersion: empty logData with maxCatalogVersion is valid") {
    // Should not throw - empty logData is allowed
    TableManager.loadSnapshot(dataPath.toString)
      .atVersion(5)
      .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
      .withLogData(Collections.emptyList())
      .withMaxCatalogVersion(5)
      .build(emptyMockEngine)
  }

  test("withMaxCatalogVersion: version time-travel with logData not including version fails") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(5)
        .withLogData(parsedRatifiedStagedCommits(Seq(0, 1, 2, 3)).toList.asJava)
        .withMaxCatalogVersion(10)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "Provided catalog commits must include versionToLoad for time-travel queries")
  }

  test("withMaxCatalogVersion: version time-travel with logData ending at version is valid") {
    // Should not throw - logData ends exactly at requested version
    TableManager.loadSnapshot(dataPath.toString)
      .atVersion(5)
      .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
      .withLogData(parsedRatifiedStagedCommits(Seq(0, 1, 2, 3, 4, 5)).toList.asJava)
      .withMaxCatalogVersion(10)
      .build(emptyMockEngine)
  }

  test("withMaxCatalogVersion: version time-travel with logData beyond version is valid") {
    // Should not throw - logData extends beyond requested version
    TableManager.loadSnapshot(dataPath.toString)
      .atVersion(5)
      .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
      .withLogData(parsedRatifiedStagedCommits(Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)).toList.asJava)
      .withMaxCatalogVersion(10)
      .build(emptyMockEngine)
  }

  test("validateMaxCatalogVersionPresence: catalogManaged table requires maxCatalogVersion") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(1)
        .withProtocolAndMetadata(protocolWithCatalogManagedSupport, metadata)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "Must provide maxCatalogVersion for catalogManaged tables")
  }

  test(
    "validateMaxCatalogVersionPresence: non-catalogManaged table cannot have maxCatalogVersion") {
    val exMsg = intercept[IllegalArgumentException] {
      TableManager.loadSnapshot(dataPath.toString)
        .atVersion(1)
        .withProtocolAndMetadata(protocol, metadata) // protocol without catalogManaged
        .withMaxCatalogVersion(1)
        .build(emptyMockEngine)
    }.getMessage

    assert(exMsg === "Should not provide maxCatalogVersion for file-system managed tables")
  }
}
