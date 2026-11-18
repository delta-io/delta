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
package io.delta.kernel.unitycatalog

import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.CommitRange
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{InvalidTableException, KernelException, VersionToLoadAfterLatestCommitException}
import io.delta.storage.commit.uccommitcoordinator.InvalidTargetTableException

import InMemoryUCClient.TableData
import org.scalatest.funsuite.AnyFunSuite

class UCCatalogManagedClientCommitRangeSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  /** Helper method with reasonable defaults */
  private def loadCommitRange(
      ucCatalogManagedClient: UCCatalogManagedClient,
      engine: Engine = defaultEngine,
      ucTableId: String = "testUcTableId",
      tablePath: String = "testUcTablePath",
      ucTableIdentifier: UCTableIdentifier = new UCTableIdentifier("cat", "sch", "tbl"),
      startVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      startTimestampOpt: Optional[java.lang.Long] = emptyLongOpt,
      endVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      endTimestampOpt: Optional[java.lang.Long] = emptyLongOpt): CommitRange = {
    ucCatalogManagedClient.loadCommitRange(
      engine,
      ucTableId,
      tablePath,
      ucTableIdentifier,
      new UCLoadCommitRangeOptions(
        startVersionOpt,
        startTimestampOpt,
        endVersionOpt,
        endTimestampOpt,
        Optional.empty(),
        Optional.empty()))
  }

  private def testLoadCommitRange(
      expectedStartVersion: Long,
      expectedEndVersion: Long,
      startVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      startTimestampOpt: Optional[java.lang.Long] = emptyLongOpt,
      endVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      endTimestampOpt: Optional[java.lang.Long] = emptyLongOpt): Unit = {
    withUCClientAndTestTable { (ucClient, tablePath, _) =>
      val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
      val commitRange = loadCommitRange(
        ucCatalogManagedClient,
        tablePath = tablePath,
        startVersionOpt = startVersionOpt,
        startTimestampOpt = startTimestampOpt,
        endVersionOpt = endVersionOpt,
        endTimestampOpt = endTimestampOpt)

      assert(commitRange.getStartVersion == expectedStartVersion)
      assert(commitRange.getEndVersion == expectedEndVersion)
      assert(ucClient.getNumGetCommitCalls == 1)
    }
  }

  test("loadCommitRange throws on null input") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    assertThrows[NullPointerException] {
      ucCatalogManagedClient.loadCommitRange(
        defaultEngine,
        "testUcTableId",
        "testUcTablePath",
        new UCTableIdentifier("cat", "sch", "tbl"),
        null)
    }
    assertThrows[NullPointerException] {
      // engine is null
      loadCommitRange(ucCatalogManagedClient, engine = null)
    }
    assertThrows[NullPointerException] {
      // ucTableId is null
      loadCommitRange(ucCatalogManagedClient, ucTableId = null)
    }
    assertThrows[NullPointerException] {
      // tablePath is null
      loadCommitRange(ucCatalogManagedClient, tablePath = null)
    }
    assertThrows[NullPointerException] {
      // ucTableIdentifier is null
      loadCommitRange(ucCatalogManagedClient, ucTableIdentifier = null)
    }
    assertThrows[NullPointerException] {
      // startVersionOpt is null
      loadCommitRange(ucCatalogManagedClient, startVersionOpt = null)
    }
    assertThrows[NullPointerException] {
      // startTimestampOpt is null
      loadCommitRange(ucCatalogManagedClient, startTimestampOpt = null)
    }
    assertThrows[NullPointerException] {
      // endVersionOpt is null
      loadCommitRange(ucCatalogManagedClient, endVersionOpt = null)
    }
    assertThrows[NullPointerException] {
      // endTimestampOpt is null
      loadCommitRange(ucCatalogManagedClient, endTimestampOpt = null)
    }
    assertThrows[NullPointerException] {
      new UCLoadCommitRangeOptions(
        emptyLongOpt,
        emptyLongOpt,
        emptyLongOpt,
        emptyLongOpt,
        null,
        Optional.empty())
    }
    assertThrows[NullPointerException] {
      new UCLoadCommitRangeOptions(
        emptyLongOpt,
        emptyLongOpt,
        emptyLongOpt,
        emptyLongOpt,
        Optional.empty(),
        null)
    }
  }

  for {
    commitRangeValidationOpt <- Seq[Optional[java.lang.Boolean]](
      Optional.empty(),
      Optional.of(true),
      Optional.of(false))
    failOnDataLossOpt <- Seq[Optional[java.lang.Boolean]](
      Optional.empty(),
      Optional.of(true),
      Optional.of(false))
    endVersionOpt <- Seq(emptyLongOpt, Optional.of[java.lang.Long](2L))
  } {
    test(s"loadCommitRange with a missing commit: validation=$commitRangeValidationOpt, " +
      s"failOnDataLoss=$failOnDataLossOpt, endVersion=$endVersionOpt") {
      withUCClientAndTestTable { (fixtureClient, tablePath, maxRatifiedVersion) =>
        // Keep published version 0 and catalog version 2, with version 1 missing between them.
        val commits = fixtureClient.getTableDataElseThrow("testUcTableId").getCommits
          .filter(_.getVersion != 1L)
        val ucClient = new InMemoryUCClientWithMetrics("ucMetastoreId")
        ucClient.insertTableData(
          "testUcTableId",
          new TableData(maxRatifiedVersion, ArrayBuffer(commits: _*)))
        val client = new UCCatalogManagedClient(ucClient)

        def loadRange(): CommitRange = client.loadCommitRange(
          defaultEngine,
          "testUcTableId",
          tablePath,
          new UCTableIdentifier("cat", "sch", "tbl"),
          new UCLoadCommitRangeOptions(
            Optional.of(0L),
            emptyLongOpt,
            endVersionOpt,
            emptyLongOpt,
            commitRangeValidationOpt,
            failOnDataLossOpt))

        if (commitRangeValidationOpt.orElse(true)) {
          val ex = intercept[InvalidTableException](loadRange())
          assert(ex.getMessage.contains("Missing delta files"))
        } else {
          val commitRange = loadRange()
          assert(commitRange.getStartVersion == 0L)
          assert(commitRange.getEndVersion == maxRatifiedVersion)
          val iter = commitRange.getCommitFiles
          try {
            if (failOnDataLossOpt.orElse(true)) {
              assert(iter.next().getVersion == 0L)
              val ex = intercept[InvalidTableException](iter.hasNext)
              assert(ex.getMessage.contains("Table version 1 is missing or unavailable"))
            } else {
              val commitFiles = iter.toInMemoryList.asScala
              assert(commitFiles.map(_.getVersion) == Seq(0L, 2L))
              assert(commitFiles.last.getFileStatus.getPath.contains("_staged_commits"))
            }
          } finally {
            iter.close()
          }
          val ex = intercept[KernelException](commitRange.getActions(null, null, null))
          assert(ex.getMessage.contains("Commit action iteration is not supported"))
        }
        assert(ucClient.getNumGetCommitCalls == 1)
      }
    }
  }

  test("loadCommitRange throws on invalid input - conflicting start boundaries") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[IllegalArgumentException] {
      loadCommitRange(
        ucCatalogManagedClient,
        startVersionOpt = Optional.of(1L),
        startTimestampOpt = Optional.of(100L))
    }
    assert(ex.getMessage.contains("Cannot provide both a start timestamp and start version"))
  }

  test("loadCommitRange throws on invalid input - conflicting end boundaries") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[IllegalArgumentException] {
      loadCommitRange(
        ucCatalogManagedClient,
        startVersionOpt = Optional.of(0L),
        endVersionOpt = Optional.of(2L),
        endTimestampOpt = Optional.of(200L))
    }
    assert(ex.getMessage.contains("Cannot provide both an end timestamp and start version"))
  }

  test("loadCommitRange throws on invalid input - start version > end version") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[IllegalArgumentException] {
      loadCommitRange(
        ucCatalogManagedClient,
        startVersionOpt = Optional.of(5L),
        endVersionOpt = Optional.of(2L))
    }
    assert(ex.getMessage.contains("Cannot provide a start version greater than the end version"))
  }

  test("loadCommitRange throws on invalid input - start timestamp > end timestamp") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[IllegalArgumentException] {
      loadCommitRange(
        ucCatalogManagedClient,
        startTimestampOpt = Optional.of(500L),
        endTimestampOpt = Optional.of(200L))
    }
    assert(ex.getMessage.contains(
      "Cannot provide a start timestamp greater than the end timestamp"))
  }

  Seq(3L, 9L).foreach { versionToLoad =>
    test(s"loadCommitRange throws a typed exception for start version $versionToLoad above max") {
      val ex = intercept[VersionToLoadAfterLatestCommitException] {
        testLoadCommitRange(
          expectedStartVersion = 0,
          expectedEndVersion = 2,
          startVersionOpt = Optional.of(versionToLoad))
      }
      assert(ex.getMessage ==
        s"[testUcTableId] Cannot load commit range with start version $versionToLoad " +
        "as the latest version ratified by UC is 2")
    }

    test(s"loadCommitRange throws a typed exception for end version $versionToLoad above max") {
      val ex = intercept[VersionToLoadAfterLatestCommitException] {
        testLoadCommitRange(
          expectedStartVersion = 0,
          expectedEndVersion = 2,
          startVersionOpt = Optional.of(0L),
          endVersionOpt = Optional.of(versionToLoad))
      }
      assert(ex.getMessage ==
        s"[testUcTableId] Cannot load commit range with end version $versionToLoad " +
        "as the latest version ratified by UC is 2")
    }
  }

  test("loadCommitRange throws when no start boundary is provided") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[IllegalArgumentException] {
      loadCommitRange(ucCatalogManagedClient)
    }
    assert(ex.getMessage.contains("Must provide either a start timestamp or start version"))
  }

  test("loadCommitRange loads with default end boundary -> latest") {
    testLoadCommitRange(
      expectedStartVersion = 0,
      expectedEndVersion = 2,
      startVersionOpt = Optional.of(0L))
  }

  test("loadCommitRange loads with version boundaries") {
    testLoadCommitRange(
      expectedStartVersion = 1,
      expectedEndVersion = 2,
      startVersionOpt = Optional.of(1L),
      endVersionOpt = Optional.of(2L))
  }

  test("loadCommitRange loads with timestamp boundaries") {
    testLoadCommitRange(
      expectedStartVersion = 0L,
      expectedEndVersion = 1L,
      startTimestampOpt = Optional.of(v0Ts),
      endTimestampOpt = Optional.of(v1Ts + 10))
  }

  test("loadCommitRange loads with mixed start timestamp and end version") {
    testLoadCommitRange(
      expectedStartVersion = 0L,
      expectedEndVersion = 2L,
      startTimestampOpt = Optional.of(v0Ts),
      endVersionOpt = Optional.of(2L))
  }

  test("loadCommitRange loads with mixed start version and end timestamp") {
    testLoadCommitRange(
      expectedStartVersion = 1L,
      expectedEndVersion = 2L,
      startVersionOpt = Optional.of(1L),
      endTimestampOpt = Optional.of(v2Ts))
  }

  test("loadCommitRange loads single version range") {
    testLoadCommitRange(
      expectedStartVersion = 1L,
      expectedEndVersion = 1L,
      startVersionOpt = Optional.of(1L),
      endVersionOpt = Optional.of(1L))
  }

  test("loadCommitRange loads single version range by timestamps") {
    testLoadCommitRange(
      expectedStartVersion = 1L,
      expectedEndVersion = 1L,
      startTimestampOpt = Optional.of(v1Ts - 50),
      endTimestampOpt = Optional.of(v1Ts + 50))
  }

  test("loadCommitRange invalid timestamp bound") {
    intercept[KernelException] {
      testLoadCommitRange(
        expectedStartVersion = 1L,
        expectedEndVersion = 1L,
        startTimestampOpt = Optional.of(v2Ts + 10))
    }
    intercept[KernelException] {
      testLoadCommitRange(
        expectedStartVersion = 1L,
        expectedEndVersion = 1L,
        startVersionOpt = Optional.of(0),
        endTimestampOpt = Optional.of(v0Ts - 10))
    }
  }

  test("loadCommitRange throws when the table doesn't exist in catalog") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[RuntimeException] {
      loadCommitRange(
        ucCatalogManagedClient,
        ucTableId = "nonExistentTableId",
        startVersionOpt = Optional.of(0L))
    }
    assert(ex.getCause.isInstanceOf[InvalidTargetTableException])
  }

  test("loadCommitRange for new table when UC maxRatifiedVersion is 0") {
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val ucCatalogManagedClient = createUCCatalogManagedClientForTableAfterCreate()
    val commitRange = loadCommitRange(
      ucCatalogManagedClient,
      tablePath = tablePath,
      startVersionOpt = Optional.of(0L))

    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 0)
  }

  test("loadCommitRange succeeds when UC returns an already-published commit") {
    withUCClientAndTestTableWithOptions(includePublishedV0 = true) {
      (ucClient, tablePath, maxVersion) =>
        val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)
        val commitRange = loadCommitRange(
          ucCatalogManagedClient,
          tablePath = tablePath,
          startVersionOpt = Optional.of(0L))
        assert(commitRange.getStartVersion == 0)
        assert(commitRange.getEndVersion == maxVersion)
    }
  }
}
