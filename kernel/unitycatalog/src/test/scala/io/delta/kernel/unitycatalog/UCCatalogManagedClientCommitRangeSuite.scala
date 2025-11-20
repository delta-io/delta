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

import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.storage.commit.uccommitcoordinator.{InvalidTargetTableException, UCClient}

import org.scalatest.funsuite.AnyFunSuite

class UCCatalogManagedClientCommitRangeSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  /** Helper method with reasonable defaults */
  private def loadCommitRange(
      ucCatalogManagedClient: UCCatalogManagedClient,
      engine: Engine = defaultEngine,
      ucTableId: String = "testUcTableId",
      tablePath: String = "testUcTablePath",
      startVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      startTimestampOpt: Optional[java.lang.Long] = emptyLongOpt,
      endVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      endTimestampOpt: Optional[java.lang.Long] = emptyLongOpt) = {
    ucCatalogManagedClient.loadCommitRange(
      engine,
      ucTableId,
      tablePath,
      startVersionOpt,
      startTimestampOpt,
      endVersionOpt,
      endTimestampOpt)
  }

  private def testLoadCommitRange(
      expectedStartVersion: Long,
      expectedEndVersion: Long,
      startVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      startTimestampOpt: Optional[java.lang.Long] = emptyLongOpt,
      endVersionOpt: Optional[java.lang.Long] = emptyLongOpt,
      endTimestampOpt: Optional[java.lang.Long] = emptyLongOpt): Unit = {
    require(!startVersionOpt.isPresent || !startTimestampOpt.isPresent)
    require(!endVersionOpt.isPresent || !endTimestampOpt.isPresent)

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

  test("loadCommitRange throws if startVersion is greater than max ratified version") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[IllegalArgumentException] {
      testLoadCommitRange(
        expectedStartVersion = 0,
        expectedEndVersion = 2,
        startVersionOpt = Optional.of(9L))
    }
    assert(ex.getMessage.contains(
      "Cannot load commit range with start version 9 as the latest version ratified by UC is 2"))
  }

  test("loadCommitRange throws if endVersion is greater than max ratified version") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[IllegalArgumentException] {
      testLoadCommitRange(
        expectedStartVersion = 0,
        expectedEndVersion = 2,
        endVersionOpt = Optional.of(9L))
    }
    assert(ex.getMessage.contains(
      "Cannot load commit range with end version 9 as the latest version ratified by UC is 2"))
  }

  test("loadCommitRange loads with default boundaries (start=0, end=latest)") {
    testLoadCommitRange(expectedStartVersion = 0, expectedEndVersion = 2)
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
        endTimestampOpt = Optional.of(v0Ts - 10))
    }
  }

  test("loadCommitRange throws when the table doesn't exist in catalog") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    val ex = intercept[RuntimeException] {
      loadCommitRange(
        ucCatalogManagedClient,
        ucTableId = "nonExistentTableId")
    }
    assert(ex.getCause.isInstanceOf[InvalidTargetTableException])
  }

  test("loadCommitRange for new table when UC maxRatifiedVersion is -1") {
    val tablePath = getTestResourceFilePath("catalog-owned-preview")
    val ucCatalogManagedClient =
      createUCCatalogManagedClientForTableWithMaxRatifiedVersionNegativeOne()
    val commitRange = loadCommitRange(
      ucCatalogManagedClient,
      tablePath = tablePath)

    assert(commitRange.getStartVersion == 0)
    assert(commitRange.getEndVersion == 0)
  }
}
