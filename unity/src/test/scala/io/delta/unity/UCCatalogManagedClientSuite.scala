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

package io.delta.unity

import scala.collection.JavaConverters._

import io.delta.kernel.internal.files.ParsedLogData.ParsedLogType

import org.scalatest.funsuite.AnyFunSuite

/** Unit tests for [[UCCatalogManagedClient]]. */
class UCCatalogManagedClientSuite extends AnyFunSuite with UCCatalogManagedTestUtils {

  test("constructor throws on invalid input") {
    assertThrows[NullPointerException] {
      new UCCatalogManagedClient(null)
    }
  }

  test("loadTable throws on invalid input") {
    val ucClient = new InMemoryUCClient("ucMetastoreId")
    val ucCatalogManagedClient = new UCCatalogManagedClient(ucClient)

    assertThrows[NullPointerException] {
      ucCatalogManagedClient.loadTable(null, "ucTableId", "tablePath", 0L) // engine is null
    }
    assertThrows[NullPointerException] {
      ucCatalogManagedClient.loadTable(defaultEngine, null, "tablePath", 0L) // ucTableId is null
    }
    assertThrows[NullPointerException] {
      ucCatalogManagedClient.loadTable(defaultEngine, "ucTableId", null, 0L) // tablePath is null
    }
    assertThrows[IllegalArgumentException] {
      ucCatalogManagedClient.loadTable(defaultEngine, "ucTableId", "tablePath", -1L) // version < 0
    }
  }

  test("converts UC Commit into Kernel ParsedLogData.RATIFIED_STAGED_COMMIT") {
    val ucCommit = createCommit(1)
    val hadoopFS = ucCommit.getFileStatus

    val kernelParsedLogData = UCCatalogManagedClient
      .getSortedKernelLogDataFromRatifiedCommits("ucTableId", Seq(ucCommit).asJava)
      .get(0)
    val kernelFS = kernelParsedLogData.getFileStatus

    assert(kernelParsedLogData.`type` == ParsedLogType.RATIFIED_STAGED_COMMIT)
    assert(kernelFS.getPath == hadoopFS.getPath.toString)
    assert(kernelFS.getSize == hadoopFS.getLen)
    assert(kernelFS.getModificationTime == hadoopFS.getModificationTime)
  }

  test("sorts UC commits by version") {
    val ucCommitsUnsorted = Seq(createCommit(1), createCommit(2), createCommit(3)).asJava

    val kernelParsedLogData = UCCatalogManagedClient
      .getSortedKernelLogDataFromRatifiedCommits("ucTableId", ucCommitsUnsorted)

    assert(kernelParsedLogData.size() == 3)
    assert(kernelParsedLogData.get(0).version == 1)
    assert(kernelParsedLogData.get(1).version == 2)
    assert(kernelParsedLogData.get(2).version == 3)
  }

}
