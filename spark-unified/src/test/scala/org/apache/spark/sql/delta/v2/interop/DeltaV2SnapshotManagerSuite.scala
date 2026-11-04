/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.v2.interop

import java.lang.{Long => JLong}
import java.util.Optional

import org.apache.spark.sql.delta.Snapshot
import io.delta.kernel.{CommitRange => KernelCommitRange}
import io.delta.kernel.engine.{Engine => KernelEngine}
import io.delta.kernel.internal.{
  DeltaHistoryManager => KernelDeltaHistoryManager
}

import org.apache.spark.SparkFunSuite

class DeltaV2SnapshotManagerSuite extends SparkFunSuite {

  test("query-context methods forward to context-free implementations by default") {
    var forwardedCalls = 0

    val manager = new DeltaV2SnapshotManager {
      override def loadLatestSnapshot(): Snapshot = {
        forwardedCalls += 1
        null
      }

      override def loadSnapshotAt(version: Long): Snapshot = {
        assert(version == 17)
        forwardedCalls += 1
        null
      }

      override def getActiveCommitAtTime(
          timestampMillis: Long,
          canReturnLastCommit: Boolean,
          mustBeRecreatable: Boolean,
          canReturnEarliestCommit: Boolean): KernelDeltaHistoryManager.Commit = {
        assert(timestampMillis == 23)
        assert(canReturnLastCommit)
        assert(mustBeRecreatable)
        assert(!canReturnEarliestCommit)
        forwardedCalls += 1
        null
      }

      override def checkVersionExists(
          version: Long,
          mustBeRecreatable: Boolean,
          allowOutOfRange: Boolean): Unit = {
        assert(version == 29)
        assert(mustBeRecreatable)
        assert(!allowOutOfRange)
        forwardedCalls += 1
      }

      override def getTableChanges(
          kernelEngine: KernelEngine,
          startVersion: Long,
          endVersion: Optional[JLong]): KernelCommitRange = {
        assert(kernelEngine == null)
        assert(startVersion == 31)
        assert(endVersion == Optional.of[JLong](37L))
        forwardedCalls += 1
        null
      }
    }

    val queryContext = DeltaV2QueryContext.empty

    assert(manager.loadLatestSnapshot(queryContext) == null)
    assert(manager.loadSnapshotAt(17, queryContext) == null)
    assert(
      manager.getActiveCommitAtTime(
        23,
        canReturnLastCommit = true,
        mustBeRecreatable = true,
        canReturnEarliestCommit = false,
        queryContext = queryContext) == null)
    manager.checkVersionExists(
      29,
      mustBeRecreatable = true,
      allowOutOfRange = false,
      queryContext = queryContext)
    assert(
      manager.getTableChanges(
        null,
        31,
        Optional.of[JLong](37L),
        queryContext) == null)

    def assertNullQueryContext(body: => Any): Unit = {
      val error = intercept[NullPointerException](body)
      assert(error.getMessage == "queryContext is null")
    }

    assertNullQueryContext(manager.loadLatestSnapshot(null))
    assertNullQueryContext(manager.loadSnapshotAt(41, null))
    assertNullQueryContext(manager.getActiveCommitAtTime(43, true, false, true, null))
    assertNullQueryContext(manager.checkVersionExists(47, false, true, null))
    assertNullQueryContext(manager.getTableChanges(null, 53, Optional.empty(), null))
    assert(forwardedCalls == 5)
  }
}
