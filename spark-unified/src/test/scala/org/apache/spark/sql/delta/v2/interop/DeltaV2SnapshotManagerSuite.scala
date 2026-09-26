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

  test("deprecated context-free methods forward an empty query context") {
    var forwardedCalls = 0

    val manager = new DeltaV2SnapshotManager {
      private def assertEmptyQueryContext(queryContext: DeltaV2QueryContext): Unit = {
        assert(queryContext.catalogTableOpt.isEmpty)
      }

      override def loadLatestSnapshot(queryContext: DeltaV2QueryContext): Snapshot = {
        assertEmptyQueryContext(queryContext)
        forwardedCalls += 1
        null
      }

      override def loadSnapshotAt(
          version: Long,
          queryContext: DeltaV2QueryContext): Snapshot = {
        assert(version == 17)
        assertEmptyQueryContext(queryContext)
        forwardedCalls += 1
        null
      }

      override def getActiveCommitAtTime(
          timestampMillis: Long,
          canReturnLastCommit: Boolean,
          mustBeRecreatable: Boolean,
          canReturnEarliestCommit: Boolean,
          queryContext: DeltaV2QueryContext): KernelDeltaHistoryManager.Commit = {
        assert(timestampMillis == 23)
        assert(canReturnLastCommit)
        assert(mustBeRecreatable)
        assert(!canReturnEarliestCommit)
        assertEmptyQueryContext(queryContext)
        forwardedCalls += 1
        null
      }

      override def checkVersionExists(
          version: Long,
          mustBeRecreatable: Boolean,
          allowOutOfRange: Boolean,
          queryContext: DeltaV2QueryContext): Unit = {
        assert(version == 29)
        assert(mustBeRecreatable)
        assert(!allowOutOfRange)
        assertEmptyQueryContext(queryContext)
        forwardedCalls += 1
      }

      override def getTableChanges(
          kernelEngine: KernelEngine,
          startVersion: Long,
          endVersion: Optional[JLong],
          queryContext: DeltaV2QueryContext): KernelCommitRange = {
        assert(kernelEngine == null)
        assert(startVersion == 31)
        assert(endVersion == Optional.of[JLong](37L))
        assertEmptyQueryContext(queryContext)
        forwardedCalls += 1
        null
      }
    }

    assert(manager.loadLatestSnapshot() == null)
    assert(manager.loadSnapshotAt(17) == null)
    assert(
      manager.getActiveCommitAtTime(
        23,
        canReturnLastCommit = true,
        mustBeRecreatable = true,
        canReturnEarliestCommit = false) == null)
    manager.checkVersionExists(
      29,
      mustBeRecreatable = true,
      allowOutOfRange = false)
    assert(
      manager.getTableChanges(
        null,
        31,
        Optional.of[JLong](37L)) == null)
    assert(forwardedCalls == 5)
  }
}
