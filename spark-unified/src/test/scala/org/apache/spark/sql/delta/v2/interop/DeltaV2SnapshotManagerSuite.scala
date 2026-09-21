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
import java.lang.reflect.Proxy
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
    var latestLoads = 0
    var versionLoaded = Option.empty[Long]
    var activeCommitArgs = Option.empty[(Long, Boolean, Boolean, Boolean)]
    var versionCheckArgs = Option.empty[(Long, Boolean, Boolean)]
    var tableChangesArgs = Option.empty[(KernelEngine, Long, Optional[JLong])]

    val manager = new DeltaV2SnapshotManager {
      override def loadLatestSnapshot(): Snapshot = {
        latestLoads += 1
        null
      }

      override def loadSnapshotAt(version: Long): Snapshot = {
        versionLoaded = Some(version)
        null
      }

      override def getActiveCommitAtTime(
          timestampMillis: Long,
          canReturnLastCommit: Boolean,
          mustBeRecreatable: Boolean,
          canReturnEarliestCommit: Boolean): KernelDeltaHistoryManager.Commit = {
        activeCommitArgs =
          Some((timestampMillis, canReturnLastCommit, mustBeRecreatable, canReturnEarliestCommit))
        null
      }

      override def checkVersionExists(
          version: Long,
          mustBeRecreatable: Boolean,
          allowOutOfRange: Boolean): Unit = {
        versionCheckArgs = Some((version, mustBeRecreatable, allowOutOfRange))
      }

      override def getTableChanges(
          kernelEngine: KernelEngine,
          startVersion: Long,
          endVersion: Optional[JLong]): KernelCommitRange = {
        tableChangesArgs = Some((kernelEngine, startVersion, endVersion))
        null
      }
    }

    val queryContextOpt = Optional.of(DeltaV2QueryContext(None))

    assert(manager.loadLatestSnapshot(queryContextOpt) == null)
    assert(latestLoads == 1)
    assert(manager.loadSnapshotAt(17, queryContextOpt) == null)
    assert(versionLoaded.contains(17))
    assert(
      manager.getActiveCommitAtTime(
        23,
        canReturnLastCommit = true,
        mustBeRecreatable = true,
        canReturnEarliestCommit = false,
        queryContextOpt = queryContextOpt) == null)
    assert(activeCommitArgs.contains((23, true, true, false)))
    manager.checkVersionExists(
      29,
      mustBeRecreatable = true,
      allowOutOfRange = false,
      queryContextOpt = queryContextOpt)
    assert(versionCheckArgs.contains((29, true, false)))
    val kernelEngine = Proxy.newProxyInstance(
      classOf[KernelEngine].getClassLoader,
      Array(classOf[KernelEngine]),
      (_, _, _) => null).asInstanceOf[KernelEngine]
    val endVersion = Optional.of[JLong](37L)
    assert(
      manager.getTableChanges(
        kernelEngine,
        31,
        endVersion,
        queryContextOpt) == null)
    assert(tableChangesArgs.exists { case (engine, startVersion, endVersionArg) =>
      (engine eq kernelEngine) && startVersion == 31 && endVersionArg == endVersion
    })

    assert(manager.loadLatestSnapshot(Optional.empty()) == null)
    assert(latestLoads == 2)

    def assertNullQueryContext(body: => Any): Unit = {
      val error = intercept[NullPointerException](body)
      assert(error.getMessage == "queryContextOpt is null")
    }

    assertNullQueryContext(manager.loadLatestSnapshot(null))
    assertNullQueryContext(manager.loadSnapshotAt(41, null))
    assertNullQueryContext(manager.getActiveCommitAtTime(43, true, false, true, null))
    assertNullQueryContext(manager.checkVersionExists(47, false, true, null))
    assertNullQueryContext(manager.getTableChanges(kernelEngine, 53, Optional.empty(), null))
    assert(latestLoads == 2)
  }
}
