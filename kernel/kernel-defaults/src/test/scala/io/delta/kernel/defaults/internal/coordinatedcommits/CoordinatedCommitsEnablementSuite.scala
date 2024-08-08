/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.internal.coordinatedcommits

import io.delta.kernel.defaults.DeltaTableWriteSuiteBase
import io.delta.kernel.{Snapshot, Table}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames

import java.io.File
import scala.collection.immutable.Seq

class CoordinatedCommitsEnablementSuite extends DeltaTableWriteSuiteBase
  with CoordinatedCommitsTestUtils {
  private def validateCoordinatedCommitsCompleteEnablement(
    engine: Engine, snapshot: SnapshotImpl, expectEnabled: Boolean): Unit = {
    assert(
      COORDINATED_COMMITS_COORDINATOR_NAME.fromMetadata(engine, snapshot.getMetadata).isPresent
        == expectEnabled)
    Seq("coordinatedCommits-preview", "inCommitTimestamp-preview")
      .foreach { feature =>
        assert(
          snapshot.getProtocol.getWriterFeatures.contains(feature) == expectEnabled)
      }
    assert(IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(engine, snapshot.getMetadata)
      == expectEnabled)
  }

  test("enablement at commit 0: CC should enable ICT") {
    InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
    withTempDirAndEngine ({ (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(
          COORDINATED_COMMITS_COORDINATOR_NAME.getKey -> "tracking-in-memory",
          COORDINATED_COMMITS_COORDINATOR_CONF.getKey -> "{}")
      )
      validateCoordinatedCommitsCompleteEnablement(
        engine, table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl], expectEnabled = true)
    }, Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "2"))
  }

  test("enablement after commit 0: CC should enable ICT") {
    InMemoryCommitCoordinatorBuilder.clearInMemoryInstances()
    withTempDirAndEngine ({ (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      )
      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      )

      validateCoordinatedCommitsCompleteEnablement(
        engine, table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl], expectEnabled = false)

      enableCoordinatedCommits(engine, tablePath, "tracking-in-memory")

      appendData(
        engine,
        tablePath,
        isNewTable = false,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      )

      validateCoordinatedCommitsCompleteEnablement(
        engine, table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl], expectEnabled = true)
    }, Map(CommitCoordinatorProvider.getCommitCoordinatorNameConfKey("tracking-in-memory") ->
      classOf[TrackingInMemoryCommitCoordinatorBuilder].getName,
      InMemoryCommitCoordinatorBuilder.BATCH_SIZE_CONF_KEY -> "2"))
  }
}
