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
package io.delta.kernel.defaults

import io.delta.kernel.Operation.CREATE_TABLE
import io.delta.kernel._
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{CommitInfo, SingleAction}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import java.util.Optional
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable
import io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED
import io.delta.kernel.utils.FileStatus;

class InCommitTimestampSuite extends DeltaTableWriteSuiteBase {
  private def getInCommitTimestamp(engine: Engine, table: Table, version: Long): Optional[Long] = {
    val logPath = new Path(table.getPath(engine), "_delta_log")
    val file = FileStatus.of(FileNames.deltaFile(logPath, version), 0, 0)
    val columnarBatches =
      engine.getJsonHandler.readJsonFiles(
        singletonCloseableIterator(file),
        SingleAction.FULL_SCHEMA,
        Optional.empty())
    if (!columnarBatches.hasNext) {
      return Optional.empty()
    }
    val commitInfoVector = columnarBatches.next().getColumnVector(6)
    for (i <- 0 until commitInfoVector.getSize) {
      if (!commitInfoVector.isNullAt(i)) {
        val commitInfo = CommitInfo.fromColumnVector(commitInfoVector, i)
        if (commitInfo != null && commitInfo.getInCommitTimestamp.isPresent) {
          return Optional.of(commitInfo.getInCommitTimestamp.get)
        }
      }
    }
    Optional.empty()
  }

  test("Enable ICT on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val beforeCommitAttemptStartTime = System.currentTimeMillis
      val table = Table.forPath(engine, tablePath)

      setTablePropAndVerify(
        engine, tablePath, isNewTable = true, IN_COMMIT_TIMESTAMPS_ENABLED, "true", true)

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assert(getInCommitTimestamp(engine, table, 0).get >= beforeCommitAttemptStartTime)
      assert(ver0Snapshot.getProtocol.getWriterFeatures.contains("inCommitTimestamp-preview"))
    }
  }

  test("Create a non-inCommitTimestamp table and then enable timestamp") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn1 = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      txn1.commit(engine, emptyIterable())

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver0Snapshot, TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED, false)
      assert(!ver0Snapshot.getProtocol.getWriterFeatures.contains("inCommitTimestamp-preview"))
      assert(!getInCommitTimestamp(engine, table, 0).isPresent)

      val beforeCommitAttemptStartTime = System.currentTimeMillis
      setTablePropAndVerify(
        engine, tablePath, isNewTable = false, IN_COMMIT_TIMESTAMPS_ENABLED, "true", true)

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assert(ver1Snapshot.getProtocol.getWriterFeatures.contains("inCommitTimestamp-preview"))
      assert(
        getInCommitTimestamp(engine, table, 1).get >= beforeCommitAttemptStartTime)
    }
  }

  test("Enablement tracking properties should not be added if ICT is enabled on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath)

      setTablePropAndVerify(
        engine, tablePath, isNewTable = true, IN_COMMIT_TIMESTAMPS_ENABLED, "true", true)

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver0Snapshot, TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
        Optional.empty())
      assertMetadataProp(ver0Snapshot, TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        Optional.empty())
    }
  }

  test("Enablement tracking works when ICT is enabled post commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      txn.commit(engine, emptyIterable())

      appendData(
        engine,
        tablePath,
        schema = testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true")
      )

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver1Snapshot, TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED, true)
      assertMetadataProp(
        ver1Snapshot,
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
        getInCommitTimestamp(engine, table, version = 1))
      assertMetadataProp(
        ver1Snapshot,
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION, Optional.of(1L))
    }
  }
}
