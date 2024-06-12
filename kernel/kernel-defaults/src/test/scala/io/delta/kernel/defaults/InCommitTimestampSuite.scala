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

import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel._
import io.delta.kernel.data.FilteredColumnarBatch
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.SingleAction
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{Clock, FileNames, ManualClock, SystemClock}
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import java.util.Optional
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.collection.mutable

import io.delta.kernel.internal.TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED;

class InCommitTimestampSuite extends DeltaTableWriteSuiteBase {
  private def getInCommitTimestamp(engine: Engine, table: Table, version: Long): Option[Long] = {
    val logPath = new Path(table.getPath(engine), "_delta_log")
    val file = engine
      .getFileSystemClient
      .listFrom(FileNames.listingPrefix(logPath, version)).next
    val row =
      engine.getJsonHandler.readJsonFiles(
        singletonCloseableIterator(file),
        SingleAction.FULL_SCHEMA,
        Optional.empty()).toSeq.map(batch => batch.getRows.next).head
    val commitInfoOrd = row.getSchema.indexOf("commitInfo")
    val commitInfoRow = row.getStruct(commitInfoOrd)
    val inCommitTimestampOrd = commitInfoRow.getSchema.indexOf("inCommitTimestamp")
    if (commitInfoRow.isNullAt(inCommitTimestampOrd)) {
      None
    } else {
      Some(commitInfoRow.getLong(inCommitTimestampOrd))
    }
  }

  test("Enable ICT on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val beforeCommitAttemptStartTime = System.currentTimeMillis
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .withTableProperties(engine, Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true").asJava)
        .build(engine)

      txn.commit(engine, emptyIterable())
      val ver0Snapshot = table.getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl]
      assert(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver0Snapshot.getMetadata))
      assert(getInCommitTimestamp(engine, table, 0).get >= beforeCommitAttemptStartTime)
      assert(getInCommitTimestamp(engine, table, 0).get == ver0Snapshot.getTimestamp(engine))
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

      val ver0Snapshot = table.getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl]
      assert(!TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver0Snapshot.getMetadata))
      assert(getInCommitTimestamp(engine, table, 0).isEmpty)

      val txn2 = createWriteTxnBuilder(Table.forPath(engine, tablePath))
        .withTableProperties(engine, Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true").asJava)
        .build(engine)

      txn2.commit(engine, emptyIterable())

      val ver1Snapshot = table.getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      assert(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver1Snapshot.getMetadata))
      assert(ver1Snapshot.getTimestamp(engine) > ver0Snapshot.getTimestamp(engine))
      assert(getInCommitTimestamp(engine, table, 1).get == ver1Snapshot.getTimestamp(engine))
    }
  }

  test("InCommitTimestamps are monotonic even when the clock is skewed") {
    withTempDirAndEngine { (tablePath, engine) =>
      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        clock = clock,
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true")
      )

      clock.setTime(startTime - 10000)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock
      )

      val table = Table.forPath(engine, tablePath)
      val ver1Snapshot = table.getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl]
      val ver1Timestamp = ver1Snapshot.getTimestamp(engine)
      assert(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver1Snapshot.getMetadata))
      val ver2Snapshot = table.getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      val ver2Timestamp = ver2Snapshot.getTimestamp(engine)
      assert(ver2Timestamp > ver1Timestamp)
    }
  }

  test("Conflict resolution of timestamps") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath, new SystemClock)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .withTableProperties(engine, Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true").asJava)
        .build(engine)

      txn.commit(engine, emptyIterable())

      val ver0Snapshot = table.getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl]
      assert(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver0Snapshot.getMetadata))

      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val txn1 = generateTxn(
        engine,
        tablePath,
        schema = testSchema,
        partCols = Seq.empty,
        clock = clock
      )
      clock.setTime(startTime)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock
      )
      clock.setTime(startTime - 10000)
      commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
      assert(
        getInCommitTimestamp(engine, table, 2).get > getInCommitTimestamp(engine, table, 1).get)
    }
  }

  test("Enablement tracking properties should not be added if ICT is enabled on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath, new SystemClock)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .withTableProperties(engine, Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true").asJava)
        .build(engine)

      txn.commit(engine, emptyIterable())
      val ver0Snapshot = table.getSnapshotAsOfVersion(engine, 0).asInstanceOf[SnapshotImpl]
      assert(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver0Snapshot.getMetadata))
      assert(!TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP
        .fromMetadata(ver0Snapshot.getMetadata).isPresent)
      assert(!TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION
        .fromMetadata(ver0Snapshot.getMetadata).isPresent)
    }
  }

  test("Enablement tracking works when ICT is enabled post commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath, new SystemClock)
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

      val ver1Snapshot = table.getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      val observedEnablementTimestamp =
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetadata(ver1Snapshot.getMetadata)
      val observedEnablementVersion =
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetadata(ver1Snapshot.getMetadata)
      assert(observedEnablementTimestamp.isPresent)
      assert(observedEnablementVersion.isPresent)
      assert(
        observedEnablementTimestamp.get == getInCommitTimestamp(engine, table, version = 1).get)
      assert(observedEnablementVersion.get == 1)
    }
  }

  test("Conflict resolution of enablement version") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath, new SystemClock)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      txn.commit(engine, emptyIterable())
      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val txn1 = generateTxn(
        engine,
        tablePath,
        schema = testSchema,
        partCols = Seq.empty,
        clock = clock,
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true")
      )
      clock.setTime(startTime)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock
      )
      commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
      val ver2Snapshot = table.getSnapshotAsOfVersion(engine, 2).asInstanceOf[SnapshotImpl]
      val observedEnablementTimestamp =
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetadata(ver2Snapshot.getMetadata)
      val observedEnablementVersion =
        TableConfig.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetadata(ver2Snapshot.getMetadata)
      assert(
        observedEnablementTimestamp.get == getInCommitTimestamp(engine, table, version = 2).get)
      assert(observedEnablementVersion.get == 2)
    }
  }
}
