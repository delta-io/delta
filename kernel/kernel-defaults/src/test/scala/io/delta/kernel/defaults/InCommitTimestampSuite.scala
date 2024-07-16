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
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.InvalidTableException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{CommitInfo, SingleAction}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.internal.{DeltaHistoryManager, SnapshotImpl, TableImpl}
import io.delta.kernel.internal.util.ManualClock
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import java.util.{Locale, Optional}
import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, Seq}
import scala.collection.mutable
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.utils.FileStatus
import io.delta.kernel.internal.actions.SingleAction.createCommitInfoSingleAction

class InCommitTimestampSuite extends DeltaTableWriteSuiteBase {

  private def removeCommitInfoFromCommit(engine: Engine, version: Long, logPath: Path): Unit = {
    val file = FileStatus.of(FileNames.deltaFile(logPath, version), 0, 0)
    val columnarBatches =
      engine.getJsonHandler.readJsonFiles(
        singletonCloseableIterator(file),
        SingleAction.FULL_SCHEMA,
        Optional.empty())
    assert(columnarBatches.hasNext)
    val rows = columnarBatches.next().getRows
    val rowsWithoutCommitInfo =
      rows.filter(row => row.isNullAt(row.getSchema.indexOf("commitInfo")))
    engine
      .getJsonHandler
      .writeJsonFileAtomically(
        FileNames.deltaFile(logPath, version), rowsWithoutCommitInfo, true /* overwrite */)
  }

  test("Enable ICT on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val beforeCommitAttemptStartTime = System.currentTimeMillis
      val clock = new ManualClock(beforeCommitAttemptStartTime)
      val table = Table.forPath(engine, tablePath)

      clock.setTime(beforeCommitAttemptStartTime + 1)
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true,
        clock = clock)

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

      assert(ver0Snapshot.getTimestamp(engine) == beforeCommitAttemptStartTime + 1)
      assert(getInCommitTimestamp(engine, table, 0).get == ver0Snapshot.getTimestamp(engine))
      assertHasWriterFeature(ver0Snapshot, "inCommitTimestamp-preview")
    }
  }

  test("Create a non-inCommitTimestamp table and then enable ICT") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn1 = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      txn1.commit(engine, emptyIterable())

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver0Snapshot, IN_COMMIT_TIMESTAMPS_ENABLED, false)
      assertHasNoWriterFeature(ver0Snapshot, "inCommitTimestamp-preview")
      assert(getInCommitTimestamp(engine, table, 0).isEmpty)

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(ver1Snapshot, "inCommitTimestamp-preview")
      assert(ver1Snapshot.getTimestamp(engine) > ver0Snapshot.getTimestamp(engine))
      assert(getInCommitTimestamp(engine, table, 1).get == ver1Snapshot.getTimestamp(engine))
    }
  }

  test("InCommitTimestamps are monotonic even when the clock is skewed") {
    withTempDirAndEngine { (tablePath, engine) =>
      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val table = Table.forPath(engine, tablePath)

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

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val ver1Timestamp = ver1Snapshot.getTimestamp(engine)
      assert(IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver1Snapshot.getMetadata))

      clock.setTime(startTime - 10000)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock
      )

      val ver2Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val ver2Timestamp = ver2Snapshot.getTimestamp(engine)
      assert(ver2Timestamp == ver1Timestamp + 1)
    }
  }

  test("Missing CommitInfo should result in a DELTA_MISSING_COMMIT_INFO exception") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)
      // Remove CommitInfo from the commit.
      val logPath = new Path(table.getPath(engine), "_delta_log")
      removeCommitInfoFromCommit(engine, 0, logPath)

      val ex = intercept[InvalidTableException] {
        table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl].getTimestamp(engine)
      }
      assert(ex.getMessage.contains(String.format(
        "This table has the feature %s enabled which requires the presence of the " +
          "CommitInfo action in every commit. However, the CommitInfo action is " +
          "missing from commit version %s.", "inCommitTimestamp-preview", "0")))
    }
  }

  test("Missing CommitInfo.inCommitTimestamp should result in a " +
    "DELTA_MISSING_COMMIT_TIMESTAMP exception") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      setTablePropAndVerify(
        engine, tablePath, isNewTable = true, IN_COMMIT_TIMESTAMPS_ENABLED, "true", true)
      // Remove CommitInfo.inCommitTimestamp from the commit.
      val logPath = new Path(table.getPath(engine), "_delta_log")
      val file = FileStatus.of(FileNames.deltaFile(logPath, 0), 0, 0)
      val columnarBatches =
        engine.getJsonHandler.readJsonFiles(
          singletonCloseableIterator(file),
          SingleAction.FULL_SCHEMA,
          Optional.empty())
      assert(columnarBatches.hasNext)
      val rows = columnarBatches.next().getRows
      val commitInfoOpt = CommitInfo.getCommitInfoOpt(engine, logPath, 0)
      assert(commitInfoOpt.isPresent)
      val commitInfo = commitInfoOpt.get
      commitInfo.setInCommitTimestamp(Optional.empty())
      val rowsWithoutCommitInfoInCommitTimestamp =
        rows.map(row => {
          val commitInfoOrd = row.getSchema.indexOf("commitInfo")
          if (row.isNullAt(commitInfoOrd)) {
            row
          } else {
            createCommitInfoSingleAction(commitInfo.toRow)
          }
        })
      engine
        .getJsonHandler
        .writeJsonFileAtomically(
          FileNames.deltaFile(logPath, 0),
          rowsWithoutCommitInfoInCommitTimestamp, true /* overwrite */)

      val ex = intercept[InvalidTableException] {
        table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl].getTimestamp(engine)
      }
      assert(ex.getMessage.contains(String.format(
        "This table has the feature %s enabled which requires the presence of " +
          "inCommitTimestamp in the CommitInfo action. However, this field has not " +
          "been set in commit version %s.", "inCommitTimestamp-preview", "0")))
    }
  }

  test("Enablement tracking properties should not be added if ICT is enabled on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)

      val ver0Snapshot =
        Table.forPath(engine, tablePath).getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasNoMetadataProp(ver0Snapshot, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP)
      assertHasNoMetadataProp(ver0Snapshot, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION)
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
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver1Snapshot, IN_COMMIT_TIMESTAMPS_ENABLED, true)
      assertMetadataProp(
        ver1Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
        Optional.of(ver1Snapshot.getTimestamp(engine)))
      assertMetadataProp(
        ver1Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        Optional.of(1L))

      appendData(
        engine,
        tablePath,
        schema = testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches2))

      val ver2Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver2Snapshot, IN_COMMIT_TIMESTAMPS_ENABLED, true)
      assertMetadataProp(
        ver2Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
        Optional.of(ver1Snapshot.getTimestamp(engine)))
      assertMetadataProp(
        ver2Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        Optional.of(1L))
    }
  }

  test("Update the protocol only if required") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)
      val protocol = getProtocolActionFromCommit(engine, table, 0)
      assert(protocol.isDefined)
      assert(VectorUtils.toJavaList(protocol.get.getArray(3)).contains("inCommitTimestamp-preview"))

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "false",
        expectedValue = false)
      assert(getProtocolActionFromCommit(engine, table, 1).isEmpty)

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)
      assert(getProtocolActionFromCommit(engine, table, 2).isEmpty)
    }
  }

  test("Metadata toString should work with ICT enabled") {
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
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val metadata = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl].getMetadata
      val inCommitTimestamp = getInCommitTimestamp(engine, table, version = 1).get
      assert(metadata.toString == String.format(
        "Metadata{id='%s', name=Optional.empty, description=Optional.empty, " +
          "format=Format{provider='parquet', options={}}, " +
          "schemaString='{\n  \"type\" : \"struct\",\n  \"fields\" : [ {\n" +
          "  \"name\" : \"id\",\n  \"type\" : \"integer\",\n  \"nullable\" : true, \n" +
          "  \"metadata\" : {}\n} ]\n}', " +
          "partitionColumns=List(), createdTime=Optional[%s], " +
          "configuration={delta.enableInCommitTimestamps-preview=true, " +
          "delta.inCommitTimestampEnablementVersion-preview=1, " +
          "delta.inCommitTimestampEnablementTimestamp-preview=%s}}",
        metadata.getId,
        metadata.getCreatedTime.get,
        inCommitTimestamp.toString))
    }
  }

  test("Table with ICT enabled is readable") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      txn.commit(engine, emptyIterable())

      val commitResult = appendData(
        engine,
        tablePath,
        schema = testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver1Snapshot, IN_COMMIT_TIMESTAMPS_ENABLED, true)
      assertMetadataProp(
        ver1Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
        Optional.of(getInCommitTimestamp(engine, table, version = 1).get))
      assertMetadataProp(
        ver1Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        Optional.of(1L))

      val expData = dataBatches1.flatMap(_.toTestRows)

      verifyCommitResult(commitResult, expVersion = 1, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tablePath, version = 1, partitionCols = null, operation = WRITE)
      verifyWrittenContent(tablePath, testSchema, expData)
      verifyTableProperties(tablePath,
        ListMap(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> true,
        "delta.feature.inCommitTimestamp-preview" -> "supported",
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey
          -> getInCommitTimestamp(engine, table, version = 1).get,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> 1L),
        3,
        7)
    }
  }

  /**
   *  Helper method to read the inCommitTimestamp from the commit file of the given version if it
   *  is not null, otherwise return null.
   */
  private def getInCommitTimestamp(engine: Engine, table: Table, version: Long): Option[Long] = {
    val logPath = new Path(table.getPath(engine), "_delta_log")
    val commitInfoOpt = CommitInfo.getCommitInfoOpt(engine, logPath, version)
    if (commitInfoOpt.isPresent && commitInfoOpt.get.getInCommitTimestamp.isPresent) {
      Some(commitInfoOpt.get.getInCommitTimestamp.get)
    } else {
      Option.empty
    }
  }

  test("Conflict resolution of timestamps") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath, () => System.currentTimeMillis)
      setTablePropAndVerify(
        engine, tablePath, isNewTable = true, IN_COMMIT_TIMESTAMPS_ENABLED, "true", true)

      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val txn1 = createTxn(
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
      clock.setTime(startTime - 1000)
      commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
      assert(
        getInCommitTimestamp(
          engine, table, 2).get == getInCommitTimestamp(engine, table, 1).get + 1)
    }
  }

  test("Conflict resolution of enablement version") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath, () => System.currentTimeMillis)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      txn.commit(engine, emptyIterable())

      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)

      val txn1 = createTxn(
        engine,
        tablePath,
        schema = testSchema,
        partCols = Seq.empty,
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"),
        clock = clock)

      clock.setTime(startTime)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock
      )

      commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))

      val ver1Snapshot = table.getSnapshotAsOfVersion(engine, 1).asInstanceOf[SnapshotImpl]
      val ver2Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val observedEnablementTimestamp =
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetadata(ver2Snapshot.getMetadata)
      val observedEnablementVersion =
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetadata(ver2Snapshot.getMetadata)
      assert(observedEnablementTimestamp.get == ver1Snapshot.getTimestamp(engine) + 1)
      assert(
        observedEnablementTimestamp.get == getInCommitTimestamp(engine, table, version = 2).get)
      assert(observedEnablementVersion.get == 2)
    }
  }

  test("Missing CommitInfo in last winning commit in conflict resolution should result in a " +
    "DELTA_MISSING_COMMIT_INFO exception") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = TableImpl.forPath(engine, tablePath, () => System.currentTimeMillis)
      setTablePropAndVerify(
        engine, tablePath, isNewTable = true, IN_COMMIT_TIMESTAMPS_ENABLED, "true", true)

      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val txn1 = createTxn(
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
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock
      )

      // Remove CommitInfo from the commit.
      val logPath = new Path(table.getPath(engine), "_delta_log")
      removeCommitInfoFromCommit(engine, 2, logPath)

      clock.setTime(startTime - 1000)
      val ex = intercept[InvalidTableException] {
        commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
      }
      assert(ex.getMessage.contains(String.format(
        "This table has the feature %s enabled which requires the presence of the " +
          "CommitInfo action in every commit. However, the CommitInfo action is " +
          "missing from commit version %s.", "inCommitTimestamp-preview", "2")))
    }
  }
}
