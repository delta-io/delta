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

import java.util.{Locale, Optional}

import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, Seq}
import scala.collection.mutable

import io.delta.kernel._
import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, WriteUtilsWithV1Builders, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{InvalidTableException, ProtocolChangedException}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{DeltaHistoryManager, SnapshotImpl, TableImpl}
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.internal.actions.{CommitInfo, SingleAction}
import io.delta.kernel.internal.actions.SingleAction.createCommitInfoSingleAction
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.internal.util.ManualClock
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types._
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

/**
 * Runs in-commit timestamp tests using the TableManager snapshot APIs and V2 transaction builders
 */
class InCommitTimestampSuite extends AbstractInCommitTimestampSuite with WriteUtilsWithV2Builders

/**
 * Runs in-commit timestamp tests using the legacy Table snapshot APIs and V1 transaction builders
 */
class LegacyInCommitTimestampSuite extends AbstractInCommitTimestampSuite
    with WriteUtilsWithV1Builders

trait AbstractInCommitTimestampSuite extends AnyFunSuite {
  self: AbstractWriteUtils =>

  private def getLogPath(engine: Engine, tablePath: String): Path = {
    val resolvedTablePath = engine.getFileSystemClient.resolvePath(tablePath)
    new Path(resolvedTablePath, "_delta_log")
  }

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
        FileNames.deltaFile(logPath, version),
        rowsWithoutCommitInfo,
        true /* overwrite */ )
  }

  test("Enable ICT on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val beforeCommitAttemptStartTime = System.currentTimeMillis
      val clock = new ManualClock(beforeCommitAttemptStartTime + 1)
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true,
        clock = clock)

      val ver0Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)

      assert(ver0Snapshot.getTimestamp(engine) === beforeCommitAttemptStartTime + 1)
      assert(
        getInCommitTimestamp(
          engine,
          tablePath,
          version = 0).get === ver0Snapshot.getTimestamp(engine))
      assertHasWriterFeature(ver0Snapshot, "inCommitTimestamp")
      // Time travel should work
      val searchedSnapshot = getTableManagerAdapter.getSnapshotAtTimestamp(
        engine,
        tablePath,
        beforeCommitAttemptStartTime + 1)
      assert(searchedSnapshot.getVersion == 0)
    }
  }

  test("Create a non-inCommitTimestamp table and then enable ICT") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn1 = getCreateTxn(engine, tablePath, testSchema)

      txn1.commit(engine, emptyIterable())

      val ver0Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      assertMetadataProp(ver0Snapshot, IN_COMMIT_TIMESTAMPS_ENABLED, false)
      assertHasNoWriterFeature(ver0Snapshot, "inCommitTimestamp")
      assert(getInCommitTimestamp(engine, tablePath, version = 0).isEmpty)

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)

      val ver1Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      assertHasWriterFeature(ver1Snapshot, "inCommitTimestamp")
      assert(ver1Snapshot.getTimestamp(engine) > ver0Snapshot.getTimestamp(engine))
      assert(
        getInCommitTimestamp(
          engine,
          tablePath,
          version = 1).get === ver1Snapshot.getTimestamp(engine))

      // Time travel should work
      // Search timestamp = ICT enablement time - 1
      val searchedSnapshot1 = getTableManagerAdapter.getSnapshotAtTimestamp(
        engine,
        tablePath,
        ver1Snapshot.getTimestamp(engine) - 1)
      assert(searchedSnapshot1.getVersion == 0)
      // Search timestamp = ICT enablement time
      val searchedSnapshot2 =
        getTableManagerAdapter.getSnapshotAtTimestamp(
          engine,
          tablePath,
          ver1Snapshot.getTimestamp(engine))
      assert(searchedSnapshot2.getVersion == 1)
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
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        clock = clock,
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val ver1Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      val ver1Timestamp = ver1Snapshot.getTimestamp(engine)
      assert(IN_COMMIT_TIMESTAMPS_ENABLED.fromMetadata(ver1Snapshot.getMetadata))

      clock.setTime(startTime - 10000)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock)

      val ver2Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      val ver2Timestamp = ver2Snapshot.getTimestamp(engine)
      assert(ver2Timestamp === ver1Timestamp + 1)
    }
  }

  test("Missing CommitInfo should result in a DELTA_MISSING_COMMIT_INFO exception") {
    withTempDirAndEngine { (tablePath, engine) =>
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)
      // Remove CommitInfo from the commit.
      removeCommitInfoFromCommit(engine, 0, getLogPath(engine, tablePath))

      val ex = intercept[InvalidTableException] {
        getTableManagerAdapter.getSnapshotAtLatest(
          engine,
          tablePath).getTimestamp(engine)
      }
      assert(ex.getMessage.contains(String.format(
        "This table has the feature %s enabled which requires the presence of the " +
          "CommitInfo action in every commit. However, the CommitInfo action is " +
          "missing from commit version %s.",
        "inCommitTimestamp",
        "0")))
    }
  }

  test("Missing CommitInfo.inCommitTimestamp should result in a " +
    "DELTA_MISSING_COMMIT_TIMESTAMP exception") {
    withTempDirAndEngine { (tablePath, engine) =>
      setTablePropAndVerify(
        engine,
        tablePath,
        isNewTable = true,
        IN_COMMIT_TIMESTAMPS_ENABLED,
        "true",
        true)
      // Remove CommitInfo.inCommitTimestamp from the commit.
      val logPath = getLogPath(engine, tablePath)
      val file = FileStatus.of(FileNames.deltaFile(logPath, 0), 0, 0)
      val columnarBatches =
        engine.getJsonHandler.readJsonFiles(
          singletonCloseableIterator(file),
          SingleAction.FULL_SCHEMA,
          Optional.empty())
      assert(columnarBatches.hasNext)
      val rows = columnarBatches.next().getRows
      val commitInfoOpt =
        CommitInfo.unsafeTryReadCommitInfoFromPublishedDeltaFile(engine, logPath, 0)
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
          rowsWithoutCommitInfoInCommitTimestamp,
          true /* overwrite */ )

      val ex = intercept[InvalidTableException] {
        getTableManagerAdapter.getSnapshotAtLatest(
          engine,
          tablePath).getTimestamp(engine)
      }
      assert(ex.getMessage.contains(String.format(
        "This table has the feature %s enabled which requires the presence of " +
          "inCommitTimestamp in the CommitInfo action. However, this field has not " +
          "been set in commit version %s.",
        "inCommitTimestamp",
        "0")))
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

      val ver0Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      assertHasNoMetadataProp(ver0Snapshot, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP)
      assertHasNoMetadataProp(ver0Snapshot, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION)
    }
  }

  test("Enablement tracking works when ICT is enabled post commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = getCreateTxn(engine, tablePath, testSchema)

      txn.commit(engine, emptyIterable())

      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val ver1Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
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
        data = Seq(Map.empty[String, Literal] -> dataBatches2))

      val ver2Snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
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
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)
      val protocol = getProtocolActionFromCommit(engine, tablePath, 0)
      assert(protocol.isDefined)
      assert(VectorUtils.toJavaList(protocol.get.getArray(3)).contains("inCommitTimestamp"))

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "false",
        expectedValue = false)
      assert(getProtocolActionFromCommit(engine, tablePath, 1).isEmpty)

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)
      assert(getProtocolActionFromCommit(engine, tablePath, 2).isEmpty)
    }
  }

  test("Metadata toString should work with ICT enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = getCreateTxn(engine, tablePath, testSchema)

      txn.commit(engine, emptyIterable())

      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val metadata = getTableManagerAdapter.getSnapshotAtLatest(
        engine,
        tablePath).getMetadata
      val inCommitTimestamp = getInCommitTimestamp(engine, tablePath, version = 1).get
      assert(metadata.toString == String.format(
        "Metadata{id='%s', name=Optional.empty, description=Optional.empty, " +
          "format=Format{provider='parquet', options={}}, " +
          "schemaString='{\"type\":\"struct\",\"fields\":[{" +
          "\"name\":\"id\",\"type\":\"integer\",\"nullable\":true," +
          "\"metadata\":{}}]}', " +
          "partitionColumns=List(), createdTime=Optional[%s], " +
          "configuration={delta.inCommitTimestampEnablementTimestamp=%s, " +
          "delta.enableInCommitTimestamps=true, " +
          "delta.inCommitTimestampEnablementVersion=1}}",
        metadata.getId,
        metadata.getCreatedTime.get,
        inCommitTimestamp.toString))
    }
  }

  test("Table with ICT enabled is readable") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = getCreateTxn(engine, tablePath, testSchema)

      txn.commit(engine, emptyIterable())

      val commitResult = appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val ver1Snapshot =
        getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      assertMetadataProp(ver1Snapshot, IN_COMMIT_TIMESTAMPS_ENABLED, true)
      assertMetadataProp(
        ver1Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP,
        Optional.of(getInCommitTimestamp(engine, tablePath, version = 1).get))
      assertMetadataProp(
        ver1Snapshot,
        IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION,
        Optional.of(1L))

      val expData = dataBatches1.flatMap(_.toTestRows)

      verifyCommitResult(commitResult, expVersion = 1, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tablePath, version = 1, partitionCols = null)
      verifyWrittenContent(tablePath, testSchema, expData)
      verifyTableProperties(
        tablePath,
        ListMap(
          // appendOnly, invariants implicitly supported as the protocol is upgraded from 2 to 7
          // These properties are not set in the table properties, but are generated by the
          // Spark describe
          IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> true,
          "delta.feature.appendOnly" -> "supported",
          "delta.feature.inCommitTimestamp" -> "supported",
          "delta.feature.invariants" -> "supported",
          IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.getKey
            -> getInCommitTimestamp(engine, tablePath, version = 1).get,
          IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.getKey -> 1L),
        1,
        7)
    }
  }

  /**
   *  Helper method to read the inCommitTimestamp from the commit file of the given version if it
   *  is not null, otherwise return null.
   */
  private def getInCommitTimestamp(
      engine: Engine,
      tablePath: String,
      version: Long): Option[Long] = {
    val commitInfoOpt =
      CommitInfo.unsafeTryReadCommitInfoFromPublishedDeltaFile(
        engine,
        getLogPath(engine, tablePath),
        version)
    if (commitInfoOpt.isPresent && commitInfoOpt.get.getInCommitTimestamp.isPresent) {
      Some(commitInfoOpt.get.getInCommitTimestamp.get)
    } else {
      Option.empty
    }
  }

  test("Conflict resolution of timestamps") {
    withTempDirAndEngine { (tablePath, engine) =>
      setTablePropAndVerify(
        engine,
        tablePath,
        isNewTable = true,
        IN_COMMIT_TIMESTAMPS_ENABLED,
        "true",
        true)

      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val txn1 = getUpdateTxn(
        engine,
        tablePath,
        clock = clock)
      clock.setTime(startTime)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock)
      clock.setTime(startTime - 1000)
      commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
      assert(
        getInCommitTimestamp(engine, tablePath, version = 2).get ===
          getInCommitTimestamp(engine, tablePath, version = 1).get + 1)
    }
  }

  Seq(10, 2).foreach { winningCommitCount =>
    test(s"Conflict resolution of enablement version(Winning Commit Count=$winningCommitCount)") {
      withTempDirAndEngine { (tablePath, engine) =>
        val txn = getCreateTxn(engine, tablePath, testSchema)

        txn.commit(engine, emptyIterable())

        val startTime = System.currentTimeMillis() // we need to fix this now!
        val txn1 = getUpdateTxn(
          engine,
          tablePath,
          tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"),
          clock = () => startTime)

        // Sleep for 1 second to ensure that due to file-system truncation, the timestamp for these
        // non-ict commits is not less than the fixed time for the txn above
        // If this happens, nothing incorrect happens, but the
        // ictEnablementTimestamp != prevVersion.timestamp + 1 since we will use the greater ts
        Thread.sleep(1000)
        for (_ <- 0 until winningCommitCount) {
          appendData(
            engine,
            tablePath,
            data = Seq(Map.empty[String, Literal] -> dataBatches2))
        }

        commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))

        val lastSnapshot = getTableManagerAdapter.getSnapshotAtVersion(
          engine,
          tablePath,
          winningCommitCount)
        val curSnapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
        val observedEnablementTimestamp =
          IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP.fromMetadata(curSnapshot.getMetadata)
        val observedEnablementVersion =
          IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION.fromMetadata(curSnapshot.getMetadata)
        assert(observedEnablementTimestamp.get === lastSnapshot.getTimestamp(engine) + 1)
        assert(
          observedEnablementTimestamp.get ===
            getInCommitTimestamp(engine, tablePath, version = winningCommitCount + 1).get)
        assert(observedEnablementVersion.get === winningCommitCount + 1)
      }
    }
  }

  test("Missing CommitInfo in last winning commit in conflict resolution should result in a " +
    "DELTA_MISSING_COMMIT_INFO exception") {
    withTempDirAndEngine { (tablePath, engine) =>
      setTablePropAndVerify(
        engine,
        tablePath,
        isNewTable = true,
        IN_COMMIT_TIMESTAMPS_ENABLED,
        "true",
        true)

      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val txn1 = getUpdateTxn(
        engine,
        tablePath,
        clock = clock)
      clock.setTime(startTime)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        clock = clock)

      // Remove CommitInfo from the commit.
      removeCommitInfoFromCommit(engine, 2, getLogPath(engine, tablePath))

      clock.setTime(startTime - 1000)
      val ex = intercept[InvalidTableException] {
        commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
      }
      assert(ex.getMessage.contains(String.format(
        "This table has the feature %s enabled which requires the presence of the " +
          "CommitInfo action in every commit. However, the CommitInfo action is " +
          "missing from commit version %s.",
        "inCommitTimestamp",
        "2")))
    }
  }

  test("Throw an error where the winning txn enables the ICT and losing txn prepares txn with " +
    "ICT enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = getCreateTxn(engine, tablePath, testSchema)

      txn.commit(engine, emptyIterable())

      val startTime = System.currentTimeMillis()
      val clock = new ManualClock(startTime)
      val txn1 = getUpdateTxn(
        engine,
        tablePath,
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"),
        clock = clock)
      clock.setTime(startTime)
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2),
        tableProperties = Map(IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"),
        clock = clock)
      clock.setTime(startTime - 1000)
      val ex = intercept[ProtocolChangedException] {
        commitAppendData(engine, txn1, Seq(Map.empty[String, Literal] -> dataBatches1))
      }
      assert(ex.getMessage.contains(String.format("Transaction has encountered a conflict and " +
        "can not be committed. Query needs to be re-executed using the latest version of the " +
        "table.")))
    }
  }

  test("Disabling ICT removes enablement tracking properties") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      createTableThenEnableIctAndVerify(engine, tablePath)

      // ===== WHEN =====
      // Disable ICT. This should remove enablement tracking properties.
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "false",
        expectedValue = false)

      // ===== THEN =====
      val snapshotV2 = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      assertMetadataProp(snapshotV2, IN_COMMIT_TIMESTAMPS_ENABLED, false)
      assertHasNoMetadataProp(snapshotV2, IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP)
      assertHasNoMetadataProp(snapshotV2, IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION)
    }
  }
}
