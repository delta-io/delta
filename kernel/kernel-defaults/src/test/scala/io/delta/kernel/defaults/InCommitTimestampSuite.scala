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
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{CommitInfo, SingleAction}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.internal.{SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import java.util.{Locale, Optional}
import scala.collection.JavaConverters._
import scala.collection.immutable.{ListMap, Seq}
import scala.collection.mutable
import io.delta.kernel.internal.TableConfig._
import io.delta.kernel.utils.FileStatus;

class InCommitTimestampSuite extends DeltaTableWriteSuiteBase {
  test("Enable ICT on commit 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val beforeCommitAttemptStartTime = System.currentTimeMillis
      val table = Table.forPath(engine, tablePath)

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assert(getInCommitTimestamp(engine, table, 0).get >= beforeCommitAttemptStartTime)
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

      val beforeCommitAttemptStartTime = System.currentTimeMillis
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = IN_COMMIT_TIMESTAMPS_ENABLED,
        value = "true",
        expectedValue = true)

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(ver1Snapshot, "inCommitTimestamp-preview")
      assert(
        getInCommitTimestamp(engine, table, 1).get >= beforeCommitAttemptStartTime)
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
        Optional.of(getInCommitTimestamp(engine, table, version = 1).get))
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
        Optional.of(getInCommitTimestamp(engine, table, version = 1).get))
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
    readCommitFile(engine, table.getPath(engine), version, row => {
      val commitInfoOrd = row.getSchema.indexOf("commitInfo")
      if (!row.isNullAt(commitInfoOrd)) {
        val commitInfo = row.getStruct(commitInfoOrd)
        val inCommitTimestampOrd = commitInfo.getSchema.indexOf("inCommitTimestamp")
        if (!commitInfo.isNullAt(inCommitTimestampOrd)) {
          return Some(commitInfo.getLong(inCommitTimestampOrd))
        }
      }
      Option.empty
    }).map{ case inCommitTimestamp: Long => Some(inCommitTimestamp)}.getOrElse(Option.empty)
  }
}
