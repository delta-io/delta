/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import java.util
import java.util.{Collections, Optional}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, ProtocolChangedException}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableImpl}
import io.delta.kernel.internal.actions.{AddFile, Protocol, SingleAction}
import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import org.apache.spark.sql.delta.DeltaLog

import org.apache.hadoop.fs.Path

class RowTrackingSuite extends DeltaTableWriteSuiteBase with ParquetSuiteBase {
  private def prepareActionsForCommit(actions: Row*): CloseableIterable[Row] = {
    inMemoryIterable(toCloseableIterator(actions.asJava.iterator()))
  }

  private def setRowTrackingSupported(engine: Engine, tablePath: String): Unit = {
    val tableProps = Map("delta.enableRowTracking" -> "true")
    val txn =
      createTxn(engine, tablePath, isNewTable = false, testSchema, tableProperties = tableProps)
    txn.commit(engine, emptyIterable())
  }

  private def disableRowTracking(engine: Engine, tablePath: String): Unit = {
    val tableProps = Map("delta.enableRowTracking" -> "false")
    val txn =
      createTxn(engine, tablePath, isNewTable = false, testSchema, tableProperties = tableProps)
    txn.commit(engine, emptyIterable())
  }

  private def createTableWithRowTrackingSupported(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema): Unit = {
    createTxn(engine, tablePath, isNewTable = true, schema, Seq.empty)
      .commit(engine, emptyIterable())
    setRowTrackingSupported(engine, tablePath)
  }

  private def verifyBaseRowIDs(
      engine: Engine,
      tablePath: String,
      expectedValue: Seq[Long]): Unit = {
    val table = TableImpl.forPath(engine, tablePath)
    val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

    val scanFileRows = collectScanFileRows(snapshot.getScanBuilder().build())
    val sortedBaseRowIds = scanFileRows
      .map(InternalScanFileUtils.getBaseRowId)
      .map(_.orElse(-1))
      .sorted

    assert(sortedBaseRowIds === expectedValue)
  }

  private def verifyDefaultRowCommitVersion(
      engine: Engine,
      tablePath: String,
      expectedValue: Seq[Long]) = {
    val table = TableImpl.forPath(engine, tablePath)
    val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

    val scanFileRows = collectScanFileRows(snapshot.getScanBuilder().build())
    val sortedAddFileDefaultRowCommitVersions = scanFileRows
      .map(InternalScanFileUtils.getDefaultRowCommitVersion)
      .map(_.orElse(-1))
      .sorted

    assert(sortedAddFileDefaultRowCommitVersions === expectedValue)
  }

  private def verifyHighWatermark(engine: Engine, tablePath: String, expectedValue: Long): Unit = {
    val table = TableImpl.forPath(engine, tablePath)
    val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
    val rowTrackingMetadataDomain = RowTrackingMetadataDomain.fromSnapshot(snapshot)

    assert(rowTrackingMetadataDomain.isPresent)
    assert(rowTrackingMetadataDomain.get().getRowIdHighWaterMark === expectedValue)
  }

  private def prepareDataForCommit(data: Seq[FilteredColumnarBatch]*)
      : Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])] = {
    data.map(Map.empty[String, Literal] -> _).toIndexedSeq
  }

  test("Base row IDs/default row commit versions are assigned to AddFile actions") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTrackingSupported(engine, tablePath)

      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1) // 100 rows
      val dataBatch2 = generateData(testSchema, Seq.empty, Map.empty, 200, 1) // 200 rows
      val dataBatch3 = generateData(testSchema, Seq.empty, Map.empty, 400, 1) // 400 rows

      // Commit three files in one transaction
      val commitVersion = appendData(
        engine,
        tablePath,
        data = prepareDataForCommit(dataBatch1, dataBatch2, dataBatch3)).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0, 100, 300))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq.fill(3)(commitVersion))
      verifyHighWatermark(engine, tablePath, 699)
    }
  }

  test("Previous Row ID high watermark can be picked up to assign base row IDs") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTrackingSupported(engine, tablePath)

      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val commitVersion1 = appendData(
        engine,
        tablePath,
        data = Seq(dataBatch1).map(Map.empty[String, Literal] -> _)).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq(commitVersion1))
      verifyHighWatermark(engine, tablePath, 99)

      val dataBatch2 = generateData(testSchema, Seq.empty, Map.empty, 200, 1)
      val commitVersion2 = appendData(
        engine,
        tablePath,
        data = prepareDataForCommit(dataBatch2)).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0, 100))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq(commitVersion1, commitVersion2))
      verifyHighWatermark(engine, tablePath, 299)
    }
  }

  test("Base row IDs/default row commit versions are preserved in checkpoint") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTrackingSupported(engine, tablePath)

      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val dataBatch2 = generateData(testSchema, Seq.empty, Map.empty, 200, 1)
      val dataBatch3 = generateData(testSchema, Seq.empty, Map.empty, 400, 1)

      val commitVersion1 = appendData(
        engine,
        tablePath,
        data = prepareDataForCommit(dataBatch1)).getVersion

      val commitVersion2 = appendData(
        engine,
        tablePath,
        data = prepareDataForCommit(dataBatch2)).getVersion

      // Checkpoint the table
      val table = TableImpl.forPath(engine, tablePath)
      val latestVersion = table.getLatestSnapshot(engine).getVersion()
      table.checkpoint(engine, latestVersion)

      val commitVersion3 = appendData(
        engine,
        tablePath,
        data = prepareDataForCommit(dataBatch3)).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0, 100, 300))
      verifyDefaultRowCommitVersion(
        engine,
        tablePath,
        Seq(commitVersion1, commitVersion2, commitVersion3))
      verifyHighWatermark(engine, tablePath, 699)
    }
  }

  test("Fail if row tracking is supported but AddFile actions are missing stats") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTrackingSupported(engine, tablePath)

      val addFileRow = AddFile.createAddFileRow(
        "fakePath",
        VectorUtils.stringStringMapValue(new util.HashMap[String, String]()),
        0L,
        0L,
        false,
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty() // No stats
      )
      val action = SingleAction.createAddFileSingleAction(addFileRow)
      val txn = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)

      // KernelException thrown inside a lambda is wrapped in a RuntimeException
      val e = intercept[RuntimeException] {
        txn.commit(engine, prepareActionsForCommit(action))
      }
      assert(
        e.getMessage.contains(
          "Cannot write to a rowTracking-supported table without 'numRecords' statistics. "
            + "Connectors are expected to populate the number of records statistics when "
            + "writing to a Delta table with 'rowTracking' table feature supported."))
    }
  }

  test("Integration test - Write table with Kernel then write with Spark") {
    withTempDirAndEngine((tablePath, engine) => {
      val tbl = "tbl"
      withTable(tbl) {
        val schema = new StructType().add("id", LONG)
        createTableWithRowTrackingSupported(engine, tablePath, schema = schema)

        // Write table using Kernel
        val dataBatch1 = generateData(schema, Seq.empty, Map.empty, 100, 1) // 100 rows
        val dataBatch2 = generateData(schema, Seq.empty, Map.empty, 200, 1) // 200 rows
        val dataBatch3 = generateData(schema, Seq.empty, Map.empty, 400, 1) // 400 rows
        appendData(
          engine,
          tablePath,
          schema = schema,
          data = prepareDataForCommit(dataBatch1, dataBatch2, dataBatch3)
        ).getVersion // version 2

        // Verify the table state
        verifyBaseRowIDs(engine, tablePath, Seq(0, 100, 300))
        verifyDefaultRowCommitVersion(engine, tablePath, Seq(2, 2, 2))
        verifyHighWatermark(engine, tablePath, 699)

        // Write 20, 80 rows to the table using Spark
        spark.range(0, 20).write.format("delta").mode("append").save(tablePath) // version 3
        spark.range(20, 100).write.format("delta").mode("append").save(tablePath) // version 4

        // Verify the table state
        verifyBaseRowIDs(engine, tablePath, Seq(0, 100, 300, 700, 720))
        verifyDefaultRowCommitVersion(engine, tablePath, Seq(2, 2, 2, 3, 4))
        verifyHighWatermark(engine, tablePath, 799)
      }
    })
  }

  test("Integration test - Write table with Spark then write with Kernel") {
    withTempDirAndEngine((tablePath, engine) => {
      val tbl = "tbl"
      withTable(tbl) {
        spark.sql(
          s"""CREATE TABLE $tbl (id LONG) USING delta
             |LOCATION '$tablePath'
             |TBLPROPERTIES (
             |  'delta.feature.domainMetadata' = 'enabled',
             |  'delta.feature.rowTracking' = 'supported'
             |)
             |""".stripMargin)

        // Write to the table using delta-spark
        spark.range(0, 20).write.format("delta").mode("append").save(tablePath) // version 1
        spark.range(20, 100).write.format("delta").mode("append").save(tablePath) // version 2

        // Verify the table state
        verifyBaseRowIDs(engine, tablePath, Seq(0, 20))
        verifyDefaultRowCommitVersion(engine, tablePath, Seq(1, 2))
        verifyHighWatermark(engine, tablePath, 99)

        // Write to the table using Kernel
        val schema = new StructType().add("id", LONG)
        val dataBatch1 = generateData(schema, Seq.empty, Map.empty, 100, 1) // 100 rows
        val dataBatch2 = generateData(schema, Seq.empty, Map.empty, 200, 1) // 200 rows
        val dataBatch3 = generateData(schema, Seq.empty, Map.empty, 400, 1) // 400 rows
        appendData(
          engine,
          tablePath,
          schema = schema,
          data = prepareDataForCommit(dataBatch1, dataBatch2, dataBatch3)
        ) // version 3

        // Verify the table state
        verifyBaseRowIDs(engine, tablePath, Seq(0, 20, 100, 200, 400))
        verifyDefaultRowCommitVersion(engine, tablePath, Seq(1, 2, 3, 3, 3))
        verifyHighWatermark(engine, tablePath, 799)
      }
    })
  }

  private def validateConflictResolution(
      engine: Engine,
      tablePath: String,
      dataSizeTxn1: Int,
      dataSizeTxn2: Int,
      useSparkTxn2: Boolean = false,
      dataSizeTxn3: Int,
      useSparkTxn3: Boolean = false): Unit = {

    /**
     * Txn1: the current transaction that commits later than winning transactions.
     * Txn2: the winning transaction that was committed first.
     * Txn3: the winning transaction that was committed second.
     *
     * Note tx is the timestamp.
     *
     * t1 ------------------------ Txn1 starts.
     * t2 ------- Txn2 starts.
     * t3 ------- Txn2 commits.
     * t4 ------- Txn3 starts.
     * t5 ------- Txn3 commits.
     * t6 ------------------------ Txn1 commits.
     */
    val schema = new StructType().add("id", LONG)

    // Create a row-tracking-supported table and bump the row ID high watermark to the initial value
    createTableWithRowTrackingSupported(engine, tablePath, schema)
    val initDataSize = 100L
    val dataBatch = generateData(schema, Seq.empty, Map.empty, initDataSize.toInt, 1)
    val v0 = appendData(engine, tablePath, data = prepareDataForCommit(dataBatch)).getVersion

    var expectedBaseRowIDs = Seq(0L)
    var expectedDefaultRowCommitVersion = Seq(v0)
    var expectedHighWatermark = initDataSize - 1

    def verifyRowTrackingStates(): Unit = {
      verifyBaseRowIDs(engine, tablePath, expectedBaseRowIDs)
      verifyDefaultRowCommitVersion(engine, tablePath, expectedDefaultRowCommitVersion)
      verifyHighWatermark(engine, tablePath, expectedHighWatermark)
    }
    verifyRowTrackingStates()

    // Create txn1 but don't commit it yet
    val txn1 = createTxn(engine, tablePath)

    // Create and commit txn2
    if (dataSizeTxn2 > 0) {
      val v = if (useSparkTxn2) {
        spark.range(0, dataSizeTxn2).write.format("delta").mode("append").save(tablePath)
        DeltaLog.forTable(spark, new Path(tablePath)).snapshot.version
      } else {
        val dataBatchTxn2 = generateData(schema, Seq.empty, Map.empty, dataSizeTxn2, 1)
        appendData(engine, tablePath, data = prepareDataForCommit(dataBatchTxn2)).getVersion
      }
      expectedBaseRowIDs = expectedBaseRowIDs ++ Seq(initDataSize)
      expectedDefaultRowCommitVersion = expectedDefaultRowCommitVersion ++ Seq(v)
      expectedHighWatermark = initDataSize + dataSizeTxn2 - 1
    } else {
      createTxn(engine, tablePath).commit(engine, emptyIterable())
    }
    verifyRowTrackingStates()

    // Create and commit txn3
    if (dataSizeTxn3 > 0) {
      val v = if (useSparkTxn3) {
        spark.range(0, dataSizeTxn3).write.format("delta").mode("append").save(tablePath)
        DeltaLog.forTable(spark, new Path(tablePath)).snapshot.version
      } else {
        val dataBatchTxn3 = generateData(schema, Seq.empty, Map.empty, dataSizeTxn3, 1)
        appendData(engine, tablePath, data = prepareDataForCommit(dataBatchTxn3)).getVersion
      }
      expectedBaseRowIDs = expectedBaseRowIDs ++ Seq(initDataSize + dataSizeTxn2)
      expectedDefaultRowCommitVersion = expectedDefaultRowCommitVersion ++ Seq(v)
      expectedHighWatermark = initDataSize + dataSizeTxn2 + dataSizeTxn3 - 1
    } else {
      createTxn(engine, tablePath).commit(engine, emptyIterable())
    }
    verifyRowTrackingStates()

    // Commit txn1
    if (dataSizeTxn1 > 0) {
      val dataBatchTxn1 = generateData(schema, Seq.empty, Map.empty, dataSizeTxn1, 1)
      val v = commitAppendData(engine, txn1, prepareDataForCommit(dataBatchTxn1)).getVersion
      expectedBaseRowIDs = expectedBaseRowIDs ++ Seq(initDataSize + dataSizeTxn2 + dataSizeTxn3)
      expectedDefaultRowCommitVersion = expectedDefaultRowCommitVersion ++ Seq(v)
      expectedHighWatermark = initDataSize + dataSizeTxn2 + dataSizeTxn3 + dataSizeTxn1 - 1
    } else {
      txn1.commit(engine, emptyIterable())
    }
    verifyRowTrackingStates()
  }

  test("Conflict resolution - two concurrent txns both added new files") {
    withTempDirAndEngine((tablePath, engine) => {
      validateConflictResolution(
        engine,
        tablePath,
        dataSizeTxn1 = 200,
        dataSizeTxn2 = 300,
        dataSizeTxn3 = 400)
    })
  }

  test("Conflict resolution - only one of the two concurrent txns added new files") {
    withTempDirAndEngine((tablePath, engine) => {
      validateConflictResolution(
        engine,
        tablePath,
        dataSizeTxn1 = 200,
        dataSizeTxn2 = 300,
        dataSizeTxn3 = 0)
    })
    withTempDirAndEngine((tablePath, engine) => {
      validateConflictResolution(
        engine,
        tablePath,
        dataSizeTxn1 = 200,
        dataSizeTxn2 = 0,
        dataSizeTxn3 = 300)
    })
  }

  test("Conflict resolution - none of the two concurrent txns added new files") {
    withTempDirAndEngine((tablePath, engine) => {
      validateConflictResolution(
        engine,
        tablePath,
        dataSizeTxn1 = 200,
        dataSizeTxn2 = 0,
        dataSizeTxn3 = 0)
    })
  }

  test("Conflict resolution - the current txn didn't add new files") {
    withTempDirAndEngine((tablePath, engine) => {
      validateConflictResolution(
        engine,
        tablePath,
        dataSizeTxn1 = 0,
        dataSizeTxn2 = 200,
        dataSizeTxn3 = 300)
    })
  }

  test(
    "Conflict resolution - two concurrent txns were commited by delta-spark " +
      "and both added new files") {
    withTempDirAndEngine((tablePath, engine) => {
      validateConflictResolution(
        engine,
        tablePath,
        dataSizeTxn1 = 200,
        dataSizeTxn2 = 300,
        useSparkTxn2 = true,
        dataSizeTxn3 = 400,
        useSparkTxn3 = true)
    })
  }

  test("Conflict resolution - Row tracking is made supported by a concurrent txn") {
    withTempDirAndEngine((tablePath, engine) => {
      // Create a table without row tracking
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      // Create a txn but don't commit it yet
      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val txn1 = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)

      // A concurrent txn makes row tracking supported
      setRowTrackingSupported(engine, tablePath)

      // Commit txn1 and expect failure
      val e = intercept[ProtocolChangedException] {
        commitAppendData(
          engine,
          txn1,
          prepareDataForCommit(dataBatch1))
      }
    })
  }

  test("Conflict resolution - Row tracking is made unsupported by a concurrent txn") {
    withTempDirAndEngine((tablePath, engine) => {
      createTableWithRowTrackingSupported(engine, tablePath)

      // Create a txn but don't commit it yet
      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val txn1 = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)

      // A concurrent txn makes row tracking unsupported
      disableRowTracking(engine, tablePath)

      // Commit txn1 and expect failure
      val e = intercept[ProtocolChangedException] {
        commitAppendData(
          engine,
          txn1,
          prepareDataForCommit(dataBatch1))
      }
    })
  }
}
