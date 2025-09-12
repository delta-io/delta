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
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, TestRow, WriteUtilsWithV1Builders, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{ConcurrentWriteException, InvalidTableException, KernelException, MaxCommitRetryLimitReachedException}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableConfig, TableImpl}
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.internal.rowtracking.{RowTracking, RowTrackingMetadataDomain}
import io.delta.kernel.internal.rowtracking.MaterializedRowTrackingColumn.{MATERIALIZED_ROW_COMMIT_VERSION, MATERIALIZED_ROW_ID}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types._
import io.delta.kernel.utils.{CloseableIterable, MetadataColumnTestUtils}
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import org.apache.spark.sql.delta.DeltaLog

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

/** Runs row tracking tests using the TableManager snapshot APIs and V2 transaction builders */
class RowTrackingSuite extends AbstractRowTrackingSuite with WriteUtilsWithV2Builders

/** Runs row tracking tests using the legacy Table snapshot APIs and V1 transaction builders */
class LegacyRowTrackingSuite extends AbstractRowTrackingSuite with WriteUtilsWithV1Builders

trait AbstractRowTrackingSuite extends AnyFunSuite with ParquetSuiteBase
    with MetadataColumnTestUtils {
  self: AbstractWriteUtils =>
  private def prepareActionsForCommit(actions: Row*): CloseableIterable[Row] = {
    inMemoryIterable(toCloseableIterator(actions.asJava.iterator()))
  }

  private def createTableWithRowTracking(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema,
      extraProps: Map[String, String] = Map.empty): Unit = {
    val tableProps = Map(TableConfig.ROW_TRACKING_ENABLED.getKey -> "true") ++ extraProps
    createEmptyTable(engine, tablePath, schema = schema, tableProperties = tableProps)
  }

  /**
   * Creates a table with row tracking enabled and inserts initial data, then performs a merge
   * operation to update some records and insert new ones. We use Spark SQL for this test table to
   * ensure that the result table has both materialized and not materialized row tracking columns.
   *
   * @param tablePath The path to the Delta table.
   * @param extraProps Additional table properties to set.
   */
  private def createRowTrackingTableWithSpark(
      tablePath: String,
      extraProps: Map[String, String] = Map.empty): Unit = {
    val tblPropsStr = (extraProps + ("delta.enableRowTracking" -> "true"))
      .map { case (k, v) => s"'$k' = '$v'" }.mkString(", ")
    spark.sql(
      s"""
         |CREATE TABLE delta.`$tablePath` (
         |  id INT,
         |  value STRING
         |) USING DELTA
         |TBLPROPERTIES ($tblPropsStr)
         |""".stripMargin)

    // Insert 5 records
    val initialData = Seq(
      (1, "A"),
      (2, "B"),
      (3, "C"),
      (4, "D"),
      (5, "E"))
    spark.createDataFrame(initialData).toDF(
      "id",
      "value").repartition(1).write.format("delta").mode("overwrite").save(tablePath)

    // Prepare source for merge
    val sourceData = Seq(
      (3, "C_updated"), // will update id=3
      (6, "F") // will insert new id=6
    )
    spark.createDataFrame(sourceData).toDF("id", "value").createOrReplaceTempView("merge_source")

    // Merge: update id=3, insert id=6
    spark.sql(
      s"""
         |MERGE INTO delta.`$tablePath` t
         |USING merge_source s
         |ON t.id = s.id
         |WHEN MATCHED THEN UPDATE SET t.value = s.value
         |WHEN NOT MATCHED THEN INSERT (id, value) VALUES (s.id, s.value)
         |""".stripMargin)
  }

  private def verifyBaseRowIDs(
      engine: Engine,
      tablePath: String,
      expectedValue: Seq[Long]): Unit = {
    val snapshot =
      getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).asInstanceOf[SnapshotImpl]

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
    val snapshot =
      getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).asInstanceOf[SnapshotImpl]

    val scanFileRows = collectScanFileRows(snapshot.getScanBuilder().build())
    val sortedAddFileDefaultRowCommitVersions = scanFileRows
      .map(InternalScanFileUtils.getDefaultRowCommitVersion)
      .map(_.orElse(-1))
      .sorted

    assert(sortedAddFileDefaultRowCommitVersions === expectedValue)
  }

  private def verifyHighWatermark(engine: Engine, tablePath: String, expectedValue: Long): Unit = {
    val snapshot =
      getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).asInstanceOf[SnapshotImpl]
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
      createTableWithRowTracking(engine, tablePath)

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
      createTableWithRowTracking(engine, tablePath)

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
      createTableWithRowTracking(engine, tablePath)

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
      val latestVersion = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).getVersion()
      TableImpl.forPath(engine, tablePath).checkpoint(engine, latestVersion)

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

  test("Provided Row ID high watermark should be set in the txn") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTracking(engine, tablePath)

      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val commitVersion1 = appendData(
        engine,
        tablePath,
        data = Seq(dataBatch1).map(Map.empty[String, Literal] -> _)).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq(commitVersion1))
      verifyHighWatermark(engine, tablePath, 99)

      val dataBatch2 = generateData(testSchema, Seq.empty, Map.empty, 200, 1)
      val rowTrackingMetadataDomain = new RowTrackingMetadataDomain(400)

      val txn2 = createTxnWithDomainMetadatas(
        engine,
        tablePath,
        List(rowTrackingMetadataDomain.toDomainMetadata))
      val commitVersion2 =
        commitAppendData(engine, txn2, prepareDataForCommit(dataBatch2)).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0, 100))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq(commitVersion1, commitVersion2))
      verifyHighWatermark(engine, tablePath, 400)
    }
  }

  test("Fail if provided Row ID high watermark is smaller than the calculated high watermark") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTracking(engine, tablePath)

      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val commitVersion1 = appendData(
        engine,
        tablePath,
        data = Seq(dataBatch1).map(Map.empty[String, Literal] -> _)).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq(commitVersion1))
      verifyHighWatermark(engine, tablePath, 99)

      val dataBatch2 = generateData(testSchema, Seq.empty, Map.empty, 200, 1)

      // Set a higher value than the calculated high watermark = 299
      val rowTrackingMetadataDomain = new RowTrackingMetadataDomain(120)
      val txn2 = createTxnWithDomainMetadatas(
        engine,
        tablePath,
        List(rowTrackingMetadataDomain.toDomainMetadata))
      val e = intercept[RuntimeException] {
        commitAppendData(engine, txn2, prepareDataForCommit(dataBatch2)).getVersion
      }

      assert(
        e.getMessage.contains(
          "The provided row ID high watermark (120) must be greater than " +
            "or equal to the calculated row ID high watermark (299) based " +
            "on the transaction's data actions."))
    }
  }

  test("Fail if row tracking is supported but AddFile actions are missing stats") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTracking(engine, tablePath)

      val addFileRow = AddFile.createAddFileRow(
        null,
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
      val txn = getUpdateTxn(engine, tablePath)

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

  test("Fail if row tracking is not supported but client call withHighWatermark in txn") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)

      val rowTrackingMetadataDomain = new RowTrackingMetadataDomain(30)

      val e = intercept[RuntimeException] {
        createTxnWithDomainMetadatas(
          engine,
          tablePath,
          List(rowTrackingMetadataDomain.toDomainMetadata))
      }

      assert(
        e.getMessage.contains(
          "Cannot assign a row id high water mark"))
    }
  }

  test("Integration test - Write table with Kernel then write with Spark") {
    withTempDirAndEngine((tablePath, engine) => {
      withTempTable { tbl =>
        val schema = new StructType().add("id", LongType.LONG)
        createTableWithRowTracking(engine, tablePath, schema)

        // Write table using Kernel
        val dataBatch1 = generateData(schema, Seq.empty, Map.empty, 100, 1) // 100 rows
        val dataBatch2 = generateData(schema, Seq.empty, Map.empty, 200, 1) // 200 rows
        val dataBatch3 = generateData(schema, Seq.empty, Map.empty, 400, 1) // 400 rows
        appendData(
          engine,
          tablePath,
          data = prepareDataForCommit(dataBatch1, dataBatch2, dataBatch3)
        ).getVersion // version 1

        // Verify the table state
        verifyBaseRowIDs(engine, tablePath, Seq(0, 100, 300))
        verifyDefaultRowCommitVersion(engine, tablePath, Seq(1, 1, 1))
        verifyHighWatermark(engine, tablePath, 699)

        // Write 20, 80 rows to the table using Spark
        spark.range(0, 20).write.format("delta").mode("append").save(tablePath) // version 2
        spark.range(20, 100).write.format("delta").mode("append").save(tablePath) // version 3

        // Verify the table state
        verifyBaseRowIDs(engine, tablePath, Seq(0, 100, 300, 700, 720))
        verifyDefaultRowCommitVersion(engine, tablePath, Seq(1, 1, 1, 2, 3))
        verifyHighWatermark(engine, tablePath, 799)
      }
    })
  }

  test("Integration test - Write table with Spark then write with Kernel") {
    withTempDirAndEngine((tablePath, engine) => {
      withTempTable { tbl =>
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
        val schema = new StructType().add("id", LongType.LONG)
        val dataBatch1 = generateData(schema, Seq.empty, Map.empty, 100, 1) // 100 rows
        val dataBatch2 = generateData(schema, Seq.empty, Map.empty, 200, 1) // 200 rows
        val dataBatch3 = generateData(schema, Seq.empty, Map.empty, 400, 1) // 400 rows
        appendData(
          engine,
          tablePath,
          data = prepareDataForCommit(dataBatch1, dataBatch2, dataBatch3)
        ) // version 3

        // Verify the table state
        verifyBaseRowIDs(engine, tablePath, Seq(0, 20, 100, 200, 400))
        verifyDefaultRowCommitVersion(engine, tablePath, Seq(1, 2, 3, 3, 3))
        verifyHighWatermark(engine, tablePath, 799)
      }
    })
  }

  /* -------- Test reading from tables with row tracking -------- */
  test("Error when reading row tracking columns from a non-row-tracking table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a new table without row tracking
      val wrongSchema =
        new StructType().add("id", IntegerType.INTEGER).add("_metadata.row_id", LongType.LONG)
      createEmptyTable(engine, tablePath, wrongSchema)

      // Try to read row tracking columns
      val e = intercept[KernelException] {
        checkTable(
          tablePath,
          expectedAnswer = Seq(),
          readCols = Seq("id", "_metadata.row_id"),
          metadataCols =
            Seq(ROW_ID, ROW_COMMIT_VERSION),
          engine = engine)
      }
      assert(e.getMessage.contains("Row tracking is not enabled, but row tracking column"))
    }
  }

  Seq("none", "name", "id").foreach(mode => {
    test(s"Read row tracking columns from delta-spark table with column mapping = $mode") {
      withTempDirAndEngine { (tablePath, _) =>
        createRowTrackingTableWithSpark(
          tablePath,
          extraProps = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> mode))

        val expectedAnswer = Seq(
          TestRow(1, "A", 0L, 1L),
          TestRow(2, "B", 1L, 1L),
          TestRow(3, "C_updated", 2L, 2L),
          TestRow(4, "D", 3L, 1L),
          TestRow(5, "E", 4L, 1L),
          TestRow(6, "F", 10L, 2L))

        // We only check whether the delta-spark table schema is inferred correctly if column
        // mapping is disabled
        val expectedSchema = if (mode == "none") {
          new StructType()
            .add(new StructField("id", IntegerType.INTEGER, true))
            .add(new StructField("value", StringType.STRING, true))
        } else { null }

        checkTable(
          path = tablePath,
          expectedAnswer,
          metadataCols =
            Seq(ROW_ID, ROW_COMMIT_VERSION),
          expectedSchema = expectedSchema)
      }
    }
  })

  test("Read subset of row tracking columns from delta-spark table") {
    withTempDirAndEngine { (tablePath, _) =>
      createRowTrackingTableWithSpark(tablePath)

      val expectedAnswer = Seq(
        TestRow("A", 0L),
        TestRow("B", 1L),
        TestRow("C_updated", 2L),
        TestRow("D", 3L),
        TestRow("E", 4L),
        TestRow("F", 10L))

      checkTable(
        path = tablePath,
        expectedAnswer,
        readCols = Seq("value"),
        metadataCols = Seq(ROW_ID))
    }
  }

  test("Only read row tracking columns from delta-spark table") {
    withTempDirAndEngine { (tablePath, _) =>
      createRowTrackingTableWithSpark(tablePath)

      val expectedAnswer = Seq(
        TestRow(1L, 0L),
        TestRow(1L, 1L),
        TestRow(2L, 2L),
        TestRow(1L, 3L),
        TestRow(1L, 4L),
        TestRow(2L, 10L))

      // This test also checks a different ordering of the metadata columns
      checkTable(
        path = tablePath,
        expectedAnswer,
        readCols = Seq(),
        metadataCols =
          Seq(ROW_COMMIT_VERSION, ROW_ID))
    }
  }

  test("Metadata columns are not read by default from delta-spark table") {
    withTempDirAndEngine { (tablePath, _) =>
      createRowTrackingTableWithSpark(tablePath)

      val expectedAnswer = Seq(
        TestRow(1, "A"),
        TestRow(2, "B"),
        TestRow(3, "C_updated"),
        TestRow(4, "D"),
        TestRow(5, "E"),
        TestRow(6, "F"))

      checkTable(
        path = tablePath,
        expectedAnswer,
        expectedSchema = new StructType()
          .add(new StructField("id", IntegerType.INTEGER, true))
          .add(new StructField("value", StringType.STRING, true)))
    }
  }

  /* -------- Conflict resolution tests -------- */
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
    val schema = new StructType().add("id", LongType.LONG)

    // Create a row-tracking-supported table and bump the row ID high watermark to the initial value
    createTableWithRowTracking(engine, tablePath, schema)
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
    val txn1 = getUpdateTxn(engine, tablePath)

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
      getUpdateTxn(engine, tablePath).commit(engine, emptyIterable())
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
      getUpdateTxn(engine, tablePath).commit(engine, emptyIterable())
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

  test("Conflict resolution - " +
    "conflict resolution is not supported when providedRowIdHighWatermark is set") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTracking(engine, tablePath)

      // Create txn1 but don't commit it yet
      val rowTrackingMetadataDomainTxn1 = new RowTrackingMetadataDomain(400)
      val txn1 = createTxnWithDomainMetadatas(
        engine,
        tablePath,
        List(rowTrackingMetadataDomainTxn1.toDomainMetadata))

      // Create and commit txn2
      val dataBatch2 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val commitVersion2 = appendData(
        engine,
        tablePath,
        data = Seq(dataBatch2).map(Map.empty[String, Literal] -> _)).getVersion
      verifyBaseRowIDs(engine, tablePath, Seq(0L))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq(commitVersion2))
      verifyHighWatermark(engine, tablePath, 99)

      // Commit txn1 with a provided row ID high watermark would fail
      intercept[MaxCommitRetryLimitReachedException] {
        txn1.commit(engine, emptyIterable())
      }
    }
  }

  private val ROW_TRACKING_ENABLED_PROP = Map(TableConfig.ROW_TRACKING_ENABLED.getKey -> "true")
  private val ROW_TRACKING_DISABLED_PROP = Map(TableConfig.ROW_TRACKING_ENABLED.getKey -> "false")

  test("row tracking can be enabled/disabled on new table") {
    withTempDirAndEngine { (tablePath, engine) =>
      getCreateTxn(
        engine,
        tablePath,
        schema = testSchema,
        tableProperties = ROW_TRACKING_ENABLED_PROP).commit(engine, emptyIterable())
      val snapshot =
        getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).asInstanceOf[SnapshotImpl]
      assertMetadataProp(snapshot, TableConfig.ROW_TRACKING_ENABLED, true)
    }

    withTempDirAndEngine { (tablePath, engine) =>
      getCreateTxn(
        engine,
        tablePath,
        schema = testSchema,
        tableProperties = ROW_TRACKING_DISABLED_PROP).commit(engine, emptyIterable())
      val snapshot =
        getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).asInstanceOf[SnapshotImpl]
      assertMetadataProp(snapshot, TableConfig.ROW_TRACKING_ENABLED, false)
    }
  }

  test("row tracking cannot be enabled on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a new table with row tracking disabled (it is disabled by default)
      getCreateTxn(engine, tablePath, testSchema, tableProperties = Map.empty)
        .commit(engine, emptyIterable())

      // Fail if try to enable row tracking on an existing table
      val e = intercept[KernelException] {
        getUpdateTxn(engine, tablePath, tableProperties = ROW_TRACKING_ENABLED_PROP)
          .commit(engine, emptyIterable())
      }
      assert(
        e.getMessage.contains("Row tracking support cannot be changed once the table is created"))

      // It's okay to continue setting it disabled on an existing table; it will be a no-op
      getUpdateTxn(engine, tablePath, tableProperties = ROW_TRACKING_DISABLED_PROP)
        .commit(engine, emptyIterable())
    }
  }

  test("row tracking cannot be disabled on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a new table with row tracking enabled
      createTableWithRowTracking(engine, tablePath)

      // Fail if try to disable row tracking on an existing table
      val e = intercept[KernelException] {
        getUpdateTxn(engine, tablePath, tableProperties = ROW_TRACKING_DISABLED_PROP)
          .commit(engine, emptyIterable())
      }
      assert(
        e.getMessage.contains("Row tracking support cannot be changed once the table is created"))

      // It's okay to continue setting it enabled on an existing table; it will be a no-op
      getUpdateTxn(engine, tablePath, tableProperties = ROW_TRACKING_ENABLED_PROP)
        .commit(engine, emptyIterable())
    }
  }

  test("materialized row tracking column names are assigned when the feature is enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTracking(engine, tablePath)
      val config = getMetadata(engine, tablePath).getConfiguration

      Seq(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION).foreach { rowTrackingColumn =>
        assert(config.containsKey(rowTrackingColumn.getMaterializedColumnNameProperty))
        assert(
          config
            .get(rowTrackingColumn.getMaterializedColumnNameProperty)
            .startsWith(rowTrackingColumn.getMaterializedColumnNamePrefix))
      }
    }
  }

  Seq("none", "name", "id").foreach(mode => {
    test(
      s"throw if materialized row tracking column name conflicts with schema, " +
        s"with column mapping = $mode") {
      withTempDirAndEngine {
        (tablePath, engine) =>
          // Create a new table with row tracking and specified column mapping mode
          val columnMappingProp = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> mode)
          createTableWithRowTracking(engine, tablePath, extraProps = columnMappingProp)
          val config = getMetadata(engine, tablePath).getConfiguration

          Seq(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION).foreach {
            rowTrackingColumn =>
              val colName =
                config.get(rowTrackingColumn.getMaterializedColumnNameProperty)

              val newSchema = testSchema.add(colName, LongType.LONG)
              val e = intercept[KernelException] {
                updateTableMetadata(engine, tablePath, schema = newSchema)
              }

              if (mode == "none") {
                assert(
                  e.getMessage
                    .contains(s"Cannot update schema for table when column mapping is disabled"))
              } else {
                assert(
                  e.getMessage.contains(
                    s"Cannot use column name '$colName' because it is reserved for internal use"))
              }
          }
      }
    }
  })

  test("manually setting materialized row tracking column names is not allowed - new table") {
    withTempDirAndEngine { (tablePath, engine) =>
      Seq(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION).foreach { rowTrackingColumn =>
        val propName = rowTrackingColumn.getMaterializedColumnNameProperty
        val customTableProps = Map(propName -> "custom_name")
        val e = intercept[KernelException] {
          createTableWithRowTracking(engine, tablePath, extraProps = customTableProps)
        }
        assert(e.getMessage.contains(
          s"The Delta table property '$propName' is an internal property and cannot be updated"))
      }
    }
  }

  test("manually setting materialized row tracking column names is not allowed - existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithRowTracking(engine, tablePath)

      Seq(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION).foreach { rowTrackingColumn =>
        val propName = rowTrackingColumn.getMaterializedColumnNameProperty
        val customTableProps = Map(propName -> "custom_name")
        val e = intercept[KernelException] {
          getUpdateTxn(engine, tablePath, tableProperties = customTableProps)
            .commit(engine, emptyIterable())
        }
        assert(e.getMessage.contains(
          s"The Delta table property '$propName' is an internal property and cannot be updated"))
      }
    }
  }

  test("throw if materialized row tracking column configs are missing on an existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create a normal table with row tracking enabled first
      createTableWithRowTracking(engine, tablePath)

      // Get the current metadata and manually remove row tracking materialized column configs
      val originalMetadata = getMetadata(engine, tablePath)
      val configWithoutMaterializedCols = originalMetadata.getConfiguration.asScala.toMap
        .filterNot {
          case (key, _) =>
            key == MATERIALIZED_ROW_ID.getMaterializedColumnNameProperty ||
            key == MATERIALIZED_ROW_COMMIT_VERSION.getMaterializedColumnNameProperty
        }

      // Create new metadata with row tracking enabled but configs missing
      val newMetadata =
        originalMetadata.withReplacedConfiguration(configWithoutMaterializedCols.asJava)

      // Manually commit this problematic metadata
      val txn = getUpdateTxn(engine, tablePath)
      val metadataAction = SingleAction.createMetadataSingleAction(newMetadata.toRow)
      commitTransaction(
        txn,
        engine,
        inMemoryIterable(toCloseableIterator(Seq(metadataAction).asJava.iterator())))

      // Verify that row tracking is enabled but configs are missing
      val metadata = getMetadata(engine, tablePath)
      assert(TableConfig.ROW_TRACKING_ENABLED.fromMetadata(metadata) == true)
      assert(!metadata.getConfiguration.containsKey(
        MATERIALIZED_ROW_ID.getMaterializedColumnNameProperty))
      assert(
        !metadata.getConfiguration
          .containsKey(MATERIALIZED_ROW_COMMIT_VERSION.getMaterializedColumnNameProperty))

      // Now try to perform an append operation on this existing table with missing configs
      // This should trigger the validation and throw the expected exception
      val e = intercept[InvalidTableException] {
        val dataBatch = generateData(testSchema, Seq.empty, Map.empty, 10, 1)
        appendData(engine, tablePath, data = prepareDataForCommit(dataBatch))
      }

      assert(
        e.getMessage.contains(
          s"Row tracking is enabled but the materialized column name " +
            s"`${MATERIALIZED_ROW_ID.getMaterializedColumnNameProperty}` is missing."))
    }
  }

  /* -------- Test row tracking with replace table -------- */
  val someData = Seq(Map.empty[String, Literal] -> dataBatches1)
  val otherData = Seq(Map.empty[String, Literal] -> dataBatches2)
  // Each tuple represents: (enableBefore, enableAfter, initialData, replaceData)
  val replaceTableTestCases = Seq(
    // Row tracking turned on
    (false, true, Seq(), Seq()),
    (false, true, Seq(), someData),
    (false, true, someData, Seq()),
    (false, true, someData, otherData),
    // Row tracking turned off
    (true, false, someData, otherData),
    (true, false, someData, Seq()),
    // Row tracking remains unchanged
    (true, true, someData, Seq()),
    (true, true, Seq(), someData),
    (true, true, someData, otherData),
    (true, true, Seq(), Seq()))

  for ((enableBefore, enableAfter, initialData, replaceData) <- replaceTableTestCases) {
    val testName = s"""Replace table with row tracking:
                      |enableBefore=$enableBefore,
                      |enableAfter=$enableAfter,
                      |initialData=${initialData.nonEmpty},
                      |replaceData=${replaceData.nonEmpty}"""
      .stripMargin
      .replace("\n", " ")

    test(testName) {
      withTempDirAndEngine { (tablePath, engine) =>
        // Create an empty table with row tracking enabled or disabled
        createEmptyTable(
          engine,
          tablePath,
          testSchema,
          tableProperties = Map(TableConfig.ROW_TRACKING_ENABLED.getKey -> enableBefore.toString))

        // Optionally fill the table with initial data if provided
        if (initialData.nonEmpty) {
          appendData(engine, tablePath, data = initialData)
        }

        val beforeSnapshot =
          getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).asInstanceOf[SnapshotImpl]

        // Create a REPLACE transaction and commit
        val replaceTableProps =
          Map(TableConfig.ROW_TRACKING_ENABLED.getKey -> enableAfter.toString)
        val txn = getReplaceTxn(
          engine,
          tablePath,
          testSchema,
          tableProperties = replaceTableProps)

        commitTransaction(txn, engine, getAppendActions(txn, replaceData))

        // Get the latest snapshot of the table after the replace operation
        val afterSnapshot =
          getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath).asInstanceOf[SnapshotImpl]

        // Assert that row tracking is enabled/disabled as expected
        assertMetadataProp(afterSnapshot, TableConfig.ROW_TRACKING_ENABLED, enableAfter)

        // Assert that the high watermark is preserved or incremented based on the operations
        // This only applies if row tracking is enabled before and after the replace operation
        // and if there is initial data present
        if (enableBefore && enableAfter && initialData.nonEmpty) {
          val beforeHighWaterMark: Optional[Long] =
            RowTrackingMetadataDomain.fromSnapshot(beforeSnapshot).map(_.getRowIdHighWaterMark)
          val afterHighWaterMark: Optional[Long] =
            RowTrackingMetadataDomain.fromSnapshot(afterSnapshot).map(_.getRowIdHighWaterMark)
          val numInitialRows = initialData.head._2.map(_.getData.getSize).sum

          assert(beforeHighWaterMark.get() == numInitialRows - 1)
          if (replaceData.nonEmpty) {
            // If replace data is provided, the high watermark should be incremented
            val numReplaceRows = replaceData.head._2.map(_.getData.getSize).sum
            assert(afterHighWaterMark.get() == numInitialRows + numReplaceRows - 1)
          } else {
            // If no replace data, the high watermark should remain the same
            assert(beforeHighWaterMark.get() == afterHighWaterMark.get())
          }
        }

        // Assert that metadata configurations are different before and after
        // Since REPLACE assigns new materialized column names, the configs should never match
        val beforeConfig = beforeSnapshot.getMetadata.getConfiguration
        val afterConfig = afterSnapshot.getMetadata.getConfiguration
        assert(!beforeConfig.equals(afterConfig))

        // Assert that materialized row tracking columns are present when row tracking is enabled
        if (enableAfter) {
          Seq(MATERIALIZED_ROW_ID, MATERIALIZED_ROW_COMMIT_VERSION).foreach { rowTrackingColumn =>
            assert(afterConfig.containsKey(rowTrackingColumn.getMaterializedColumnNameProperty))
            assert(
              afterConfig
                .get(rowTrackingColumn.getMaterializedColumnNameProperty)
                .startsWith(rowTrackingColumn.getMaterializedColumnNamePrefix))
          }
        }

        // Check that AddFile actions in the new table have a base row ID and default commit version
        if (replaceData.nonEmpty) {
          // Base row IDs do not start from 0 if we had initial data and row tracking was enabled
          val baseRowIds = if (initialData.nonEmpty && enableBefore) {
            initialData.head._2.map(_.getData.getSize).sum
          } else {
            0L
          }
          // There was one more previous commit if initialData was present
          val defaultCommitVersion = if (initialData.nonEmpty) 2 else 1

          verifyBaseRowIDs(engine, tablePath, Seq(baseRowIds))
          verifyDefaultRowCommitVersion(engine, tablePath, Seq(defaultCommitVersion))
        }
      }
    }
  }
}
