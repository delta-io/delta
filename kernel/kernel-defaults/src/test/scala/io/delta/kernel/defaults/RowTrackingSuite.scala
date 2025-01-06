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

import io.delta.kernel.data.Row
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableImpl}
import io.delta.kernel.internal.actions.{AddFile, Protocol, SingleAction}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.rowtracking.RowTrackingMetadataDomain
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.types.StructType
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import java.util
import java.util.{Collections, Optional}
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

class RowTrackingSuite extends DeltaTableWriteSuiteBase with ParquetSuiteBase {
  private def prepareActionsForCommit(actions: Row*): CloseableIterable[Row] = {
    inMemoryIterable(toCloseableIterator(actions.asJava.iterator()))
  }

  private def setWriterFeatureSupported(
      engine: Engine,
      tablePath: String,
      schema: StructType,
      writerFeatures: Seq[String]): Unit = {
    val protocol = new Protocol(
      3, // minReaderVersion
      7, // minWriterVersion
      Collections.emptyList(), // readerFeatures
      writerFeatures.asJava // writerFeatures
    )
    val protocolAction = SingleAction.createProtocolSingleAction(protocol.toRow)
    val txn = createTxn(engine, tablePath, isNewTable = false, schema, Seq.empty)
    txn.commit(engine, prepareActionsForCommit(protocolAction))
  }

  private def createTableWithRowTrackingSupported(
      engine: Engine,
      tablePath: String,
      schema: StructType = testSchema): Unit = {
    createTxn(engine, tablePath, isNewTable = true, schema, Seq.empty)
      .commit(engine, emptyIterable())
    setWriterFeatureSupported(engine, tablePath, schema, Seq("domainMetadata", "rowTracking"))
  }

  private def verifyBaseRowIDs(
      engine: Engine,
      tablePath: String,
      expectedValue: Seq[Long]): Unit = {
    val table = TableImpl.forPath(engine, tablePath)
    val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]

    val scanFileRows = collectScanFileRows(
      snapshot.getScanBuilder(engine).build()
    )
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

    val scanFileRows = collectScanFileRows(
      snapshot.getScanBuilder(engine).build()
    )
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
        data = Seq(dataBatch1, dataBatch2, dataBatch3).map(Map.empty[String, Literal] -> _)
      ).getVersion

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
        data = Seq(dataBatch1).map(Map.empty[String, Literal] -> _)
      ).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0))
      verifyDefaultRowCommitVersion(engine, tablePath, Seq(commitVersion1))
      verifyHighWatermark(engine, tablePath, 99)

      val dataBatch2 = generateData(testSchema, Seq.empty, Map.empty, 200, 1)
      val commitVersion2 = appendData(
        engine,
        tablePath,
        data = Seq(dataBatch2).map(Map.empty[String, Literal] -> _)
      ).getVersion

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
        data = Seq(dataBatch1).map(Map.empty[String, Literal] -> _)
      ).getVersion

      val commitVersion2 = appendData(
        engine,
        tablePath,
        data = Seq(dataBatch2).map(Map.empty[String, Literal] -> _)
      ).getVersion

      // Checkpoint the table
      val table = TableImpl.forPath(engine, tablePath)
      val latestVersion = table.getLatestSnapshot(engine).getVersion(engine)
      table.checkpoint(engine, latestVersion)

      val commitVersion3 = appendData(
        engine,
        tablePath,
        data = Seq(dataBatch3).map(Map.empty[String, Literal] -> _)
      ).getVersion

      verifyBaseRowIDs(engine, tablePath, Seq(0, 100, 300))
      verifyDefaultRowCommitVersion(
        engine,
        tablePath,
        Seq(commitVersion1, commitVersion2, commitVersion3)
      )
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
          "Cannot write to a rowTracking-supported table without 'numRecord' statistics. "
          + "Connectors are expected to populate the number of records statistics when "
          + "writing to a Delta table with 'rowTracking' table feature supported."
        )
      )
    }
  }

  test("Fail if row tracking is supported but domain metadata is not supported") {
    withTempDirAndEngine((tablePath, engine) => {
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      // Only 'rowTracking' is supported, not 'domainMetadata'
      setWriterFeatureSupported(engine, tablePath, testSchema, Seq("rowTracking"))

      val dataBatch1 = generateData(testSchema, Seq.empty, Map.empty, 100, 1)
      val e = intercept[KernelException] {
        appendData(
          engine,
          tablePath,
          data = Seq(dataBatch1).map(Map.empty[String, Literal] -> _)
        ).getVersion
      }

      assert(
        e.getMessage
          .contains(
            "Feature 'rowTracking' is supported and depends on feature 'domainMetadata',"
            + " but 'domainMetadata' is unsupported"
          )
      )
    })
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
          data = Seq(dataBatch1, dataBatch2, dataBatch3).map(Map.empty[String, Literal] -> _)
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
    // TODO: Implement this test. Creating and writing a table using Spark with row tracking also
    //  enables the 'invariants' feature, which is not yet supported by the Kernel.
  }
}
