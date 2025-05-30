/*
 * Copyright (2025) The Delta Lake Project Authors.
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

package io.delta.kernel.defaults.utils

import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import io.delta.kernel.{Scan, Snapshot}
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.test.AbstractTestResolvedTable
import io.delta.kernel.defaults.test.TestAdapterImplicits._
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Predicate
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl}
import io.delta.kernel.internal.data.ScanStateRow
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterator

trait AbstractReadUtils { self: AbstractTestUtils =>

  /**
   * Compares the rows in the tables latest snapshot with the expected answer and fails if they
   * do not match. The comparison is order independent. If expectedSchema is provided, checks
   * that the latest snapshot's schema is equivalent.
   *
   * @param path fully qualified path of the table to check
   * @param expectedAnswer expected rows
   * @param readCols subset of columns to read; if null then all columns will be read
   * @param engine engine to use to read the table
   * @param expectedSchema expected schema to check for; if null then no check is performed
   * @param filter Filter to select a subset of rows form the table
   * @param expectedRemainingFilter Remaining predicate out of the `filter` that is not enforced
   *                                by Kernel.
   * @param expectedVersion expected version of the latest snapshot for the table
   */
  def checkTable(
      path: String,
      expectedAnswer: Seq[TestRow],
      readCols: Seq[String] = null,
      engine: Engine = defaultEngine,
      expectedSchema: StructType = null,
      filter: Predicate = null,
      version: Option[Long] = None,
      timestamp: Option[Long] = None,
      expectedRemainingFilter: Predicate = null,
      expectedVersion: Option[Long] = None): Unit = {
    assert(version.isEmpty || timestamp.isEmpty, "Cannot provide both a version and timestamp")

    val testResolvedTable = if (version.isDefined) {
      getTestTableManager.getTestResolvedTableAtVersion(engine, path, version.get)
    } else if (timestamp.isDefined) {
      getTestTableManager.getTestResolvedTableAtTimestamp(engine, path, timestamp.get)
    } else {
      getTestTableManager.getTestResolvedTableAtLatest(engine, path)
    }

    val readSchema = if (readCols == null) {
      null
    } else {
      val schema = testResolvedTable.getSchema()
      new StructType(readCols.map(schema.get(_)).asJava)
    }

    if (expectedSchema != null) {
      assert(
        expectedSchema == testResolvedTable.getSchema(),
        s"""
           |Expected schema does not match actual schema:
           |Expected schema: $expectedSchema
           |Actual schema: ${testResolvedTable.getSchema()}
           |""".stripMargin)
    }

    val actualVersion = testResolvedTable.getVersion()

    expectedVersion.foreach { version =>
      assert(
        version == actualVersion,
        s"Expected version $version does not match actual version $actualVersion}")
    }

    val result =
      readTestResolvedTable(testResolvedTable, readSchema, filter, expectedRemainingFilter, engine)
    checkAnswer(result, expectedAnswer)
  }

  def readSnapshot(
      snapshot: Snapshot,
      readSchema: StructType = null,
      filter: Predicate = null,
      expectedRemainingFilter: Predicate = null,
      engine: Engine = defaultEngine): Seq[Row] = {
    readTestResolvedTable(
      snapshot.toTestAdapter,
      readSchema,
      filter,
      expectedRemainingFilter,
      engine)
  }

  def readTestResolvedTable(
      testResolvedTable: AbstractTestResolvedTable,
      readSchema: StructType = null,
      filter: Predicate = null,
      expectedRemainingFilter: Predicate = null,
      engine: Engine = defaultEngine): Seq[Row] = {

    val result = ArrayBuffer[Row]()

    var scanBuilder = testResolvedTable.getScanBuilder()

    if (readSchema != null) {
      scanBuilder = scanBuilder.withReadSchema(readSchema)
    }

    if (filter != null) {
      scanBuilder = scanBuilder.withFilter(filter)
    }

    val scan = scanBuilder.build()

    if (filter != null) {
      val actRemainingPredicate = scan.getRemainingFilter()
      assert(
        actRemainingPredicate.toString === Optional.ofNullable(expectedRemainingFilter).toString)
    }

    val scanState = scan.getScanState(engine);
    val fileIter = scan.getScanFiles(engine)

    val physicalDataReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState)
    fileIter.forEach { fileColumnarBatch =>
      fileColumnarBatch.getRows().forEach { scanFileRow =>
        val fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow)
        val physicalDataIter = engine.getParquetHandler().readParquetFiles(
          singletonCloseableIterator(fileStatus),
          physicalDataReadSchema,
          Optional.empty())
        var dataBatches: CloseableIterator[FilteredColumnarBatch] = null
        try {
          dataBatches = Scan.transformPhysicalData(
            engine,
            scanState,
            scanFileRow,
            physicalDataIter)

          dataBatches.forEach { batch =>
            val selectionVector = batch.getSelectionVector()
            val data = batch.getData()

            var i = 0
            val rowIter = data.getRows()
            try {
              while (rowIter.hasNext) {
                val row = rowIter.next()
                if (!selectionVector.isPresent || selectionVector.get.getBoolean(i)) {
                  // row is valid
                  result.append(row)
                }
                i += 1
              }
            } finally {
              rowIter.close()
            }
          }
        } finally {
          dataBatches.close()
        }
      }
    }
    result
  }
}
