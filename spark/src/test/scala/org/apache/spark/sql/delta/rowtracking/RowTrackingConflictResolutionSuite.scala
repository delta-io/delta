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

package org.apache.spark.sql.delta.rowtracking

import org.apache.spark.sql.delta.{DeltaLog, DeltaOperations, RowId, RowTrackingFeature}
import org.apache.spark.sql.delta.actions.{Action, AddFile}
import org.apache.spark.sql.delta.rowid.RowIdTestUtils

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class RowTrackingConflictResolutionSuite extends QueryTest
  with SharedSparkSession with RowIdTestUtils {

  private val testTableName = "test_table"

  private def deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
  private def latestSnapshot = deltaLog.update()

  private def withTestTable(testBlock: => Unit): Unit = {
    withTable(testTableName) {
      withRowTrackingEnabled(enabled = false) {
        // Table is initially empty.
        spark.range(end = 0).toDF().write.format("delta").saveAsTable(testTableName)

        testBlock
      }
    }
  }

  /** Create an AddFile action for testing purposes. */
  private def addFile(path: String): AddFile = {
    AddFile(
      path = path,
      partitionValues = Map.empty,
      size = 1337,
      modificationTime = 1,
      dataChange = true,
      stats = """{ "numRecords": 1 }"""
    )
  }

  /** Add Row tracking table feature support. */
  private def activateRowTracking(): Unit = {
    require(!latestSnapshot.protocol.isFeatureSupported(RowTrackingFeature))
    deltaLog.upgradeProtocol(Action.supportedProtocolVersion())
  }

  // Add 'numRecords' records to the table.
  private def commitRecords(numRecords: Int): Unit = {
    spark.range(numRecords).write.format("delta").mode("append").saveAsTable(testTableName)
  }

  test("Set baseRowId if table feature was committed concurrently") {
    withTestTable {
      val txn = deltaLog.startTransaction()
      activateRowTracking()
      txn.commit(Seq(addFile(path = "file_path")), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
    }
  }

  test("Set valid baseRowId if table feature and RowIdHighWaterMark are committed concurrently") {
    withTestTable {
      val filePath = "file_path"
      val numConcurrentRecords = 11

      val txn = deltaLog.startTransaction()
      activateRowTracking()
      commitRecords(numConcurrentRecords)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
      val committedAddFile = latestSnapshot.allFiles.collect().filter(_.path == filePath)
      assert(committedAddFile.size === 1)
      assert(committedAddFile.head.baseRowId === Some(numConcurrentRecords))
    }
  }

  test("Conflict resolution if table feature and initial AddFiles are in the same commit") {
    withTestTable {
      val filePath = "file_path"

      val txn = deltaLog.startTransaction()
      deltaLog.startTransaction().commit(
        Seq(Action.supportedProtocolVersion(), addFile("other_path")), DeltaOperations.ManualUpdate)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
      val committedAddFile = latestSnapshot.allFiles.collect().filter(_.path == filePath)
      assert(committedAddFile.size === 1)
      assert(committedAddFile.head.baseRowId === Some(1))
    }
  }

  test("Conflict resolution with concurrent INSERT") {
    withTestTable {
      val filePath = "file_path"
      val numInitialRecords = 7
      val numConcurrentRecords = 11

      activateRowTracking()
      commitRecords(numInitialRecords)
      val txn = deltaLog.startTransaction()
      commitRecords(numConcurrentRecords)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
      val committedAddFile = latestSnapshot.allFiles.collect().filter(_.path == filePath)
      assert(committedAddFile.size === 1)
      assert(committedAddFile.head.baseRowId === Some(numInitialRecords + numConcurrentRecords))
      val currentHighWaterMark = RowId.extractHighWatermark(latestSnapshot).get
      assert(currentHighWaterMark === numInitialRecords + numConcurrentRecords)
    }
  }

  test("Handle commits that do not bump the high water mark") {
    withTestTable {
      val filePath = "file_path"
      val numInitialRecords = 7
      activateRowTracking()
      commitRecords(numInitialRecords)

      val txn = deltaLog.startTransaction()
      val concurrentTxn = deltaLog.startTransaction()
      val updatedProtocol = latestSnapshot.protocol
      concurrentTxn.commit(Seq(updatedProtocol), DeltaOperations.ManualUpdate)
      txn.commit(Seq(addFile(filePath)), DeltaOperations.ManualUpdate)

      assertRowIdsAreValid(deltaLog)
    }
  }
}
