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
package io.delta.kernel.defaults

import java.util.{Collections, Optional}
import java.util.Collections.emptyMap

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.Table
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.{TableConfig, TableImpl}
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.actions.{AddFile, GenerateIcebergCompatActionUtils, RemoveFile}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.DataFileStatus

import org.apache.spark.sql.delta.DeltaLog

class CommitIcebergActionSuite extends DeltaTableWriteSuiteBase {

  private val tblPropertiesIcebergWriterCompatV1Enabled = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true")

  /* ----- Error cases ----- */

  test("requires that maxRetries = 0") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = createWriteTxnBuilder(Table.forPath(engine, tablePath))
        .withSchema(engine, testSchema)
        .withTableProperties(engine, tblPropertiesIcebergWriterCompatV1Enabled.asJava)
        .build(engine)
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txn.getTransactionState(engine),
          generateDataFileStatus(tablePath, "file1.parquet"),
          Collections.emptyMap(),
          true /* dataChange */
        )
      }
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          txn.getTransactionState(engine),
          generateDataFileStatus(tablePath, "file1.parquet"),
          Collections.emptyMap(),
          true /* dataChange */
        )
      }
    }
  }

  test("requires that icebergWriterCompatV1 enabled") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = createWriteTxnBuilder(Table.forPath(engine, tablePath))
        .withSchema(engine, testSchema)
        .withMaxRetries(0)
        .build(engine)
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txn.getTransactionState(engine),
          generateDataFileStatus(tablePath, "file1.parquet"),
          Collections.emptyMap(),
          true /* dataChange */
        )
      }
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          txn.getTransactionState(engine),
          generateDataFileStatus(tablePath, "file1.parquet"),
          Collections.emptyMap(),
          true /* dataChange */
        )
      }
    }
  }

  test("partitioned tables unsupported") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = createWriteTxnBuilder(Table.forPath(engine, tablePath))
        .withSchema(engine, testPartitionSchema)
        .withTableProperties(engine, tblPropertiesIcebergWriterCompatV1Enabled.asJava)
        .withMaxRetries(0)
        .withPartitionColumns(engine, testPartitionColumns.asJava)
        .build(engine)
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txn.getTransactionState(engine),
          generateDataFileStatus(tablePath, "file1.parquet"),
          Collections.emptyMap(),
          true /* dataChange */
        )
      }
      intercept[UnsupportedOperationException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          txn.getTransactionState(engine),
          generateDataFileStatus(tablePath, "file1.parquet"),
          Collections.emptyMap(),
          true /* dataChange */
        )
      }
    }
  }

  test("cannot create add without stats present") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = createWriteTxnBuilder(Table.forPath(engine, tablePath))
        .withSchema(engine, testSchema)
        .withTableProperties(engine, tblPropertiesIcebergWriterCompatV1Enabled.asJava)
        .withMaxRetries(0)
        .build(engine)
      intercept[KernelException] {
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txn.getTransactionState(engine),
          generateDataFileStatus(tablePath, "file1.parquet", includeStats = false),
          Collections.emptyMap(),
          true /* dataChange */
        )
      }
    }
  }

  /* ----- E2E commit tests + read back with Spark ----- */
  // Note - since we don't fully support column mapping writes (i.e. transformLogicalData doesn't
  // support id mode) we cannot really test these APIs with actual tables with data since
  // we cannot write column-mapping-id-based data
  // For now - we write the actions and check that they are correct in Spark
  // After we have full column mapping support we will add E2E tests with data that can be read

  trait ExpectedFileAction

  case class ExpectedAdd(path: String, size: Long, modificationTime: Long, dataChange: Boolean)
      extends ExpectedFileAction

  case class ExpectedRemove(path: String, size: Long, deletionTimestamp: Long, dataChange: Boolean)
      extends ExpectedFileAction

  private def checkActionsWrittenInJson(
      engine: Engine,
      tablePath: String,
      version: Long,
      expectedFileActions: Set[ExpectedFileAction]): Unit = {
    val rows = Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
      .getChanges(engine, version, version, Set(DeltaAction.ADD, DeltaAction.REMOVE).asJava)
      .toSeq
      .flatMap(_.getRows.toSeq)
    val fileActions = rows.flatMap { row =>
      if (!row.isNullAt(row.getSchema.indexOf("add"))) {
        val addFile = new AddFile(row.getStruct(row.getSchema.indexOf("add")))
        assert(addFile.getPartitionValues.getSize == 0)
        assert(!addFile.getTags.isPresent)
        assert(!addFile.getBaseRowId.isPresent)
        assert(!addFile.getDefaultRowCommitVersion.isPresent)
        assert(!addFile.getDeletionVector.isPresent)
        assert(addFile.getStats.isPresent)
        Some(ExpectedAdd(
          addFile.getPath,
          addFile.getSize,
          addFile.getModificationTime,
          addFile.getDataChange))
      } else if (!row.isNullAt(row.getSchema.indexOf("remove"))) {
        val removeFile = new RemoveFile(row.getStruct(row.getSchema.indexOf("remove")))
        assert(removeFile.getDeletionTimestamp.isPresent)
        assert(removeFile.getExtendedFileMetadata.toScala.contains(true))
        assert(removeFile.getPartitionValues.toScala.exists(_.getSize == 0))
        assert(removeFile.getSize.isPresent)
        assert(removeFile.getStats.isPresent)
        assert(!removeFile.getTags.isPresent)
        assert(!removeFile.getDeletionVector.isPresent)
        assert(!removeFile.getBaseRowId.isPresent)
        assert(!removeFile.getDefaultRowCommitVersion.isPresent)
        Some(ExpectedRemove(
          removeFile.getPath,
          removeFile.getSize.get,
          removeFile.getDeletionTimestamp.get,
          removeFile.getDataChange))
      } else {
        None
      }
    }
    assert(fileActions.size == expectedFileActions.size)
    assert(fileActions.toSet == expectedFileActions)
  }

  private def checkSparkLogReplay(
      tablePath: String,
      version: Long,
      expectedAdds: Set[ExpectedAdd]): Unit = {
    val snapshot = DeltaLog.forTable(spark, tablePath).getSnapshotAt(version)
    assert(snapshot.allFiles.count() == expectedAdds.size)
    val addsFoundSet = snapshot.allFiles.collect().map { add =>
      assert(add.partitionValues.isEmpty)
      assert(!add.dataChange)
      assert(add.baseRowId.isEmpty)
      assert(add.defaultRowCommitVersion.isEmpty)
      assert(add.deletionVector == null)
      assert(add.stats != null)
      assert(add.clusteringProvider.isEmpty)
      ExpectedAdd(
        add.path,
        add.size,
        add.modificationTime,
        // Always false because Delta Spark copies add with dataChange=false during log replay
        add.dataChange)
    }.toSet
    // We must "hack" all the expectedAdds to have dataChange=false since Delta Sparkd does this
    // in log replay
    assert(addsFoundSet == expectedAdds.map(_.copy(dataChange = false)))
  }

  test("Correctly commits adds to table and compat with Spark") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)

      // Append 1 add with dataChange = true
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet"),
            Collections.emptyMap(),
            true /* dataChange */ ))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
      }

      // Append 1 add with dataChange = false (in theory this could involve updating stats but
      // once we support remove add a case that looks like optimize/compaction)
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet"),
            Collections.emptyMap(),
            false /* dataChange */ ))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
      }

      // Verify we wrote the adds we expected into the JSON files using Kernel's getChanges
      checkActionsWrittenInJson(engine, tablePath, 0, Set())
      checkActionsWrittenInJson(
        engine,
        tablePath,
        1,
        Set(ExpectedAdd("file1.parquet", 1000, 10, true)))
      checkActionsWrittenInJson(
        engine,
        tablePath,
        2,
        Set(ExpectedAdd("file1.parquet", 1000, 10, false)))

      // Verify that Spark can read the actions written via log replay
      checkSparkLogReplay(tablePath, 0, Set())
      checkSparkLogReplay(tablePath, 1, Set(ExpectedAdd("file1.parquet", 1000, 10, true)))
      // We added the same path twice so only the second remains after log replay
      checkSparkLogReplay(tablePath, 2, Set(ExpectedAdd("file1.parquet", 1000, 10, false)))
    }
  }

  test("Correctly commits adds and removes to table and compat with Spark") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled)

      // Append 1 add with dataChange = true
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet"),
            Collections.emptyMap(),
            true /* dataChange */ ))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
      }

      // Re-arrange data by removing that Add and adding a new Add
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet"),
            Collections.emptyMap(),
            false /* dataChange */ ),
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file2.parquet"),
            Collections.emptyMap(),
            false /* dataChange */ ))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
      }

      // Remove that add so that the table is empty
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file2.parquet"),
            Collections.emptyMap(),
            true /* dataChange */ ))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
      }

      // Verify we wrote the adds we expected into the JSON files using Kernel's getChanges
      checkActionsWrittenInJson(engine, tablePath, 0, Set())
      checkActionsWrittenInJson(
        engine,
        tablePath,
        1,
        Set(ExpectedAdd("file1.parquet", 1000, 10, true)))
      checkActionsWrittenInJson(
        engine,
        tablePath,
        2,
        Set(
          ExpectedAdd("file2.parquet", 1000, 10, false),
          ExpectedRemove("file1.parquet", 1000, 10, false)))
      checkActionsWrittenInJson(
        engine,
        tablePath,
        3,
        Set(ExpectedRemove("file2.parquet", 1000, 10, true)))

      // Verify that Spark can read the actions written via log replay
      checkSparkLogReplay(tablePath, 0, Set())
      checkSparkLogReplay(tablePath, 1, Set(ExpectedAdd("file1.parquet", 1000, 10, true)))
      checkSparkLogReplay(tablePath, 2, Set(ExpectedAdd("file2.parquet", 1000, 10, false)))
      checkSparkLogReplay(tablePath, 3, Set())
    }
  }

  test("append-only configuration is observed when committing removes") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = tblPropertiesIcebergWriterCompatV1Enabled ++ Map(
          TableConfig.APPEND_ONLY_ENABLED.getKey -> "true"))

      // Append 1 add with dataChange = true
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet"),
            Collections.emptyMap(),
            true /* dataChange */ ))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
      }

      // Re-arrange data by removing that Add and adding a new Add
      // (can commit remove with dataChange=false)
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        val actionsToCommit = Seq(
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet"),
            Collections.emptyMap(),
            false /* dataChange */ ),
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file2.parquet"),
            Collections.emptyMap(),
            false /* dataChange */ ))
        commitTransaction(
          txn,
          engine,
          inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
      }

      // Cannot create remove with dataChange=true
      {
        val txn = createTxn(engine, tablePath, maxRetries = 0)
        intercept[KernelException] {
          GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
            txn.getTransactionState(engine),
            generateDataFileStatus(tablePath, "file1.parquet"),
            Collections.emptyMap(),
            true /* dataChange */
          )
        }
      }
    }
  }
}
