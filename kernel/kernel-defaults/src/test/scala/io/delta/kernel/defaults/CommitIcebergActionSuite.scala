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

import io.delta.kernel.{Table, Transaction}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.{TableConfig, TableImpl}
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.actions.{AddFile, DeletionVectorDescriptor, GenerateIcebergCompatActionUtils, RemoveFile}
import io.delta.kernel.internal.data.TransactionStateRow
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.DataFileStatus

import org.apache.spark.sql.delta.DeltaLog

import org.scalatest.funsuite.AnyFunSuite

class CommitIcebergActionSuite extends AnyFunSuite with WriteUtils {

  private val tblPropertiesIcebergWriterCompatV1Enabled = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V1_ENABLED.getKey -> "true")

  private val tblPropertiesIcebergWriterCompatV3Enabled = Map(
    TableConfig.ICEBERG_WRITER_COMPAT_V3_ENABLED.getKey -> "true")

  private def createIcebergCompatAction(
      actionType: String, // "ADD" or "REMOVE"
      version: String,
      txn: Transaction,
      engine: Engine,
      fileStatus: DataFileStatus,
      dataChange: Boolean,
      tags: Map[String, String] = Map.empty, // ignored for REMOVE
      deletionVector: Optional[DeletionVectorDescriptor] = Optional.empty() // ignored for V1
  ): Row = {

    (actionType, version) match {
      case ("ADD", "V1") =>
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1AddAction(
          txn.getTransactionState(engine),
          fileStatus,
          Collections.emptyMap(),
          dataChange,
          tags.asJava,
          Optional.of(TransactionStateRow.getPhysicalSchema(txn.getTransactionState(engine))))
      case ("ADD", "V3") =>
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV3AddAction(
          txn.getTransactionState(engine),
          fileStatus,
          Collections.emptyMap(),
          dataChange,
          tags.asJava,
          Optional.empty[java.lang.Long](),
          Optional.empty[java.lang.Long](),
          deletionVector,
          Optional.of(TransactionStateRow.getPhysicalSchema(txn.getTransactionState(engine))))
      case ("REMOVE", "V1") =>
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV1RemoveAction(
          txn.getTransactionState(engine),
          fileStatus,
          Collections.emptyMap(),
          dataChange,
          Optional.of(TransactionStateRow.getPhysicalSchema(txn.getTransactionState(engine))))
      case ("REMOVE", "V3") =>
        GenerateIcebergCompatActionUtils.generateIcebergCompatWriterV3RemoveAction(
          txn.getTransactionState(engine),
          fileStatus,
          Collections.emptyMap(),
          dataChange,
          Optional.empty[java.lang.Long](),
          Optional.empty[java.lang.Long](),
          deletionVector,
          Optional.of(TransactionStateRow.getPhysicalSchema(txn.getTransactionState(engine))))
      case _ => throw new IllegalArgumentException(
          s"Unsupported actionType: $actionType or version: $version")
    }
  }

  /* ----- Error cases ----- */

  Seq("V1", "V3").foreach { version =>
    test(s"$version: requires that maxRetries = 0") {
      withTempDirAndEngine { (tablePath, engine) =>
        val properties = if (version == "V1") tblPropertiesIcebergWriterCompatV1Enabled
        else tblPropertiesIcebergWriterCompatV3Enabled

        val txn = getCreateTxn(engine, tablePath, testSchema, tableProperties = properties)
        intercept[UnsupportedOperationException] {
          createIcebergCompatAction(
            "ADD",
            version,
            txn,
            engine,
            generateDataFileStatus(tablePath, "file1.parquet"),
            dataChange = true)
        }
        intercept[UnsupportedOperationException] {
          createIcebergCompatAction(
            "REMOVE",
            version,
            txn,
            engine,
            generateDataFileStatus(tablePath, "file1.parquet"),
            dataChange = true)
        }
      }
    }

    test(s"$version: requires that icebergWriterCompat${version} enabled") {
      withTempDirAndEngine { (tablePath, engine) =>
        val txn = getCreateTxn(engine, tablePath, testSchema, maxRetries = 0)
        intercept[UnsupportedOperationException] {
          createIcebergCompatAction(
            "ADD",
            version,
            txn,
            engine,
            generateDataFileStatus(tablePath, "file1.parquet"),
            dataChange = true)
        }
        intercept[UnsupportedOperationException] {
          createIcebergCompatAction(
            "REMOVE",
            version,
            txn,
            engine,
            generateDataFileStatus(tablePath, "file1.parquet"),
            dataChange = true)
        }
      }
    }

    test(s"$version: partitioned tables unsupported") {
      withTempDirAndEngine { (tablePath, engine) =>
        val properties = if (version == "V1") tblPropertiesIcebergWriterCompatV1Enabled
        else tblPropertiesIcebergWriterCompatV3Enabled

        val txn = getCreateTxn(
          engine,
          tablePath,
          testPartitionSchema,
          testPartitionColumns,
          properties,
          maxRetries = 0)
        intercept[UnsupportedOperationException] {
          createIcebergCompatAction(
            "ADD",
            version,
            txn,
            engine,
            generateDataFileStatus(tablePath, "file1.parquet"),
            dataChange = true)
        }
        intercept[UnsupportedOperationException] {
          createIcebergCompatAction(
            "REMOVE",
            version,
            txn,
            engine,
            generateDataFileStatus(tablePath, "file1.parquet"),
            dataChange = true)
        }
      }
    }

    test(s"$version: cannot create add without stats present") {
      withTempDirAndEngine { (tablePath, engine) =>
        val properties = if (version == "V1") tblPropertiesIcebergWriterCompatV1Enabled
        else tblPropertiesIcebergWriterCompatV3Enabled

        val txn =
          getCreateTxn(engine, tablePath, testSchema, tableProperties = properties, maxRetries = 0)
        intercept[KernelException] {
          createIcebergCompatAction(
            "ADD",
            version,
            txn,
            engine,
            generateDataFileStatus(tablePath, "file1.parquet", includeStats = false),
            dataChange = true)
        }
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
      expectedFileActions: Set[ExpectedFileAction],
      icebergCompatWriterVersion: String = "V1"): Unit = {
    val rows = Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
      .getChanges(engine, version, version, Set(DeltaAction.ADD, DeltaAction.REMOVE).asJava)
      .toSeq
      .flatMap(_.getRows.toSeq)
    val fileActions = rows.flatMap { row =>
      if (!row.isNullAt(row.getSchema.indexOf("add"))) {
        val addFile = new AddFile(row.getStruct(row.getSchema.indexOf("add")))
        assert(addFile.getPartitionValues.getSize == 0)
        assert(!addFile.getTags.isPresent)

        if (icebergCompatWriterVersion == "V1") {
          assert(!addFile.getBaseRowId.isPresent)
          assert(!addFile.getDefaultRowCommitVersion.isPresent)
        } else { // V3
          // In V3, baseRowId and defaultRowCommitVersion are required for row lineage
          assert(addFile.getBaseRowId.isPresent)
          assert(addFile.getDefaultRowCommitVersion.isPresent)
        }

        assert(!addFile.getDeletionVector.isPresent)
        assert(addFile.getStats(null).isPresent)
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
        assert(removeFile.getStats(null).isPresent)
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
      expectedAdds: Set[ExpectedAdd],
      icebergCompatWriterVersion: String = "V1"): Unit = {
    val snapshot = DeltaLog.forTable(spark, tablePath).getSnapshotAt(version)
    assert(snapshot.allFiles.count() == expectedAdds.size)
    val addsFoundSet = snapshot.allFiles.collect().map { add =>
      assert(add.partitionValues.isEmpty)
      assert(!add.dataChange)

      // Row lineage fields - different behavior for V1 vs V3
      if (icebergCompatWriterVersion == "V1") {
        assert(add.baseRowId.isEmpty)
        assert(add.defaultRowCommitVersion.isEmpty)
      } else { // V3
        assert(add.baseRowId.nonEmpty)
        assert(add.defaultRowCommitVersion.nonEmpty)
      }

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
    // We must "hack" all the expectedAdds to have dataChange=false since Delta Spark does this
    // in log replay
    assert(addsFoundSet == expectedAdds.map(_.copy(dataChange = false)))
  }

  Seq("V1", "V3").foreach { version =>
    test(s"$version: Correctly commits adds to table and compat with Spark") {
      withTempDirAndEngine { (tablePath, engine) =>
        val properties = if (version == "V1") tblPropertiesIcebergWriterCompatV1Enabled
        else tblPropertiesIcebergWriterCompatV3Enabled

        // Create table
        createEmptyTable(
          engine,
          tablePath,
          testSchema,
          tableProperties = properties)

        // Append 1 add with dataChange = true
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "ADD",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = true))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Append 1 add with dataChange = false (in theory this could involve updating stats but
        // once we support remove add a case that looks like optimize/compaction)
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "ADD",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = false))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Verify we wrote the adds we expected into the JSON files using Kernel's getChanges
        checkActionsWrittenInJson(engine, tablePath, 0, Set(), version)
        checkActionsWrittenInJson(
          engine,
          tablePath,
          1,
          Set(ExpectedAdd("file1.parquet", 1000, 10, true)),
          version)
        checkActionsWrittenInJson(
          engine,
          tablePath,
          2,
          Set(ExpectedAdd("file1.parquet", 1000, 10, false)),
          version)

        // Verify that Spark can read the actions written via log replay
        checkSparkLogReplay(tablePath, 0, Set(), version)
        checkSparkLogReplay(
          tablePath,
          1,
          Set(ExpectedAdd("file1.parquet", 1000, 10, true)),
          version)
        // We added the same path twice so only the second remains after log replay
        checkSparkLogReplay(
          tablePath,
          2,
          Set(ExpectedAdd("file1.parquet", 1000, 10, false)),
          version)
      }
    }

    test(s"$version: Correctly commits adds and removes to table and compat with Spark") {
      withTempDirAndEngine { (tablePath, engine) =>
        val properties = if (version == "V1") tblPropertiesIcebergWriterCompatV1Enabled
        else tblPropertiesIcebergWriterCompatV3Enabled

        // Create table
        createEmptyTable(
          engine,
          tablePath,
          testSchema,
          tableProperties = properties)

        // Append 1 add with dataChange = true
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "ADD",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = true))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Re-arrange data by removing that Add and adding a new Add
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "REMOVE",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = false),
            createIcebergCompatAction(
              "ADD",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file2.parquet"),
              dataChange = false))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Remove that add so that the table is empty
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "REMOVE",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file2.parquet"),
              dataChange = true))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Verify we wrote the adds we expected into the JSON files using Kernel's getChanges
        checkActionsWrittenInJson(engine, tablePath, 0, Set(), version)
        checkActionsWrittenInJson(
          engine,
          tablePath,
          1,
          Set(ExpectedAdd("file1.parquet", 1000, 10, true)),
          version)
        checkActionsWrittenInJson(
          engine,
          tablePath,
          2,
          Set(
            ExpectedAdd("file2.parquet", 1000, 10, false),
            ExpectedRemove("file1.parquet", 1000, 10, false)),
          version)
        checkActionsWrittenInJson(
          engine,
          tablePath,
          3,
          Set(ExpectedRemove("file2.parquet", 1000, 10, true)),
          version)

        // Verify that Spark can read the actions written via log replay
        checkSparkLogReplay(tablePath, 0, Set(), version)
        checkSparkLogReplay(
          tablePath,
          1,
          Set(ExpectedAdd("file1.parquet", 1000, 10, true)),
          version)
        checkSparkLogReplay(
          tablePath,
          2,
          Set(ExpectedAdd("file2.parquet", 1000, 10, false)),
          version)
        checkSparkLogReplay(tablePath, 3, Set(), version)
      }
    }

    test(s"$version: append-only configuration is observed when committing removes") {
      withTempDirAndEngine { (tablePath, engine) =>
        val properties = if (version == "V1") tblPropertiesIcebergWriterCompatV1Enabled
        else tblPropertiesIcebergWriterCompatV3Enabled

        // Create table
        createEmptyTable(
          engine,
          tablePath,
          testSchema,
          tableProperties = properties ++ Map(
            TableConfig.APPEND_ONLY_ENABLED.getKey -> "true"))

        // Append 1 add with dataChange = true
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "ADD",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = true))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Re-arrange data by removing that Add and adding a new Add
        // (can commit remove with dataChange=false)
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "REMOVE",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = false),
            createIcebergCompatAction(
              "ADD",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file2.parquet"),
              dataChange = true))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Cannot create remove with dataChange=true
        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          intercept[KernelException] {
            createIcebergCompatAction(
              "REMOVE",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = true)
          }
        }
      }
    }

    test(s"$version: Tags can be successfully passed for generating addFile") {
      withTempDirAndEngine { (tablePath, engine) =>
        val properties = if (version == "V1") tblPropertiesIcebergWriterCompatV1Enabled
        else tblPropertiesIcebergWriterCompatV3Enabled

        // Create table
        createEmptyTable(
          engine,
          tablePath,
          testSchema,
          tableProperties = properties)

        // Commit one add file with tags
        val tags = Map("tag1" -> "abc", "tag2" -> "def")

        {
          val txn = getUpdateTxn(engine, tablePath, maxRetries = 0)
          val actionsToCommit = Seq(
            createIcebergCompatAction(
              "ADD",
              version,
              txn,
              engine,
              generateDataFileStatus(tablePath, "file1.parquet"),
              dataChange = true,
              tags = tags))
          commitTransaction(
            txn,
            engine,
            inMemoryIterable(toCloseableIterator(actionsToCommit.asJava.iterator())))
        }

        // Read back committed ADD actions
        val tableVersion = 1
        val rows = Table.forPath(engine, tablePath).asInstanceOf[TableImpl]
          .getChanges(engine, tableVersion, tableVersion, Set(DeltaAction.ADD).asJava)
          .toSeq
          .flatMap(_.getRows.toSeq)
          .filterNot(row => row.isNullAt(row.getSchema.indexOf("add")))

        assert(rows.size == 1)

        val addFile = new AddFile(rows.head.getStruct(rows.head.getSchema.indexOf("add")))
        assert(addFile.getTags.isPresent)
        assert(VectorUtils.toJavaMap(addFile.getTags.get()).asScala.equals(tags))
      }
    }
  }
}
