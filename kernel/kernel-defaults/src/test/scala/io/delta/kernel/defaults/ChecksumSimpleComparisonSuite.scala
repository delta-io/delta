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

import io.delta.kernel.Operation.CREATE_TABLE
import io.delta.kernel.{Operation, Table, Transaction}
import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.internal.checksum.{CRCInfo, ChecksumReader}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.internal.util.Utils.{singletonCloseableIterator, toCloseableIterator}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus, FileStatus}
import org.scalatest.funsuite.AnyFunSuite

import java.io.File
import java.nio.file.Files
import java.util
import java.util.Collections.{emptyMap, singletonMap}
import java.util.Optional
import scala.collection.JavaConverters._

/**
 * Test suite to verify checksum file correctness by comparing
 * Delta Spark and Delta Kernel generated checksum files.
 */
class ChecksumSimpleComparisonSuite extends AnyFunSuite with TestUtils {

  private val PARTITION_COLUMN = "part"

  test("create table, insert data and verify checksum") {
    withTempDirAndEngine { (tablePath, engine) =>
      val sparkTablePath = tablePath + "spark"
      val kernelTablePath = tablePath + "kernel"
      Table
        .forPath(engine, kernelTablePath)
        .createTransactionBuilder(engine, "test-engine", CREATE_TABLE)
        .withSchema(engine, new StructType().add("id", INTEGER))
        .build(engine)
        .commit(engine, emptyIterable())
        .getPostCommitHooks
        .forEach(hook => hook.threadSafeInvoke(engine))
      spark.sql(s"CREATE OR REPLACE TABLE delta.`${sparkTablePath}` (id Integer) USING DELTA")
      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach(
        version => insertIntoUnpartitionedTableAndCheckCrc(
          engine, sparkTablePath, kernelTablePath, version)
      )

    }
  }

  test("create table as select and verify checksum") {
    withTempDirAndEngine { (tablePath, engine) =>
      val sparkTablePath = tablePath + "spark"
      val kernelTablePath = tablePath + "kernel"
      spark.sql(
        s"CREATE OR REPLACE TABLE delta.`${tablePath + "spark"}` USING DELTA AS SELECT 1 as id"
      )
      val txn = Table
        .forPath(engine, kernelTablePath)
        .createTransactionBuilder(engine, "test-engine", CREATE_TABLE)
        .withSchema(engine, new StructType().add("id", INTEGER))
        .build(engine)

      copyAndCommitTxnForUnpartitionedTable(txn, engine, sparkTablePath, versionAtCommit = 0)

      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach(
        version => insertIntoUnpartitionedTableAndCheckCrc(
          engine, sparkTablePath, kernelTablePath, version)
      )
    }
  }

  test("create partitioned table and verify checksum") {
    withTempDirAndEngine { (tablePath, engine) =>
      val sparkTablePath = tablePath + "spark"
      val kernelTablePath = tablePath + "kernel"
      Table
        .forPath(engine, kernelTablePath)
        .createTransactionBuilder(engine, "test-engine", CREATE_TABLE)
        .withSchema(engine, new StructType().add("id", INTEGER).add(PARTITION_COLUMN, INTEGER))
        .withPartitionColumns(engine, Seq(PARTITION_COLUMN).asJava)
        .build(engine)
        .commit(engine, emptyIterable())
        .getPostCommitHooks
        .forEach(hook => hook.threadSafeInvoke(engine))
      spark.sql(
        s"CREATE OR REPLACE TABLE delta.`${sparkTablePath}` " +
        s"(id Integer, part Integer) USING DELTA PARTITIONED BY (part)"
      )
      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach(
        version => insertIntoUnpartitionedTableAndCheckCrc(
          engine, sparkTablePath, kernelTablePath, version)
      )
    }
  }

  test("create partition table as select and verify checksum") {
    withTempDirAndEngine { (tablePath, engine) =>
      val sparkTablePath = tablePath + "spark"
      val kernelTablePath = tablePath + "kernel"
      spark.sql(
        s"CREATE OR REPLACE TABLE delta.`${sparkTablePath}` USING DELTA " +
        s"PARTITIONED BY (part) AS " +
        s"SELECT 1 as id, 1 as part union all select 2 as id, 2 as part"
      )
      val txn = Table
        .forPath(engine, kernelTablePath)
        .createTransactionBuilder(engine, "test-engine", CREATE_TABLE)
        .withSchema(engine, new StructType().add("id", INTEGER).add(PARTITION_COLUMN, INTEGER))
        .withPartitionColumns(engine, Seq(PARTITION_COLUMN).asJava)
        .build(engine)

        copyAndCommitTxnForPartitionedTable(
        txn,
        engine,
        sparkTablePath,
        Seq(1, 2).toSet,
        versionAtCommit = 0
      )
      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach(
        version => insertIntoPartitionedTableAndCheckCrc(
          engine, sparkTablePath, kernelTablePath, version)
      )
    }
  }

  private def assertChecksumEquals(
      engine: Engine,
      sparkTablePath: String,
      kernelTablePath: String,
      version: Long): Unit = {
    val sparkCrcPath = buildCrcPath(sparkTablePath, version)
    val kernelCrcPath = buildCrcPath(kernelTablePath, version)

    assert(
      Files.exists(sparkCrcPath) && Files.exists(kernelCrcPath),
      s"CRC files not found for version $version"
    )

    val sparkCrc = readCrcInfo(engine, sparkTablePath, version)
    val kernelCrc = readCrcInfo(engine, kernelTablePath, version)

    assertCrcInfoEquals(sparkCrc, kernelCrc)
  }

  private def readCrcInfo(engine: Engine, path: String, version: Long): CRCInfo = {
    ChecksumReader
      .getCRCInfo(engine, new Path(s"$path/_delta_log"), version, version)
      .orElseThrow(() => new IllegalStateException(s"CRC info not found for version $version"))
  }

  private def buildCrcPath(basePath: String, version: Long): java.nio.file.Path = {
    new File(f"$basePath/_delta_log/$version%020d.crc").toPath
  }

  private def assertCrcInfoEquals(crc1: CRCInfo, crc2: CRCInfo): Unit = {
    assert(crc1.getVersion === crc2.getVersion)
    assert(crc1.getNumFiles === crc2.getNumFiles)
    assert(crc1.getTableSizeBytes === crc2.getTableSizeBytes)
    assert(crc1.getMetadata.getSchema === crc2.getMetadata.getSchema)
    assert(crc1.getMetadata.getPartitionColNames === crc2.getMetadata.getPartitionColNames)
  }

  private def insertIntoPartitionedTableAndCheckCrc(
      engine: Engine,
      sparkTablePath: String,
      kernelTablePath: String,
      versionAtCommit: Long): Unit = {
    var valueToAppend = "(0, 0)"
    var addedPartition = Set(0)
    (0L to versionAtCommit).foreach(i => {
      val partitionValue = 2 * i
      addedPartition = addedPartition + partitionValue.toInt
      valueToAppend = valueToAppend + s",($i, $partitionValue)"
    })
    spark.sql(
      s"INSERT INTO delta.`$sparkTablePath` values $valueToAppend"
    )

    val txn = Table
      .forPath(engine, kernelTablePath)
      .createTransactionBuilder(engine, "test-engine", Operation.WRITE)
      .build(engine)

    copyAndCommitTxnForPartitionedTable(
      txn,
      engine,
      sparkTablePath,
      addedPartition,
      versionAtCommit
    )
    assertChecksumEquals(engine, sparkTablePath, kernelTablePath, versionAtCommit)
  }

  private def insertIntoUnpartitionedTableAndCheckCrc(
      engine: Engine,
      sparkTablePath: String,
      kernelTablePath: String,
      versionAtCommit: Long): Unit = {
    var valueToAppend = "(0)"
    (0L to versionAtCommit).foreach(i => valueToAppend = valueToAppend + s",($i)")
    spark.sql(
      s"INSERT INTO delta.`$sparkTablePath` values $valueToAppend"
    )

    val txn = Table
      .forPath(engine, kernelTablePath)
      .createTransactionBuilder(engine, "test-engine", Operation.WRITE)
      .build(engine)

    copyAndCommitTxnForUnpartitionedTable(txn, engine, sparkTablePath, versionAtCommit)
    assertChecksumEquals(engine, sparkTablePath, kernelTablePath, versionAtCommit)
  }

  private def copyAndCommitTxnForUnpartitionedTable(
      txn: Transaction,
      engine: Engine,
      sparkTablePath: String,
      versionAtCommit: Long): Unit = {
    val txnState = txn.getTransactionState(engine);

    val writeContext = Transaction
      .getWriteContext(engine, txnState, emptyMap())

    val dataActions = Transaction
      .generateAppendActions(
        engine,
        txnState,
        convertDeltaLogToAppendActions(engine, sparkTablePath, versionAtCommit, Option.empty),
        writeContext
      )

    txn
      .commit(engine, inMemoryIterable(dataActions))
      .getPostCommitHooks
      .forEach(hook => hook.threadSafeInvoke(engine))

  }

  private def copyAndCommitTxnForPartitionedTable(
      txn: Transaction,
      engine: Engine,
      sparkTablePath: String,
      addedPartition: Set[Int],
      versionAtCommit: Long): Unit = {
    val txnState = txn.getTransactionState(engine);

    val dataActions = new util.ArrayList[Row]()

    addedPartition.foreach({ partition =>
      val writeContext = Transaction
        .getWriteContext(
          engine,
          txnState,
          singletonMap(PARTITION_COLUMN, Literal.ofInt(partition))
        )

      Transaction
        .generateAppendActions(
          engine,
          txnState,
          convertDeltaLogToAppendActions(
            engine,
            sparkTablePath,
            versionAtCommit,
            Some(partition.toString)
          ),
          writeContext
        )
        .forEach(
          action => dataActions.add(action)
        )
    })

    txn
      .commit(engine, inMemoryIterable(toCloseableIterator(dataActions.iterator())))
      .getPostCommitHooks
      .forEach(
        hook => hook.threadSafeInvoke(engine)
      )

  }

  private def convertDeltaLogToAppendActions(
      engine: Engine,
      tablePath: String,
      version: Long,
      partition: Option[String]): CloseableIterator[DataFileStatus] = {

    val logPath = new Path(tablePath, "_delta_log")
    val deltaFile = FileStatus.of(FileNames.deltaFile(logPath, version), 0, 0)

    val addFiles = new util.ArrayList[DataFileStatus]()

    val columnarBatches = engine.getJsonHandler.readJsonFiles(
      singletonCloseableIterator(deltaFile),
      SingleAction.FULL_SCHEMA,
      Optional.empty())

    while (columnarBatches.hasNext) {
      processColumnarBatch(columnarBatches.next(), partition, addFiles)
    }

    toCloseableIterator(addFiles.iterator())
  }

  private def processColumnarBatch(
      batch: ColumnarBatch,
      partition: Option[String],
      addFiles: util.ArrayList[DataFileStatus]): Unit = {

    val rows = batch.getRows
    while (rows.hasNext) {
      val row = rows.next()
      val addIndex = row.getSchema.indexOf("add")

      if (!row.isNullAt(addIndex)) {
        processAddFile(row.getStruct(addIndex), partition, addFiles)
      }
    }
  }

  private def processAddFile(
      addFileRow: Row,
      partition: Option[String],
      addFiles: util.ArrayList[DataFileStatus]): Unit = {

    val addFile = new AddFile(addFileRow)

    if (shouldIncludeFile(addFile, partition)) {
      addFiles.add(
        new DataFileStatus(
          addFile.getPath,
          addFile.getSize,
          addFile.getModificationTime,
          Optional.empty() // TODO: populate stats once #4139 is fixed
        )
      )
    }
  }

  private def shouldIncludeFile(addFile: AddFile, partition: Option[String]): Boolean = {
    partition.isEmpty ||
    partition.get == VectorUtils.toJavaMap(addFile.getPartitionValues).get(PARTITION_COLUMN)
  }
}
