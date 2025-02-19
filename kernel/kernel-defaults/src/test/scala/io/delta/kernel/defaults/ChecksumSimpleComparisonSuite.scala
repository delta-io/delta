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

import io.delta.kernel.data.{ColumnarBatch, Row}
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.internal.checksum.{CRCInfo, ChecksumReader}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.Utils.{singletonCloseableIterator, toCloseableIterator}
import io.delta.kernel.internal.util.{FileNames, VectorUtils}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus, FileStatus}
import io.delta.kernel.{Operation, Table, Transaction}

import java.io.File
import java.nio.file.Files
import java.util
import java.util.Collections.{emptyMap, singletonMap}
import java.util.Optional
import scala.collection.immutable.Seq

/**
 * Test suite to verify checksum file correctness by comparing
 * Delta Spark and Delta Kernel generated checksum files.
 * This suite ensures that both implementations generate consistent checksums
 * for various table operations.
 */
class ChecksumSimpleComparisonSuite extends DeltaTableWriteSuiteBase with TestUtils {

  private val PARTITION_COLUMN = "part"

  test("create table, insert data and verify checksum") {
    withTempDirAndEngine { (tablePath, engine) =>
      val sparkTablePath = tablePath + "spark"
      val kernelTablePath = tablePath + "kernel"

      createTxn(
        engine,
        kernelTablePath,
        isNewTable = true,
        schema = new StructType().add("id", INTEGER),
        partCols = Seq.empty
      ).commit(engine, emptyIterable())
        .getPostCommitHooks
        .forEach(hook => hook.threadSafeInvoke(engine))
      spark.sql(s"CREATE OR REPLACE TABLE delta.`${sparkTablePath}` (id Integer) USING DELTA")
      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach(
        version =>
          insertIntoUnpartitionedTableAndCheckCrc(engine, sparkTablePath, kernelTablePath, version)
      )

    }
  }

  test("create partitioned table, insert and verify checksum") {
    withTempDirAndEngine { (tablePath, engine) =>
      val sparkTablePath = tablePath + "spark"
      val kernelTablePath = tablePath + "kernel"

      createTxn(
        engine,
        kernelTablePath,
        isNewTable = true,
        schema = new StructType().add("id", INTEGER).add(PARTITION_COLUMN, INTEGER),
        partCols = Seq(PARTITION_COLUMN)
      ).commit(engine, emptyIterable())
        .getPostCommitHooks
        .forEach(hook => hook.threadSafeInvoke(engine))
      spark.sql(
        s"CREATE OR REPLACE TABLE delta.`${sparkTablePath}` " +
        s"(id Integer, part Integer) USING DELTA PARTITIONED BY (part)"
      )
      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach(
        version =>
          insertIntoPartitionedTableAndCheckCrc(engine, sparkTablePath, kernelTablePath, version)
      )
    }
  }

  /**
   * Insert into unpartitioned spark table, read the added file from the commit log,
   * commit them to kernel table and verify the checksum files are consistent
   * between spark and kernel
   * */
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

    convertSparkDeltaLogToKernelCommit(txn, engine, sparkTablePath, versionAtCommit)
    assertChecksumEquals(engine, sparkTablePath, kernelTablePath, versionAtCommit)
  }

  /**
   * Insert into partitioned spark table, read the added files from the commit log,
   * commit them to kernel table and verify the checksum files are consistent
   * between spark and kernel
   * */
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

    convertSparkDeltaLogToKernelCommit(
      txn,
      engine,
      sparkTablePath,
      versionAtCommit,
      Some(addedPartition)
    )
    assertChecksumEquals(engine, sparkTablePath, kernelTablePath, versionAtCommit)
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

  // TODO: Add equals/hashCode to metadata, protocol then CRCInfo.
  private def assertCrcInfoEquals(crc1: CRCInfo, crc2: CRCInfo): Unit = {
    assert(crc1.getVersion === crc2.getVersion)
    assert(crc1.getNumFiles === crc2.getNumFiles)
    assert(crc1.getTableSizeBytes === crc2.getTableSizeBytes)
    assert(crc1.getMetadata.getSchema === crc2.getMetadata.getSchema)
    assert(crc1.getMetadata.getPartitionColNames === crc2.getMetadata.getPartitionColNames)
  }

  private def convertSparkDeltaLogToKernelCommit(
      txn: Transaction,
      engine: Engine,
      sparkTablePath: String,
      versionToConvert: Long,
      addedPartition: Option[(Set[Int])] = None): Unit = {

    val txnState = txn.getTransactionState(engine)

    val dataActionsIterator = addedPartition match {
      case None =>
        // Unpartitioned table case
        val writeContext = Transaction.getWriteContext(engine, txnState, emptyMap())
        Transaction.generateAppendActions(
          engine,
          txnState,
          convertSparkTableDeltaLogToKernelAppendActions(
            engine,
            sparkTablePath,
            versionToConvert,
            None
          ),
          writeContext
        )

      case Some(partitions) =>
        // Partitioned table case
        val actions = new util.ArrayList[Row]()
        partitions.foreach { partition =>
          val writeContext = Transaction.getWriteContext(
            engine,
            txnState,
            singletonMap(PARTITION_COLUMN, Literal.ofInt(partition))
          )

          Transaction
            .generateAppendActions(
              engine,
              txnState,
              convertSparkTableDeltaLogToKernelAppendActions(
                engine,
                sparkTablePath,
                versionToConvert,
                Some(partition.toString)
              ),
              writeContext
            )
            .forEach(action => actions.add(action))
        }
        actions.iterator()
    }

    txn
      .commit(engine, inMemoryIterable(toCloseableIterator(dataActionsIterator)))
      .getPostCommitHooks
      .forEach(_.threadSafeInvoke(engine))
  }

  private def convertSparkTableDeltaLogToKernelAppendActions(
      engine: Engine,
      sparkTablePath: String,
      version: Long,
      partition: Option[String]): CloseableIterator[DataFileStatus] = {

    val logPath = new Path(sparkTablePath, "_delta_log")
    val deltaFile = FileStatus.of(FileNames.deltaFile(logPath, version), 0, 0)

    val addFiles = new util.ArrayList[DataFileStatus]()

    val columnarBatches = engine.getJsonHandler.readJsonFiles(
      singletonCloseableIterator(deltaFile),
      SingleAction.FULL_SCHEMA,
      Optional.empty()
    )

    while (columnarBatches.hasNext) {
      collectAddFilesFromLogRows(columnarBatches.next(), partition, addFiles)
    }
    toCloseableIterator(addFiles.iterator())
  }

  private def collectAddFilesFromLogRows(
      logFileRows: ColumnarBatch,
      partition: Option[String],
      addFiles: util.ArrayList[DataFileStatus]): Unit = {
    val rows = logFileRows.getRows
    while (rows.hasNext) {
      val row = rows.next()
      val addIndex = row.getSchema.indexOf("add")

      if (!row.isNullAt(addIndex)) {
        val addFile = new AddFile(row.getStruct(addIndex))
        if (partition.isEmpty ||
          partition.get == VectorUtils
            .toJavaMap(addFile.getPartitionValues)
            .get(PARTITION_COLUMN)) {
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
    }
  }
}
