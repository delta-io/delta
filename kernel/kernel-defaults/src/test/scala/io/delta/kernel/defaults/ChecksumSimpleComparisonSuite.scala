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

import java.io.File
import java.nio.file.Files
import java.util

import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.setAsJavaSetConverter

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.TableImpl
import io.delta.kernel.internal.actions.{AddFile, SingleAction}
import io.delta.kernel.internal.checksum.{ChecksumReader, CRCInfo}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import org.apache.spark.sql.functions.col

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
        schema = new StructType().add("id", LONG),
        partCols = Seq.empty).commit(engine, emptyIterable())
        .getPostCommitHooks
        .forEach(hook => hook.threadSafeInvoke(engine))
      spark.sql(s"CREATE OR REPLACE TABLE delta.`${sparkTablePath}` (id LONG) USING DELTA")
      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach { version =>
        spark.range(0, version).write.format("delta").mode("append").save(sparkTablePath)
        commitSparkChangeToKernel(kernelTablePath, engine, sparkTablePath, version)
        assertChecksumEquals(engine, sparkTablePath, kernelTablePath, version)
      }
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
        schema = new StructType().add("id", LONG).add(PARTITION_COLUMN, LONG),
        partCols = Seq(PARTITION_COLUMN)).commit(engine, emptyIterable())
        .getPostCommitHooks
        .forEach(hook => hook.threadSafeInvoke(engine))
      spark.sql(
        s"CREATE OR REPLACE TABLE delta.`${sparkTablePath}` " +
          s"(id LONG, part LONG) USING DELTA PARTITIONED BY (part)")
      assertChecksumEquals(engine, sparkTablePath, kernelTablePath, 0)

      (1 to 10).foreach { version =>
        spark.range(0, version).withColumn(PARTITION_COLUMN, col("id") % 2)
          .write.format("delta").mode("append").save(sparkTablePath)
        commitSparkChangeToKernel(kernelTablePath, engine, sparkTablePath, version)
        assertChecksumEquals(engine, sparkTablePath, kernelTablePath, version)
      }
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
      s"CRC files not found for version $version")

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
    new File(FileNames.checksumFile(new Path(f"$basePath/_delta_log"), version).toString).toPath
  }

  // TODO: Add equals/hashCode to metadata, protocol then CRCInfo.
  private def assertCrcInfoEquals(crc1: CRCInfo, crc2: CRCInfo): Unit = {
    assert(crc1.getVersion === crc2.getVersion)
    assert(crc1.getNumFiles === crc2.getNumFiles)
    assert(crc1.getTableSizeBytes === crc2.getTableSizeBytes)
    assert(crc1.getMetadata.getSchema === crc2.getMetadata.getSchema)
    assert(crc1.getMetadata.getPartitionColNames === crc2.getMetadata.getPartitionColNames)
  }

  // TODO docs
  private def commitSparkChangeToKernel(
      path: String,
      engine: Engine,
      sparkTablePath: String,
      versionToConvert: Long): Unit = {

    val txn = Table.forPath(engine, path)
      .createTransactionBuilder(engine, "test-engine", Operation.WRITE)
      .build(engine)

    val tableChange = Table.forPath(engine, sparkTablePath).asInstanceOf[TableImpl].getChanges(
      engine,
      versionToConvert,
      versionToConvert,
      // TODO include REMOVE action as well once we support it
      Set(DeltaAction.ADD).asJava)

    val addFilesRows = new util.ArrayList[Row]()
    tableChange.forEach(batch =>
      batch.getRows.forEach(row => {
        val addIndex = row.getSchema.indexOf("add")
        if (!row.isNullAt(addIndex)) {
          addFilesRows.add(
            SingleAction.createAddFileSingleAction(new AddFile(row.getStruct(addIndex)).toRow))
        }
      }))

    txn
      .commit(engine, inMemoryIterable(toCloseableIterator(addFilesRows.iterator())))
      .getPostCommitHooks
      .forEach(_.threadSafeInvoke(engine))
  }
}
