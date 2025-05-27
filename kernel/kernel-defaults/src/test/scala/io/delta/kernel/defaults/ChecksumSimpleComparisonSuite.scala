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
import java.util.Optional

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Table}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.hook.PostCommitHook.PostCommitHookType
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction
import io.delta.kernel.internal.TableImpl
import io.delta.kernel.internal.actions.{AddFile, Metadata, SingleAction}
import io.delta.kernel.internal.checksum.{ChecksumReader, CRCInfo}
import io.delta.kernel.internal.fs.Path
import io.delta.kernel.internal.util.FileNames.checksumFile
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.FileStatus

import org.apache.spark.sql.functions.col

/**
 * Test suite to verify checksum file correctness by comparing
 * Delta Spark and Delta Kernel generated checksum files.
 * This suite ensures that both implementations generate consistent checksums
 * for various table operations.
 */
trait ChecksumComparisonSuiteBase extends DeltaTableWriteSuiteBase with TestUtils {

  private val PARTITION_COLUMN = "part"

  protected def getPostCommitHookType: PostCommitHookType

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

  implicit class MetadataOpt(private val metadata: Metadata) {
    def withDeterministicIdAndCreateTime: Metadata = {
      new Metadata(
        "id",
        metadata.getName,
        metadata.getDescription,
        metadata.getFormat,
        metadata.getSchemaString,
        metadata.getSchema,
        metadata.getPartitionColumns,
        Optional.empty(),
        metadata.getConfigurationMapValue)
    }
  }

  implicit class CrcInfoOpt(private val crcInfo: CRCInfo) {
    def withoutTransactionId: CRCInfo = {
      new CRCInfo(
        crcInfo.getVersion,
        crcInfo.getMetadata.withDeterministicIdAndCreateTime,
        crcInfo.getProtocol,
        crcInfo.getTableSizeBytes,
        crcInfo.getNumFiles,
        Optional.empty(),
        // TODO: check domain metadata.
        Optional.empty(),
        // TODO: check file size histogram once https://github.com/delta-io/delta/pull/3907 merged.
        Optional.empty())
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
    // Remove the randomly generated TxnId
    assert(sparkCrc.withoutTransactionId === kernelCrc.withoutTransactionId)
  }

  private def readCrcInfo(engine: Engine, path: String, version: Long): CRCInfo = {
    ChecksumReader
      .getCRCInfo(
        engine,
        FileStatus.of(checksumFile(new Path(f"$path/_delta_log/"), version).toString))
      .orElseThrow(() => new IllegalStateException(s"CRC info not found for version $version"))
  }

  // Extracts the changes from spark table and commit the exactly same change to kernel table
  protected def commitSparkChangeToKernel(
      path: String,
      engine: Engine,
      sparkTablePath: String,
      versionToConvert: Long): Unit = {

    val txn = Table.forPath(engine, path)
      .createTransactionBuilder(engine, "test-engine", Operation.WRITE)
      .withLogCompactionInverval(0) // disable compaction
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
      .stream().filter(_.getType == getPostCommitHookType)
      .forEach(_.threadSafeInvoke(engine))
  }
}

class ChecksumSimpleComparisonSuite extends ChecksumComparisonSuiteBase {

  override def getPostCommitHookType
      : PostCommitHookType =
    PostCommitHookType.CHECKSUM_SIMPLE
}

class ChecksumFullComparisonSuite extends ChecksumComparisonSuiteBase {

  override def getPostCommitHookType
      : PostCommitHookType =
    PostCommitHookType.CHECKSUM_FULL

  override def commitSparkChangeToKernel(
      kernelTablePath: String,
      engine: Engine,
      sparkTablePath: String,
      versionToConvert: Long): Unit = {

    // Delete previous version's checksum to force CHECKSUM_FULL for next commit
    if (versionToConvert > 0) {
      deleteChecksumFileForTable(kernelTablePath, Seq((versionToConvert - 1).toInt))
    }

    super.commitSparkChangeToKernel(kernelTablePath, engine, sparkTablePath, versionToConvert)
  }
}
