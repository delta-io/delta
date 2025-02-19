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
import io.delta.kernel.data.Row
import io.delta.kernel.{Transaction, TransactionCommitResult}
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.hook.PostCommitHook.PostCommitHookType
import io.delta.kernel.internal.TransactionImpl
import io.delta.kernel.internal.actions.Metadata
import io.delta.kernel.internal.checksum.CRCInfo
import io.delta.kernel.internal.util.Utils.singletonCloseableIterator
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.{CloseableIterable, FileStatus}

import java.io.File
import java.nio.file.Files
import java.util.{Locale, Optional}
import scala.collection.immutable.Seq
import scala.jdk.CollectionConverters.{asScalaBufferConverter, asScalaSetConverter}
import scala.language.implicitConversions

class DeltaTableWriteWithCrcSuite extends DeltaTableWritesSuite {

  implicit class TransactionOps(txn: Transaction) {
    def commitAndGenerateCrc(
        engine: Engine,
        dataActions: CloseableIterable[Row]): TransactionCommitResult = {
      val result = txn.commit(engine, dataActions)
      result.getPostCommitHooks
        .stream()
        .filter(hook => hook.getType == PostCommitHookType.CHECKSUM_SIMPLE)
        .forEach(
          hook => hook.threadSafeInvoke(engine)
        )
      result
    }
  }

  override def commitTransaction(
      txn: Transaction,
      engine: Engine,
      dataActions: CloseableIterable[Row]): TransactionCommitResult = {
    txn.commitAndGenerateCrc(engine, dataActions)
  }

  override def verifyWrittenContent(
      path: String,
      expSchema: StructType,
      expData: Seq[TestRow],
      expPartitionColumns: Seq[String] = Seq(),
      version: Option[Long] = Option.empty): Unit = {
    val actSchema = tableSchema(path)
    assert(actSchema === expSchema)

    // verify data using Kernel reader
    checkTable(path, expData)

    // verify data using Spark reader.
    // Spark reads the timestamp partition columns in local timezone vs. Kernel reads in UTC. We
    // need to set the timezone to UTC before reading the data using Spark to make the tests pass
    withSparkTimeZone("UTC") {
      val resultSpark = spark
        .sql(s"SELECT * FROM delta.`$path`" + {
          if (version.isDefined) s" VERSION AS OF ${version.get}" else ""
        })
        .collect()
        .map(TestRow(_))
      checkAnswer(resultSpark, expData)
    }

    checkChecksumContent(path, version, expSchema, expPartitionColumns)
  }

  def checkChecksumContent(
      tablePath: String,
      version: Option[Long],
      expSchema: StructType,
      expPartitionColumns: Seq[String]): Unit = {
    val checksumVersion = version.getOrElse(latestSnapshot(tablePath, defaultEngine).getVersion)
    val checksumFile = new File(f"$tablePath/_delta_log/$checksumVersion%020d.crc")

    assert(Files.exists(checksumFile.toPath), s"Checksum file not found: ${checksumFile.getPath}")

    val columnarBatches = defaultEngine
      .getJsonHandler()
      .readJsonFiles(
        singletonCloseableIterator(FileStatus.of(checksumFile.getPath)),
        CRCInfo.CRC_FILE_SCHEMA,
        Optional.empty()
      )

    assert(columnarBatches.hasNext, "Empty checksum file")
    val crcRow = columnarBatches.next()
    assert(crcRow.getSize === 1, s"Expected single row, found ${crcRow.getSize}")

    val metadata = Metadata.fromColumnVector(
      crcRow.getColumnVector(CRCInfo.CRC_FILE_SCHEMA.indexOf("metadata")),
      /* rowId= */ 0
    )

    assert(
      metadata.getSchema === expSchema,
      s"Schema mismatch.\nExpected: $expSchema\nActual: ${metadata.getSchema}"
    )

    val normalizedPartitions = expPartitionColumns.map(_.toLowerCase(Locale.ROOT)).toSet
    assert(
      metadata.getPartitionColNames.asScala === normalizedPartitions,
      s"Partition columns mismatch.\n" +
      s"Expected: $normalizedPartitions\n" +
      s"Actual: ${metadata.getPartitionColNames.asScala}"
    )

    assert(!columnarBatches.hasNext, "Unexpected additional data in checksum file")
  }
}
