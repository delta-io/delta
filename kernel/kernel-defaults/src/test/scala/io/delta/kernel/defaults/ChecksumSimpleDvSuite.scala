/*
 * Copyright (2026) The Delta Lake Project Authors.
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

import java.nio.file.FileAlreadyExistsException
import java.util.{Collections, Optional}

import scala.jdk.CollectionConverters._

import io.delta.kernel.{Table, Transaction, TransactionCommitResult}
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler}
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.engine.Engine
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.{AddFile, DeletionVectorDescriptor, SingleAction}
import io.delta.kernel.internal.checksum.{ChecksumReader, ChecksumUtils, CRCInfo}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.utils.{CloseableIterable, CloseableIterator, FileStatus}
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

import org.apache.hadoop.conf.Configuration
import org.scalatest.funsuite.AnyFunSuite

class ChecksumSimpleDvSuite extends AnyFunSuite with WriteUtils with LogReplayBaseSuite {

  private val dvProperties = Map("delta.enableDeletionVectors" -> "true")

  private def addFile(path: String, cardinality: Option[Long] = None): AddFile = {
    val dv = cardinality.map { count =>
      new DeletionVectorDescriptor(
        "p",
        s"/deletion_vector_${path}_$count.bin",
        Optional.empty(),
        32,
        count)
    }
    new AddFile(AddFile.createAddFileRow(
      testSchema,
      path,
      VectorUtils.stringStringMapValue(Collections.emptyMap[String, String]()),
      100L,
      0L,
      true,
      Optional.ofNullable(dv.orNull),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.empty()))
  }

  private def add(file: AddFile): Row = SingleAction.createAddFileSingleAction(file.toRow)

  private def remove(file: AddFile): Row =
    SingleAction.createRemoveFileSingleAction(file.toRemoveFileRow(true, Optional.empty()))

  private def actions(rows: Row*): CloseableIterable[Row] =
    inMemoryIterable(toCloseableIterator(rows.toSeq.asJava.iterator()))

  private def commitSimple(
      txn: Transaction,
      engine: Engine,
      rows: Row*): TransactionCommitResult = {
    val result = txn.commit(engine, actions(rows: _*))
    if (result.getPostCommitSnapshot.isPresent) {
      result.getPostCommitSnapshot.get().writeChecksum(engine, ChecksumWriteMode.SIMPLE)
    } else {
      // Conflict-rebased commits currently expose the CRC through the hook only.
      executeCrcSimple(result, engine)
    }
    result
  }

  private def readCrc(engine: Engine, path: String, version: Long): CRCInfo =
    ChecksumReader.tryReadChecksumFile(
      engine,
      FileStatus.of(buildCrcPath(path, version).toString)).get()

  private def checkDvMetrics(crc: CRCInfo, vectors: Long, records: Long): Unit = {
    assert(crc.getNumDeletionVectors === Optional.of(Long.box(vectors)))
    assert(crc.getNumDeletedRecords === Optional.of(Long.box(records)))
  }

  for (supportsDv <- Seq(false, true)) {
    test(s"SIMPLE CRC initializes empty-table DV metrics only with DV support=$supportsDv") {
      withTempDirAndEngine { (path, engine) =>
        commitSimple(
          getCreateTxn(
            engine,
            path,
            testSchema,
            tableProperties = if (supportsDv) dvProperties else Map.empty[String, String]),
          engine)
        val crc = readCrc(engine, path, 0)
        if (supportsDv) {
          checkDvMetrics(crc, 0, 0)
        } else {
          assert(!crc.getNumDeletionVectors.isPresent)
          assert(!crc.getNumDeletedRecords.isPresent)
        }
      }
    }
  }

  test("SIMPLE CRC maintains DV metrics across appends, DV updates, and removals") {
    withTempDirAndEngine { (path, engine) =>
      val first = addFile("first.parquet", Some(3L))
      val second = addFile("second.parquet", Some(5L))
      val plain = addFile("plain.parquet")
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        add(first))
      checkDvMetrics(readCrc(engine, path, 0), 1, 3)

      commitSimple(getUpdateTxn(engine, path), engine, add(second))
      checkDvMetrics(readCrc(engine, path, 1), 2, 8)
      commitSimple(getUpdateTxn(engine, path), engine, add(plain))
      checkDvMetrics(readCrc(engine, path, 2), 2, 8)

      val updatedFirst = addFile("first.parquet", Some(7L))
      commitSimple(getUpdateTxn(engine, path), engine, remove(first), add(updatedFirst))
      checkDvMetrics(readCrc(engine, path, 3), 2, 12)
      commitSimple(getUpdateTxn(engine, path), engine, remove(second), remove(plain))
      checkDvMetrics(readCrc(engine, path, 4), 1, 7)
      commitSimple(getUpdateTxn(engine, path), engine, remove(updatedFirst))
      checkDvMetrics(readCrc(engine, path, 5), 0, 0)
    }
  }

  test("SIMPLE CRC replacement counts only the new files") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        add(addFile("old.parquet", Some(9L))))
      commitSimple(
        getReplaceTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        add(addFile("new.parquet", Some(2L))))
      checkDvMetrics(readCrc(engine, path, 1), 1, 2)

      commitSimple(
        getReplaceTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine)
      checkDvMetrics(readCrc(engine, path, 2), 0, 0)
    }
  }

  test("SIMPLE CRC seeds DV metrics from zero when enabling DV support") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(getCreateTxn(engine, path, testSchema), engine, add(addFile("plain.parquet")))
      commitSimple(
        getUpdateTxn(engine, path, tableProperties = dvProperties),
        engine,
        add(addFile("dv.parquet", Some(4L))))
      checkDvMetrics(readCrc(engine, path, 1), 1, 4)
    }
  }

  test("SIMPLE CRC leaves missing DV metrics unknown on an existing DV table") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        add(addFile("old.parquet", Some(3L))))
      rewriteChecksumFileToExcludeDvMetrics(engine, path, 0)
      commitSimple(getUpdateTxn(engine, path), engine, add(addFile("new.parquet", Some(5L))))
      val crc = readCrc(engine, path, 1)
      assert(!crc.getNumDeletionVectors.isPresent)
      assert(!crc.getNumDeletedRecords.isPresent)
    }
  }

  test("SIMPLE CRC rebases DV metrics and counts retried actions only once") {
    withTempDirAndEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        add(addFile("initial.parquet", Some(3L))))

      val fileIO = new HadoopFileIO(new Configuration())
      var attempts = 0
      val retryEngine = new DefaultEngine(fileIO) {
        private val handler = new DefaultJsonHandler(fileIO) {
          override def writeJsonFileAtomically(
              filePath: String,
              data: CloseableIterator[Row],
              overwrite: Boolean): Unit = {
            if (filePath.endsWith(".json")) {
              attempts += 1
              if (attempts == 1) {
                // A failed attempt can consume actions before discovering the conflict.
                while (data.hasNext) data.next()
                throw new FileAlreadyExistsException(filePath)
              }
            }
            super.writeJsonFileAtomically(filePath, data, overwrite)
          }
        }
        override def getJsonHandler: DefaultJsonHandler = handler
      }
      val losingTxn = getUpdateTxn(retryEngine, path)
      commitSimple(getUpdateTxn(engine, path), engine, add(addFile("winner.parquet", Some(5L))))
      val result = commitSimple(losingTxn, retryEngine, add(addFile("loser.parquet", Some(7L))))
      assert(result.getVersion === 2)
      assert(attempts === 2)
      checkDvMetrics(readCrc(engine, path, 2), 3, 15)
    }
  }

  test("FULL CRC can incrementally replay from a SIMPLE CRC on a DV table") {
    withTempDirAndMetricsEngine { (path, engine) =>
      commitSimple(
        getCreateTxn(engine, path, testSchema, tableProperties = dvProperties),
        engine,
        add(addFile("first.parquet", Some(3L))))
      commitSimple(getUpdateTxn(engine, path), engine, add(addFile("plain.parquet")))
      getUpdateTxn(engine, path).commit(engine, actions(add(addFile("second.parquet", Some(5L)))))
      val snapshot = Table.forPath(engine, path).getLatestSnapshot(engine)
        .asInstanceOf[SnapshotImpl]
      engine.resetMetrics()

      val crc = ChecksumUtils.computeChecksum(engine, snapshot.getLogSegment)
      checkDvMetrics(crc, 2, 8)
      assertMetrics(engine, Seq(2L), Nil, expChecksumReadSet = Seq(1L))
    }
  }
}
