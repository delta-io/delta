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

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Paths}
import java.util.{Collections, Optional}

import scala.jdk.CollectionConverters._

import io.delta.kernel.{Operation, TableManager}
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.{AddFile, CommitInfo, DeletionVectorDescriptor}
import io.delta.kernel.internal.actions.SingleAction._
import io.delta.kernel.internal.checksum.{ChecksumReader, CRCInfo}
import io.delta.kernel.internal.stats.DeletedRecordCountsHistogram
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.FileStatus

import org.scalatest.funsuite.AnyFunSuite

class DeletedRecordCountsHistogramSimpleCrcSuite extends AnyFunSuite with WriteUtils
    with LogReplayBaseSuite {
  private def file(name: String, cardinality: Option[Long]): AddFile =
    new AddFile(AddFile.createAddFileRowWithStatsJson(
      name,
      VectorUtils.stringStringMapValue(Collections.emptyMap[String, String]()),
      100L,
      0L,
      true,
      Optional.ofNullable(cardinality.map(n =>
        new DeletionVectorDescriptor(
          "p",
          s"file:/dv-$name.bin",
          Optional.empty(),
          32,
          n)).orNull),
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      Optional.of("""{"numRecords":1000,"tightBounds":false}""")))

  private val a = file("a.parquet", Some(0L))
  private val b = file("b.parquet", Some(9L))
  private val c = file("c.parquet", Some(10L))

  private def commit(path: String, version: Long, op: String, actions: Row*): Unit = {
    val info = new CommitInfo(
      Optional.empty(),
      version,
      Optional.empty(),
      Optional.of(op),
      Collections.emptyMap(),
      Optional.empty(),
      Optional.empty(),
      Collections.emptyMap())
    val json = (createCommitInfoSingleAction(info.toRow) +: actions)
      .map(JsonUtils.rowToJson).mkString("", "\n", "\n")
    Files.write(Paths.get(path, "_delta_log", f"$version%020d.json"), json.getBytes(UTF_8))
  }

  private def readCrc(path: String, engine: MetricsEngine, version: Long): CRCInfo =
    ChecksumReader.tryReadChecksumFile(
      engine,
      FileStatus.of(Paths.get(path, "_delta_log", f"$version%020d.crc").toString)).get()

  private def histogram(counts: Long*): DeletedRecordCountsHistogram =
    new DeletedRecordCountsHistogram(counts.toArray.padTo(10, 0L))

  private def withLegacySeedCrc(f: (String, MetricsEngine) => Unit): Unit = {
    withTempDirAndMetricsEngine { (path, engine) =>
      createEmptyTable(
        engine,
        path,
        testSchema,
        tableProperties = Map("delta.enableDeletionVectors" -> "true"))
      commit(
        path,
        1,
        "WRITE",
        createAddFileSingleAction(a.toRow),
        createAddFileSingleAction(b.toRow))
      TableManager.loadSnapshot(path).build(engine).writeChecksum(engine, ChecksumWriteMode.FULL)
      f(path, engine)
    }
  }

  private def removeHistogram(
      path: String,
      engine: MetricsEngine,
      retainAllFiles: Boolean): Unit = {
    val crcPath = Paths.get(path, "_delta_log", f"${1L}%020d.crc")
    val crc = OBJ_MAPPER.readTree(Files.readAllBytes(crcPath))
      .asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
    crc.remove("deletedRecordCountsHistogramOpt")
    if (!retainAllFiles) crc.remove("allFiles")
    engine.getFileSystemClient.delete(crcPath.toString)
    Files.write(crcPath, (crc.toString + "\n").getBytes(UTF_8))
  }

  Seq(true, false).foreach { retainAllFiles =>
    test(s"SIMPLE CRC recovers or omits a legacy histogram: allFiles=$retainAllFiles") {
      withLegacySeedCrc { (path, engine) =>
        removeHistogram(path, engine, retainAllFiles)
        val snapshot = TableManager.loadSnapshot(path).build(engine)
        val txn = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine)
        val postCommit = txn.commit(
          engine,
          inMemoryIterable(toCloseableIterator(Seq(createAddFileSingleAction(c.toRow))
            .iterator.asJava))).getPostCommitSnapshot.get()
        engine.resetMetrics()
        postCommit.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
        assertMetrics(engine, Nil, Nil, expChecksumReadSet = Nil)
        assert(readCrc(path, engine, 2).getDeletedRecordCountsHistogram ===
          (if (retainAllFiles) Optional.of(histogram(1, 1, 1)) else Optional.empty()))
      }
    }
  }

  test("SIMPLE post-commit CRC maintains histogram through adds, removes and replacement") {
    withTempDirAndMetricsEngine { (path, engine) =>
      val txn = TableManager.buildCreateTableTransaction(path, testSchema, "test")
        .withTableProperties(Map("delta.enableDeletionVectors" -> "true").asJava).build(engine)
      var snapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(readCrc(path, engine, 0).getDeletedRecordCountsHistogram.get() === histogram())
      val add = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine)
      val rows = Seq(
        createAddFileSingleAction(file("plain.parquet", None).toRow),
        createAddFileSingleAction(b.toRow))
      snapshot = add.commit(engine, inMemoryIterable(toCloseableIterator(rows.iterator.asJava)))
        .getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      val crc = readCrc(path, engine, 1)
      assert(crc.getDeletedRecordCountsHistogram.get() === histogram(1, 1))
      assert(CRCInfo.fromRow(1, crc.toRow) === crc)
      val stamped = crc.withInCommitTimestamp(Optional.of(123L))
      assert(CRCInfo.fromRow(1, stamped.toRow) === stamped)
      val json = OBJ_MAPPER.readTree(Files.readAllBytes(
        Paths.get(path, "_delta_log", f"${1L}%020d.crc")))
      assert(json.get("deletedRecordCountsHistogramOpt").toString ===
        """{"deletedRecordCounts":[1,1,0,0,0,0,0,0,0,0]}""")
      val remove = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine)
      snapshot = remove.commit(
        engine,
        inMemoryIterable(toCloseableIterator(Seq(
          createRemoveFileSingleAction(b.toRemoveFileRow(true, Optional.of(0L)))).iterator.asJava)))
        .getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(readCrc(path, engine, 2).getDeletedRecordCountsHistogram.get() === histogram(1))
      val replace = snapshot.asInstanceOf[SnapshotImpl]
        .buildReplaceTableTransaction(testSchema, "test").build(engine)
      snapshot = replace.commit(
        engine,
        inMemoryIterable(toCloseableIterator(Seq(createAddFileSingleAction(c.toRow))
          .iterator.asJava))).getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(readCrc(path, engine, 3).getDeletedRecordCountsHistogram.get() === histogram(0, 0, 1))
    }
  }
}
