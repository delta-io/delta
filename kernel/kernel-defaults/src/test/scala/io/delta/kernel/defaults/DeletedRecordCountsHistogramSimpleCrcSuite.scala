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
import io.delta.kernel.commit.{CommitFailedException, Committer}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.internal.json.JsonUtils
import io.delta.kernel.defaults.utils.WriteUtils
import io.delta.kernel.internal.{SnapshotImpl, TransactionImpl}
import io.delta.kernel.internal.actions.{AddFile, CommitInfo, DeletionVectorDescriptor, Protocol}
import io.delta.kernel.internal.actions.SingleAction._
import io.delta.kernel.internal.checksum.{ChecksumReader, CRCInfo}
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter
import io.delta.kernel.internal.stats.DeletedRecordCountsHistogram
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.utils.{CloseableIterable, FileStatus}
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.funsuite.AnyFunSuite

class DeletedRecordCountsHistogramSimpleCrcSuite extends AnyFunSuite with WriteUtils
    with LogReplayBaseSuite {
  private def addFileWithDv(name: String, cardinality: Option[Long]): AddFile =
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

  private val a = addFileWithDv("a.parquet", Some(0L))
  private val b = addFileWithDv("b.parquet", Some(9L))
  private val c = addFileWithDv("c.parquet", Some(10L))

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

  private def readCrcJson(path: String, version: Long): ObjectNode =
    OBJ_MAPPER.readTree(Files.readAllBytes(Paths.get(path, "_delta_log", f"$version%020d.crc")))
      .asInstanceOf[ObjectNode]

  private def histogram(counts: Long*): DeletedRecordCountsHistogram =
    new DeletedRecordCountsHistogram(counts.toArray.padTo(10, 0L))

  private def addFileToActionRow(file: AddFile): CloseableIterable[Row] = inMemoryIterable(
    toCloseableIterator(Iterator(createAddFileSingleAction(file.toRow)).asJava))

  // Write version 1 with two DV-bearing files. Its CRC is the base for the version 2 commit.
  private def withBaseCrc(f: (String, MetricsEngine) => Unit): Unit = {
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

  private def rewriteCrc(path: String, engine: MetricsEngine, version: Long)(
      edit: ObjectNode => Unit): Unit = {
    val crcPath = Paths.get(path, "_delta_log", f"$version%020d.crc")
    val crc = readCrcJson(path, version)
    edit(crc)
    engine.getFileSystemClient.delete(crcPath.toString)
    Files.write(crcPath, (crc.toString + "\n").getBytes(UTF_8))
  }

  private val enableDvs = "delta.enableDeletionVectors"
  private case class DvState(name: String, properties: Map[String, String])
  private val unsupported = DvState("unsupported", Map.empty)
  private val disabled = DvState(
    "disabled",
    Map("delta.feature.deletionVectors" -> "supported", enableDvs -> "false"))
  private val enabled = DvState("enabled", Map(enableDvs -> "true"))
  private val dvStates = Seq(unsupported, disabled, enabled)

  private case class Modification(addFile: AddFile, baseHasDvHistogram: Boolean)
  private val histogramField = "deletedRecordCountsHistogramOpt"
  private val baseFileHistogram = histogram(1)

  private def checkWrittenDvHistogram(
      path: String,
      engine: MetricsEngine,
      version: Long,
      state: DvState,
      expected: Option[DeletedRecordCountsHistogram]): Unit = {
    val crc = readCrc(path, engine, version)
    assert(crc.getProtocol.getReaderFeatures.contains("deletionVectors") === (state != unsupported))
    assert(crc.getMetadata.getConfiguration.getOrDefault(enableDvs, "false") ===
      (state == enabled).toString)
    assert(crc.getDeletedRecordCountsHistogram.toScala === expected)
    assert(readCrcJson(path, version).has(histogramField) === expected.isDefined)
  }

  // Kernel has no DROP FEATURE builder. Supply the target protocol to the transaction directly.
  // This fixture has only a plain file and has never contained a DV.
  private def dropDvSupport(snapshot: SnapshotImpl): TransactionImpl =
    new TransactionImpl(
      false,
      snapshot.getDataPath,
      Optional.of(snapshot),
      "test",
      Operation.MANUAL_UPDATE,
      Optional.of(new Protocol(1, 2)),
      Optional.of(
        snapshot.getMetadata.withConfigurationKeysUnset(disabled.properties.keySet.asJava)),
      snapshot.getCommitter,
      Optional.empty(),
      Optional.empty(),
      Optional.empty(),
      0,
      () => System.currentTimeMillis())

  private def createPlainFileTable(path: String, engine: MetricsEngine, state: DvState): Unit = {
    val create = TableManager.buildCreateTableTransaction(path, testSchema, "test")
      .withTableProperties(state.properties.asJava).build(engine)
    create.commit(engine, addFileToActionRow(addFileWithDv("base.parquet", None)))
      .getPostCommitSnapshot.get().writeChecksum(engine, ChecksumWriteMode.SIMPLE)
  }

  Seq(unsupported -> None, disabled -> None, enabled -> Some(baseFileHistogram)).foreach {
    case (state, expected) =>
      test(s"SIMPLE CRC on create with DVs ${state.name}") {
        withTempDirAndMetricsEngine { (path, engine) =>
          createPlainFileTable(path, engine, state)
          checkWrittenDvHistogram(path, engine, 0, state, expected)
        }
      }
  }

  private val transitionCases = {
    val plainFile = addFileWithDv("next.parquet", None)
    val dvFile = addFileWithDv("next.parquet", Some(9L))
    val targets = Seq(
      (unsupported, Modification(plainFile, baseHasDvHistogram = false), None),
      (unsupported, Modification(plainFile, baseHasDvHistogram = true), None),
      (disabled, Modification(plainFile, baseHasDvHistogram = false), None),
      (disabled, Modification(plainFile, baseHasDvHistogram = true), Some(histogram(2))),
      (enabled, Modification(dvFile, baseHasDvHistogram = false), None),
      (enabled, Modification(dvFile, baseHasDvHistogram = true), Some(histogram(1, 1))))
    for {
      baseDvState <- dvStates
      (targetDvState, modification, expected) <- targets
    } yield (baseDvState, targetDvState, modification, expected)
  }

  // The base and target versions can each lack DV support, support DVs with writes disabled,
  // or support DVs with writes enabled. Test each pair with and without a base CRC histogram.
  transitionCases.foreach { case (baseDvState, targetDvState, modification, expected) =>
    test(s"SIMPLE CRC DVs ${baseDvState.name} -> ${targetDvState.name}, " +
      s"baseHasDvHistogram=${modification.baseHasDvHistogram}") {
      withTempDirAndMetricsEngine { (path, engine) =>
        createPlainFileTable(path, engine, baseDvState)
        rewriteCrc(path, engine, 0) { crc =>
          if (modification.baseHasDvHistogram) {
            val buckets = crc.putObject(histogramField).putArray("deletedRecordCounts")
            baseFileHistogram.getCounts.foreach(n => buckets.add(n))
          } else {
            crc.remove(histogramField)
          }
        }

        val snapshot = TableManager.loadSnapshot(path).build(engine).asInstanceOf[SnapshotImpl]
        val txn = if (baseDvState != unsupported && targetDvState == unsupported) {
          dropDvSupport(snapshot)
        } else {
          snapshot.buildUpdateTableTransaction("test", Operation.WRITE)
            .withTablePropertiesAdded(targetDvState.properties.asJava).build(engine)
        }
        val postCommit = txn.commit(engine, addFileToActionRow(modification.addFile))
          .getPostCommitSnapshot.get()
        engine.resetMetrics()
        postCommit.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
        assertMetrics(engine, Nil, Nil, expChecksumReadSet = Nil)
        checkWrittenDvHistogram(path, engine, 1, targetDvState, expected)
      }
    }
  }

  Seq(true, false).foreach { retainAllFiles =>
    test(s"SIMPLE CRC omits the DV histogram when the base lacks it (allFiles=$retainAllFiles)") {
      withBaseCrc { (path, engine) =>
        rewriteCrc(path, engine, 1) { crc =>
          crc.remove("deletedRecordCountsHistogramOpt")
          if (!retainAllFiles) crc.remove("allFiles")
        }
        val snapshot = TableManager.loadSnapshot(path).build(engine)
        val txn = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine)
        val postCommit = txn.commit(
          engine,
          inMemoryIterable(toCloseableIterator(Seq(createAddFileSingleAction(c.toRow))
            .iterator.asJava))).getPostCommitSnapshot.get()
        engine.resetMetrics()
        postCommit.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
        assertMetrics(engine, Nil, Nil, expChecksumReadSet = Nil)
        val crc = readCrc(path, engine, 2)
        assert(!crc.getDeletedRecordCountsHistogram.isPresent)
        assert(crc.getNumDeletionVectors.get().longValue() === 3L)
        assert(crc.getNumDeletedRecords.get().longValue() === 19L)
      }
    }
  }

  test("SIMPLE CRC updates the DV histogram when it is the only optional field in the base CRC") {
    withTempDirAndMetricsEngine { (path, engine) =>
      val create = TableManager.buildCreateTableTransaction(path, testSchema, "test")
        .withTableProperties(Map("delta.enableDeletionVectors" -> "true").asJava).build(engine)
      create.commit(engine, addFileToActionRow(b)).getPostCommitSnapshot.get()
        .writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      rewriteCrc(path, engine, 0) { crc =>
        val fields = Set(
          "tableSizeBytes",
          "numFiles",
          "numMetadata",
          "numProtocol",
          "metadata",
          "protocol",
          "deletedRecordCountsHistogramOpt")
        crc.retain(fields.asJava)
        assert(crc.fieldNames().asScala.toSet === fields)
      }

      val snapshot = TableManager.loadSnapshot(path).build(engine)
      val append = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine)
      val postCommit = append.commit(engine, addFileToActionRow(c)).getPostCommitSnapshot.get()
      engine.resetMetrics()
      postCommit.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assertMetrics(engine, Nil, Nil, expChecksumReadSet = Nil)
      val crc = readCrc(path, engine, 1)
      assert(crc.getDeletedRecordCountsHistogram === Optional.of(histogram(0, 1, 1)))
      assert(!crc.getNumDeletedRecords.isPresent)
      assert(!crc.getNumDeletionVectors.isPresent)
    }
  }

  test("SIMPLE CRC maintains the DV histogram through adds, removes and REPLACE TABLE") {
    withTempDirAndMetricsEngine { (path, engine) =>
      val txn = TableManager.buildCreateTableTransaction(path, testSchema, "test")
        .withTableProperties(Map("delta.enableDeletionVectors" -> "true").asJava).build(engine)
      var snapshot = txn.commit(engine, emptyIterable()).getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(readCrc(path, engine, 0).getDeletedRecordCountsHistogram.get() === histogram())
      val add = snapshot.buildUpdateTableTransaction("test", Operation.WRITE).build(engine)
      val rows = Seq(
        createAddFileSingleAction(addFileWithDv("plain.parquet", None).toRow),
        createAddFileSingleAction(b.toRow))
      snapshot = add.commit(engine, inMemoryIterable(toCloseableIterator(rows.iterator.asJava)))
        .getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      val crc = readCrc(path, engine, 1)
      assert(crc.getDeletedRecordCountsHistogram.get() === histogram(1, 1))
      assert(CRCInfo.fromRow(1, crc.toRow) === crc)
      val stamped = crc.withInCommitTimestamp(Optional.of(123L))
      assert(CRCInfo.fromRow(1, stamped.toRow) === stamped)
      val json = readCrcJson(path, 1)
      assert(json.get("deletedRecordCountsHistogramOpt").toString ===
        """{"deletedRecordCounts":[1,1,0,0,0,0,0,0,0,0]}""")

      // Reload version 1 so the remove uses the histogram from the CRC on disk.
      val remove = TableManager.loadSnapshot(path).build(engine)
        .buildUpdateTableTransaction("test", Operation.WRITE).build(engine)
      snapshot = remove.commit(
        engine,
        inMemoryIterable(toCloseableIterator(Seq(
          createRemoveFileSingleAction(b.toRemoveFileRow(true, Optional.of(0L)))).iterator.asJava)))
        .getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(readCrc(path, engine, 2).getDeletedRecordCountsHistogram.get() === histogram(1))

      // REPLACE TABLE removes the remaining plain file and adds c, which has 10 deleted rows.
      val replaceTable = snapshot.asInstanceOf[SnapshotImpl]
        .buildReplaceTableTransaction(testSchema, "test").build(engine)
      snapshot = replaceTable.commit(
        engine,
        inMemoryIterable(toCloseableIterator(Seq(createAddFileSingleAction(c.toRow))
          .iterator.asJava))).getPostCommitSnapshot.get()
      snapshot.writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(readCrc(path, engine, 3).getDeletedRecordCountsHistogram.get() === histogram(0, 0, 1))
    }
  }

  test("SIMPLE CRC counts each file once after a commit retry") {
    withTempDirAndMetricsEngine { (path, engine) =>
      var firstAttempt = true
      val committer: Committer = (commitEngine, actions, metadata) => {
        if (firstAttempt) {
          firstAttempt = false
          // Consuming the actions records their histogram deltas before the attempt fails.
          actions.asScala.foreach(_ => ())
          throw new CommitFailedException(true, false, "retryable failure without a conflict")
        }
        DefaultFileSystemManagedTableOnlyCommitter.INSTANCE.commit(commitEngine, actions, metadata)
      }
      val txn = TableManager.buildCreateTableTransaction(path, testSchema, "test")
        .withTableProperties(Map("delta.enableDeletionVectors" -> "true").asJava)
        .withCommitter(committer)
        .withMaxRetries(1)
        .build(engine)
      // A DV with cardinality zero is empty and contributes one file to the first bucket.
      val rows = Seq(0L, 1L, 100L, 1000L).map { n =>
        createAddFileSingleAction(addFileWithDv(s"file-$n.parquet", Some(n)).toRow)
      }
      val result = txn.commit(engine, inMemoryIterable(toCloseableIterator(rows.iterator.asJava)))
      val metrics = result.getTransactionReport.getTransactionMetrics
      assert(metrics.getNumCommitAttempts === 2)
      val expected = histogram(1, 1, 0, 1, 1)
      assert(metrics.getDeletedRecordCountsHistogramDelta.get().toSeq === expected.getCounts.toSeq)
      result.getPostCommitSnapshot.get().writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(readCrc(path, engine, 0).getDeletedRecordCountsHistogram.get() === expected)
    }
  }
}
