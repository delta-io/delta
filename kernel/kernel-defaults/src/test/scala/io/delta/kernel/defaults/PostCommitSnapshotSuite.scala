/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Snapshot}
import io.delta.kernel.defaults.utils.{TestRow, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableConfig}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for validating the behavior of postCommitSnapshot in various scenarios.
 *
 * Note that we use "PCS" in our test names for brevity.
 */
class PostCommitSnapshotSuite extends AnyFunSuite with WriteUtilsWithV2Builders {

  private def assertAddFilesMatch(
      actual: SnapshotImpl,
      expected: SnapshotImpl,
      engine: Engine): Unit = {
    val actualFiles = collectScanFileRows(actual.getScanBuilder.build(), engine)
      .map(InternalScanFileUtils.getAddFileStatus)
    val expectedFiles = collectScanFileRows(expected.getScanBuilder.build(), engine)
      .map(InternalScanFileUtils.getAddFileStatus)

    assert(actualFiles.size === expectedFiles.size)

    val actualFilePaths = actualFiles.map(_.getPath).sorted
    val expectedFilePaths = expectedFiles.map(_.getPath).sorted

    assert(actualFilePaths === expectedFilePaths)
  }

  private def verifyPostCommitSnapshotMatchesLatest(
      postCommitSnapshot: Snapshot,
      tablePath: String,
      engine: Engine): Unit = {
    val actual = postCommitSnapshot.asInstanceOf[SnapshotImpl]
    val expected = latestSnapshot(tablePath, engine)

    assert(actual.getVersion === expected.getVersion)
    assert(actual.getSchema === expected.getSchema)
    assert(actual.getProtocol === expected.getProtocol)
    assert(actual.getMetadata === expected.getMetadata)
    assertAddFilesMatch(actual, expected, engine)

    // TODO: We need better visibility into when the below information is loaded from the log,
    //       loaded from CRC, or already stored in memory (i.e. injected during post-commit snapshot
    //       creation)
    assert(actual.getTimestamp(engine) === expected.getTimestamp(engine))

    // The postCommitSnapshot might have the CRCInfo (injected in memory) even if it wasn't written
    // out to disk
    if (expected.getCurrentCrcInfo.isPresent) {
      assert(actual.getCurrentCrcInfo === expected.getCurrentCrcInfo)
    }

    assert(actual.getActiveDomainMetadataMap === expected.getActiveDomainMetadataMap)
  }

  test("creating a new empty table => yields a PCS") {
    withTempDirAndEngine { (tablePath, engine) =>
      val result = createEmptyTable(engine, tablePath, testSchema)

      verifyPostCommitSnapshotMatchesLatest(result.getPostCommitSnapshot.get(), tablePath, engine)
    }
  }

  test("creating a new table with data => yields a PCS") {
    withTempDirAndEngine { (tablePath, engine) =>
      val result = appendData(
        engine,
        tablePath,
        isNewTable = true,
        schema = testSchema,
        data = seqOfUnpartitionedDataBatch1)

      verifyPostCommitSnapshotMatchesLatest(result.getPostCommitSnapshot.get(), tablePath, engine)
    }
  }

  test("commit at readVersion + 1 (*with* CRC at readVersion) => yields a PCS with CRC") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      val result0 = createEmptyTable(engine, tablePath, testSchema)
      executeCrcSimple(result0, engine)

      val snapshot0 = latestSnapshot(tablePath, engine)
      assert(snapshot0.getCurrentCrcInfo.isPresent)

      // ===== WHEN =====
      val result1 = appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch1)

      // ===== THEN =====
      verifyPostCommitSnapshotMatchesLatest(result1.getPostCommitSnapshot.get(), tablePath, engine)
    }
  }

  test("commit at readVersion + 1 (*without* CRC at readVersion) => yields a PCS without CRC") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      createEmptyTable(engine, tablePath, testSchema)

      val snapshot0 = latestSnapshot(tablePath, engine)
      assert(snapshot0.getCurrentCrcInfo.isEmpty)

      // ===== WHEN =====
      val result1 = appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch1)

      // ===== THEN =====
      verifyPostCommitSnapshotMatchesLatest(result1.getPostCommitSnapshot.get(), tablePath, engine)
    }
  }

  test("commit at readVersion + 2 => does NOT yield a PCS") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      createEmptyTable(engine, tablePath, testSchema)

      val txn = getUpdateTxn(engine, tablePath) // Create a transaction that reads at version 0
      assert(txn.getReadTableVersion == 0)

      // Create winning commits at v1 and v2
      appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch1)
      appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch2)
      assert(latestSnapshot(tablePath, engine).getVersion == 2)

      // ===== WHEN =====
      // Now commit the original txn that read at v0. This will commit at v3
      val txnState = txn.getTransactionState(engine)
      val actions = inMemoryIterable(stageData(txnState, Map.empty[String, Literal], dataBatches1))
      val result = commitTransaction(txn, engine, actions)

      // ===== THEN =====
      assert(!result.getPostCommitSnapshot.isPresent)
      assert(result.getVersion == 3)
    }
  }

  test("commit schema change at readVersion + 1 => yields a PCS with updated schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
      val newSchema = latestSnapshot(tablePath, engine).getSchema.add("newCol", INTEGER)

      // ===== WHEN =====
      val result = updateTableMetadata(engine, tablePath, schema = newSchema)

      // ===== THEN =====
      verifyPostCommitSnapshotMatchesLatest(result.getPostCommitSnapshot.get(), tablePath, engine)
    }
  }

  test("commit tbl property change at readVersion + 1 => yields a PCS with updated property") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      createEmptyTable(engine, tablePath, testSchema)

      // ===== WHEN =====
      val result = updateTableMetadata(engine, tablePath, tableProperties = Map("foo" -> "bar"))

      // ===== THEN =====
      verifyPostCommitSnapshotMatchesLatest(result.getPostCommitSnapshot.get(), tablePath, engine)
    }
  }

  test("commit protocol change at readVersion + 1 => yields a PCS with updated protocol") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      createEmptyTable(engine, tablePath, testSchema)

      val snapshot0 = latestSnapshot(tablePath, engine)
      assert(!snapshot0.getProtocol.getWriterFeatures.contains("deletionVectors"))

      // ===== WHEN =====
      val result = updateTableMetadata(
        engine,
        tablePath,
        tableProperties = Map("delta.enableDeletionVectors" -> "true"))

      // ===== THEN =====
      verifyPostCommitSnapshotMatchesLatest(result.getPostCommitSnapshot.get(), tablePath, engine)
    }
  }

  test("postCommitSnapshot can be used to read data") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== WHEN =====
      val result = appendData(
        engine,
        tablePath,
        isNewTable = true,
        schema = testSchema,
        data = seqOfUnpartitionedDataBatch1)

      // ===== THEN =====
      val postCommitSnapshot = result.getPostCommitSnapshot.get()
      val expectedData = dataBatches1.flatMap(_.toTestRows)
      val dataFromPostCommit = readSnapshot(postCommitSnapshot, engine = engine).map(TestRow(_))
      checkAnswer(dataFromPostCommit, expectedData)
    }
  }

  test("postCommitSnapshot can be used to start and commit a new transaction") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      val result0 = appendData(
        engine,
        tablePath,
        isNewTable = true,
        schema = testSchema,
        data = seqOfUnpartitionedDataBatch1)

      val postCommitSnapshot0 = result0.getPostCommitSnapshot.get().asInstanceOf[SnapshotImpl]
      assert(postCommitSnapshot0.getVersion == 0)

      // ===== WHEN =====
      // Use the postCommitSnapshot to start a new transaction and commit v1
      val txn = postCommitSnapshot0
        .buildUpdateTableTransaction(testEngineInfo, Operation.WRITE)
        .build(engine)
      val txnState = txn.getTransactionState(engine)
      val actions = inMemoryIterable(stageData(txnState, Map.empty[String, Literal], dataBatches2))
      val result1 = commitTransaction(txn, engine, actions)

      // ===== THEN =====
      assert(result1.getVersion == 1)

      val postCommitSnapshot1 = result1.getPostCommitSnapshot.get()
      verifyPostCommitSnapshotMatchesLatest(postCommitSnapshot1, tablePath, engine)
    }
  }
}
