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

import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Snapshot, TransactionCommitResult}
import io.delta.kernel.Snapshot.ChecksumWriteMode
import io.delta.kernel.defaults.utils.{TestRow, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{InternalScanFileUtils, SnapshotImpl, TableConfig}
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.test.MockEngineUtils
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.utils.CloseableIterable.inMemoryIterable

import org.scalatest.funsuite.AnyFunSuite

/**
 * Test suite for validating the behavior of postCommitSnapshot in various scenarios.
 *
 * Note that we use "PCS" in our test names for brevity.
 */
class PostCommitSnapshotSuite
    extends AnyFunSuite
    with WriteUtilsWithV2Builders
    with MockEngineUtils {

  //////////////////
  // Test Helpers //
  //////////////////

  private def assertAddFilesMatch(
      engine: Engine,
      actual: SnapshotImpl,
      expected: SnapshotImpl): Unit = {
    val actualFiles = collectScanFileRows(actual.getScanBuilder.build(), engine)
      .map(x => InternalScanFileUtils.getAddFileStatus(x).getPath)
    val expectedFiles = collectScanFileRows(expected.getScanBuilder.build(), engine)
      .map(x => InternalScanFileUtils.getAddFileStatus(x).getPath)

    assert(actualFiles === expectedFiles)
  }

  private def checkPostCommitSnapshot(
      engine: Engine,
      postCommitSnapshot: Snapshot,
      expectCrc: Boolean = false): Unit = {
    val actual = postCommitSnapshot.asInstanceOf[SnapshotImpl]
    val expected = latestSnapshot(actual.getPath, engine)

    if (expectCrc) {
      assert(actual.getCurrentCrcInfo.isPresent)
    }

    // TODO: We need better visibility into when the below information is loaded from the log,
    //       loaded from CRC, or already stored in memory (i.e. injected during post-commit snapshot
    //       creation)

    assert(actual.getVersion === expected.getVersion)
    assert(actual.getSchema === expected.getSchema)
    assert(actual.getProtocol === expected.getProtocol)
    assert(actual.getMetadata === expected.getMetadata)
    assert(actual.getPartitionColumnNames === expected.getPartitionColumnNames)
    assert(actual.getPhysicalClusteringColumns === expected.getPhysicalClusteringColumns)
    assert(actual.getTimestamp(engine) === expected.getTimestamp(engine))
    assert(actual.getActiveDomainMetadataMap === expected.getActiveDomainMetadataMap)

    assertAddFilesMatch(engine, actual, expected)
  }

  ////////////////////////////
  // Create new table tests //
  ////////////////////////////

  test("creating a new empty table => yields a PCS") {
    withTempDirAndEngine { (tablePath, engine) =>
      val result = createEmptyTable(engine, tablePath, testSchema)

      checkPostCommitSnapshot(engine, result.getPostCommitSnapshot.get(), expectCrc = true)
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

      checkPostCommitSnapshot(engine, result.getPostCommitSnapshot.get(), expectCrc = true)
    }
  }

  /////////////////////////
  // CRC existence tests //
  /////////////////////////

  test("commit at readVersion + 1 (*with* CRC at readVersion) => yields a PCS with CRC") {
    withTempDirAndEngine { (tablePath, engine) =>
      val result0 = createEmptyTable(engine, tablePath, testSchema)
      result0.getPostCommitSnapshot.get().writeChecksum(engine, ChecksumWriteMode.SIMPLE)
      assert(latestSnapshot(tablePath, engine).getCurrentCrcInfo.isPresent)

      val result1 = appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch1)

      checkPostCommitSnapshot(engine, result1.getPostCommitSnapshot.get(), expectCrc = true)
    }
  }

  test("commit at readVersion + 1 (*without* CRC at readVersion) => yields a PCS without CRC") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)

      val result1 = appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch1)

      checkPostCommitSnapshot(engine, result1.getPostCommitSnapshot.get(), expectCrc = false)
    }
  }

  test("commit at readVersion + 2 => does NOT yield a PCS") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      createEmptyTable(engine, tablePath, testSchema) // V0

      val txn = getUpdateTxn(engine, tablePath) // Create a transaction that reads at V0
      assert(txn.getReadTableVersion == 0)

      // Create winning commits at V1 and V2
      appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch1) // V1
      appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch2) // V2
      assert(latestSnapshot(tablePath, engine).getVersion == 2)

      // ===== WHEN =====
      // Now commit the original txn that read at v0. This will commit at v3
      val txnState = txn.getTransactionState(engine)
      val actions = inMemoryIterable(stageData(txnState, Map.empty[String, Literal], dataBatches1))
      val result = commitTransaction(txn, engine, actions) // V3

      // ===== THEN =====
      assert(result.getVersion == 3)
      assert(!result.getPostCommitSnapshot.isPresent)
    }
  }

  ////////////////////////////////////////////////////////////
  // PostCommitSnapshot has certain fields pre-loaded tests //
  ////////////////////////////////////////////////////////////

  test("PCS has ICT pre-loaded") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = Map(TableConfig.IN_COMMIT_TIMESTAMPS_ENABLED.getKey -> "true"))

      val result = appendData(engine, tablePath, data = seqOfUnpartitionedDataBatch1)
      val postCommitSnapshot = result.getPostCommitSnapshot.get().asInstanceOf[SnapshotImpl]

      val failingEngine = mockEngine()

      // should *not* use the engine to try and read ICT from delta file
      postCommitSnapshot.getTimestamp(failingEngine)
    }
  }

  // TODO: Test CRC is also pre-loaded. Requires
  //       (1) SnapshotImpl::getCurrentCrcInfo to take in an engine param
  //       (2) LogReplay to *not* be injected into SnapshotImpl constructor
  //       (3) CRC to be injected into SnapshotImpl constructor

  // TODO: Test clusteringColumns are pre-loaded (when txn sets new clustering columns)

  ///////////////////////////
  // Metadata change tests //
  ///////////////////////////

  case class MetadataChangeTestCase(
      changeType: String,
      initTableProperties: Map[String, String] = Map.empty,
      updateFn: (Engine, String) => TransactionCommitResult)

  Seq(
    MetadataChangeTestCase(
      changeType = "schema",
      initTableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"),
      updateFn = (engine, tablePath) => {
        val newSchema = latestSnapshot(tablePath, engine).getSchema.add("newCol", INTEGER)
        updateTableMetadata(engine, tablePath, schema = newSchema)
      }),
    MetadataChangeTestCase(
      changeType = "tbl property",
      updateFn = (engine, tablePath) =>
        updateTableMetadata(engine, tablePath, tableProperties = Map("foo" -> "bar"))),
    MetadataChangeTestCase(
      changeType = "protocol",
      updateFn = (engine, tablePath) => {
        val snapshot = latestSnapshot(tablePath, engine)
        assert(!snapshot.getProtocol.getWriterFeatures.contains("deletionVectors"))

        updateTableMetadata(
          engine,
          tablePath,
          tableProperties = Map("delta.enableDeletionVectors" -> "true"))
      }),
    MetadataChangeTestCase(
      changeType = "clustering columns",
      updateFn = (engine, tablePath) =>
        updateTableMetadata(
          engine,
          tablePath,
          clusteringColsOpt = Some(testClusteringColumns)))).foreach {
    case MetadataChangeTestCase(changeType, initTableProperties, updateFn) =>
      test(
        s"commit $changeType change at readVersion + 1 => yields a PCS with updated $changeType") {
        withTempDirAndEngine { (tablePath, engine) =>
          createEmptyTable(
            engine,
            tablePath,
            schema = testPartitionSchema,
            tableProperties = initTableProperties)

          val result = updateFn(engine, tablePath)

          checkPostCommitSnapshot(engine, result.getPostCommitSnapshot.get())
        }
      }
  }

  ////////////////////////////////////////////////////////////////
  // Using PostCommitSnapshot to read data and write data tests //
  ////////////////////////////////////////////////////////////////

  test("postCommitSnapshot can be used to read data") {
    withTempDirAndEngine { (tablePath, engine) =>
      val result = appendData(
        engine,
        tablePath,
        isNewTable = true,
        schema = testSchema,
        data = seqOfUnpartitionedDataBatch1)

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
      val postCommitSnapshot0 = result0.getPostCommitSnapshot.get()
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
      checkPostCommitSnapshot(engine, result1.getPostCommitSnapshot.get())
    }
  }

  ////////////////
  // Publishing //
  ////////////////

  test("publishing on a postCommitSnapshot for a filesystem-based table is a no-op") {
    withTempDirAndEngine { (tablePath, engine) =>
      val result = appendData(
        engine,
        tablePath,
        isNewTable = true,
        schema = testSchema,
        data = seqOfUnpartitionedDataBatch1)

      val postCommitSnapshot = result.getPostCommitSnapshot.get().asInstanceOf[SnapshotImpl]

      assert(!TableFeatures.isCatalogManagedSupported(postCommitSnapshot.getProtocol))

      postCommitSnapshot.publish(engine) // Should not throw -- there are no catalog commits!
    }
  }
}
