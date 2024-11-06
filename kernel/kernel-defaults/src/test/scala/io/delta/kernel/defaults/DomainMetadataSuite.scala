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

import io.delta.kernel._
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions._
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.{DomainMetadata, Protocol, SingleAction}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.golden.GoldenTableUtils.goldenTablePath

import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

class DomainMetadataSuite extends DeltaTableWriteSuiteBase with ParquetSuiteBase {

  private def generateDMActionsForCommit(actions: Seq[DomainMetadata]): CloseableIterable[Row] = {
    val actionsIter = actions
      .map(dmAction => SingleAction.createDomainMetadataSingleAction(dmAction.toRow))
      .asJava
      .iterator

    inMemoryIterable(toCloseableIterator(actionsIter))
  }

  private def assertDomainMetadata(
      snapshot: SnapshotImpl,
      expectedValue: Map[String, DomainMetadata]): Unit = {
    assert(expectedValue === snapshot.getDomainMetadataMap.asScala)
  }

  private def assertDomainMetadata(
      table: Table,
      engine: Engine,
      expectedValue: Map[String, DomainMetadata]): Unit = {
    // Get the latest snapshot of the table
    val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
    assertDomainMetadata(snapshot, expectedValue)
  }

  private def commitDomainMetadataAndVerify(
      engine: Engine,
      tablePath: String,
      isNewTable: Boolean,
      domainMetadatas: Seq[DomainMetadata],
      expectedValue: Map[String, DomainMetadata]): Unit = {
    val table = Table.forPath(engine, tablePath)
    createTxn(engine, tablePath, isNewTable, testSchema, Seq.empty)
      .commit(engine, generateDMActionsForCommit(domainMetadatas))

    assertDomainMetadata(table, engine, expectedValue)
  }

  private def setDomainMetadataSupport(engine: Engine, tablePath: String): Unit = {
    val protocol = new Protocol(
      1, // minReaderVersion
      7, // minWriterVersion
      Collections.emptyList(), // readerFeatures
      Seq("domainMetadata").asJava // writerFeatures
    )

    val protocolAction = SingleAction.createProtocolSingleAction(protocol.toRow)
    val txn = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)
    txn.commit(engine, inMemoryIterable(toCloseableIterator(Seq(protocolAction).asJava.iterator())))
  }

  private def createTableWithDomainMetadataSupported(engine: Engine, tablePath: String): Unit = {
    // Create an empty table
    createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
      .commit(engine, emptyIterable())

    // Set writer version and writer feature to support domain metadata
    setDomainMetadataSupport(engine, tablePath)
  }

  private def validateDomainMetadataConflictResolution(
      engine: Engine,
      tablePath: String,
      currentTxn1DomainMetadatas: Seq[DomainMetadata],
      winningTxn2DomainMetadatas: Seq[DomainMetadata],
      winningTxn3DomainMetadatas: Seq[DomainMetadata],
      expectedConflict: Boolean): Unit = {
    // Create table with domain metadata support
    createTableWithDomainMetadataSupported(engine, tablePath)
    val table = Table.forPath(engine, tablePath)

    /**
     * Txn1: i.e. the current transaction that comes later than winning transactions.
     * Txn2: i.e. the winning transaction that was committed first.
     * Txn3: i.e. the winning transaction that was committed secondly.
     *
     * Note tx is the timestamp.
     *
     * t1 ------------------------ Txn1 starts.
     * t2 ------- Txn2 starts.
     * t3 ------- Txn2 commits.
     * t4 ------- Txn3 starts.
     * t5 ------- Txn3 commits.
     * t6 ------------------------ Txn1 commits (SUCCESS or FAIL).
     */
    val txn1 = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)

    val txn2 = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)
    txn2.commit(engine, generateDMActionsForCommit(winningTxn2DomainMetadatas))

    val txn3 = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)
    txn3.commit(engine, generateDMActionsForCommit(winningTxn3DomainMetadatas))

    if (expectedConflict) {
      // We expect the commit of txn1 to fail because of the conflicting DM actions
      val ex = intercept[KernelException] {
        txn1.commit(engine, generateDMActionsForCommit(currentTxn1DomainMetadatas))
      }
      assert(
        ex.getMessage.contains(
          "A concurrent writer added a domainMetadata action for the same domain"
        )
      )
    } else {
      // We expect the commit of txn1 to succeed
      txn1.commit(engine, generateDMActionsForCommit(currentTxn1DomainMetadatas))
      // Verify the final state includes merged domain metadata
      val expectedMetadata =
        (winningTxn2DomainMetadatas ++ winningTxn3DomainMetadatas ++ currentTxn1DomainMetadatas)
          .groupBy(_.getDomain)
          .mapValues(_.last)
      assertDomainMetadata(table, engine, expectedMetadata)
    }
  }

  test("create table w/o domain metadata") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      // Create an empty table
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      // Verify that the table doesn't have any domain metadata
      assertDomainMetadata(table, engine, Map.empty)
    }
  }

  test("table w/o domain metadata support fails domain metadata commits") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create an empty table
      // Its minWriterVersion is 2 and doesn't have 'domainMetadata' in its writerFeatures
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      val dm1 = new DomainMetadata("domain1", "", false)
      val txn1 = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)

      // We expect the commit to fail because the table doesn't support domain metadata
      val e = intercept[KernelException] {
        txn1.commit(engine, generateDMActionsForCommit(List(dm1)))
      }
      assert(
        e.getMessage
          .contains(
            "Found DomainMetadata action(s) but table feature 'domainMetadata' "
            + "is not supported on this table."
          )
      )

      // Set writer version and writer feature to support domain metadata
      setDomainMetadataSupport(engine, tablePath)

      // Commit domain metadata again and expect success
      val txn2 = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)
      txn2.commit(engine, generateDMActionsForCommit(List(dm1)))
    }
  }

  test("multiple DomainMetadatas for the same domain should fail in single transaction") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1_1 = new DomainMetadata("domain1", """{"key1":"1"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}"""", false)

      val txn = createTxn(engine, tablePath, isNewTable = false, testSchema, Seq.empty)

      val e = intercept[KernelException] {
        txn.commit(engine, generateDMActionsForCommit(List(dm1_1, dm2, dm1_2)))
      }
      assert(
        e.getMessage.contains(
          "Multiple actions detected for domain 'domain1' in single transaction"
        )
      )
    }
  }

  test("latest domain metadata overwriting existing ones") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}, {"key2":"2"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain3", """{"key3":"3"}""", false)

      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}""", false)
      val dm3_2 = new DomainMetadata("domain3", """{"key3":"30"}""", false)

      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm1),
        expectedValue = Map("domain1" -> dm1)
      )

      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm2, dm3, dm1_2),
        expectedValue = Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3)
      )

      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm3_2),
        expectedValue = Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2)
      )
    }
  }

  test("domain metadata persistence across log replay") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}, {"key2":"2"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)

      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm1, dm2),
        expectedValue = Map("domain1" -> dm1, "domain2" -> dm2)
      )

      // Restart the table and verify the domain metadata
      val table2 = Table.forPath(engine, tablePath)
      assertDomainMetadata(table2, engine, Map("domain1" -> dm1, "domain2" -> dm2))
    }
  }

  test("only the latest domain metadata per domain is stored in checkpoints") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1 = new DomainMetadata("domain1", """{"key1":"1"}, {"key2":"2"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain3", """{"key3":"3"}""", false)
      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}""", false)
      val dm3_2 = new DomainMetadata("domain3", """{"key3":"30"}""", true)

      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm1),
        expectedValue = Map("domain1" -> dm1)
      )
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm2),
        expectedValue = Map("domain1" -> dm1, "domain2" -> dm2)
      )
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm3),
        expectedValue = Map("domain1" -> dm1, "domain2" -> dm2, "domain3" -> dm3)
      )
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        isNewTable = false,
        domainMetadatas = Seq(dm1_2, dm3_2),
        expectedValue = Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2)
      )

      // Checkpoint the table
      val latestVersion = table.getLatestSnapshot(engine).getVersion(engine)
      table.checkpoint(engine, latestVersion)

      // Verify that only the latest domain metadata is persisted in the checkpoint
      val table2 = Table.forPath(engine, tablePath)
      assertDomainMetadata(
        table2,
        engine,
        Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2)
      )
    }
  }

  test("Conflict resolution - one of three concurrent txns has DomainMetadata") {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action.
       * Txn2: does NOT include DomainMetadata action.
       * Txn3: does NOT include DomainMetadata action.
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (SUCCESS).
       */
      val dm1 = new DomainMetadata("domain1", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq.empty,
        winningTxn3DomainMetadatas = Seq.empty,
        expectedConflict = false
      )
    }
  }

  test(
    "Conflict resolution - three concurrent txns have DomainMetadata w/o conflicting domains"
  ) {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action for "domain1".
       * Txn2: include DomainMetadata action for "domain2".
       * Txn3: include DomainMetadata action for "domain3".
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (SUCCESS).
       */
      val dm1 = new DomainMetadata("domain1", "", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain3", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq(dm2),
        winningTxn3DomainMetadatas = Seq(dm3),
        expectedConflict = false
      )
    }
  }

  test(
    "Conflict resolution - three concurrent txns have DomainMetadata w/ conflicting domains"
  ) {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action for "domain1".
       * Txn2: include DomainMetadata action for "domain2".
       * Txn3: include DomainMetadata action for "domain1".
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (FAIL).
       */
      val dm1 = new DomainMetadata("domain1", "", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm3 = new DomainMetadata("domain1", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq(dm2),
        winningTxn3DomainMetadatas = Seq(dm3),
        expectedConflict = true
      )
    }
  }

  test(
    "Conflict resolution - three concurrent txns have DomainMetadata w/ conflict domains - 2"
  ) {
    withTempDirAndEngine { (tablePath, engine) =>
      /**
       * Txn1: include DomainMetadata action for "domain1".
       * Txn2: include DomainMetadata action for "domain1".
       * Txn3: include DomainMetadata action for "domain2".
       *
       * t1 ------------------------ Txn1 starts.
       * t2 ------- Txn2 starts.
       * t3 ------- Txn2 commits.
       * t4 ------- Txn3 starts.
       * t5 ------- Txn3 commits.
       * t6 ------------------------ Txn1 commits (FAIL).
       */
      val dm1 = new DomainMetadata("domain1", "", false)
      val dm2 = new DomainMetadata("domain1", "", false)
      val dm3 = new DomainMetadata("domain2", "", false)

      validateDomainMetadataConflictResolution(
        engine,
        tablePath,
        currentTxn1DomainMetadatas = Seq(dm1),
        winningTxn2DomainMetadatas = Seq(dm2),
        winningTxn3DomainMetadatas = Seq(dm3),
        expectedConflict = true
      )
    }
  }

  test("Integration test - read a golden table with checkpoints and log files") {
    withTempDirAndEngine((tablePath, engine) => {
      val path = goldenTablePath("kernel-domain-metadata")
      val snapshot = latestSnapshot(path).asInstanceOf[SnapshotImpl]

      // We need to read 1 checkpoint file and 1 log file to replay the golden table
      // The state of the domain metadata should be:
      // testDomain1: "{\"key1\":\"10\"}", removed = false  (from 03.checkpoint)
      // testDomain2: "", removed = true                    (from 03.checkpoint)
      // testDomain3: "", removed = false                   (from 04.json)

      val dm1 = new DomainMetadata("testDomain1", """{"key1":"10"}""", false)
      val dm2 = new DomainMetadata("testDomain2", "", true)
      val dm3 = new DomainMetadata("testDomain3", "", false)

      assertDomainMetadata(
        snapshot,
        Map("testDomain1" -> dm1, "testDomain2" -> dm2, "testDomain3" -> dm3)
      )
    })
  }

}
