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
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions._
import io.delta.kernel.expressions.Literal
import io.delta.kernel.internal.{SnapshotImpl, TableImpl, TransactionBuilderImpl, TransactionImpl}
import io.delta.kernel.internal.actions.{DomainMetadata, Protocol, SingleAction}
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.TableConfig.CHECKPOINT_INTERVAL
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.{DomainMetadata => SparkDomainMetadata}
import org.apache.spark.sql.delta.test.DeltaTestImplicits.OptimisticTxnTestHelper

import java.util.Collections
import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

class DomainMetadataSuite extends DeltaTableWriteSuiteBase with ParquetSuiteBase {

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

  private def createTxnWithDomainMetadatas(
      engine: Engine,
      tablePath: String,
      domainMetadatas: Seq[DomainMetadata]): Transaction = {

    val txnBuilder = createWriteTxnBuilder(TableImpl.forPath(engine, tablePath))
      .asInstanceOf[TransactionBuilderImpl]

    val txn = txnBuilder.build(engine).asInstanceOf[TransactionImpl]
    txn.addDomainMetadatas(domainMetadatas.asJava)
    txn
  }

  private def commitDomainMetadataAndVerify(
      engine: Engine,
      tablePath: String,
      domainMetadatas: Seq[DomainMetadata],
      expectedValue: Map[String, DomainMetadata]): Unit = {
    // Create the transaction with domain metadata and commit
    val txn = createTxnWithDomainMetadatas(engine, tablePath, domainMetadatas)
    txn.commit(engine, emptyIterable())

    // Verify the final state includes the expected domain metadata
    val table = Table.forPath(engine, tablePath)
    assertDomainMetadata(table, engine, expectedValue)
  }

  private def setDomainMetadataSupport(engine: Engine, tablePath: String): Unit = {
    val protocol = new Protocol(
      3, // minReaderVersion
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
    val txn1 = createTxnWithDomainMetadatas(engine, tablePath, currentTxn1DomainMetadatas)

    val txn2 = createTxnWithDomainMetadatas(engine, tablePath, winningTxn2DomainMetadatas)
    txn2.commit(engine, emptyIterable())

    val txn3 = createTxnWithDomainMetadatas(engine, tablePath, winningTxn3DomainMetadatas)
    txn3.commit(engine, emptyIterable())

    if (expectedConflict) {
      // We expect the commit of txn1 to fail because of the conflicting DM actions
      val ex = intercept[KernelException] {
        txn1.commit(engine, emptyIterable())
      }
      assert(
        ex.getMessage.contains(
          "A concurrent writer added a domainMetadata action for the same domain"
        )
      )
    } else {
      // We expect the commit of txn1 to succeed
      txn1.commit(engine, emptyIterable())
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
      val txn1 = createTxnWithDomainMetadatas(engine, tablePath, List(dm1))

      // We expect the commit to fail because the table doesn't support domain metadata
      val e = intercept[KernelException] {
        txn1.commit(engine, emptyIterable())
      }
      assert(
        e.getMessage
          .contains(
            "Cannot commit DomainMetadata action(s) because the feature 'domainMetadata' "
            + "is not supported on this table."
          )
      )

      // Set writer version and writer feature to support domain metadata
      setDomainMetadataSupport(engine, tablePath)

      // Commit domain metadata again and expect success
      commitDomainMetadataAndVerify(
        engine,
        tablePath,
        domainMetadatas = Seq(dm1),
        expectedValue = Map("domain1" -> dm1)
      )
    }
  }

  test("multiple DomainMetadatas for the same domain should fail in single transaction") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTableWithDomainMetadataSupported(engine, tablePath)

      val dm1_1 = new DomainMetadata("domain1", """{"key1":"1"}""", false)
      val dm2 = new DomainMetadata("domain2", "", false)
      val dm1_2 = new DomainMetadata("domain1", """{"key1":"10"}"""", false)

      val txn = createTxnWithDomainMetadatas(engine, tablePath, List(dm1_1, dm2, dm1_2))

      val e = intercept[IllegalArgumentException] {
        txn.commit(engine, emptyIterable())
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

      Seq(
        (Seq(dm1), Map("domain1" -> dm1)),
        (Seq(dm2, dm3, dm1_2), Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3)),
        (Seq(dm3_2), Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2))
      ).foreach {
        case (domainMetadatas, expectedValue) =>
          commitDomainMetadataAndVerify(engine, tablePath, domainMetadatas, expectedValue)
      }
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

      Seq(
        (Seq(dm1), Map("domain1" -> dm1)),
        (Seq(dm2), Map("domain1" -> dm1, "domain2" -> dm2)),
        (Seq(dm3), Map("domain1" -> dm1, "domain2" -> dm2, "domain3" -> dm3)),
        (Seq(dm1_2, dm3_2), Map("domain1" -> dm1_2, "domain2" -> dm2, "domain3" -> dm3_2))
      ).foreach {
        case (domainMetadatas, expectedValue) =>
          commitDomainMetadataAndVerify(engine, tablePath, domainMetadatas, expectedValue)
      }

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

  test("Integration test - create a table with Spark and read its domain metadata using Kernel") {
    withTempDir(dir => {
      val tbl = "tbl"
      withTable(tbl) {
        val tablePath = dir.getCanonicalPath
        // Create table with domain metadata enabled
        spark.sql(s"CREATE TABLE $tbl (id LONG) USING delta LOCATION '$tablePath'")
        spark.sql(
          s"ALTER TABLE $tbl SET TBLPROPERTIES(" +
          s"'delta.feature.domainMetadata' = 'enabled'," +
          s"'delta.checkpointInterval' = '3')"
        )

        // Manually commit domain metadata actions. This will create 02.json
        val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
        deltaLog
          .startTransaction()
          .commitManually(
            List(
              SparkDomainMetadata("testDomain1", "{\"key1\":\"1\"}", removed = false),
              SparkDomainMetadata("testDomain2", "", removed = false),
              SparkDomainMetadata("testDomain3", "", removed = false)
            ): _*
          )

        // This will create 03.json and 03.checkpoint
        spark.range(0, 2).write.format("delta").mode("append").save(tablePath)

        // Manually commit domain metadata actions. This will create 04.json
        deltaLog
          .startTransaction()
          .commitManually(
            List(
              SparkDomainMetadata("testDomain1", "{\"key1\":\"10\"}", removed = false),
              SparkDomainMetadata("testDomain2", "", removed = true)
            ): _*
          )

        // Use Delta Kernel to read the table's domain metadata and verify the result.
        // We will need to read 1 checkpoint file and 1 log file to replay the table.
        // The state of the domain metadata should be:
        // testDomain1: "{\"key1\":\"10\"}", removed = false  (from 03.checkpoint)
        // testDomain2: "", removed = true                    (from 03.checkpoint)
        // testDomain3: "", removed = false                   (from 04.json)

        val dm1 = new DomainMetadata("testDomain1", """{"key1":"10"}""", false)
        val dm2 = new DomainMetadata("testDomain2", "", true)
        val dm3 = new DomainMetadata("testDomain3", "", false)

        val snapshot = latestSnapshot(tablePath).asInstanceOf[SnapshotImpl]
        assertDomainMetadata(
          snapshot,
          Map("testDomain1" -> dm1, "testDomain2" -> dm2, "testDomain3" -> dm3)
        )
      }
    })
  }

  test("Integration test - create a table using Kernel and read its domain metadata using Spark") {
    withTempDirAndEngine { (tablePath, engine) =>
      val tbl = "tbl"
      withTable(tbl) {
        // Create table with domain metadata enabled
        createTableWithDomainMetadataSupported(engine, tablePath)

        // Manually commit three domain metadata actions
        val dm1 = new DomainMetadata("testDomain1", """{"key1":"1"}""", false)
        val dm2 = new DomainMetadata("testDomain2", "", false)
        val dm3 = new DomainMetadata("testDomain3", "", false)
        commitDomainMetadataAndVerify(
          engine,
          tablePath,
          domainMetadatas = Seq(dm1, dm2, dm3),
          expectedValue = Map("testDomain1" -> dm1, "testDomain2" -> dm2, "testDomain3" -> dm3)
        )

        appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> dataBatches1)
        )

        // Checkpoint the table so domain metadata is distributed to both checkpoint and log files
        val table = Table.forPath(engine, tablePath)
        val latestVersion = table.getLatestSnapshot(engine).getVersion(engine)
        table.checkpoint(engine, latestVersion)

        // Manually commit two domain metadata actions
        val dm1_2 = new DomainMetadata("testDomain1", """{"key1":"10"}""", false)
        val dm2_2 = new DomainMetadata("testDomain2", "", true)
        commitDomainMetadataAndVerify(
          engine,
          tablePath,
          domainMetadatas = Seq(dm1_2, dm2_2),
          expectedValue = Map("testDomain1" -> dm1_2, "testDomain2" -> dm2_2, "testDomain3" -> dm3)
        )

        // Use Spark to read the table's domain metadata and verify the result
        val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
        val domainMetadata = deltaLog.snapshot.domainMetadata.groupBy(_.domain).map {
          case (name, domains) =>
            assert(domains.size == 1)
            name -> domains.head
        }
        // Note that in Delta-Spark, the deltaLog.snapshot.domainMetadata does not include
        // domain metadata that are removed.
        assert(
          domainMetadata === Map(
            "testDomain1" -> SparkDomainMetadata(
              "testDomain1",
              """{"key1":"10"}""",
              removed = false
            ),
            "testDomain3" -> SparkDomainMetadata("testDomain3", "", removed = false)
          )
        )
      }
    }
  }
}
