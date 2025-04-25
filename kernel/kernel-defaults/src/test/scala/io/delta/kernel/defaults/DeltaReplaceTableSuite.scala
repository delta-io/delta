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

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.{Operation, Table, Transaction}
import io.delta.kernel.data.{FilteredColumnarBatch, Row}
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.{SnapshotImpl, TableImpl}
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.{ClusteringMetadataDomain => SparkClusteringMetadataDomain}

class DeltaReplaceTableSuite extends DeltaTableWriteSuiteBase {

  def getAppendActions(
      txn: Transaction,
      data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])]): CloseableIterable[Row] = {

    val txnState = txn.getTransactionState(defaultEngine)

    val actions = data.map { case (partValues, partData) =>
      stageData(txnState, partValues, partData)
    }

    actions.reduceLeftOption(_ combine _) match {
      case Some(combinedActions) =>
        inMemoryIterable(combinedActions)
      case None =>
        emptyIterable[Row]
    }
  }

  def verifyClusteringColumns(tablePath: String, clusteringCols: Seq[String]): Unit = {
    val table = Table.forPath(defaultEngine, tablePath)
    // Verify the clustering feature is included in the protocol
    val snapshot = table.getLatestSnapshot(defaultEngine).asInstanceOf[SnapshotImpl]
    assertHasWriterFeature(snapshot, "clustering")

    // Verify the clustering columns using Spark
    val deltaLog = DeltaLog.forTable(spark, new org.apache.hadoop.fs.Path(tablePath))
    val clusteringMetadataDomainRead =
      SparkClusteringMetadataDomain.fromSnapshot(deltaLog.update())
    assert(clusteringMetadataDomainRead.exists(_.clusteringColumns ===
      clusteringCols.map(Seq(_))))
  }

  def verifyNoClustering(tablePath: String): Unit = {
    val deltaLog = DeltaLog.forTable(spark, new org.apache.hadoop.fs.Path(tablePath))
    val clusteringMetadataDomainRead =
      SparkClusteringMetadataDomain.fromSnapshot(deltaLog.update())
    assert(clusteringMetadataDomainRead.isEmpty)
  }

  test("replace with empty table") {
    withTempDirAndEngine { (tblPath, engine) =>
      appendData(
        engine,
        tblPath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1)))

      checkTable(tblPath, dataBatches1.flatMap(_.toTestRows))

      // Change the schema and partition columns
      Table.forPath(engine, tblPath).asInstanceOf[TableImpl]
        .createReplaceTableTransactionBuilder(engine, "test-info")
        .withSchema(engine, testPartitionSchema)
        .withPartitionColumns(engine, testPartitionColumns.asJava)
        .build(engine)
        .commit(engine, emptyIterable())

      // Table should be empty
      checkTable(tblPath, Seq(), expectedSchema = testPartitionSchema, expectedVersion = Some(1))
    }
  }

  test("replace with CTAS") {
    withTempDirAndEngine { (tblPath, engine) =>
      appendData(
        engine,
        tblPath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1)))

      checkTable(tblPath, dataBatches1.flatMap(_.toTestRows))

      // Replace with new data
      val txn = Table.forPath(engine, tblPath).asInstanceOf[TableImpl]
        .createReplaceTableTransactionBuilder(engine, "test-info")
        .withSchema(engine, testSchema)
        .build(engine)
      txn.commit(engine, getAppendActions(txn, Seq(Map.empty[String, Literal] -> (dataBatches2))))

      // Table should be not be empty
      checkTable(
        tblPath,
        dataBatches2.flatMap(_.toTestRows),
        expectedSchema = testSchema,
        expectedVersion = Some(1))
    }
  }

  test("replace clustering table with clustering table") {
    withTempDirAndEngine { (tblPath, engine) =>
      appendData(
        engine,
        tblPath,
        isNewTable = true,
        testPartitionSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataClusteringBatches1)),
        clusteringColsOpt = Some(testClusteringColumns))

      checkTable(tblPath, dataClusteringBatches1.flatMap(_.toTestRows))
      verifyClusteringColumns(tblPath, Seq("part1", "part2"))

      // Change the clustering columns
      val txn = Table.forPath(engine, tblPath).asInstanceOf[TableImpl]
        .createReplaceTableTransactionBuilder(engine, "test-info")
        .withSchema(engine, testPartitionSchema)
        .withClusteringColumns(engine, Seq(new Column("id"), new Column("part1")).asJava)
        .build(engine)
      txn.commit(
        engine,
        getAppendActions(txn, Seq(Map.empty[String, Literal] -> (dataClusteringBatches2))))

      // Table should be not be empty
      checkTable(
        tblPath,
        dataClusteringBatches2.flatMap(_.toTestRows),
        expectedSchema = testPartitionSchema,
        expectedVersion = Some(1))
      verifyClusteringColumns(tblPath, Seq("id", "part1"))
    }
  }

  test("replace unclustered table with clustering table") {
    withTempDirAndEngine { (tblPath, engine) =>
      appendData(
        engine,
        tblPath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1)))

      checkTable(tblPath, dataBatches1.flatMap(_.toTestRows))
      verifyNoClustering(tblPath)

      // Add clustering cols + change schema + write data
      val txn = Table.forPath(engine, tblPath).asInstanceOf[TableImpl]
        .createReplaceTableTransactionBuilder(engine, "test-info")
        .withSchema(engine, testPartitionSchema)
        .withClusteringColumns(engine, Seq(new Column("id"), new Column("part1")).asJava)
        .build(engine)
      txn.commit(
        engine,
        getAppendActions(txn, Seq(Map.empty[String, Literal] -> (dataClusteringBatches2))))

      // Table should be not be empty
      checkTable(
        tblPath,
        dataClusteringBatches2.flatMap(_.toTestRows),
        expectedSchema = testPartitionSchema,
        expectedVersion = Some(1))
      verifyClusteringColumns(tblPath, Seq("id", "part1"))
    }
  }

  test("replace clustering table with unclustered table") {
    withTempDirAndEngine { (tblPath, engine) =>
      appendData(
        engine,
        tblPath,
        isNewTable = true,
        testPartitionSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataClusteringBatches1)),
        clusteringColsOpt = Some(testClusteringColumns))

      checkTable(tblPath, dataClusteringBatches1.flatMap(_.toTestRows))
      verifyClusteringColumns(tblPath, Seq("part1", "part2"))

      // Replace with unclustered table and write data
      val txn = Table.forPath(engine, tblPath).asInstanceOf[TableImpl]
        .createReplaceTableTransactionBuilder(engine, "test-info")
        .withSchema(engine, testSchema)
        .build(engine)
      txn.commit(
        engine,
        getAppendActions(txn, Seq(Map.empty[String, Literal] -> (dataBatches1))))

      // Table should be not be empty
      checkTable(
        tblPath,
        dataBatches1.flatMap(_.toTestRows),
        expectedSchema = testSchema)
      verifyNoClustering(tblPath)
    }
  }

  test("replace with domain metadata for same domain every txn") {
    // (1) checks that we don't remove plus add new one in replace txn (duplicate domain in 1 txn)
    // (2) we remove stale ones that are not touched
    withTempDirAndEngine { (tblPath, engine) =>
      // Initial table with domain foo
      val txn1 = Table.forPath(engine, tblPath).asInstanceOf[TableImpl]
        .createTransactionBuilder(engine, "test-info", Operation.CREATE_TABLE)
        .withSchema(engine, testSchema)
        .withDomainMetadataSupported()
        .build(engine)
      txn1.addDomainMetadata("foo", "check1")
      txn1.addDomainMetadata("foo2", "check2")
      txn1.commit(
        engine,
        getAppendActions(txn1, Seq(Map.empty[String, Literal] -> (dataBatches1))))

      val snapshot1 = Table.forPath(engine, tblPath).getLatestSnapshot(engine)
      assert(snapshot1.getDomainMetadata("foo").toScala.contains("check1"))
      assert(snapshot1.getDomainMetadata("foo2").toScala.contains("check2"))

      // Replace table and commit same domain
      val txn2 = Table.forPath(engine, tblPath).asInstanceOf[TableImpl]
        .createReplaceTableTransactionBuilder(engine, "test-info")
        .withSchema(engine, testSchema)
        .withDomainMetadataSupported()
        .build(engine)
      txn2.addDomainMetadata("foo", "check2")
      txn2.commit(
        engine,
        getAppendActions(txn2, Seq(Map.empty[String, Literal] -> (dataBatches1))))
      val snapshot2 = Table.forPath(engine, tblPath).getLatestSnapshot(engine)
      assert(snapshot2.getDomainMetadata("foo").toScala.contains("check2"))
      assert(!snapshot2.getDomainMetadata("foo2").isPresent)
    }
  }

  // TODO test removes are correctly created (incl with DVs, partitionValues, other fields?)
  // TODO tests with column mapping
  // TODO validates inputs correctly (i.e. requires a schema is provided)
  //  and other stuff in txnBuilder checks
  // TODO you can do things like enable CM mode ID, enable icebergWriterCompatV1
  // TODO correct protocol (doesn't downgrade, includes any new features / upgrades)
  // TODO test in TransactionReportSuite

  // and I'm sure plenty more tests.. just noting down things as I think of them
}
