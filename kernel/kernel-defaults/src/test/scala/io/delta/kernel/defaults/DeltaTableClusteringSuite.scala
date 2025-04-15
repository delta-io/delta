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

import io.delta.kernel.{Table, Transaction, TransactionCommitResult}
import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel.data.Row
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, TableAlreadyExistsException}
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.expressions.Literal.ofInt
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.DomainMetadata
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.types.{MapType, StructType}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.{ClusteringMetadataDomain => SparkClusteringMetadataDomain}

import org.apache.hadoop.fs.Path

class DeltaTableClusteringSuite extends DeltaTableWriteSuiteBase with CrcTestUtils {

  private val testingDomainMetadata = new DomainMetadata(
    "delta.clustering",
    """{"clusteringColumns":[["part1"],["part2"]]}""",
    false)

  private def verifyClusteringDomainMetadata(
      snapshot: SnapshotImpl,
      expectedDomainMetadata: DomainMetadata = testingDomainMetadata): Unit = {
    assert(snapshot.getDomainMetadataMap.get(ClusteringMetadataDomain.DOMAIN_NAME)
      == expectedDomainMetadata)
    // verifyChecksumValid will check the domain metadata in CRC against the latest snapshot.
    verifyChecksumValid(snapshot.getDataPath.toString)
  }

  override def commitTransaction(
      txn: Transaction,
      engine: Engine,
      dataActions: CloseableIterable[Row]): TransactionCommitResult = {
    withCrcSimpleExecuted(txn.commit(engine, dataActions), engine)
  }

  test("build table txn: clustering column should be part of the schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[KernelException] {
        createTxn(
          engine,
          tablePath,
          isNewTable = true,
          testPartitionSchema,
          clusteringCols = List(new Column("PART1"), new Column("part3")))
      }
      assert(ex.getMessage.contains("Column 'column(`part3`)' was not found in the table schema"))
    }
  }

  test("build table txn: " +
    "clustering column and partition column cannot be set at same time") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[IllegalArgumentException] {
        createTxn(
          engine,
          tablePath,
          isNewTable = true,
          testPartitionSchema,
          partCols = Seq("part1"),
          clusteringCols = List(new Column("PART1"), new Column("part2")))
      }
      assert(
        ex.getMessage
          .contains("Partition Columns and Clustering Columns cannot be set at the same time"))
    }
  }

  test("build table txn: clustering column should be data skipping supported data type") {
    withTempDirAndEngine { (tablePath, engine) =>
      val testPartitionSchema = new StructType()
        .add("id", INTEGER)
        .add("part1", INTEGER) // partition column
        .add("mapType", new MapType(INTEGER, INTEGER, false));
      val ex = intercept[KernelException] {
        createTxn(
          engine,
          tablePath,
          isNewTable = true,
          testPartitionSchema,
          clusteringCols = List(new Column("mapType")))
      }
      assert(ex.getMessage.contains("Clustering is not supported because the following column(s)"))
    }
  }

  test("create a clustered table should succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringCols = testClusteringColumns)

      val table = Table.forPath(engine, tablePath)
      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")

      // Verify the clustering domain metadata is written
      verifyClusteringDomainMetadata(snapshot)

      // Use Spark to read the table's clustering metadata domain and verify the result
      val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
      val clusteringMetadataDomainRead =
        SparkClusteringMetadataDomain.fromSnapshot(deltaLog.snapshot)
      assert(clusteringMetadataDomainRead.exists(_.clusteringColumns === Seq(
        Seq("part1"),
        Seq("part2"))))
    }
  }

  test("clustering column should store as physical name with column mapping") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringCols = testClusteringColumns,
        tableProperties = Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id"))

      val table = Table.forPath(engine, tablePath)
      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")

      // Verify the clustering domain metadata is written
      val schema = table.getLatestSnapshot(engine).getSchema
      val col1 = schema.get("part1").getMetadata.get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)
      val col2 = schema.get("part2").getMetadata.get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY)
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        s"""{"clusteringColumns":[["$col1"],["$col2"]]}""",
        false)
      verifyClusteringDomainMetadata(snapshot, expectedDomainMetadata)
    }
  }

  test("create a clustered table should succeed with column case matches schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringCols = List(new Column("pArT1"), new Column("PaRt2")))

      val table = Table.forPath(engine, tablePath)
      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")

      // Verify the clustering domain metadata is written
      verifyClusteringDomainMetadata(snapshot)
    }
  }

  test("update a table with clustering columns should be blocked") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testPartitionSchema)
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, WRITE)

      val ex = intercept[TableAlreadyExistsException] {
        txnBuilder
          .withClusteringColumns(engine, testClusteringColumns.asJava)
          .build(engine)
      }
      assert(
        ex.getMessage
          .contains("Table already exists, but provided new clustering columns"))
    }
  }

  test("insert into clustered table - table create from scratch") {
    withTempDirAndEngine { (tablePath, engine) =>
      val testData = Seq(Map.empty[String, Literal] -> dataClusteringBatches1)

      val commitResult = appendData(
        engine,
        tablePath,
        isNewTable = true,
        testPartitionSchema,
        clusteringCols = testClusteringColumns,
        data = testData)

      verifyCommitResult(commitResult, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tablePath, version = 0, operation = WRITE)
      verifyWrittenContent(
        tablePath,
        testPartitionSchema,
        dataClusteringBatches1.flatMap(_.toTestRows))

      val table = Table.forPath(engine, tablePath)
      verifyClusteringDomainMetadata(table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl])
    }
  }

  test("insert into clustered table - already existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      {
        val commitResult0 = appendData(
          engine,
          tablePath,
          isNewTable = true,
          testPartitionSchema,
          clusteringCols = testClusteringColumns,
          data = Seq(Map.empty[String, Literal] -> dataClusteringBatches1))

        val expData = dataClusteringBatches1.flatMap(_.toTestRows)

        verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath, version = 0, operation = WRITE)
        verifyWrittenContent(tablePath, testPartitionSchema, expData)
        verifyClusteringDomainMetadata(table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl])
      }
      {
        val commitResult1 = appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> dataClusteringBatches2))

        val expData = dataClusteringBatches1.flatMap(_.toTestRows) ++
          dataClusteringBatches2.flatMap(_.toTestRows)

        verifyCommitResult(commitResult1, expVersion = 1, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath, version = 1, partitionCols = null, operation = WRITE)
        verifyWrittenContent(tablePath, testPartitionSchema, expData)
        verifyClusteringDomainMetadata(table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl])
      }
    }
  }
}
