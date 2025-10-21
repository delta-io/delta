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
import io.delta.kernel.defaults.utils.{AbstractWriteUtils, WriteUtils, WriteUtilsWithV2Builders}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, TableAlreadyExistsException}
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.expressions.Literal.ofInt
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.DomainMetadata
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.internal.util.ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY
import io.delta.kernel.types.{MapType, StructType}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.emptyIterable

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.{ClusteringMetadataDomain => SparkClusteringMetadataDomain}

import org.apache.hadoop.fs.Path
import org.scalatest.funsuite.AnyFunSuite

class DeltaTableClusteringTransactionBuilderV1Suite extends DeltaTableClusteringSuiteBase
    with WriteUtils {

  // It is not possible on an API level to set both clustering and partition columns in V2 builders
  test("build table txn: " +
    "clustering column and partition column cannot be set at same time") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[IllegalArgumentException] {
        getCreateTxn(
          engine,
          tablePath,
          testPartitionSchema,
          partCols = Seq("part1"),
          clusteringColsOpt = Some(List(new Column("PART1"), new Column("part2"))))
      }
      assert(
        ex.getMessage
          .contains("Partition Columns and Clustering Columns cannot be set at the same time"))
    }
  }
}

class DeltaTableClusteringTransactionBuilderV2Suite extends DeltaTableClusteringSuiteBase
    with WriteUtilsWithV2Builders {}

trait DeltaTableClusteringSuiteBase extends AnyFunSuite with AbstractWriteUtils {

  private val testingDomainMetadata = new DomainMetadata(
    "delta.clustering",
    """{"clusteringColumns":[["part1"],["part2"]]}""",
    false)

  override def commitTransaction(
      txn: Transaction,
      engine: Engine,
      dataActions: CloseableIterable[Row]): TransactionCommitResult = {
    executeCrcSimple(txn.commit(engine, dataActions), engine)
  }

  private def verifyClusteringDMAndCRC(
      snapshot: SnapshotImpl,
      expectedDomainMetadata: DomainMetadata): Unit = {
    verifyClusteringDomainMetadata(snapshot, expectedDomainMetadata)
    // verifyChecksum will check the domain metadata in CRC against the latest snapshot.
    verifyChecksum(snapshot.getDataPath.toString)
  }

  test("build table txn: clustering column should be part of the schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[KernelException] {
        getCreateTxn(
          engine,
          tablePath,
          testPartitionSchema,
          clusteringColsOpt = Some(List(new Column("PART1"), new Column("part3"))))
      }
      assert(ex.getMessage.contains("Column 'column(`part3`)' was not found in the table schema"))
    }
  }

  test("build table txn: clustering column should be data skipping supported data type") {
    withTempDirAndEngine { (tablePath, engine) =>
      val testPartitionSchema = new StructType()
        .add("id", INTEGER)
        .add("part1", INTEGER) // partition column
        .add("mapType", new MapType(INTEGER, INTEGER, false));
      val ex = intercept[KernelException] {
        getCreateTxn(
          engine,
          tablePath,
          testPartitionSchema,
          clusteringColsOpt = Some(List(new Column("mapType"))))
      }
      assert(ex.getMessage.contains("Clustering is not supported because the following column(s)"))
    }
  }

  test("create a clustered table should succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      val commitResult = createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(testClusteringColumns))

      assertCommitResultHasClusteringCols(
        commitResult,
        expectedClusteringCols = testClusteringColumns)

      val table = Table.forPath(engine, tablePath)
      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")

      // Verify the clustering domain metadata is written
      verifyClusteringDMAndCRC(snapshot, testingDomainMetadata)

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
      val commitResult = createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(testClusteringColumns),
        tableProperties = Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id"))

      val table = Table.forPath(engine, tablePath)
      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")

      // Verify the clustering domain metadata is written
      val schema = table.getLatestSnapshot(engine).getSchema
      val col1 = schema.get("part1").getMetadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
      val col2 = schema.get("part2").getMetadata.getString(COLUMN_MAPPING_PHYSICAL_NAME_KEY)
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        s"""{"clusteringColumns":[["$col1"],["$col2"]]}""",
        false)
      verifyClusteringDMAndCRC(snapshot, expectedDomainMetadata)

      assertCommitResultHasClusteringCols(
        commitResult,
        expectedClusteringCols = Seq(new Column(col1), new Column(col2)))
    }
  }

  test("create a clustered table should succeed with column case matches schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(List(new Column("pArT1"), new Column("PaRt2"))))

      val table = Table.forPath(engine, tablePath)
      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")

      // Verify the clustering domain metadata is written
      verifyClusteringDMAndCRC(snapshot, testingDomainMetadata)
    }
  }

  test("update a non-clustered table with clustering columns should succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testPartitionSchema)
      val table = Table.forPath(engine, tablePath)
      updateTableMetadata(engine, tablePath, clusteringColsOpt = Some(testClusteringColumns))

      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")
      verifyClusteringDMAndCRC(snapshot, testingDomainMetadata)
    }
  }

  test("update a clustered table with subset of previous clustering columns should succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(testClusteringColumns))
      val table = Table.forPath(engine, tablePath)
      updateTableMetadata(engine, tablePath, clusteringColsOpt = Some(List(new Column("part1"))))

      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        """{"clusteringColumns":[["part1"]]}""",
        false)
      verifyClusteringDMAndCRC(snapshot, expectedDomainMetadata)
    }
  }

  test("update a clustered table with a overlap clustering columns should succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(testClusteringColumns)
      ) // Seq("part1", "part2")
      val table = Table.forPath(engine, tablePath)
      updateTableMetadata(
        engine,
        tablePath,
        clusteringColsOpt = Some(List(new Column("part2"), new Column("id"))))

      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        """{"clusteringColumns":[["part2"],["id"]]}""",
        false)
      verifyClusteringDMAndCRC(snapshot, expectedDomainMetadata)
    }
  }

  test("update a clustered table with a non-overlap clustering columns should succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(List(new Column("part1"))))
      val table = Table.forPath(engine, tablePath)
      updateTableMetadata(engine, tablePath, clusteringColsOpt = Some(List(new Column("part2"))))

      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        """{"clusteringColumns":[["part2"]]}""",
        false)
      assertHasWriterFeature(snapshot, "clustering")
      verifyClusteringDMAndCRC(snapshot, expectedDomainMetadata)
    }
  }

  test("update a clustered table with empty clustering columns should succeed") {
    withTempDirAndEngine { (tablePath, engine) =>
      val commitResult0 = createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(testClusteringColumns))
      assertCommitResultHasClusteringCols(
        commitResult0,
        expectedClusteringCols = testClusteringColumns)

      val table = Table.forPath(engine, tablePath)
      val commitResult1 = updateTableMetadata(engine, tablePath, clusteringColsOpt = Some(List()))
      assertCommitResultHasClusteringCols(commitResult1, expectedClusteringCols = Seq.empty)

      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        """{"clusteringColumns":[]}""",
        false)
      assertHasWriterFeature(snapshot, "clustering")
      verifyClusteringDMAndCRC(snapshot, expectedDomainMetadata)
    }
  }

  test("update a table with clustering columns doesn't exist in the table should fail") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testPartitionSchema)
      val ex = intercept[KernelException] {
        updateTableMetadata(
          engine,
          tablePath,
          clusteringColsOpt = Some(List(new Column("non-exist"))))
      }
      assert(
        ex.getMessage.contains("Column 'column(`non-exist`)' was not found in the table schema"))
    }
  }

  test("update a partitioned table with clustering columns should fail") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testPartitionSchema, partCols = testPartitionColumns)
      // test case 1: update with non-empty clustering columns
      val ex1 = intercept[KernelException] {
        updateTableMetadata(
          engine,
          tablePath,
          clusteringColsOpt = Some(List(new Column("non-exist"))))
      }
      assert(
        ex1.getMessage.contains("Cannot enable clustering on a partitioned table"))

      // test case 2: update with empty clustering columns,
      // this would still be regarded as enabling clustering
      val ex2 = intercept[KernelException] {
        updateTableMetadata(
          engine,
          tablePath,
          clusteringColsOpt = Some(List()))
      }
      assert(
        ex2.getMessage.contains("Cannot enable clustering on a partitioned table"))
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
        clusteringColsOpt = Some(testClusteringColumns),
        data = testData)

      verifyCommitResult(commitResult, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tablePath, version = 0)
      verifyWrittenContent(
        tablePath,
        testPartitionSchema,
        dataClusteringBatches1.flatMap(_.toTestRows))

      val table = Table.forPath(engine, tablePath)
      verifyClusteringDMAndCRC(
        table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl],
        testingDomainMetadata)
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
          clusteringColsOpt = Some(testClusteringColumns),
          data = Seq(Map.empty[String, Literal] -> dataClusteringBatches1))
        assertCommitResultHasClusteringCols(
          commitResult0,
          expectedClusteringCols = testClusteringColumns)

        val expData = dataClusteringBatches1.flatMap(_.toTestRows)

        verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath, version = 0)
        verifyWrittenContent(tablePath, testPartitionSchema, expData)
        verifyClusteringDMAndCRC(
          table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl],
          testingDomainMetadata)
      }
      {
        val commitResult1 = appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> dataClusteringBatches2))
        assertCommitResultHasClusteringCols(
          commitResult1,
          expectedClusteringCols = testClusteringColumns)

        val expData = dataClusteringBatches1.flatMap(_.toTestRows) ++
          dataClusteringBatches2.flatMap(_.toTestRows)

        verifyCommitResult(commitResult1, expVersion = 1, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath, version = 1, partitionCols = null)
        verifyWrittenContent(tablePath, testPartitionSchema, expData)
        verifyClusteringDMAndCRC(
          table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl],
          testingDomainMetadata)
      }
    }
  }

  test("insert into clustered table after update clusteringColumns should still work") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val expectedDomainMetadataAfterUpdate = new DomainMetadata(
        "delta.clustering",
        """{"clusteringColumns":[["id"],["part1"]]}""",
        false)

      val newClusteringCols = List(new Column("id"), new Column("part1")) // will be updated in v1

      {
        val commitResult0 = appendData(
          engine,
          tablePath,
          isNewTable = true,
          testPartitionSchema,
          clusteringColsOpt = Some(testClusteringColumns),
          data = Seq(Map.empty[String, Literal] -> dataClusteringBatches1))
        assertCommitResultHasClusteringCols(
          commitResult0,
          expectedClusteringCols = testClusteringColumns)

        val expData = dataClusteringBatches1.flatMap(_.toTestRows)

        verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath, version = 0)
        verifyWrittenContent(tablePath, testPartitionSchema, expData)
        verifyClusteringDMAndCRC(
          table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl],
          testingDomainMetadata)
      }
      {
        val commitResult1 = updateTableMetadata(
          engine,
          tablePath,
          clusteringColsOpt = Some(newClusteringCols))
        assertCommitResultHasClusteringCols(
          commitResult1,
          expectedClusteringCols = newClusteringCols)

        verifyCommitResult(commitResult1, expVersion = 1, expIsReadyForCheckpoint = false)
        verifyClusteringDMAndCRC(
          table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl],
          expectedDomainMetadataAfterUpdate)
      }
      {
        val commitResult2 = appendData(
          engine,
          tablePath,
          data = Seq(Map.empty[String, Literal] -> dataClusteringBatches2))
        assertCommitResultHasClusteringCols(
          commitResult2,
          expectedClusteringCols = newClusteringCols)

        val expData = dataClusteringBatches1.flatMap(_.toTestRows) ++
          dataClusteringBatches2.flatMap(_.toTestRows)

        verifyCommitResult(commitResult2, expVersion = 2, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath, version = 2, partitionCols = null)
        verifyWrittenContent(tablePath, testPartitionSchema, expData)
        verifyClusteringDMAndCRC(
          table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl],
          expectedDomainMetadataAfterUpdate)
      }
    }
  }

  test("can convert physical clustering columns to logical on column-mapping-enabled table") {
    withTempDirAndEngine { (tablePath, engine) =>
      // ===== GIVEN =====
      val tableProperties = Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id")
      val clusteringColumns = List(new Column("part1"), new Column("part2"))

      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        tableProperties = tableProperties,
        clusteringColsOpt = Some(clusteringColumns))

      // ===== WHEN =====
      val snapshot = getTableManagerAdapter.getSnapshotAtLatest(engine, tablePath)
      val physicalClusteringColumns = snapshot.getPhysicalClusteringColumns.get().asScala

      // ===== THEN =====
      assert(physicalClusteringColumns.size == 2)
      physicalClusteringColumns.foreach { c => assert(c.getNames()(0).startsWith("col-")) }

      val schema = snapshot.getSchema
      physicalClusteringColumns.zipWithIndex.foreach { case (physicalColumn, idx) =>
        val logicalColumn = ColumnMapping.getLogicalColumnNameAndDataType(schema, physicalColumn)._1
        val expectedLogicalName = if (idx == 0) "part1" else "part2"

        assert(logicalColumn.getNames.length == 1)
        assert(logicalColumn.getNames()(0) == expectedLogicalName)
      }
    }
  }
}
