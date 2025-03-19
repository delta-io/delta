package io.delta.kernel.defaults

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel.Table
import io.delta.kernel.exceptions.TableAlreadyExistsException
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.expressions.Literal.ofInt
import io.delta.kernel.internal.SnapshotImpl
import io.delta.kernel.internal.actions.DomainMetadata
import io.delta.kernel.internal.clustering.ClusteringMetadataDomain
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StructType
import io.delta.kernel.utils.CloseableIterable.emptyIterable

class DeltaClusteringSuite extends DeltaTableWriteSuiteBase {

  test("create clustered table - clustering column is not part of the schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val ex = intercept[IllegalArgumentException] {
        val txn = txnBuilder
          .withSchema(engine, testPartitionSchema)
          .withClusteringColumns(engine, List(new Column("PART1"), new Column("part3")).asJava)
          .build(engine)
        commitTransaction(txn, engine, emptyIterable())
      }
      assert(ex.getMessage.contains("Column not found in schema"))
    }
  }

  test(
    "create clustered table - clustering column and partition column cannot be set at same time") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val ex = intercept[IllegalArgumentException] {
        txnBuilder
          .withSchema(engine, testPartitionSchema)
          .withClusteringColumns(engine, testClusteringColumns.asJava)
          .withPartitionColumns(engine, Seq("part1").asJava)
          .build(engine)
      }
      assert(
        ex.getMessage
          .contains("Partition Columns and Clustering Columns cannot be set at the same time"))
    }
  }

  test("create a clustered table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testPartitionSchema)
        // clustering columns is case-insensitive and stored in lower case
        .withClusteringColumns(engine, testClusteringColumns.asJava)
        .build(engine)

      assert(txn.getSchema(engine) === testPartitionSchema)
      commitTransaction(txn, engine, emptyIterable())

      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertHasWriterFeature(snapshot, "clustering")

      // Verify the clustering domain metadata is written
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        """{"clusteringColumns":[["part1"],["part2"]]}""",
        false)
      assert(snapshot.getDomainMetadataMap.get(ClusteringMetadataDomain.DOMAIN_NAME)
        == expectedDomainMetadata)
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
          .contains("Table already exists, but provided new clustering columns."))
    }
  }

  test("insert into clustered table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val testData = Seq(Map.empty[String, Literal] -> dataClusteringBatches)
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testPartitionSchema)
        .withClusteringColumns(engine, testClusteringColumns.asJava)
        .build(engine)

      val commitResult = commitAppendData(engine, txn, testData)

      verifyCommitResult(commitResult, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tablePath, version = 0)
      verifyWrittenContent(
        tablePath,
        testPartitionSchema,
        dataClusteringBatches.flatMap(_.toTestRows))
    }
  }
}
