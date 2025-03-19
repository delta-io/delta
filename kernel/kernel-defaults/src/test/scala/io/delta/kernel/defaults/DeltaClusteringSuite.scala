package io.delta.kernel.defaults

import scala.collection.JavaConverters._

import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel.Table
import io.delta.kernel.expressions.Column
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
          .withClusteringColumns(engine, List(new Column(Array("PART1", "part3"))).asJava)
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
          .withClusteringColumns(engine, List(new Column(Array("part1", "part2"))).asJava)
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

      val schema = new StructType()
        .add("id", INTEGER)
        .add("part1", INTEGER) // clustering column
        .add("part2", INTEGER) // clustering column

      val txn = txnBuilder
        .withSchema(engine, schema)
        // clustering columns is case-insensitive and stored in lower case
        .withClusteringColumns(engine, List(new Column(Array("Part1", "paRt2"))).asJava)
        .build(engine)

      assert(txn.getSchema(engine) === schema)
      commitTransaction(txn, engine, emptyIterable())

      // Verify the clustering feature is included in the protocol
      val snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assert(snapshot.getProtocol.getWriterFeatures.contains("clustering"))

      // Verify the clustering domain metadata is written
      val expectedDomainMetadata = new DomainMetadata(
        "delta.clustering",
        """{"clusteringColumns":[["part1"],["part2"]]}""",
        false)
      assert(snapshot.getDomainMetadataMap.get(ClusteringMetadataDomain.DOMAIN_NAME)
        == expectedDomainMetadata)
    }
  }

//  test("insert into partitioned table - table created from scratch") {
  //    withTempDirAndEngine { (tblPath, engine) =>
  //      val commitResult0 = appendData(
  //        engine,
  //        tblPath,
  //        isNewTable = true,
  //        testPartitionSchema,
  //        testPartitionColumns,
  //        Seq(
  //          Map("part1" -> ofInt(1), "part2" -> ofInt(2)) -> dataPartitionBatches1,
  //          Map("part1" -> ofInt(4), "part2" -> ofInt(5)) -> dataPartitionBatches2))
  //
  //      val expData = dataPartitionBatches1.flatMap(_.toTestRows) ++
  //        dataPartitionBatches2.flatMap(_.toTestRows)
  //
  //      verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
  //      verifyCommitInfo(tblPath, version = 0, testPartitionColumns, operation = WRITE)
  //      verifyWrittenContent(tblPath, testPartitionSchema, expData)
  //    }
  //  }

}
