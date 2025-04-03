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

class DeltaTableClusteringSuite extends DeltaTableWriteSuiteBase {

  private val testingDomainMetadata = new DomainMetadata(
    "delta.clustering",
    """{"clusteringColumns":[["part1"],["part2"]]}""",
    false)

  private def verifyClusteringDomainMetadata(
      snapshot: SnapshotImpl,
      expectedDomainMetadata: DomainMetadata = testingDomainMetadata): Unit = {
    assert(snapshot.getDomainMetadataMap.get(ClusteringMetadataDomain.DOMAIN_NAME)
      == expectedDomainMetadata)
  }

  test("create clustered table - clustering column is not part of the schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[IllegalArgumentException] {
        createTxn(
          engine,
          tablePath,
          isNewTable = true,
          testPartitionSchema,
          clusteringCols = List(new Column("PART1"), new Column("part3")))
      }
      assert(ex.getMessage.contains("Clustering column column(`part3`) not found in the schema"))
    }
  }

  test(
    "create clustered table - clustering column and partition column cannot be set at same time") {
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

  test("create a clustered table") {
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
      }
    }
  }
}
