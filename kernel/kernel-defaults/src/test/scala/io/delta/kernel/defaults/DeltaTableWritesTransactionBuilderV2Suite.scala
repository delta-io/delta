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

import io.delta.kernel.{Operation, TableManager}
import io.delta.kernel.commit.{CommitMetadata, CommitResponse, Committer}
import io.delta.kernel.data.Row
import io.delta.kernel.defaults.engine.{DefaultEngine, DefaultJsonHandler}
import io.delta.kernel.defaults.engine.hadoopio.HadoopFileIO
import io.delta.kernel.defaults.utils.WriteUtilsWithV2Builders
import io.delta.kernel.engine.{Engine, JsonHandler}
import io.delta.kernel.exceptions.{KernelException, MaxCommitRetryLimitReachedException, TableAlreadyExistsException}
import io.delta.kernel.expressions.Column
import io.delta.kernel.internal.commit.DefaultFileSystemManagedTableOnlyCommitter
import io.delta.kernel.internal.table.SnapshotBuilderImpl
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.types.IntegerType
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.utils.CloseableIterator

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.clustering.{ClusteringMetadataDomain => SparkClusteringMetadataDomain}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

/**
 * Tests for the V2 transaction builders [[CreateTableTransactionBuilder]] and
 * [[UpdateTableTransactionBuilder]]. We don't cover the full scope of tests we have for
 * TransactionBuilderV1 (to do so, we would have to duplicate many, many of the existing suites).
 * Instead, we selectively run everything in [[DeltaTableWritesSuite]] as well as some additional
 * white-box-tests for the logic specific to the new builders. The main metadata validation +
 * update logic is shared by both V1 + V2 builders in
 * [[io.delta.kernel.internal.TransactionMetadataFactory]] and thus is covered by all the existing
 * tests we have for the V1 builder (ideally we would have unit tests for just
 * TransactionMetadataFactory in the future).
 *
 * <p>In the future, we should consider duplicating additional test suites with these builders
 * (requires sharding our Kernel CI tests first to avoid increasing CI runtime too much).
 */
class DeltaTableWritesTransactionBuilderV2Suite extends DeltaTableWritesSuite
    with WriteUtilsWithV2Builders {

  ///////////////////////////////////////////////////
  // Tests for code logic within the builder impls //
  ///////////////////////////////////////////////////

  // TablePropertiesTransactionBuilderV2Suite tests table property validation, normalization and
  // unset/set overlap for Create + Update

  // Tested in DeltaTableWritesSuite: setTxnOpt (covered by idempotent writes test)
  // Tested in DeltaTableWritesSuite: validateKernelCanWriteToTable (covered by unsupported
  // writer feature test)

  test("Cannot add clustering columns to a partitioned table") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        partCols = testPartitionColumns)
      val e = intercept[KernelException] {
        TableManager.loadSnapshot(tablePath)
          .asInstanceOf[SnapshotBuilderImpl].build(engine)
          .buildUpdateTableTransaction(testEngineInfo, Operation.WRITE)
          .withClusteringColumns(List(new Column("part1")).asJava)
      }
      assert(e.getMessage.contains("Cannot enable clustering on a partitioned table"))

    }
  }

  test("Cannot use UpdateTableTransactionBuilder with incompatible operations") {
    Seq(Operation.CREATE_TABLE, Operation.REPLACE_TABLE).foreach { op =>
      withTempDirAndEngine { (tablePath, engine) =>
        createEmptyTable(
          engine,
          tablePath,
          testSchema)
        val e = intercept[IllegalArgumentException] {
          TableManager.loadSnapshot(tablePath)
            .build(engine)
            .buildUpdateTableTransaction(testEngineInfo, op)
        }
        assert(e.getMessage.contains(
          s"Operation $op is not compatible with Snapshot::buildUpdateTableTransaction"))
      }
    }
  }

  test("UpdateTableTransactionBuilder uses the committer provided during snapshot building") {
    withTempDirAndEngine { (tablePath, engine) =>
      class FakeCommitter extends Committer {
        override def commit(
            engine: Engine,
            finalizedActions: CloseableIterator[Row],
            commitMetadata: CommitMetadata): CommitResponse = {
          throw new RuntimeException("This is a fake committer")
        }
      }
      createEmptyTable(
        engine,
        tablePath,
        testSchema)

      // Build snapshot with committer and start txn
      val txn = TableManager.loadSnapshot(tablePath)
        .withCommitter(new FakeCommitter())
        .build(engine)
        .buildUpdateTableTransaction(testEngineInfo, Operation.WRITE)
        .build(engine)

      // Check the txn returns the correct committer
      assert(txn.getCommitter.isInstanceOf[FakeCommitter])
      // Check that the txn invokes the provided committer upon commit
      val e = intercept[RuntimeException] {
        txn.commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains("This is a fake committer"))
    }
  }

  test("create table fails when the table already exists (non-catalog-managed)") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)
      intercept[TableAlreadyExistsException] {
        TableManager.buildCreateTableTransaction(
          tablePath,
          testSchema,
          testEngineInfo).build(engine)
      }
    }
  }

  test("create table does NOT fail when the table already exists for catalog managed") {
    // Note - the catalog is responsible for determining that the table loc provided is empty,
    // so technically this should not be a real-world scenario, but just checks that we correctly
    // omit checking the location when the catalog managed feature is enabled in the table props
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(engine, tablePath, testSchema)
      // Instead of failing earlier (due to the table existing) we fail later due to the table
      // feature being unsupported
      val e = intercept[KernelException] {
        TableManager.buildCreateTableTransaction(tablePath, testSchema, testEngineInfo)
          .withTableProperties(
            Map(TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX +
              TableFeatures.CATALOG_MANAGED_R_W_FEATURE_PREVIEW.featureName -> "supported").asJava)
          .build(engine)
      }
      assert(e.getMessage.contains("Unsupported Delta writer feature"))
    }
  }

  //////////////////////////////////////////////////////////////////////////////////////////////////
  // Tests that builder impls correctly propagate inputs + outputs for TransactionMetadataFactory //
  //////////////////////////////////////////////////////////////////////////////////////////////////

  // Table props are checked in TablePropertiesTransactionBuilderV2Suite + DeltaTableWritesSuite
  // Transaction Id is checked in DeltaTableWritesSuite
  // Max retries is checked DeltaTableWritesSuite for Update
  // Creating partitioned table is covered in DeltaTableWritesSuite

  test("Creating a table with clustering columns and updating the clustering columns") {
    withTempDirAndEngine { (tablePath, engine) =>
      def checkClusteringColsWithSpark(expectedCols: Seq[Seq[String]]): Unit = {
        val deltaLog = DeltaLog.forTable(spark, new Path(tablePath))
        val clusteringMetadataDomainRead =
          SparkClusteringMetadataDomain.fromSnapshot(deltaLog.update())
        assert(clusteringMetadataDomainRead.exists(_.clusteringColumns === expectedCols))
      }
      createEmptyTable(
        engine,
        tablePath,
        testPartitionSchema,
        clusteringColsOpt = Some(testClusteringColumns))
      // Validate with Spark that the clustering columns are set
      checkClusteringColsWithSpark(Seq(Seq("part1"), Seq("part2")))
      // Update clustering columns and check that they are updated
      updateTableMetadata(engine, tablePath, clusteringColsOpt = Some(List(new Column("part1"))))
      checkClusteringColsWithSpark(Seq(Seq("part1")))
    }
  }

  test("Can evolve schema using withUpdatedSchema") {
    withTempDirAndEngine { (tablePath, engine) =>
      createEmptyTable(
        engine,
        tablePath,
        testSchema,
        tableProperties = Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "id"))
      val currentSchema = getMetadata(engine, tablePath).getSchema
      assert(currentSchema.indexOf("newCol") == -1)
      val newSchema = currentSchema.add("newCol", IntegerType.INTEGER)
      updateTableMetadata(engine, tablePath, schema = newSchema)
      // Validate that the new column exits
      assert(getMetadata(engine, tablePath).getSchema.indexOf("newCol") >= 0)
    }
  }

  test("maxRetries is obeyed for Create table (error not a conflict)") {
    withTempDirAndEngine { (tablePath, engine) =>
      val fileIO = new HadoopFileIO(new Configuration())
      class CustomJsonHandler extends DefaultJsonHandler(fileIO) {
        var attemptCount = 0
        override def writeJsonFileAtomically(
            filePath: String,
            data: CloseableIterator[Row],
            overwrite: Boolean): Unit = {
          attemptCount += 1
          if (attemptCount == 1) {
            // The default committer will turn this into a CFE(isRetryable=true, isConflict=false)
            throw new java.io.IOException("Transient network error")
          }
          super.writeJsonFileAtomically(filePath, data, overwrite)
        }
      }

      class CustomEngine extends DefaultEngine(fileIO) {
        val jsonHandler = new CustomJsonHandler()
        override def getJsonHandler: JsonHandler = jsonHandler
      }

      // Commit fails when maxRetries = 0
      {
        val transientErrorEngine = new CustomEngine()
        intercept[MaxCommitRetryLimitReachedException] {
          getCreateTxn(
            transientErrorEngine,
            tablePath,
            schema = testSchema,
            maxRetries = 0).commit(transientErrorEngine, emptyIterable())
        }
      }

      // Commit succeeds when maxRetries > 1
      {
        val transientErrorEngine = new CustomEngine()
        getCreateTxn(
          transientErrorEngine,
          tablePath,
          schema = testSchema,
          maxRetries = 10).commit(transientErrorEngine, emptyIterable())
      }
    }
  }

  test("CreateTableTransactionBuilder uses the committer when provided") {
    withTempDirAndEngine { (tablePath, engine) =>
      class FakeCommitter extends Committer {
        override def commit(
            engine: Engine,
            finalizedActions: CloseableIterator[Row],
            commitMetadata: CommitMetadata): CommitResponse = {
          throw new RuntimeException("This is a fake committer")
        }
      }
      val txn = TableManager.buildCreateTableTransaction(tablePath, testSchema, testEngineInfo)
        .withCommitter(new FakeCommitter())
        .build(engine)

      // Check the txn returns the correct committer
      assert(txn.getCommitter.isInstanceOf[FakeCommitter])

      // Check that the txn invokes the provided committer upon commit
      val e = intercept[RuntimeException] {
        txn.commit(engine, emptyIterable())
      }
      assert(e.getMessage.contains("This is a fake committer"))
    }
  }

  test("CreateTableTransactionBuilder uses the default file system committer if none is provided") {
    withTempDirAndEngine { (tablePath, engine) =>
      val txn = TableManager.buildCreateTableTransaction(tablePath, testSchema, testEngineInfo)
        .build(engine)
      // Check the txn returns the correct committer
      assert(txn.getCommitter == DefaultFileSystemManagedTableOnlyCommitter.INSTANCE)
    }
  }
}
