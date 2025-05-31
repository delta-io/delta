/*
 * Copyright (2021) The Delta Lake Project Authors.
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

import java.io.File
import java.nio.file.Files
import java.util.{Locale, Optional}

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel._
import io.delta.kernel.Operation.{CREATE_TABLE, MANUAL_UPDATE, WRITE}
import io.delta.kernel.data.{ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions._
import io.delta.kernel.expressions.Literal
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.internal.{ScanImpl, SnapshotImpl, TableConfig}
import io.delta.kernel.internal.checkpoints.CheckpointerSuite.selectSingleElement
import io.delta.kernel.internal.util.JsonUtils
import io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames
import io.delta.kernel.types._
import io.delta.kernel.types.ByteType.BYTE
import io.delta.kernel.types.DateType.DATE
import io.delta.kernel.types.DecimalType
import io.delta.kernel.types.DoubleType.DOUBLE
import io.delta.kernel.types.FloatType.FLOAT
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.ShortType.SHORT
import io.delta.kernel.types.StringType.STRING
import io.delta.kernel.types.StructType
import io.delta.kernel.types.TimestampType.TIMESTAMP
import io.delta.kernel.utils.CloseableIterable
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}

/** Transaction commit in this suite IS REQUIRED TO use commitTransaction than .commit */
class DeltaTableWritesSuite extends DeltaTableWriteSuiteBase with ParquetSuiteBase {

  ///////////////////////////////////////////////////////////////////////////
  // Create table tests
  ///////////////////////////////////////////////////////////////////////////

  test("create table - provide no schema - expect failure") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val ex = intercept[TableNotFoundException] {
        txnBuilder.build(engine)
      }
      assert(ex.getMessage.contains("Must provide a new schema to write to a new table"))
    }
  }

  test("create table - provide partition columns but no schema - expect failure") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table
        .createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
        .withPartitionColumns(engine, Seq("part1", "part2").asJava)

      val ex = intercept[TableNotFoundException] {
        txnBuilder.build(engine)
      }
      assert(ex.getMessage.contains("Must provide a new schema to write to a new table"))
    }
  }

  test("create table - provide unsupported column types - expect failure") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
      val ex = intercept[KernelException] {
        txnBuilder
          .withSchema(engine, new StructType().add("variant_type", VariantType.VARIANT))
          .build(engine)
      }
      assert(ex.getMessage.contains("Kernel doesn't support writing data of type: variant"))
    }
  }

  test("create table - table already exists at the location") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder.withSchema(engine, testSchema).build(engine)
      commitTransaction(txn, engine, emptyIterable())

      {
        intercept[TableAlreadyExistsException] {
          table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
            .build(engine)
        }
      }

      // Provide schema
      {
        intercept[TableAlreadyExistsException] {
          table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
            .withSchema(engine, testSchema)
            .build(engine)
        }
      }

      // Provide partition columns
      {
        intercept[TableAlreadyExistsException] {
          table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
            .withSchema(engine, testPartitionSchema)
            .withPartitionColumns(engine, testPartitionColumns.asJava)
            .build(engine)
        }
      }
    }
  }

  test("create table - table is concurrently created before txn commits") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txn1 = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
        .withSchema(engine, testSchema).build(engine)

      val txn2 = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
        .withSchema(engine, testSchema).build(engine)
      commitTransaction(txn2, engine, emptyIterable())

      intercept[ConcurrentWriteException] {
        commitTransaction(txn1, engine, emptyIterable())
      }
    }
  }

  test("cannot provide partition columns for existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder.withSchema(engine, testSchema).build(engine)
      commitTransaction(txn, engine, emptyIterable())

      val ex = intercept[TableAlreadyExistsException] {
        // Use operation != CREATE_TABLE since this fails earlier if the table already exists
        table.createTransactionBuilder(engine, testEngineInfo, WRITE)
          .withSchema(engine, testPartitionSchema)
          .withPartitionColumns(engine, testPartitionColumns.asJava)
          .build(engine)
      }
      assert(ex.getMessage.contains("Table already exists, but provided new partition columns." +
        " Partition columns can only be set on a new table."))
    }
  }

  test("create un-partitioned table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder
        .withSchema(engine, testSchema)
        .build(engine)

      assert(txn.getSchema(engine) === testSchema)
      assert(txn.getPartitionColumns(engine) === Seq.empty.asJava)
      assert(txn.getReadTableVersion == -1)
      val txnResult = commitTransaction(txn, engine, emptyIterable())

      assert(txnResult.getVersion === 0)
      assertCheckpointReadiness(txnResult, isReadyForCheckpoint = false)

      verifyCommitInfo(tablePath = tablePath, version = 0, operation = CREATE_TABLE)
      verifyWrittenContent(tablePath, testSchema, Seq.empty)
    }
  }

  test("create table and set properties") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txn1 = createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)

      txn1.commit(engine, emptyIterable())

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver0Snapshot, TableConfig.CHECKPOINT_INTERVAL, 10)

      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        isNewTable = false,
        key = TableConfig.CHECKPOINT_INTERVAL,
        value = "2",
        expectedValue = 2)
    }
  }

  test("create table with properties and they should be retained") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = TableConfig.CHECKPOINT_INTERVAL,
        value = "2",
        expectedValue = 2)

      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))
      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver1Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)
    }
  }

  test("create table and configure properties with retries") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table
      val table = Table.forPath(engine, tablePath)
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())
      // Create txn1 with config changes
      val txn1 = createTxn(
        engine,
        tablePath,
        tableProperties = Map(TableConfig.CHECKPOINT_INTERVAL.getKey -> "2"))
      // Create and commit txn2
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver1Snapshot, TableConfig.CHECKPOINT_INTERVAL, 10)

      // Try to commit txn1
      txn1.commit(engine, emptyIterable())

      val ver2Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver2Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)
    }
  }

  test("Setting retries to 0 disables retries") {
    withTempDirAndEngine { (tablePath, engine) =>
      // Create table
      val table = Table.forPath(engine, tablePath)
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      // Create txn1 with config changes
      val txn1 = createWriteTxnBuilder(table)
        .withTableProperties(engine, Map(TableConfig.CHECKPOINT_INTERVAL.getKey -> "2").asJava)
        .withMaxRetries(0)
        .build(engine)

      // Create and commit txn2
      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver1Snapshot, TableConfig.CHECKPOINT_INTERVAL, 10)

      // Try to commit txn1 but expect failure
      val ex1 = intercept[ConcurrentWriteException] {
        txn1.commit(engine, emptyIterable())
      }
      assert(
        ex1.getMessage.contains("Transaction has encountered a conflict and can not be committed"))

      // check that we're still set to 10
      val ver2Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver2Snapshot, TableConfig.CHECKPOINT_INTERVAL, 10)
    }
  }

  test("create table and configure the same properties") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      setTablePropAndVerify(
        engine = engine,
        tablePath = tablePath,
        key = TableConfig.CHECKPOINT_INTERVAL,
        value = "2",
        expectedValue = 2)
      assert(getMetadataActionFromCommit(engine, table, 0).isDefined)

      appendData(
        engine,
        tablePath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties =
          Map(TableConfig.CHECKPOINT_INTERVAL.getKey.toLowerCase(Locale.ROOT) -> "2"))
      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver1Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)
      assert(getMetadataActionFromCommit(engine, table, 1).isEmpty)
    }
  }

  test("create table and configure verifying that the case of the property is same as the one in" +
    "TableConfig and not the one passed by the user.") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)

      appendData(
        engine,
        tablePath,
        isNewTable = true,
        testSchema,
        Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1),
        tableProperties =
          Map(TableConfig.CHECKPOINT_INTERVAL.getKey.toLowerCase(Locale.ROOT) -> "2"))

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(ver0Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)

      val configurations = ver0Snapshot.getMetadata.getConfiguration
      assert(configurations.containsKey(TableConfig.CHECKPOINT_INTERVAL.getKey))
      assert(
        !configurations.containsKey(
          TableConfig.CHECKPOINT_INTERVAL.getKey.toLowerCase(Locale.ROOT)))
    }
  }

  test("create partitioned table - partition column is not part of the schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val ex = intercept[IllegalArgumentException] {
        txnBuilder
          .withSchema(engine, testPartitionSchema)
          .withPartitionColumns(engine, Seq("PART1", "part3").asJava)
          .build(engine)
      }
      assert(ex.getMessage.contains("Partition column part3 not found in the schema"))
    }
  }

  test("create partitioned table - partition column type is not supported") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val schema = new StructType()
        .add("p1", new ArrayType(INTEGER, true))
        .add("c1", DATE)
        .add("c2", new DecimalType(14, 2))

      val ex = intercept[KernelException] {
        txnBuilder
          .withSchema(engine, schema)
          .withPartitionColumns(engine, Seq("p1", "c1").asJava)
          .build(engine)
      }
      assert(ex.getMessage.contains(
        "Kernel doesn't support writing data with partition column (p1) of type: array[integer]"))
    }
  }

  test("create a partitioned table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val schema = new StructType()
        .add("id", INTEGER)
        .add("Part1", INTEGER) // partition column
        .add("part2", INTEGER) // partition column

      val txn = txnBuilder
        .withSchema(engine, schema)
        // partition columns should preserve the same case the one in the schema
        .withPartitionColumns(engine, Seq("part1", "PART2").asJava)
        .build(engine)

      assert(txn.getSchema(engine) === schema)
      // Expect the partition column name is exactly same as the one in the schema
      assert(txn.getPartitionColumns(engine) === Seq("Part1", "part2").asJava)
      val txnResult = commitTransaction(txn, engine, emptyIterable())

      assert(txnResult.getVersion === 0)
      assertCheckpointReadiness(txnResult, isReadyForCheckpoint = false)

      verifyCommitInfo(tablePath, version = 0, Seq("Part1", "part2"), operation = CREATE_TABLE)
      verifyWrittenContent(tablePath, schema, Seq.empty)
    }
  }

  Seq(true, false).foreach { includeTimestampNtz =>
    test(s"create table with all supported types - timestamp_ntz included=$includeTimestampNtz") {
      withTempDirAndEngine { (tablePath, engine) =>
        val parquetAllTypes = goldenTablePath("parquet-all-types")
        val goldenTableSchema = tableSchema(parquetAllTypes)
        val schema = if (includeTimestampNtz) goldenTableSchema
        else removeTimestampNtzTypeColumns(goldenTableSchema)

        val table = Table.forPath(engine, tablePath)
        val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
        val txn = txnBuilder.withSchema(engine, schema).build(engine)
        val txnResult = commitTransaction(txn, engine, emptyIterable())

        assert(txnResult.getVersion === 0)
        assertCheckpointReadiness(txnResult, isReadyForCheckpoint = false)

        verifyCommitInfo(tablePath, version = 0, operation = CREATE_TABLE)
        verifyWrittenContent(tablePath, schema, Seq.empty)
      }
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Create table and insert data tests (CTAS & INSERT)
  ///////////////////////////////////////////////////////////////////////////
  test("insert into table - table created from scratch") {
    withTempDirAndEngine { (tblPath, engine) =>
      val commitResult0 = appendData(
        engine,
        tblPath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> (dataBatches1 ++ dataBatches2)))

      val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++ dataBatches2.flatMap(_.toTestRows)

      verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tblPath, version = 0, partitionCols = Seq.empty, operation = WRITE)
      verifyWrittenContent(tblPath, testSchema, expectedAnswer)
    }
  }

  test("insert into table - already existing table") {
    withTempDirAndEngine { (tblPath, engine) =>
      val commitResult0 = appendData(
        engine,
        tblPath,
        isNewTable = true,
        testSchema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))

      verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tblPath, version = 0, partitionCols = Seq.empty, operation = WRITE)
      verifyWrittenContent(tblPath, testSchema, dataBatches1.flatMap(_.toTestRows))

      val txn = createTxn(engine, tblPath)
      assert(txn.getReadTableVersion == 0)
      val commitResult1 =
        commitAppendData(engine, txn, data = Seq(Map.empty[String, Literal] -> dataBatches2))

      val expAnswer = dataBatches1.flatMap(_.toTestRows) ++ dataBatches2.flatMap(_.toTestRows)

      verifyCommitResult(commitResult1, expVersion = 1, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tblPath, version = 1, partitionCols = null, operation = WRITE)
      verifyWrittenContent(tblPath, testSchema, expAnswer)
    }
  }

  test("insert into table - fails when committing the same txn twice") {
    withTempDirAndEngine { (tblPath, engine) =>
      val table = Table.forPath(engine, tblPath)

      val txn = createWriteTxnBuilder(table)
        .withSchema(engine, testSchema)
        .build(engine)

      val txnState = txn.getTransactionState(engine)
      val stagedFiles = stageData(txnState, Map.empty, dataBatches1)

      val stagedActionsIterable = inMemoryIterable(stagedFiles)
      val commitResult = commitTransaction(txn, engine, stagedActionsIterable)
      assert(commitResult.getVersion == 0)

      // try to commit the same transaction and expect failure
      val ex = intercept[IllegalStateException] {
        commitTransaction(txn, engine, stagedActionsIterable)
      }
      assert(ex.getMessage.contains(
        "Transaction is already attempted to commit. Create a new transaction."))
    }
  }

  test("insert into partitioned table - table created from scratch") {
    withTempDirAndEngine { (tblPath, engine) =>
      val commitResult0 = appendData(
        engine,
        tblPath,
        isNewTable = true,
        testPartitionSchema,
        testPartitionColumns,
        Seq(
          Map("part1" -> ofInt(1), "part2" -> ofInt(2)) -> dataPartitionBatches1,
          Map("part1" -> ofInt(4), "part2" -> ofInt(5)) -> dataPartitionBatches2))

      val expData = dataPartitionBatches1.flatMap(_.toTestRows) ++
        dataPartitionBatches2.flatMap(_.toTestRows)

      verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tblPath, version = 0, testPartitionColumns, operation = WRITE)
      verifyWrittenContent(tblPath, testPartitionSchema, expData)
    }
  }

  test("insert into partitioned table - already existing table") {
    withTempDirAndEngine { (tempTblPath, engine) =>
      val tblPath = tempTblPath + "/table+ with special chars"
      val partitionCols = Seq("part1", "part2")

      {
        val commitResult0 = appendData(
          engine,
          tblPath,
          isNewTable = true,
          testPartitionSchema,
          testPartitionColumns,
          data = Seq(Map("part1" -> ofInt(1), "part2" -> ofInt(2)) -> dataPartitionBatches1))

        val expData = dataPartitionBatches1.flatMap(_.toTestRows)

        verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tblPath, version = 0, partitionCols, operation = WRITE)
        verifyWrittenContent(tblPath, testPartitionSchema, expData)
      }
      {
        val commitResult1 = appendData(
          engine,
          tblPath,
          data = Seq(Map("part1" -> ofInt(4), "part2" -> ofInt(5)) -> dataPartitionBatches2))

        val expData = dataPartitionBatches1.flatMap(_.toTestRows) ++
          dataPartitionBatches2.flatMap(_.toTestRows)

        verifyCommitResult(commitResult1, expVersion = 1, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tblPath, version = 1, partitionCols = null, operation = WRITE)
        verifyWrittenContent(tblPath, testPartitionSchema, expData)
      }
    }
  }

  test("insert into partitioned table - handling case sensitivity of partition columns") {
    withTempDirAndEngine { (tblPath, engine) =>
      val schema = new StructType()
        .add("id", INTEGER)
        .add("Name", STRING)
        .add("Part1", DOUBLE) // partition column
        .add("parT2", TIMESTAMP) // partition column

      val partCols = Seq("part1", "Part2") // given as input to the txn builder

      // expected partition cols in the commit info or elsewhere in the Delta log.
      // it is expected to contain the partition columns in the same case as the schema
      val expPartCols = Seq("Part1", "parT2")

      val v0Part0Values = Map(
        "PART1" -> ofDouble(1.0),
        "pART2" -> ofTimestamp(1231212L))
      val v0Part0Data =
        generateData(schema, expPartCols, v0Part0Values, batchSize = 200, numBatches = 3)

      val v0Part1Values = Map(
        "Part1" -> ofDouble(7),
        "PARt2" -> ofTimestamp(123112L))
      val v0Part1Data =
        generateData(schema, expPartCols, v0Part1Values, batchSize = 100, numBatches = 7)

      val v1Part0Values = Map(
        "PART1" -> ofNull(DOUBLE),
        "pART2" -> ofTimestamp(1231212L))
      val v1Part0Data =
        generateData(schema, expPartCols, v1Part0Values, batchSize = 200, numBatches = 3)

      val v1Part1Values = Map(
        "Part1" -> ofDouble(7),
        "PARt2" -> ofNull(TIMESTAMP))
      val v1Part1Data =
        generateData(schema, expPartCols, v1Part1Values, batchSize = 100, numBatches = 7)

      val dataPerVersion = Map(
        0 -> Seq(v0Part0Values -> v0Part0Data, v0Part1Values -> v0Part1Data),
        1 -> Seq(v1Part0Values -> v1Part0Data, v1Part1Values -> v1Part1Data))

      val expV0Data = v0Part0Data.flatMap(_.toTestRows) ++ v0Part1Data.flatMap(_.toTestRows)
      val expV1Data = v1Part0Data.flatMap(_.toTestRows) ++ v1Part1Data.flatMap(_.toTestRows)

      for (i <- 0 until 2) {
        val commitResult = appendData(
          engine,
          tblPath,
          isNewTable = i == 0,
          schema,
          partCols,
          dataPerVersion(i))

        verifyCommitResult(commitResult, expVersion = i, expIsReadyForCheckpoint = false)
        // partition cols are not written in the commit info for inserts
        val partitionBy = if (i == 0) expPartCols else null
        verifyCommitInfo(tblPath, version = i, partitionBy, operation = WRITE)
        verifyWrittenContent(
          tblPath,
          schema,
          if (i == 0) expV0Data else expV0Data ++ expV1Data)
      }
    }
  }

  Seq(10, 2).foreach { checkpointInterval =>
    test(s"insert into partitioned table - isReadyForCheckpoint(interval=$checkpointInterval)") {
      withTempDirAndEngine { (tblPath, engine) =>
        val schema = new StructType()
          .add("id", INTEGER)
          .add("Name", STRING)
          .add("Part1", DOUBLE) // partition column
          .add("parT2", TIMESTAMP) // partition column

        val partCols = Seq("Part1", "parT2")
        val partValues = Map("PART1" -> ofDouble(1.0), "pART2" -> ofTimestamp(1231212L))
        val data = Seq(
          partValues -> generateData(schema, partCols, partValues, batchSize = 200, numBatches = 3))

        // create a table first
        appendData(engine, tblPath, isNewTable = true, schema, partCols, data) // version 0
        var expData = data.map(_._2).flatMap(_.flatMap(_.toTestRows))
        var currentTableVersion = 0

        if (checkpointInterval != 10) {
          // If it is not the default interval alter the table using Spark to set a
          // custom checkpoint interval
          setCheckpointInterval(tblPath, interval = checkpointInterval) // version 1
          currentTableVersion = 1
        }

        def isCheckpointExpected(version: Long): Boolean = {
          version != 0 && version % checkpointInterval == 0
        }

        for (i <- currentTableVersion + 1 until 31) {
          val commitResult = appendData(engine, tblPath, data = data)

          val parquetFileCount = dataFileCount(tblPath)
          assert(parquetFileCount > 0)
          checkpointIfReady(engine, tblPath, commitResult, expSize = parquetFileCount)

          verifyCommitResult(commitResult, expVersion = i, isCheckpointExpected(i))

          expData = expData ++ data.map(_._2).flatMap(_.flatMap(_.toTestRows))
        }

        // expect the checkpoints created at expected versions
        Seq.range(0, 31).filter(isCheckpointExpected(_)).foreach { version =>
          assertCheckpointExists(tblPath, atVersion = version)
        }

        // delete all commit files before version 30 in both cases and expect the read to pass as
        // there is a checkpoint at version 30 and should be used for state reconstruction.
        deleteDeltaFilesBefore(tblPath, beforeVersion = 30)
        verifyWrittenContent(tblPath, schema, expData)
      }
    }
  }

  Seq(true, false).foreach { includeTimestampNtz =>
    test(s"insert into table - all supported types data - " +
      s"timestamp_ntz included = $includeTimestampNtz") {
      withTempDirAndEngine { (tblPath, engine) =>
        val parquetAllTypes = goldenTablePath("parquet-all-types")
        val goldenTableSchema = tableSchema(parquetAllTypes)
        val schema = if (includeTimestampNtz) goldenTableSchema
        else removeTimestampNtzTypeColumns(goldenTableSchema)

        val data = readTableUsingKernel(engine, parquetAllTypes, schema)
        val dataWithPartInfo = Seq(Map.empty[String, Literal] -> data)

        appendData(engine, tblPath, isNewTable = true, schema, Seq.empty, dataWithPartInfo)
        var expData = dataWithPartInfo.flatMap(_._2).flatMap(_.toTestRows)

        val checkpointInterval = 4
        setCheckpointInterval(tblPath, checkpointInterval)

        for (i <- 2 until 5) {
          // insert until a checkpoint is required
          val commitResult = appendData(engine, tblPath, data = dataWithPartInfo)

          expData = expData ++ dataWithPartInfo.flatMap(_._2).flatMap(_.toTestRows)
          checkpointIfReady(engine, tblPath, commitResult, expSize = i /* one file per version */ )

          verifyCommitResult(commitResult, expVersion = i, i % checkpointInterval == 0)
          verifyCommitInfo(tblPath, version = i, null, operation = WRITE)
          verifyWrittenContent(tblPath, schema, expData)
        }
        assertCheckpointExists(tblPath, atVersion = checkpointInterval)
      }
    }
  }

  Seq(true, false).foreach { includeTimestampNtz =>
    test(s"insert into partitioned table - all supported partition column types data - " +
      s"timestamp_ntz included = $includeTimestampNtz") {
      withTempDirAndEngine { (tblPath, engine) =>
        val parquetAllTypes = goldenTablePath("parquet-all-types")
        val goldenTableSchema = tableSchema(parquetAllTypes)
        val schema = if (includeTimestampNtz) goldenTableSchema
        else removeTimestampNtzTypeColumns(goldenTableSchema)

        val partCols = Seq(
          "byteType",
          "shortType",
          "integerType",
          "longType",
          "floatType",
          "doubleType",
          "decimal",
          "booleanType",
          "stringType",
          "binaryType",
          "dateType",
          "timestampType") ++ (if (includeTimestampNtz) Seq("timestampNtzType") else Seq.empty)
        val casePreservingPartCols =
          casePreservingPartitionColNames(schema, partCols.asJava).asScala

        // get the partition values from the data batch at the given rowId
        def getPartitionValues(batch: ColumnarBatch, rowId: Int): Map[String, Literal] = {
          casePreservingPartCols.map { partCol =>
            val colIndex = schema.indexOf(partCol)
            val vector = batch.getColumnVector(colIndex)

            val literal = if (vector.isNullAt(rowId)) {
              Literal.ofNull(vector.getDataType)
            } else {
              vector.getDataType match {
                case _: ByteType => Literal.ofByte(vector.getByte(rowId))
                case _: ShortType => Literal.ofShort(vector.getShort(rowId))
                case _: IntegerType => Literal.ofInt(vector.getInt(rowId))
                case _: LongType => Literal.ofLong(vector.getLong(rowId))
                case _: FloatType => Literal.ofFloat(vector.getFloat(rowId))
                case _: DoubleType => Literal.ofDouble(vector.getDouble(rowId))
                case dt: DecimalType =>
                  Literal.ofDecimal(vector.getDecimal(rowId), dt.getPrecision, dt.getScale)
                case _: BooleanType => Literal.ofBoolean(vector.getBoolean(rowId))
                case _: StringType => Literal.ofString(vector.getString(rowId))
                case _: BinaryType => Literal.ofBinary(vector.getBinary(rowId))
                case _: DateType => Literal.ofDate(vector.getInt(rowId))
                case _: TimestampType => Literal.ofTimestamp(vector.getLong(rowId))
                case _: TimestampNTZType => Literal.ofTimestampNtz(vector.getLong(rowId))
                case _ =>
                  throw new IllegalArgumentException(s"Unsupported type: ${vector.getDataType}")
              }
            }
            (partCol, literal)
          }.toMap
        }

        val data = readTableUsingKernel(engine, parquetAllTypes, schema)

        // From the above table read data, convert each row as a new batch with partition info
        // Take the values of the partitionCols from the data and create a new batch with the
        // selection vector to just select a single row.
        var dataWithPartInfo = Seq.empty[(Map[String, Literal], Seq[FilteredColumnarBatch])]

        data.foreach { filteredBatch =>
          val batch = filteredBatch.getData
          Seq.range(0, batch.getSize).foreach { rowId =>
            val partValues = getPartitionValues(batch, rowId)
            val filteredBatch = new FilteredColumnarBatch(
              batch,
              Optional.of(selectSingleElement(batch.getSize, rowId)))
            dataWithPartInfo = dataWithPartInfo :+ (partValues, Seq(filteredBatch))
          }
        }

        appendData(engine, tblPath, isNewTable = true, schema, partCols, dataWithPartInfo)
        verifyCommitInfo(tblPath, version = 0, casePreservingPartCols.toSeq, operation = WRITE)

        var expData = dataWithPartInfo.flatMap(_._2).flatMap(_.toTestRows)

        val checkpointInterval = 2
        setCheckpointInterval(tblPath, checkpointInterval) // version 1

        for (i <- 2 until 4) {
          // insert until a checkpoint is required
          val commitResult = appendData(engine, tblPath, data = dataWithPartInfo)

          expData = expData ++ dataWithPartInfo.flatMap(_._2).flatMap(_.toTestRows)

          val fileCount = dataFileCount(tblPath)
          checkpointIfReady(engine, tblPath, commitResult, expSize = fileCount)

          verifyCommitResult(commitResult, expVersion = i, i % checkpointInterval == 0)
          verifyCommitInfo(tblPath, version = i, partitionCols = null, operation = WRITE)
          verifyWrittenContent(tblPath, schema, expData)
        }

        assertCheckpointExists(tblPath, atVersion = checkpointInterval)
      }
    }
  }

  test("insert into table - given data schema mismatch") {
    withTempDirAndEngine { (tblPath, engine) =>
      val ex = intercept[KernelException] {
        val data = Seq(Map.empty[String, Literal] -> dataPartitionBatches1) // data schema mismatch
        appendData(engine, tblPath, isNewTable = true, testSchema, partCols = Seq.empty, data)
      }
      assert(ex.getMessage.contains("The schema of the data to be written to " +
        "the table doesn't match the table schema"))
    }
  }

  test("insert into table - missing partition column info") {
    withTempDirAndEngine { (tblPath, engine) =>
      val ex = intercept[IllegalArgumentException] {
        appendData(
          engine,
          tblPath,
          isNewTable = true,
          testPartitionSchema,
          testPartitionColumns,
          data = Seq(Map("part1" -> ofInt(1)) -> dataPartitionBatches1) // missing part2
        )
      }
      assert(ex.getMessage.contains(
        "Partition values provided are not matching the partition columns."))
    }
  }

  test("insert into partitioned table - invalid type of partition value") {
    withTempDirAndEngine { (tblPath, engine) =>
      val ex = intercept[IllegalArgumentException] {
        // part2 type should be int, be giving a string value
        val data = Seq(Map("part1" -> ofInt(1), "part2" -> ofString("sdsd"))
          -> dataPartitionBatches1)
        appendData(
          engine,
          tblPath,
          isNewTable = true,
          testPartitionSchema,
          testPartitionColumns,
          data)
      }
      assert(ex.getMessage.contains(
        "Partition column part2 is of type integer but the value provided is of type string"))
    }
  }

  test("insert into table - idempotent writes") {
    withTempDirAndEngine { (tblPath, engine) =>
      val data = Seq(Map("part1" -> ofInt(1), "part2" -> ofInt(2)) -> dataPartitionBatches1)
      var expData = Seq.empty[TestRow] // as the data in inserted, update this.

      def prepTxnAndActions(newTbl: Boolean, appId: String, txnVer: Long)
          : (Transaction, CloseableIterable[Row]) = {
        var txnBuilder = createWriteTxnBuilder(Table.forPath(engine, tblPath))

        if (appId != null) txnBuilder = txnBuilder.withTransactionId(engine, appId, txnVer)

        if (newTbl) {
          txnBuilder = txnBuilder.withSchema(engine, testPartitionSchema)
            .withPartitionColumns(engine, testPartitionColumns.asJava)
        }
        val txn = txnBuilder.build(engine)

        val combinedActions = inMemoryIterable(
          data.map { case (partValues, partData) =>
            stageData(txn.getTransactionState(engine), partValues, partData)
          }.reduceLeft(_ combine _))

        (txn, combinedActions)
      }

      def commitAndVerify(
          newTbl: Boolean,
          txn: Transaction,
          actions: CloseableIterable[Row],
          expTblVer: Long): Unit = {
        val commitResult = commitTransaction(txn, engine, actions)

        expData = expData ++ data.flatMap(_._2).flatMap(_.toTestRows)

        verifyCommitResult(commitResult, expVersion = expTblVer, expIsReadyForCheckpoint = false)
        val expPartCols = if (newTbl) testPartitionColumns else null
        verifyCommitInfo(tblPath, version = expTblVer, expPartCols, operation = WRITE)
        verifyWrittenContent(tblPath, testPartitionSchema, expData)
      }

      def addDataWithTxnId(newTbl: Boolean, appId: String, txnVer: Long, expTblVer: Long): Unit = {
        val (txn, combinedActions) = prepTxnAndActions(newTbl, appId, txnVer)
        commitAndVerify(newTbl, txn, combinedActions, expTblVer)
      }

      def expFailure(appId: String, txnVer: Long, latestTxnVer: Long)(fn: => Any): Unit = {
        val ex = intercept[ConcurrentTransactionException] {
          fn
        }
        assert(ex.getMessage.contains(s"This error occurs when multiple updates are using the " +
          s"same transaction identifier to write into this table.\nApplication ID: $appId, " +
          s"Attempted version: $txnVer, Latest version in table: $latestTxnVer"))
      }

      // Create a transaction with id (txnAppId1, 0) and commit it
      addDataWithTxnId(newTbl = true, appId = "txnAppId1", txnVer = 0, expTblVer = 0)

      // Try to create a transaction with id (txnAppId1, 0) and commit it - should be valid
      addDataWithTxnId(newTbl = false, appId = "txnAppId1", txnVer = 1, expTblVer = 1)

      // Try to create a transaction with id (txnAppId1, 1) and try to commit it
      // Should fail the it is already committed above.
      expFailure("txnAppId1", txnVer = 1, latestTxnVer = 1) {
        addDataWithTxnId(newTbl = false, "txnAppId1", txnVer = 1, expTblVer = 2)
      }

      // append with no txn id
      addDataWithTxnId(newTbl = false, appId = null, txnVer = 0, expTblVer = 2)

      // Try to create a transaction with id (txnAppId2, 1) and commit it
      // Should be successful as the transaction app id is different
      addDataWithTxnId(newTbl = false, "txnAppId2", txnVer = 1, expTblVer = 3)

      // Try to create a transaction with id (txnAppId2, 0) and commit it
      // Should fail as the transaction app id is same but the version is less than the committed
      expFailure("txnAppId2", txnVer = 0, latestTxnVer = 1) {
        addDataWithTxnId(newTbl = false, "txnAppId2", txnVer = 0, expTblVer = 4)
      }

      // Start a transaction (txnAppId2, 2), but don't commit it yet
      val (txn, combinedActions) = prepTxnAndActions(newTbl = false, "txnAppId2", txnVer = 2)
      // Now start a new transaction with the same id (txnAppId2, 2) and commit it
      addDataWithTxnId(newTbl = false, "txnAppId2", txnVer = 2, expTblVer = 4)
      // Now try to commit the previous transaction (txnAppId2, 2) - should fail
      expFailure("txnAppId2", txnVer = 2, latestTxnVer = 2) {
        commitAndVerify(newTbl = false, txn, combinedActions, expTblVer = 5)
      }

      // Start a transaction (txnAppId2, 3), but don't commit it yet
      val (txn2, combinedActions2) = prepTxnAndActions(newTbl = false, "txnAppId2", txnVer = 3)
      // Now start a new transaction with the different id (txnAppId1, 10) and commit it
      addDataWithTxnId(newTbl = false, "txnAppId1", txnVer = 10, expTblVer = 5)
      // Now try to commit the previous transaction (txnAppId2, 3) - should pass
      commitAndVerify(newTbl = false, txn2, combinedActions2, expTblVer = 6)
    }
  }

  test("insert into table - write stats and validate they can be read by Spark ") {
    withTempDirAndEngine { (tblPath, engine) =>
      // Configure the table property for stats collection via TableConfig.
      val numIndexedCols = 5
      val tableProperties = Map(TableConfig.
      DATA_SKIPPING_NUM_INDEXED_COLS.getKey -> numIndexedCols.toString)

      // Schema of the table with some nested types
      val schema = new StructType()
        .add("id", INTEGER)
        .add("name", STRING)
        .add("height", DOUBLE)
        .add("timestamp", TIMESTAMP)
        .add(
          "metrics",
          new StructType()
            .add("temperature", DoubleType.DOUBLE)
            .add("humidity", FloatType.FLOAT))

      // Create the table with the given schema and table properties.
      val txn = createTxn(
        engine,
        tblPath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        tableProperties = tableProperties)
      commitTransaction(txn, engine, emptyIterable())

      val dataBatches1 = generateData(schema, Seq.empty, Map.empty, batchSize = 10, numBatches = 1)
      val dataBatches2 = generateData(schema, Seq.empty, Map.empty, batchSize = 20, numBatches = 1)

      // Write initial data via Kernel.
      val commitResult0 = appendData(
        engine,
        tblPath,
        data = Seq(Map.empty[String, Literal] -> dataBatches1))
      verifyCommitResult(commitResult0, expVersion = 1, expIsReadyForCheckpoint = false)
      verifyWrittenContent(tblPath, schema, dataBatches1.flatMap(_.getRows().toSeq).map(TestRow(_)))

      // Append additional data.
      val commitResult1 = appendData(
        engine,
        tblPath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2))
      val expectedRows = dataBatches1.flatMap(_.getRows().toSeq) ++
        dataBatches2.flatMap(_.getRows().toSeq)
      verifyCommitResult(commitResult1, expVersion = 2, expIsReadyForCheckpoint = false)
      verifyWrittenContent(tblPath, schema, expectedRows.map(TestRow(_)))
    }
  }

  test("insert - validate DATA_SKIPPING_NUM_INDEXED_COLS is respected when collecting stats") {
    withTempDirAndEngine { (tblPath, engine) =>
      val numIndexedCols = 2
      val tableProps = Map(TableConfig.DATA_SKIPPING_NUM_INDEXED_COLS
        .getKey -> numIndexedCols.toString)
      val schema = new StructType()
        .add("id", INTEGER)
        .add(
          "name",
          new StructType()
            .add("height", DoubleType.DOUBLE)
            .add("timestamp", TimestampType.TIMESTAMP))

      // Create table with stats collection enabled.
      val txn = createTxn(engine, tblPath, isNewTable = true, schema, Seq.empty, tableProps)
      txn.commit(engine, emptyIterable())

      // Write one batch of data.
      val dataBatches = generateData(schema, Seq.empty, Map.empty, batchSize = 10, numBatches = 1)
      val commitResult =
        appendData(engine, tblPath, data = Seq(Map.empty[String, Literal] -> dataBatches))
      verifyCommitResult(commitResult, expVersion = 1, expIsReadyForCheckpoint = false)

      // Retrieve the stats JSON from the file.
      val snapshot = Table.forPath(engine, tblPath).getLatestSnapshot(engine)
      val scan = snapshot.getScanBuilder().build()
      val scanFiles = scan.asInstanceOf[ScanImpl].getScanFiles(engine, true)
        .toSeq.flatMap(_.getRows.toSeq)
      val statsJson = scanFiles.headOption.flatMap { row =>
        val addFile = row.getStruct(row.getSchema.indexOf("add"))
        val statsIdx = addFile.getSchema.indexOf("stats")
        if (statsIdx >= 0 && !addFile.isNullAt(statsIdx)) {
          Some(addFile.getString(statsIdx))
        } else {
          None
        }
      }.getOrElse(fail("Stats JSON not found"))

      // With numIndexedCols = 2, we expect stats for id and name.height, but not for name.timestamp
      assert(statsJson.contains("\"id\""), "Stats should contain 'id' field")
      assert(statsJson.contains("\"height\""), "Stats should contain 'height' field")
      assert(
        !statsJson.contains("\"timestamp\""),
        "Stats should not contain 'timestamp' field, as it exceeds numIndexedCols")
    }
  }

  test("conflicts - creating new table - table created by other txn after current txn start") {
    withTempDirAndEngine { (tablePath, engine) =>
      val losingTx = createTestTxn(engine, tablePath, Some(testSchema))

      // don't commit losingTxn, instead create a new txn and commit it
      val winningTx = createTestTxn(engine, tablePath, Some(testSchema))
      val winningTxResult = commitTransaction(winningTx, engine, emptyIterable())

      // now attempt to commit the losingTxn
      val ex = intercept[ProtocolChangedException] {
        commitTransaction(losingTx, engine, emptyIterable())
      }
      assert(ex.getMessage.contains(
        "Transaction has encountered a conflict and can not be committed."))
      // helpful message for table creation conflict
      assert(ex.getMessage.contains("This happens when multiple writers are " +
        "writing to an empty directory. Creating the table ahead of time will avoid " +
        "this conflict."))

      verifyCommitResult(winningTxResult, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tablePath = tablePath, version = 0)
      verifyWrittenContent(tablePath, testSchema, Seq.empty)
    }
  }

  test("insert into table - validate serialized json stats equal Spark written stats") {
    withTempDirAndEngine { (kernelPath, engine) =>
      // Test with all Skipping eligible types.
      // TODO(Issue: 4284): Validate TIMESTAMP and TIMESTAMP_NTZ serialization
      // format.
      val schema = new StructType()
        .add("byteCol", BYTE)
        .add("shortCol", SHORT)
        .add("intCol", INTEGER)
        .add("longCol", LONG)
        .add("floatCol", FLOAT)
        .add("doubleCol", DOUBLE)
        .add("stringCol", STRING)
        .add("dateCol", DATE)
        .add(
          "structCol",
          new StructType()
            .add("nestedDecimal", DecimalType.USER_DEFAULT)
            .add("nestedDoubleCol", DOUBLE))

      // Write a batch of data using the Kernel
      val batch =
        generateData(schema, Seq.empty, Map.empty, batchSize = 10, numBatches = 1)
      appendData(
        engine,
        kernelPath,
        isNewTable = true,
        schema,
        partCols = Seq.empty,
        data = Seq(Map.empty[String, Literal] -> batch))

      // Write the same batch of data through Spark to a copy table.
      withTempDir(tempDir => {})

      val sparkPath = new File(kernelPath, "spark-copy").getAbsolutePath
      spark.read.format("delta").load(kernelPath)
        .write.format("delta").mode("overwrite").save(sparkPath)

      val mapper = JsonUtils.mapper()
      val kernelStats = collectStatsFromAddFiles(engine, kernelPath).map(mapper.readTree)
      val sparkStats = collectStatsFromAddFiles(engine, sparkPath).map(mapper.readTree)

      require(
        kernelStats.nonEmpty && sparkStats.nonEmpty,
        "stats collected from AddFiles should be non-empty")
      assert(
        kernelStats.toSet == sparkStats.toSet,
        s"\nKernel stats:\n${kernelStats.mkString("\n")}\n" +
          s"Spark  stats:\n${sparkStats.mkString("\n")}")
    }
  }

  test("conflicts - table metadata has changed after the losing txn has started") {
    withTempDirAndEngine { (tablePath, engine) =>
      val testData = Seq(Map.empty[String, Literal] -> dataBatches1)

      // create a new table and commit it
      appendData(engine, tablePath, isNewTable = true, testSchema, partCols = Seq.empty, testData)

      // start the losing transaction
      val losingTx = createTestTxn(engine, tablePath)

      // don't commit losingTxn, instead create a new txn (that changes metadata) and commit it
      spark.sql("ALTER TABLE delta.`" + tablePath + "` ADD COLUMN newCol INT")

      // now attempt to commit the losingTxn
      val ex = intercept[MetadataChangedException] {
        commitTransaction(losingTx, engine, emptyIterable())
      }
      assert(ex.getMessage.contains("The metadata of the Delta table has been changed " +
        "by a concurrent update. Please try the operation again."))
    }
  }

  // Different scenarios that have multiple winning txns and with a checkpoint in between.
  Seq(1, 5, 12).foreach { numWinningTxs =>
    test(s"conflicts - concurrent data append ($numWinningTxs) after the losing txn has started") {
      withTempDirAndEngine { (tablePath, engine) =>
        val testData = Seq(Map.empty[String, Literal] -> dataBatches1)
        var expData = Seq.empty[TestRow]

        // create a new table and commit it
        appendData(engine, tablePath, isNewTable = true, testSchema, partCols = Seq.empty, testData)
        expData ++= testData.flatMap(_._2).flatMap(_.toTestRows)

        // start the losing transaction
        val txn1 = createTestTxn(engine, tablePath)

        // don't commit txn1 yet, instead commit nex txns (that appends data) and commit it
        Seq.range(0, numWinningTxs).foreach { i =>
          appendData(engine, tablePath, data = Seq(Map.empty[String, Literal] -> dataBatches2))
          expData ++= dataBatches2.flatMap(_.toTestRows)
        }

        // add data using the txn1
        val txn1State = txn1.getTransactionState(engine)
        val actions = inMemoryIterable(stageData(txn1State, Map.empty, dataBatches2))
        expData ++= dataBatches2.flatMap(_.toTestRows)

        val txn1Result = commitTransaction(txn1, engine, actions)

        verifyCommitResult(
          txn1Result,
          expVersion = numWinningTxs + 1,
          expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath = tablePath, version = 0, operation = WRITE)
        verifyWrittenContent(tablePath, testSchema, expData)
      }
    }
  }

  def removeTimestampNtzTypeColumns(structType: StructType): StructType = {
    def process(dataType: DataType): Option[DataType] = dataType match {
      case a: ArrayType =>
        val newElementType = process(a.getElementType)
        newElementType.map(new ArrayType(_, a.containsNull()))
      case m: MapType =>
        val newKeyType = process(m.getKeyType)
        val newValueType = process(m.getValueType)
        (newKeyType, newValueType) match {
          case (Some(newKeyType), Some(newValueType)) =>
            Some(new MapType(newKeyType, newValueType, m.isValueContainsNull))
          case _ => None
        }
      case _: TimestampNTZType => None // ignore
      case s: StructType =>
        val newType = removeTimestampNtzTypeColumns(s);
        if (newType.length() > 0) {
          Some(newType)
        } else {
          None
        }
      case _ => Some(dataType)
    }

    var newStructType = new StructType();
    structType.fields().forEach { field =>
      val newDataType = process(field.getDataType)
      if (newDataType.isDefined) {
        newStructType = newStructType.add(field.getName(), newDataType.get)
      }
    }
    newStructType
  }

  def createTestTxn(
      engine: Engine,
      tablePath: String,
      schema: Option[StructType] = None): Transaction = {
    val table = Table.forPath(engine, tablePath)
    var txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, MANUAL_UPDATE)
    schema.foreach(s => txnBuilder = txnBuilder.withSchema(engine, s))
    txnBuilder.build(engine)
  }

  def collectStatsFromAddFiles(engine: Engine, path: String): Seq[String] = {
    val snapshot = Table.forPath(engine, path).getLatestSnapshot(engine)
    val scan = snapshot.getScanBuilder.build()
    val scanFiles = scan.asInstanceOf[ScanImpl].getScanFiles(engine, true)

    scanFiles.asScala.toList.flatMap { scanFile =>
      scanFile.getRows.asScala.toList.flatMap { row =>
        val add = row.getStruct(row.getSchema.indexOf("add"))
        val idx = add.getSchema.indexOf("stats")
        if (idx >= 0 && !add.isNullAt(idx)) List(add.getString(idx)) else Nil
      }
    }
  }
}
