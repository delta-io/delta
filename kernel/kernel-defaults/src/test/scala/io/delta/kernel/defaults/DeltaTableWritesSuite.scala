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

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel._
import io.delta.kernel.data.{ColumnVector, ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.{DefaultIntVector, DefaultStringVector, DefaultSubFieldVector}
import io.delta.kernel.defaults.internal.expressions.DefaultExpressionEvaluator
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.defaults.utils.TestRow
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions._
import io.delta.kernel.expressions.{Column, Expression, Literal, ScalarExpression}
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.internal.checkpoints.CheckpointerSuite.selectSingleElement
import io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames
import io.delta.kernel.internal.{SnapshotImpl, TableConfig}
import io.delta.kernel.internal.util.ColumnMapping
import io.delta.kernel.types.DateType.DATE
import io.delta.kernel.types.DoubleType.DOUBLE
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StringType.STRING
import io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ
import io.delta.kernel.types.TimestampType.TIMESTAMP
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.CloseableIterable

import java.util.{Locale, Optional}
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.immutable.Seq

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
          .withSchema(engine, new StructType().add("ts_ntz", TIMESTAMP_NTZ))
          .build(engine)
      }
      assert(ex.getMessage.contains("Kernel doesn't support writing data of type: timestamp_ntz"))
    }
  }

  test("create table - table already exists at the location") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val txn = txnBuilder.withSchema(engine, testSchema).build(engine)
      txn.commit(engine, emptyIterable())

      {
        val ex = intercept[TableAlreadyExistsException] {
          table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
            .withSchema(engine, testSchema)
            .build(engine)
        }
        assert(ex.getMessage.contains("Table already exists, but provided a new schema. " +
          "Schema can only be set on a new table."))
      }
      {
        val ex = intercept[TableAlreadyExistsException] {
          table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
            .withPartitionColumns(engine, Seq("part1", "part2").asJava)
            .build(engine)
        }
        assert(ex.getMessage.contains("Table already exists, but provided new partition columns." +
          " Partition columns can only be set on a new table."))
      }
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
      val txnResult = txn.commit(engine, emptyIterable())

      assert(txnResult.getVersion === 0)
      assert(!txnResult.isReadyForCheckpoint)

      verifyCommitInfo(tablePath = tablePath, version = 0)
      verifyWrittenContent(tablePath, testSchema, Seq.empty)
    }
  }

  test("create table and set properties") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txn1 = createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)

      txn1.commit(engine, emptyIterable())

      val ver0Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(engine, ver0Snapshot, TableConfig.CHECKPOINT_INTERVAL, 10)

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
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      )
      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(engine, ver1Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)
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
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      )

      val ver1Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(engine, ver1Snapshot, TableConfig.CHECKPOINT_INTERVAL, 10)

      // Try to commit txn1
      txn1.commit(engine, emptyIterable())

      val ver2Snapshot = table.getLatestSnapshot(engine).asInstanceOf[SnapshotImpl]
      assertMetadataProp(engine, ver2Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)
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
      assertMetadataProp(engine, ver1Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)
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
      assertMetadataProp(engine, ver0Snapshot, TableConfig.CHECKPOINT_INTERVAL, 2)

      val configurations = ver0Snapshot.getMetadata.getConfiguration
      assert(configurations.containsKey(TableConfig.CHECKPOINT_INTERVAL.getKey))
      assert(
        !configurations.containsKey(
          TableConfig.CHECKPOINT_INTERVAL.getKey.toLowerCase(Locale.ROOT)))
    }
  }

  test("create table - invalid properties - expect failure") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex1 = intercept[UnknownConfigurationException] {
        createTxn(
          engine, tablePath, isNewTable = true, testSchema, Seq.empty, Map("invalid key" -> "10"))
      }
      assert(ex1.getMessage.contains("Unknown configuration was specified: invalid key"))

      val ex2 = intercept[InvalidConfigurationValueException] {
        createTxn(
          engine,
          tablePath,
          isNewTable = true,
          testSchema, Seq.empty, Map(TableConfig.CHECKPOINT_INTERVAL.getKey -> "-1"))
      }
      assert(
        ex2.getMessage.contains(
          String.format(
            "Invalid value for table property '%s': '%s'. %s",
            TableConfig.CHECKPOINT_INTERVAL.getKey, "-1", "needs to be a positive integer.")))
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
      val txnResult = txn.commit(engine, emptyIterable())

      assert(txnResult.getVersion === 0)
      assert(!txnResult.isReadyForCheckpoint)

      verifyCommitInfo(tablePath, version = 0, Seq("Part1", "part2"))
      verifyWrittenContent(tablePath, schema, Seq.empty)
    }
  }

  test("create table with all supported types") {
    withTempDirAndEngine { (tablePath, engine) =>
      val parquetAllTypes = goldenTablePath("parquet-all-types")
      val schema = removeUnsupportedTypes(tableSchema(parquetAllTypes))

      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
      val txn = txnBuilder.withSchema(engine, schema).build(engine)
      val txnResult = txn.commit(engine, emptyIterable())

      assert(txnResult.getVersion === 0)
      assert(!txnResult.isReadyForCheckpoint)

      verifyCommitInfo(tablePath, version = 0)
      verifyWrittenContent(tablePath, schema, Seq.empty)
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
        data = Seq(Map.empty[String, Literal] -> (dataBatches1 ++ dataBatches2))
      )

      val expectedAnswer = dataBatches1.flatMap(_.toTestRows) ++ dataBatches2.flatMap(_.toTestRows)

      verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tblPath, version = 0, partitionCols = Seq.empty, operation = WRITE)
      verifyWrittenContent(tblPath, testSchema, expectedAnswer)
    }
  }

  test("create table with collation string") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)

      val schema = new StructType()
        .add("c1", new StringType("UTF8_LCASE"), true)
        .add("c2", STRING, true)

      val txn = txnBuilder
        .withSchema(engine, schema)
        .build(engine)

      val txnResult = txn.commit(engine, emptyIterable())

      val dataStringC1 = List("A", "b", "a", "A", "c")
      val dataStringC2 = List("b", "c", "d", "d", "e")
      val columnVectorStringC1 = new DefaultStringVector(dataStringC1.length, Optional.empty(),
        dataStringC1.asJava.toArray(Array.empty[String]), "UTF8_LCASE")
      val columnVectorStringC2 = new DefaultStringVector(dataStringC1.length, Optional.empty(),
        dataStringC2.asJava.toArray(Array.empty[String]), "UTF8_BINARY")
      val columnarBatch = new DefaultColumnarBatch(dataStringC1.length, schema,
        List(columnVectorStringC1, columnVectorStringC2).asJava.toArray(Array.empty[ColumnVector]))
      val filteredColumnarBatch = new FilteredColumnarBatch(
        columnarBatch, Optional.empty())

      appendData(engine = engine,
        tablePath = tablePath,
        data = Seq(Map.empty[String, Literal] -> Seq(filteredColumnarBatch)))

      //checkTable(tablePath, Seq(TestRow("a", "b"), TestRow("A", "c")))

      val scalarExpression = new CollatedPredicate("=",
        List[Expression](new Column("c1"),
          Literal.ofString("a", "UTF8_LCASE")).asJava)
      val output1 = engine.getExpressionHandler()
        .getEvaluator(schema, scalarExpression, BooleanType.BOOLEAN).eval(columnarBatch)
      println(output1)
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
        data = Seq(Map.empty[String, Literal] -> dataBatches1)
      )

      verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
      verifyCommitInfo(tblPath, version = 0, partitionCols = Seq.empty, operation = WRITE)
      verifyWrittenContent(tblPath, testSchema, dataBatches1.flatMap(_.toTestRows))

      val commitResult1 = appendData(
        engine,
        tblPath,
        data = Seq(Map.empty[String, Literal] -> dataBatches2)
      )

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
      val commitResult = txn.commit(engine, stagedActionsIterable)
      assert(commitResult.getVersion == 0)

      // try to commit the same transaction and expect failure
      val ex = intercept[IllegalStateException] {
        txn.commit(engine, stagedActionsIterable)
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
          Map("part1" -> ofInt(4), "part2" -> ofInt(5)) -> dataPartitionBatches2
        )
      )

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
          data = Seq(Map("part1" -> ofInt(1), "part2" -> ofInt(2)) -> dataPartitionBatches1)
        )

        val expData = dataPartitionBatches1.flatMap(_.toTestRows)

        verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tblPath, version = 0, partitionCols, operation = WRITE)
        verifyWrittenContent(tblPath, testPartitionSchema, expData)
      }
      {
        val commitResult1 = appendData(
          engine,
          tblPath,
          data = Seq(Map("part1" -> ofInt(4), "part2" -> ofInt(5)) -> dataPartitionBatches2)
        )

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
        1 -> Seq(v1Part0Values -> v1Part0Data, v1Part1Values -> v1Part1Data)
      )

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
        verifyWrittenContent(tblPath, schema, if (i == 0) expV0Data else expV0Data ++ expV1Data)
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

  test("insert into table - all supported types data") {
    withTempDirAndEngine { (tblPath, engine) =>
      val parquetAllTypes = goldenTablePath("parquet-all-types")
      val schema = removeUnsupportedTypes(tableSchema(parquetAllTypes))

      val data = readTableUsingKernel(engine, parquetAllTypes, schema).to[Seq]
      val dataWithPartInfo = Seq(Map.empty[String, Literal] -> data)

      appendData(engine, tblPath, isNewTable = true, schema, Seq.empty, dataWithPartInfo)
      var expData = dataWithPartInfo.flatMap(_._2).flatMap(_.toTestRows)

      val checkpointInterval = 4
      setCheckpointInterval(tblPath, checkpointInterval)

      for (i <- 2 until 5) {
        // insert until a checkpoint is required
        val commitResult = appendData(engine, tblPath, data = dataWithPartInfo)

        expData = expData ++ dataWithPartInfo.flatMap(_._2).flatMap(_.toTestRows)
        checkpointIfReady(engine, tblPath, commitResult, expSize = i /* one file per version */)

        verifyCommitResult(commitResult, expVersion = i, i % checkpointInterval == 0)
        verifyCommitInfo(tblPath, version = i, null, operation = WRITE)
        verifyWrittenContent(tblPath, schema, expData)
      }
      assertCheckpointExists(tblPath, atVersion = checkpointInterval)
    }
  }

  test("insert into partitioned table - all supported partition column types data") {
    withTempDirAndEngine { (tblPath, engine) =>
      val parquetAllTypes = goldenTablePath("parquet-all-types")
      val schema = removeUnsupportedTypes(tableSchema(parquetAllTypes))
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
        "timestampType"
      )
      val casePreservingPartCols =
        casePreservingPartitionColNames(schema, partCols.asJava).asScala.to[Seq]

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
              case st: StringType => Literal.ofString(vector.getString(rowId), st.getCollationName)
              case _: BinaryType => Literal.ofBinary(vector.getBinary(rowId))
              case _: DateType => Literal.ofDate(vector.getInt(rowId))
              case _: TimestampType => Literal.ofTimestamp(vector.getLong(rowId))
              case _ =>
                throw new IllegalArgumentException(s"Unsupported type: ${vector.getDataType}")
            }
          }
          (partCol, literal)
        }.toMap
      }

      val data = readTableUsingKernel(engine, parquetAllTypes, schema).to[Seq]

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
      verifyCommitInfo(tblPath, version = 0, casePreservingPartCols, operation = WRITE)

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
        appendData(engine,
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
        val data = Seq(Map("part1" -> ofInt(1), "part2" -> ofString("sdsd", "UTF8_LCASE"))
          -> dataPartitionBatches1)
        appendData(engine,
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

      def commitAndVerify(newTbl: Boolean, txn: Transaction,
          actions: CloseableIterable[Row], expTblVer: Long): Unit = {
        val commitResult = txn.commit(engine, actions)

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

  test("conflicts - creating new table - table created by other txn after current txn start") {
    withTempDirAndEngine { (tablePath, engine) =>
      val losingTx = createTestTxn(engine, tablePath, Some(testSchema))

      // don't commit losingTxn, instead create a new txn and commit it
      val winningTx = createTestTxn(engine, tablePath, Some(testSchema))
      val winningTxResult = winningTx.commit(engine, emptyIterable())

      // now attempt to commit the losingTxn
      val ex = intercept[ProtocolChangedException] {
        losingTx.commit(engine, emptyIterable())
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
        losingTx.commit(engine, emptyIterable())
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

        val txn1Result = txn1.commit(engine, actions)

        verifyCommitResult(
          txn1Result, expVersion = numWinningTxs + 1, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tablePath = tablePath, version = 0, operation = WRITE)
        verifyWrittenContent(tablePath, testSchema, expData)
      }
    }
  }

  def removeUnsupportedTypes(structType: StructType): StructType = {
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
        val newType = removeUnsupportedTypes(s);
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
    engine: Engine, tablePath: String, schema: Option[StructType] = None): Transaction = {
    val table = Table.forPath(engine, tablePath)
    var txnBuilder = table.createTransactionBuilder(engine, testEngineInfo, CREATE_TABLE)
    schema.foreach(s => txnBuilder = txnBuilder.withSchema(engine, s))
    txnBuilder.build(engine)
  }

  test("create table with unsupported column mapping mode") {
    withTempDirAndEngine { (tablePath, engine) =>
      val ex = intercept[InvalidConfigurationValueException] {
        createTxn(engine, tablePath, isNewTable = true, testSchema, partCols = Seq.empty,
          tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "invalid"))
          .commit(engine, emptyIterable())
      }
      assert(ex.getMessage.contains("Invalid value for table property " +
        "'delta.columnMapping.mode': 'invalid'. Needs to be one of: [none, id, name]."))
    }
  }

  test("create table with column mapping mode = none") {
    withTempDirAndEngine { (tablePath, engine) =>
      createTxn(engine, tablePath, isNewTable = true, testSchema, partCols = Seq.empty,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none"))
        .commit(engine, emptyIterable())

      val table = Table.forPath(engine, tablePath)
      assert(table.getLatestSnapshot(engine).getSchema(engine).equals(testSchema))
    }
  }

  test("cannot update table with unsupported column mapping mode") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      createTxn(engine, tablePath, isNewTable = true, testSchema, Seq.empty)
        .commit(engine, emptyIterable())

      val ex = intercept[InvalidConfigurationValueException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.WRITE)
          .withTableProperties(
            engine,
            Map(ColumnMapping.COLUMN_MAPPING_MODE_KEY -> "invalid").asJava)
          .build(engine)
      }
      assert(ex.getMessage.contains("Invalid value for table property " +
        "'delta.columnMapping.mode': 'invalid'. Needs to be one of: [none, id, name]."))
    }
  }

  test("cannot update table with unsupported column mapping mode change") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      createTxn(engine, tablePath, isNewTable = true, testSchema, partCols = Seq.empty,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
        .commit(engine, emptyIterable())

      val ex = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.WRITE)
          .withTableProperties(
            engine,
            Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "none").asJava)
          .build(engine)
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'name' to 'none' is not supported"))
    }
  }

  test("cannot update column mapping mode from id to name on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("b", IntegerType.INTEGER, true)

      createTxn(engine, tablePath, isNewTable = true, schema, partCols = Seq.empty,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id"))
        .commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema(engine)
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      val ex = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.WRITE)
          .withTableProperties(
            engine,
            Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name").asJava)
          .build(engine)
          .commit(engine, emptyIterable())
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'id' to 'name' is not supported"))
    }
  }

  test("cannot update column mapping mode from name to id on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("b", IntegerType.INTEGER, true)

      createTxn(engine, tablePath, isNewTable = true, schema, partCols = Seq.empty,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
        .commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema(engine)
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)

      val ex = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.WRITE)
          .withTableProperties(
            engine,
            Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id").asJava)
          .build(engine)
          .commit(engine, emptyIterable())
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'name' to 'id' is not supported"))
    }
  }

  test("cannot update column mapping mode from none to id on existing table") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("b", IntegerType.INTEGER, true)

      createTxn(engine, tablePath, isNewTable = true, schema, partCols = Seq.empty)
        .commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema(engine)
      assert(structType.equals(schema))

      val ex = intercept[IllegalArgumentException] {
        table.createTransactionBuilder(engine, testEngineInfo, Operation.WRITE)
          .withTableProperties(
            engine,
            Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id").asJava)
          .build(engine)
          .commit(engine, emptyIterable())
      }
      assert(ex.getMessage.contains("Changing column mapping mode " +
        "from 'none' to 'id' is not supported"))
    }
  }


  test("unsupported protocol version with column mapping mode and no protocol update in metadata") {
    // TODO
  }

  test("unsupported protocol version in existing table and new metadata with column mapping mode") {
    // TODO
  }

  test("new table with column mapping mode = name") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("b", IntegerType.INTEGER, true)

      createTxn(engine, tablePath, isNewTable = true, schema, partCols = Seq.empty,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name"))
        .commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema(engine)
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)
    }
  }

  test("new table with column mapping mode = id") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("b", IntegerType.INTEGER, true)

      createTxn(engine, tablePath, isNewTable = true, schema, partCols = Seq.empty,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id"))
        .commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema(engine)
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)
    }
  }

  test("can update existing table to column mapping mode = name") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("b", IntegerType.INTEGER, true)

      createTxn(engine, tablePath, isNewTable = true, schema, partCols = Seq.empty)
        .commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema(engine)
      assert(structType.equals(schema))

      table.createTransactionBuilder(engine, testEngineInfo, Operation.WRITE)
        .withTableProperties(
          engine,
          Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "name").asJava)
        .build(engine)
        .commit(engine, emptyIterable())

      val updatedSchema = table.getLatestSnapshot(engine).getSchema(engine)
      assertColumnMapping(updatedSchema.get("a"), 1, "a")
      assertColumnMapping(updatedSchema.get("b"), 2, "b")
    }
  }

  test("new table with column mapping mode = id and nested schema") {
    withTempDirAndEngine { (tablePath, engine) =>
      val table = Table.forPath(engine, tablePath)
      val schema = new StructType()
        .add("a", StringType.STRING, true)
        .add("b",
          new StructType()
            .add("d", IntegerType.INTEGER, true)
            .add("e", IntegerType.INTEGER, true))
        .add("c", IntegerType.INTEGER, true)

      createTxn(engine, tablePath, isNewTable = true, schema, partCols = Seq.empty,
        tableProperties = Map(TableConfig.COLUMN_MAPPING_MODE.getKey -> "id",
          TableConfig.ICEBERG_COMPAT_V2_ENABLED.getKey -> "true"))
        .commit(engine, emptyIterable())

      val structType = table.getLatestSnapshot(engine).getSchema(engine)
      assertColumnMapping(structType.get("a"), 1)
      assertColumnMapping(structType.get("b"), 2)
      val innerStruct = structType.get("b").getDataType.asInstanceOf[StructType]
      assertColumnMapping(innerStruct.get("d"), 3)
      assertColumnMapping(innerStruct.get("e"), 4)
      assertColumnMapping(structType.get("c"), 5)
    }
  }

  private def assertColumnMapping(
    field: StructField,
    expId: Long,
    expPhyName: String = "UUID"): Unit = {
    val meta = field.getMetadata
    assert(meta.get(ColumnMapping.COLUMN_MAPPING_ID_KEY) == expId)
    // For new tables the physical column name is a UUID. For existing tables, we
    // try to keep the physical column name same as the one in the schema
    if (expPhyName == "UUID") {
      assert(meta.get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY).toString.startsWith("col-"))
    } else {
      assert(meta.get(ColumnMapping.COLUMN_MAPPING_PHYSICAL_NAME_KEY) == expPhyName)
    }
  }
}
