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

import com.fasterxml.jackson.databind.ObjectMapper
import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.Operation.{CREATE_TABLE, WRITE}
import io.delta.kernel._
import io.delta.kernel.data.{ColumnVector, ColumnarBatch, FilteredColumnarBatch, Row}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, TableAlreadyExistsException, TableNotFoundException}
import io.delta.kernel.expressions.Literal
import io.delta.kernel.expressions.Literal.{ofDouble, ofInt, ofNull, ofTimestamp}
import io.delta.kernel.internal.util.SchemaUtils.casePreservingPartitionColNames
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.types.DateType.DATE
import io.delta.kernel.types.DoubleType.DOUBLE
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StringType.STRING
import io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ
import io.delta.kernel.types.TimestampType.TIMESTAMP
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.{emptyIterable, inMemoryIterable}
import io.delta.kernel.utils.CloseableIterator
import org.scalatest.funsuite.AnyFunSuite

import java.util.Optional
import scala.collection.JavaConverters._

class DeltaTableWritesSuite extends AnyFunSuite with TestUtils with ParquetSuiteBase {
  val OBJ_MAPPER = new ObjectMapper()
  val testEngineInfo = "test-engine"

  /** Test table schemas and test */
  val testSchema = new StructType().add("id", INTEGER)
  val dataBatches1 = generateData(testSchema, Seq.empty, Map.empty, 200, 3)
  val dataBatches2 = generateData(testSchema, Seq.empty, Map.empty, 400, 5)

  val testPartitionColumns = Seq("part1", "part2")
  val testPartitionSchema = new StructType()
    .add("id", INTEGER)
    .add("part1", INTEGER) // partition column
    .add("part2", INTEGER) // partition column

  val dataPartitionBatches1 = generateData(
    testPartitionSchema,
    testPartitionColumns,
    Map("part1" -> ofInt(1), "part2" -> ofInt(2)),
    batchSize = 237,
    numBatches = 3)

  val dataPartitionBatches2 = generateData(
    testPartitionSchema,
    testPartitionColumns,
    Map("part1" -> ofInt(4), "part2" -> ofInt(5)),
    batchSize = 876,
    numBatches = 7)

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
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath
      val commitResult0 = appendData(
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

  test("insert into table - already existing table") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

      {
        val commitResult0 = appendData(
          tblPath,
          isNewTable = true,
          testSchema,
          partCols = Seq.empty,
          data = Seq(Map.empty[String, Literal] -> dataBatches1)
        )

        verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tblPath, version = 0, partitionCols = Seq.empty, operation = WRITE)
        verifyWrittenContent(tblPath, testSchema, dataBatches1.flatMap(_.toTestRows))
      }
      {
        val commitResult1 = appendData(
          tblPath,
          data = Seq(Map.empty[String, Literal] -> dataBatches2)
        )

        val expAnswer = dataBatches1.flatMap(_.toTestRows) ++ dataBatches2.flatMap(_.toTestRows)

        verifyCommitResult(commitResult1, expVersion = 1, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tblPath, version = 1, partitionCols = null, operation = WRITE)
        verifyWrittenContent(tblPath, testSchema, expAnswer)
      }
    }
  }

  test("insert into table - fails when committing the same txn twice") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath
      val table = Table.forPath(defaultEngine, tblPath)

      val txn = createWriteTxnBuilder(table)
        .withSchema(defaultEngine, testSchema)
        .build(defaultEngine)

      val txnState = txn.getTransactionState(defaultEngine)
      val stagedFiles = stageData(txnState, Map.empty, dataBatches1)

      val stagedActionsIterable = inMemoryIterable(stagedFiles)
      val commitResult = txn.commit(defaultEngine, stagedActionsIterable)
      assert(commitResult.getVersion == 0)

      // try to commit the same transaction and expect failure
      val ex = intercept[IllegalStateException] {
        txn.commit(defaultEngine, stagedActionsIterable)
      }
      assert(ex.getMessage.contains(
        "Transaction is already attempted to commit. Create a new transaction."))
    }
  }

  test("insert into partitioned table - table created from scratch") {
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

      val commitResult0 = appendData(
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
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath
      val partitionCols = Seq("part1", "part2")

      {
        val commitResult0 = appendData(
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
    withTempDir { tempDir =>
      val tblPath = tempDir.getAbsolutePath

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

      val expV0Data = v0Part0Data.flatMap(_.toTestRows) ++ v0Part1Data.flatMap(_.toTestRows)

      {
        val commitResult0 = appendData(
          tblPath,
          isNewTable = true,
          schema,
          partCols,
          Seq(v0Part0Values -> v0Part0Data, v0Part1Values -> v0Part1Data))

        verifyCommitResult(commitResult0, expVersion = 0, expIsReadyForCheckpoint = false)
        verifyCommitInfo(tblPath, version = 0, expPartCols, operation = WRITE)
        verifyWrittenContent(tblPath, schema, expV0Data)
      }

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

      val expV1Data = v1Part0Data.flatMap(_.toTestRows) ++ v1Part1Data.flatMap(_.toTestRows)

      {
        val commitResult1 = appendData(
          tblPath,
          data = Seq(v1Part0Values -> v1Part0Data, v1Part1Values -> v1Part1Data))

        verifyCommitResult(commitResult1, expVersion = 1, expIsReadyForCheckpoint = false)
        // For just inserts, we don't write the `partitionBy` in the commit info op parameters
        // That is the behavior of the Delta-Spark
        verifyCommitInfo(tblPath, version = 1, partitionCols = null, operation = WRITE)
        verifyWrittenContent(tblPath, schema, expV0Data ++ expV1Data)
      }
    }
  }

  def withTempDirAndEngine(f: (String, Engine) => Unit): Unit = {
    withTempDir { dir => f(dir.getAbsolutePath, defaultEngine) }
  }

  def verifyWrittenContent(path: String, expSchema: StructType, expData: Seq[TestRow]): Unit = {
    val actSchema = tableSchema(path)
    assert(actSchema === expSchema)

    // verify data using Kernel reader
    checkTable(path, expData)

    // verify data using Spark reader.
    // Spark reads the timestamp partition columns in local timezone vs. Kernel reads in UTC.
    // So, we need to set the timezone to UTC before reading the data using Spark.
    withSparkTimeZone("UTC") { () =>
      val resultSpark = spark.sql(s"SELECT * FROM delta.`$path`").collect().map(TestRow(_))
      checkAnswer(resultSpark, expData)
    }
  }

  def verifyCommitInfo(
    tablePath: String,
    version: Long,
    partitionCols: Seq[String] = Seq.empty,
    isBlindAppend: Boolean = true,
    operation: Operation = CREATE_TABLE): Unit = {
    val row = spark.sql(s"DESCRIBE HISTORY delta.`$tablePath`")
      .filter(s"version = $version")
      .select(
        "version",
        "operationParameters.partitionBy",
        "isBlindAppend",
        "engineInfo",
        "operation")
      .collect().last

    assert(row.getAs[Long]("version") === version)
    assert(row.getAs[Long]("partitionBy") ===
      (if (partitionCols == null) null else OBJ_MAPPER.writeValueAsString(partitionCols.asJava)))
    assert(row.getAs[Boolean]("isBlindAppend") === isBlindAppend)
    assert(row.getAs[Seq[String]]("engineInfo") ===
      "Kernel-" + Meta.KERNEL_VERSION + "/" + testEngineInfo)
    assert(row.getAs[String]("operation") === operation.getDescription)
  }

  def verifyCommitResult(
    result: TransactionCommitResult,
    expVersion: Long,
    expIsReadyForCheckpoint: Boolean): Unit = {
    assert(result.getVersion === expVersion)
    assert(result.isReadyForCheckpoint === expIsReadyForCheckpoint)
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

  def createWriteTxnBuilder(table: Table): TransactionBuilder = {
    table.createTransactionBuilder(defaultEngine, testEngineInfo, Operation.WRITE)
  }

  def generateData(
    schema: StructType,
    partitionCols: Seq[String],
    partitionValues: Map[String, Literal],
    batchSize: Int,
    numBatches: Int): Seq[FilteredColumnarBatch] = {
    val partitionValuesSchemaCase =
      casePreservingPartitionColNames(partitionCols.asJava, partitionValues.asJava)

    var batches = Seq.empty[ColumnarBatch]
    for (_ <- 0 until numBatches) {
      var vectors = Seq.empty[ColumnVector]
      schema.fields().forEach { field =>
        val colType = field.getDataType
        val partValue = partitionValuesSchemaCase.get(field.getName)
        if (partValue != null) {
          // handle the partition column by inserting a vector with single value
          val vector = testSingleValueVector(colType, batchSize, partValue.getValue)
          vectors = vectors :+ vector
        } else {
          // handle the regular columns
          val vector = testColumnVector(batchSize, colType)
          vectors = vectors :+ vector
        }
      }
      batches = batches :+ new DefaultColumnarBatch(batchSize, schema, vectors.toArray)
    }
    batches.map(batch => new FilteredColumnarBatch(batch, Optional.empty()))
  }

  def stageData(
    state: Row,
    partitionValues: Map[String, Literal],
    data: Seq[FilteredColumnarBatch])
  : CloseableIterator[Row] = {
    val physicalDataIter = Transaction.transformLogicalData(
      defaultEngine,
      state,
      toCloseableIterator(data.toIterator.asJava),
      partitionValues.asJava)

    val writeContext = Transaction.getWriteContext(defaultEngine, state, partitionValues.asJava)

    val writeResultIter = defaultEngine
      .getParquetHandler
      .writeParquetFiles(
        writeContext.getTargetDirectory,
        physicalDataIter,
        writeContext.getStatisticsColumns)

    Transaction.generateAppendActions(defaultEngine, state, writeResultIter, writeContext)
  }

  def appendData(
    tablePath: String,
    isNewTable: Boolean = false,
    schema: StructType = null,
    partCols: Seq[String] = null,
    data: Seq[(Map[String, Literal], Seq[FilteredColumnarBatch])]): TransactionCommitResult = {

    var txnBuilder = createWriteTxnBuilder(Table.forPath(defaultEngine, tablePath))

    if (isNewTable) {
      txnBuilder = txnBuilder.withSchema(defaultEngine, schema)
        .withPartitionColumns(defaultEngine, partCols.asJava)
    }

    val txn = txnBuilder.build(defaultEngine)
    val txnState = txn.getTransactionState(defaultEngine)

    val actions = data.map { case (partValues, partData) =>
      stageData(txnState, partValues, partData)
    }

    val combineActions = inMemoryIterable(actions.reduceLeft(_ combine _))
    txn.commit(defaultEngine, combineActions)
  }
}
