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
import io.delta.kernel.Operation.CREATE_TABLE
import io.delta.kernel.defaults.internal.parquet.ParquetSuiteBase
import io.delta.kernel.defaults.utils.{TestRow, TestUtils}
import io.delta.kernel.engine.Engine
import io.delta.kernel.exceptions.{KernelException, TableAlreadyExistsException, TableNotFoundException}
import io.delta.kernel.types.DateType.DATE
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.TimestampNTZType.TIMESTAMP_NTZ
import io.delta.kernel.types._
import io.delta.kernel.utils.CloseableIterable.emptyIterable
import io.delta.kernel.{Meta, Operation, Table}
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters._

class DeltaTableWritesSuite extends AnyFunSuite with TestUtils with ParquetSuiteBase {
  val OBJ_MAPPER = new ObjectMapper()
  val testEngineInfo = "test-engine"

  /** Test table schemas */
  val testSchema = new StructType().add("id", INTEGER)
  val testPartitionColumns = Seq("part1", "part2")
  val testPartitionSchema = new StructType()
    .add("id", INTEGER)
    .add("part1", INTEGER) // partition column
    .add("part2", INTEGER) // partition column

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

  def withTempDirAndEngine(f: (String, Engine) => Unit): Unit = {
    withTempDir { dir => f(dir.getAbsolutePath, defaultEngine) }
  }

  def verifyWrittenContent(
    tablePath: String,
    expSchema: StructType,
    expData: Seq[TestRow]): Unit = {
    val actSchema = tableSchema(tablePath)
    assert(actSchema === expSchema)

    // verify data using Kernel reader
    checkTable(tablePath, expData)

    // verify data using Spark reader
    val resultSpark = spark.sql(s"SELECT * FROM delta.`$tablePath`").collect().map(TestRow(_))
    checkAnswer(resultSpark, expData)
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
}
