/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel

import java.lang.{Long => JLong}
import java.util
import java.util.Optional

import scala.collection.JavaConverters._

import io.delta.kernel.Transaction.{generateAppendActions, transformLogicalData}
import io.delta.kernel.data._
import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.expressions.{Column, Literal}
import io.delta.kernel.internal.{DataWriteContextImpl, TableConfig, TransactionImpl}
import io.delta.kernel.internal.TableConfig.ICEBERG_COMPAT_V2_ENABLED
import io.delta.kernel.internal.actions.{Format, Metadata}
import io.delta.kernel.internal.data.TransactionStateRow
import io.delta.kernel.internal.types.DataTypeJsonSerDe
import io.delta.kernel.internal.util.Utils.toCloseableIterator
import io.delta.kernel.internal.util.VectorUtils
import io.delta.kernel.internal.util.VectorUtils.stringStringMapValue
import io.delta.kernel.statistics.DataFileStatistics
import io.delta.kernel.test.{MockEngineUtils, VectorTestUtils}
import io.delta.kernel.types.{DoubleType, FloatType, IntegerType, LongType, StringType, StructType, TimestampType}
import io.delta.kernel.utils.{CloseableIterator, DataFileStatus}

import org.scalatest.funsuite.AnyFunSuite

class TransactionSuite extends AnyFunSuite with VectorTestUtils with MockEngineUtils {

  import io.delta.kernel.TransactionSuite._

  Seq(true, false).foreach { icebergCompatV2Enabled =>
    test("transformLogicalData: un-partitioned table, " +
      s"icebergCompatV2Enabled=$icebergCompatV2Enabled") {
      val transformedDateIter = transformLogicalData(
        mockEngine(),
        testTxnState(testSchema, enableIcebergCompatV2 = icebergCompatV2Enabled),
        testData(includePartitionCols = false),
        Map.empty[String, Literal].asJava /* partition values */ )
      transformedDateIter.map(_.getData).forEachRemaining(batch => {
        assert(batch.getSchema === testSchema)
      })
    }
  }

  Seq(true, false).foreach { icebergCompatV2Enabled =>
    test("transformLogicalData: partitioned table, " +
      s"icebergCompatV2Enabled=$icebergCompatV2Enabled") {
      val transformedDateIter = transformLogicalData(
        mockEngine(),
        testTxnState(
          testSchemaWithPartitions,
          testPartitionColNames,
          enableIcebergCompatV2 = icebergCompatV2Enabled),
        testData(includePartitionCols = true),
        /* partition values */
        Map("state" -> Literal.ofString("CA"), "country" -> Literal.ofString("USA")).asJava)

      transformedDateIter.map(_.getData).forEachRemaining(batch => {
        if (icebergCompatV2Enabled) {
          // when icebergCompatV2Enabled is true, the partition columns included in the output
          assert(batch.getSchema === testSchemaWithPartitions)
        } else {
          assert(batch.getSchema === testSchema)
        }
      })
    }
  }

  Seq(true, false).foreach { icebergCompatV2Enabled =>
    test(s"generateAppendActions: iceberg comaptibily checks, " +
      s"icebergCompatV2Enabled=$icebergCompatV2Enabled") {
      val txnState = testTxnState(testSchema, enableIcebergCompatV2 = icebergCompatV2Enabled)
      val engine = mockEngine()

      Seq(
        // missing stats
        (
          testDataFileStatuses(
            "file1" -> testStats(Some(10)), // valid stats
            "file2" -> None // missing stats
          ),
          "icebergCompatV2 compatibility requires 'numRecords' statistic." // expected error message
        )).foreach { case (actionRows, expectedErrorMsg) =>
        if (icebergCompatV2Enabled) {
          val ex = intercept[KernelException] {
            generateAppendActions(engine, txnState, actionRows, testDataWriteContext())
              .forEachRemaining(_ => ()) // consume the iterator
          }
          assert(ex.getMessage.contains(expectedErrorMsg))
        } else {
          // when icebergCompatV2Enabled is disabled, no exception should be thrown
          generateAppendActions(engine, txnState, actionRows, testDataWriteContext())
            .forEachRemaining(_ => ()) // consume the iterator
        }
      }

      // valid stats
      val dataFileStatuses = testDataFileStatuses(
        "file1" -> testStats(Some(10)),
        "file2" -> testStats(Some(20)))
      var actStats: Seq[String] = Seq.empty
      generateAppendActions(engine, txnState, dataFileStatuses, testDataWriteContext())
        .forEachRemaining { addActionRow =>
          val addOrdinal = addActionRow.getSchema.indexOf("add")
          val add = addActionRow.getStruct(addOrdinal)
          val statsOrdinal = add.getSchema.indexOf("stats")
          actStats = actStats :+ add.getString(statsOrdinal)
        }

      assert(actStats === Seq(
        "{\"numRecords\":10,\"minValues\":{},\"maxValues\":{},\"nullCount\":{}}",
        "{\"numRecords\":20,\"minValues\":{},\"maxValues\":{},\"nullCount\":{}}"))
    }
  }

  Seq(0, -1).foreach { numIndexedCols =>
    test(s"stats: validate DATA_SKIPPING_NUM_INDEXED_COLS limit" +
      s" is respected when set to: $numIndexedCols") {
      // Create schema with simple and nested columns
      val schema = new StructType()
        .add("id", IntegerType.INTEGER)
        .add("name", StringType.STRING)
        .add(
          "metrics",
          new StructType()
            .add("temperature", DoubleType.DOUBLE)
            .add("humidity", FloatType.FLOAT))
        .add("timestamp", TimestampType.TIMESTAMP)

      // Create transaction state with specified numIndexedCols
      val configMap = Map(TableConfig
        .DATA_SKIPPING_NUM_INDEXED_COLS.getKey -> numIndexedCols.toString)
      val metadata = new Metadata(
        "id",
        Optional.empty(),
        Optional.empty(),
        new Format(),
        DataTypeJsonSerDe.serializeDataType(schema),
        schema,
        VectorUtils.buildArrayValue(Seq.empty.asJava, StringType.STRING),
        Optional.empty(),
        stringStringMapValue(configMap.asJava))
      val txnState = TransactionStateRow.of(metadata, "table path", 200 /* maxRetries */ )

      // Get statistics columns and define expected result
      val statsColumns = TransactionImpl.getStatisticsColumns(txnState)
      if (numIndexedCols == -1) {
        // For -1, all leaf columns should be included
        val expectedColumns = Set(
          new Column("id"),
          new Column("name"),
          new Column(Array("metrics", "temperature")),
          new Column(Array("metrics", "humidity")),
          new Column("timestamp"))

        assert(
          statsColumns.size == 5,
          s"With numIndexedCols=$numIndexedCols, expected 5 columns but got ${statsColumns.size}")

        // Verify the expected columns are present
        val statsColumnsSet = statsColumns.asScala.toSet
        assert(statsColumnsSet == expectedColumns, s"Expected columns do not match actual columns")
      } else if (numIndexedCols == 0) {
        // For 0, no columns should be included
        assert(
          statsColumns.isEmpty,
          s"With numIndexedCols=$numIndexedCols," +
            s" expected no columns but got ${statsColumns.size} columns")
      }
    }
  }
}

object TransactionSuite extends VectorTestUtils with MockEngineUtils {
  def testData(includePartitionCols: Boolean): CloseableIterator[FilteredColumnarBatch] = {
    toCloseableIterator(
      Seq.range(0, 5).map(_ => testBatch(includePartitionCols)).asJava.iterator()).map(batch =>
      new FilteredColumnarBatch(batch, Optional.empty()))
  }

  def testBatch(includePartitionCols: Boolean): ColumnarBatch = {
    val testColumnVectors = Seq(
      stringVector(Seq("Alice", "Bob", "Charlie", "David", "Eve")), // name
      longVector(20L, 30L, 40L, 50L, 60L), // id
      stringVector(Seq("Campbell", "Roanoke", "Dallas", "Monte Sereno", "Minneapolis")) // city
    ) ++ {
      if (includePartitionCols) {
        Seq(
          stringVector(Seq("CA", "TX", "NC", "CA", "MN")), // state
          stringVector(Seq("USA", "USA", "USA", "USA", "USA")) // country
        )
      } else Seq.empty
    }

    columnarBatch(
      schema = if (includePartitionCols) testSchemaWithPartitions else testSchema,
      testColumnVectors)
  }

  val testSchema: StructType = new StructType()
    .add("name", StringType.STRING)
    .add("id", LongType.LONG)
    .add("city", StringType.STRING)

  val testSchemaWithPartitions: StructType = new StructType(testSchema.fields())
    .add("state", StringType.STRING) // partition column
    .add("country", StringType.STRING) // partition column

  val testPartitionColNames = Seq("state", "country")

  def columnarBatch(schema: StructType, vectors: Seq[ColumnVector]): ColumnarBatch = {
    new ColumnarBatch {
      override def getSchema: StructType = schema

      override def getColumnVector(ordinal: Int): ColumnVector = vectors(ordinal)

      override def withDeletedColumnAt(ordinal: Int): ColumnarBatch = {
        // Update the schema
        val newStructFields = new util.ArrayList(schema.fields)
        newStructFields.remove(ordinal)
        val newSchema: StructType = new StructType(newStructFields)

        // Update the vectors
        val newColumnVectors = vectors.toBuffer
        newColumnVectors.remove(ordinal)
        columnarBatch(newSchema, newColumnVectors.toSeq)
      }

      override def getSize: Int = vectors.head.getSize
    }
  }

  def testTxnState(
      schema: StructType,
      partitionCols: Seq[String] = Seq.empty,
      enableIcebergCompatV2: Boolean = false): Row = {
    val configurationMap = Map(ICEBERG_COMPAT_V2_ENABLED.getKey -> enableIcebergCompatV2.toString)
    val metadata = new Metadata(
      "id",
      Optional.empty(), /* name */
      Optional.empty(), /* description */
      new Format(),
      DataTypeJsonSerDe.serializeDataType(schema),
      schema,
      VectorUtils.buildArrayValue(partitionCols.asJava, StringType.STRING), // partitionColumns
      Optional.empty(), // createdTime
      stringStringMapValue(configurationMap.asJava) // configurationMap
    )
    TransactionStateRow.of(metadata, "table path", 200 /* maxRetries */ )
  }

  def testStats(numRowsOpt: Option[Long]): Option[DataFileStatistics] = {
    numRowsOpt.map(numRows => {
      new DataFileStatistics(
        numRows,
        Map.empty[Column, Literal].asJava, // minValues - empty value as this is just for tests.
        Map.empty[Column, Literal].asJava, // maxValues - empty value as this is just for tests.
        Map.empty[Column, JLong].asJava // nullCount - empty value as this is just for tests.
      )
    })
  }

  def testDataFileStatuses(fileNameStatsPairs: (String, Option[DataFileStatistics])*)
      : CloseableIterator[DataFileStatus] = {

    toCloseableIterator(
      fileNameStatsPairs.map { case (fileName, statsOpt) =>
        new DataFileStatus(
          fileName,
          23L, // size - arbitrary value as this is just for tests.
          23L, // modificationTime - arbitrary value as this is just for tests.
          Optional.ofNullable(statsOpt.orNull))
      }.asJava.iterator())
  }

  /** Test [[DataWriteContext]]. As of now we don't need any custom values in this suite. */
  def testDataWriteContext(): DataWriteContext = {
    new DataWriteContextImpl("targetDir", Map.empty[String, Literal].asJava, Seq.empty.asJava)
  }
}
