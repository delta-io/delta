/*
 * Copyright (2023) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.parquet

import io.delta.golden.GoldenTableUtils.goldenTablePath
import io.delta.kernel.defaults.utils.{ExpressionTestUtils, TestRow}
import io.delta.kernel.expressions.Literal.{ofBinary, ofBoolean, ofDate, ofDouble, ofFloat, ofInt, ofLong, ofNull, ofString}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import io.delta.kernel.expressions._
import io.delta.kernel.internal.util.InternalUtils.daysSinceEpoch
import io.delta.kernel.types.{IntegerType, StructType}
import org.apache.spark.sql.{Row, types => sparktypes}

import java.nio.file.Files
import java.sql.Date
import java.util.Optional
import io.delta.kernel.test.VectorTestUtils

class ParquetReaderPredicatePushdownSuite extends AnyFunSuite
    with BeforeAndAfterAll with ParquetSuiteBase with VectorTestUtils with ExpressionTestUtils {

  //////////////////////////////////////////////////////////////////////////////////
  // Test data generation and helper methods
  //////////////////////////////////////////////////////////////////////////////////

  var testParquetTable: String = ""

  override def beforeAll(): Unit = {
    super.beforeAll()

    testParquetTable = Files.createTempDirectory("tempDir").toString

    // Generate a test Parquet file with 20 row groups. Each row group has 100 rows.
    // Parquet-mr checks whether the current row group has reached the limit or for every 100 rows.
    // We set the `parquet.block.size` to very low, so for every 100 rows, it will create a
    // new row group.
    val rows = Seq.range(0, 20).flatMap(i => generateRowsGroup(i))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), testTableSchema)
    withSQLConf("parquet.block.size" -> 1.toString) {
      df.repartition(1)
        .orderBy("rowId")
        .write
        .format("delta")
        .mode("append")
        .save(testParquetTable)
    }
  }

  // test table schema
  val testTableSchema: sparktypes.StructType = {
    // These are the only supported column types in Parquet filter push down
    def allTypesSchema(): Array[sparktypes.StructField] = {
      Seq(
        sparktypes.StructField("byteCol", sparktypes.ByteType),
        sparktypes.StructField("shortCol", sparktypes.ShortType),
        sparktypes.StructField("intCol", sparktypes.IntegerType),
        sparktypes.StructField("longCol", sparktypes.LongType),
        sparktypes.StructField("floatCol", sparktypes.FloatType),
        sparktypes.StructField("doubleCol", sparktypes.DoubleType),
        sparktypes.StructField("stringCol", sparktypes.StringType),
        // column with values that are truncated in stats
        sparktypes.StructField("truncatedStringCol", sparktypes.StringType),
        sparktypes.StructField("binaryCol", sparktypes.BinaryType),
        sparktypes.StructField("truncatedBinaryCol", sparktypes.BinaryType),
        sparktypes.StructField("booleanCol", sparktypes.BooleanType),
        sparktypes.StructField("dateCol", sparktypes.DateType)
      ).toArray
    }

    // supported data type columns as top level columns
    new sparktypes.StructType(allTypesSchema())
      // supported data type columns as nested columns
      .add("nested", sparktypes.StructType(allTypesSchema()))
      // row id to help with the test results verification
      .add("rowId", sparktypes.IntegerType)
  }

  private def generateRowsGroup(rowGroupIdx: Int): Seq[Row] = {
    def values(rowId: Int): Seq[Any] = {
      Seq(
        if (rowId % 72 != 0) rowId.byteValue() else null,
        if (rowId % 56 != 0) rowId.shortValue() else null,
        if (rowId % 23 != 0) rowId else null,
        if (rowId % 25 != 0) (rowId + 1).longValue() else null,
        if (rowId % 28 != 0) (rowId + 0.125).floatValue() else null,
        if (rowId % 54 != 0) (rowId + 0.000001).doubleValue() else null,
        if (rowId % 57 != 0) "%05d".format(rowId) else null,
        if (rowId % 57 != 0) "%050d".format(rowId) else null, // truncated stats
        if (rowId % 59 != 0) "%06d".format(rowId).getBytes else null,
        if (rowId % 59 != 0) "%060d".format(rowId).getBytes else null, // truncated stats
        // alternate between true and false for each row group
        (rowId / 100) % 2 == 0,
        if (rowId % 61 != 0) new Date(rowId * 86400000L /* millis in a day */) else null
      )
    }

    Seq.range(rowGroupIdx * 100, (rowGroupIdx + 1) * 100).map { rowId =>
      Row.fromSeq(
        values(rowId) ++ // top-level column values
          Seq(
            Row.fromSeq(values(rowId)), // nested column values
            rowId // row id to help with the test results verification
          )
      )
    }
  }

  def generateExpData(rowGroupIndexes: Seq[Int]): Seq[TestRow] = {
    spark.createDataFrame(
      spark.sparkContext.parallelize(rowGroupIndexes.flatMap(i => generateRowsGroup(i))),
      testTableSchema)
      .collect
      .map(TestRow(_))
  }

  private def readUsingKernel(tablePath: String, predicate: Predicate): Seq[TestRow] = {
    val readSchema: StructType = tableSchema(testParquetTable)
    readParquetFilesUsingKernel(tablePath, readSchema, Optional.of(predicate))
  }

  private def assertConvertedFilterIsEmpty(predicate: Predicate, tablePath: String): Unit = {
    val parquetFileSchema = parquetFiles(tablePath).map(footer(_)).head.getFileMetaData.getSchema

    assert(
      !ParquetFilterUtils.toParquetFilter(parquetFileSchema, predicate).isPresent,
      "Predicate should not be converted to Parquet filter")
  }

  //////////////////////////////////////////////////////////////////////////////////
  // End-2-end tests
  //////////////////////////////////////////////////////////////////////////////////

  Seq(
    // filter on int type column
    (
      eq(col("intCol"), ofInt(20)), // top-level column
      eq(col("nested", "intCol"), ofInt(20)), // nested column
      Seq(0) // expected row groups
    ),
    // filter on long type column
    (
      gt(col("longCol"), ofLong(1600)),
      gt(col("nested", "longCol"), ofLong(1600)),
      Seq(16, 17, 18, 19) // expected row groups
    ),
    // filter on float type column
    (
      lt(col("floatCol"), ofFloat(1000.0f)),
      lt(col("nested", "floatCol"), ofFloat(1000.0f)),
      Seq(0, 1, 2, 3, 4, 5, 6, 7, 8, 9) // expected row groups
    ),
    // filter on double type column
    (
      gt(col("doubleCol"), ofDouble(1000.0)),
      gt(col("nested", "doubleCol"), ofDouble(1000.0)),
      Seq(10, 11, 12, 13, 14, 15, 16, 17, 18, 19) // expected row groups
    ),
    // filter on boolean type column
    (
      eq(col("booleanCol"), ofBoolean(true)),
      eq(col("nested", "booleanCol"), ofBoolean(true)),
      Seq(0, 2, 4, 6, 8, 10, 12, 14, 16, 18) // expected row groups
    ),
    // filter on date type column
    (
      lte(col("dateCol"), ofDate(
        daysSinceEpoch(new Date(500 * 86400000L /* millis in a day */)))),
      lte(col("nested", "dateCol"), ofDate(
        daysSinceEpoch(new Date(500 * 86400000L /* millis in a day */)))),
      Seq(0, 1, 2, 3, 4, 5) // expected row groups
    ),
    // filter on string type column
    (
      eq(col("stringCol"), ofString("%05d".format(300))),
      eq(col("nested", "stringCol"), ofString("%05d".format(300))),
      Seq(3) // expected row groups
    ),
    // filter on binary type column
    (
      gte(col("binaryCol"), ofBinary("%06d".format(1700).getBytes)),
      gte(col("nested", "binaryCol"), ofBinary("%06d".format(1700).getBytes)),
      Seq(17, 18, 19) // expected row groups
    ),
    // filter on truncated stats string type column
    (
      gte(col("truncatedStringCol"), ofString("%050d".format(300))),
      gte(col("nested", "truncatedStringCol"), ofString("%050d".format(300))),
      Seq(3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19) // expected row groups
    ),
    // filter on truncated stats binary type column
    (
      lte(col("truncatedBinaryCol"), ofBinary("%060d".format(600).getBytes)),
      lte(col("nested", "truncatedBinaryCol"), ofBinary("%060d".format(600).getBytes)),
      Seq(0, 1, 2, 3, 4, 5, 6) // expected row groups
    )
  ).foreach {
    // boolean, int32, data, int64, float, double, binary, string
    // Test table has 20 row groups, each with 100 rows.
    case (predicateTopLevelCol, predicateNestedCol, expRowGroups) =>
      Seq(predicateTopLevelCol, predicateNestedCol).foreach { predicate =>
        test(s"filter pushdown: $predicate") {
          val actualData = readUsingKernel(testParquetTable, predicate)
          val expOutputRowCount = expRowGroups.length * 100 // 100 rows per row group
          assert(actualData.size === expOutputRowCount, s"predicate: $predicate")
          checkAnswer(actualData, generateExpData(expRowGroups))
        }
      }
  }

  test("for a column that doesn't exist in the table") {
    val testPredicate = predicate("=", col("nonExistentCol"), ofInt(20))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("literal and column are swapped") {
    val testPredicate = predicate("=", ofInt(20), col("intCol"))
    val actData = readUsingKernel(testParquetTable, testPredicate)
    checkAnswer(actData, generateExpData(Seq(0)))
  }

  test("comparator literal value is null") {
    val testPredicate = predicate("=", col("intCol"), ofNull(IntegerType.INTEGER))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("comparator that compare column and column") {
    val testPredicate = predicate("=", col("intCol"), col("longCol"))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("comparator that compare literal and literal") {
    val testPredicate = predicate("=", ofInt(20), ofInt(20))
    assertConvertedFilterIsEmpty(testPredicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, testPredicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("OR support") {
    val predicate = or(
      eq(col("intCol"), ofInt(20)),
      eq(col("longCol"), ofLong(1600))
    )
    val actData = readUsingKernel(testParquetTable, predicate)
    checkAnswer(actData, generateExpData(Seq(0, 15)))
  }

  test("one end of the OR is not convertible") {
    val predicate = or(
      eq(col("intCol"), ofInt(1599)),
      eq(col("nonExistentCol"), ofInt(1600))
    )
    assertConvertedFilterIsEmpty(predicate, testParquetTable)

    val actData = readUsingKernel(testParquetTable, predicate)
    // contains all the data in the table as the predicate is not pushed down
    checkAnswer(actData, generateExpData(Seq.range(0, 20)))
  }

  test("AND support") {
    val predicate = and(
      eq(col("intCol"), ofInt(1599)),
      eq(col("longCol"), ofLong(1600))
    )
    val actData = readUsingKernel(testParquetTable, predicate)
    checkAnswer(actData, generateExpData(Seq(15)))
  }

  test("one end of the AND is not convertible") {
    val predicate = and(
      eq(col("intCol"), ofInt(1599)),
      eq(col("nonExistentCol"), ofInt(1600))
    )
    val actData = readUsingKernel(testParquetTable, predicate)
    checkAnswer(actData, generateExpData(Seq(15)))
  }

  test("not support") {
    val predicate = not(eq(col("booleanCol"), ofBoolean(true)))
    val actData = readUsingKernel(testParquetTable, predicate)
    // every odd rowgroup has true for booleanCol
    checkAnswer(actData, generateExpData(Seq(1, 3, 5, 7, 9, 11, 13, 15, 17, 19)))
  }

  test("doesn't work on the repeated columns") {
    val testTable = goldenTablePath("parquet-all-types")
    val readSchema = tableSchema(testTable)

    val predicate = eq(col("array_of_prims"), ofInt(20))
    assertConvertedFilterIsEmpty(predicate, testTable)

    val actResult = readParquetFilesUsingKernel(testTable, readSchema, Optional.of(predicate))
    val expResult = readParquetFilesUsingSpark(testTable, readSchema)

    checkAnswer(actResult, expResult)
  }
}
