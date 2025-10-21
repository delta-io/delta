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
package io.delta.kernel.defaults.internal.expressions

import java.lang.{Boolean => BooleanJ, Double => DoubleJ, Float => FloatJ, Integer => IntegerJ, Long => LongJ}
import java.math.{BigDecimal => BigDecimalJ}
import java.sql.{Date, Timestamp}
import java.util
import java.util.Optional

import scala.jdk.CollectionConverters._

import io.delta.kernel.data.{ColumnarBatch, ColumnVector}
import io.delta.kernel.defaults.internal.data.DefaultColumnarBatch
import io.delta.kernel.defaults.internal.data.vector.{DefaultIntVector, DefaultStructVector}
import io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getValueAsObject
import io.delta.kernel.expressions._
import io.delta.kernel.expressions.AlwaysFalse.ALWAYS_FALSE
import io.delta.kernel.expressions.AlwaysTrue.ALWAYS_TRUE
import io.delta.kernel.expressions.Literal._
import io.delta.kernel.internal.util.InternalUtils
import io.delta.kernel.types._
import io.delta.kernel.types.CollationIdentifier.SPARK_UTF8_BINARY

import org.scalatest.funsuite.AnyFunSuite

class DefaultExpressionEvaluatorSuite extends AnyFunSuite with ExpressionSuiteBase {
  test("evaluate expression: literal") {
    val testLiterals = Seq(
      Literal.ofBoolean(true),
      Literal.ofBoolean(false),
      Literal.ofNull(BooleanType.BOOLEAN),
      ofByte(24.toByte),
      Literal.ofNull(ByteType.BYTE),
      Literal.ofShort(876.toShort),
      Literal.ofNull(ShortType.SHORT),
      Literal.ofInt(2342342),
      Literal.ofNull(IntegerType.INTEGER),
      Literal.ofLong(234234223L),
      Literal.ofNull(LongType.LONG),
      Literal.ofFloat(23423.4223f),
      Literal.ofNull(FloatType.FLOAT),
      Literal.ofDouble(23423.422233d),
      Literal.ofNull(DoubleType.DOUBLE),
      Literal.ofString("string_val"),
      Literal.ofNull(StringType.STRING),
      Literal.ofBinary("binary_val".getBytes),
      Literal.ofNull(BinaryType.BINARY),
      Literal.ofDate(4234),
      Literal.ofNull(DateType.DATE),
      Literal.ofTimestamp(2342342342232L),
      Literal.ofNull(TimestampType.TIMESTAMP),
      Literal.ofTimestampNtz(2342342342L),
      Literal.ofNull(TimestampNTZType.TIMESTAMP_NTZ))

    val inputBatches: Seq[ColumnarBatch] = Seq[ColumnarBatch](
      zeroColumnBatch(rowCount = 0),
      zeroColumnBatch(rowCount = 25),
      zeroColumnBatch(rowCount = 128))

    for (literal <- testLiterals) {
      val outputDataType = literal.getDataType
      for (inputBatch <- inputBatches) {
        val outputVector: ColumnVector =
          evaluator(inputBatch.getSchema, literal, literal.getDataType)
            .eval(inputBatch)

        assert(inputBatch.getSize === outputVector.getSize)
        assert(outputDataType === outputVector.getDataType)

        for (rowId <- 0 until outputVector.getSize) {
          if (literal.getValue == null) {
            assert(
              outputVector.isNullAt(rowId),
              s"expected a null at $rowId for $literal expression")
          } else {
            assert(
              literal.getValue === getValueAsObject(outputVector, rowId),
              s"invalid value at $rowId for $literal expression")
          }
        }
      }
    }
  }

  PRIMITIVE_TYPES.foreach { dataType =>
    test(s"evaluate expression: column of type $dataType") {
      val batchSize = 78;
      val batchSchema = new StructType().add("col1", dataType)
      val batch = new DefaultColumnarBatch(
        batchSize,
        batchSchema,
        Array[ColumnVector](testColumnVector(batchSize, dataType)))

      val outputVector = evaluator(batchSchema, new Column("col1"), dataType)
        .eval(batch)

      assert(batchSize === outputVector.getSize)
      assert(dataType === outputVector.getDataType)
      Seq.range(0, outputVector.getSize).foreach { rowId =>
        assert(
          testIsNullValue(dataType, rowId) === outputVector.isNullAt(rowId),
          s"unexpected nullability at $rowId for $dataType type vector")
        if (!outputVector.isNullAt(rowId)) {
          assert(
            testColumnValue(dataType, rowId) === getValueAsObject(outputVector, rowId),
            s"unexpected value at $rowId for $dataType type vector")
        }
      }
    }
  }

  test("evaluate expression: nested column reference") {
    val col3Type = IntegerType.INTEGER
    val col2Type = new StructType().add("col3", col3Type)
    val col1Type = new StructType().add("col2", col2Type)
    val batchSchema = new StructType().add("col1", col1Type)

    val numRows = 5
    val col3Nullability = Seq(false, true, false, true, false).toArray
    val col3Values = Seq(27, 24, 29, 100, 125).toArray
    val col3Vector =
      new DefaultIntVector(col3Type, numRows, Optional.of(col3Nullability), col3Values)

    val col2Nullability = Seq(false, true, true, true, false).toArray
    val col2Vector =
      new DefaultStructVector(numRows, col2Type, Optional.of(col2Nullability), Array(col3Vector))

    val col1Nullability = Seq(false, false, false, true, false).toArray
    val col1Vector =
      new DefaultStructVector(numRows, col1Type, Optional.of(col1Nullability), Array(col2Vector))

    val batch = new DefaultColumnarBatch(numRows, batchSchema, Array(col1Vector))

    def assertTypeAndNullability(
        actVector: ColumnVector,
        expType: DataType,
        expNullability: Array[Boolean]): Unit = {
      assert(actVector.getDataType === expType)
      assert(actVector.getSize === numRows)
      Seq.range(0, numRows).foreach { rowId =>
        assert(actVector.isNullAt(rowId) === expNullability(rowId))
      }
    }

    val col3Ref = new Column(Array("col1", "col2", "col3"))
    val col3RefResult = evaluator(batchSchema, col3Ref, col3Type).eval(batch)
    assertTypeAndNullability(col3RefResult, col3Type, col3Nullability);
    Seq.range(0, numRows).foreach { rowId =>
      assert(col3RefResult.getInt(rowId) === col3Values(rowId))
    }

    val col2Ref = new Column(Array("col1", "col2"))
    val col2RefResult = evaluator(batchSchema, col2Ref, col2Type).eval(batch)
    assertTypeAndNullability(col2RefResult, col2Type, col2Nullability)

    val col1Ref = new Column(Array("col1"))
    val col1RefResult = evaluator(batchSchema, col1Ref, col1Type).eval(batch)
    assertTypeAndNullability(col1RefResult, col1Type, col1Nullability)

    // try to reference non-existent nested column
    val colNotValid = new Column(Array("col1", "colX`X"))
    val ex = intercept[IllegalArgumentException] {
      evaluator(batchSchema, colNotValid, col1Type).eval(batch)
    }
    assert(ex.getMessage.contains("column(`col1`.`colX``X`) doesn't exist in input data schema"))
  }

  test("evaluate expression: always true, always false") {
    Seq(ALWAYS_TRUE, ALWAYS_FALSE).foreach { expr =>
      val batch = zeroColumnBatch(rowCount = 87)
      val outputVector = evaluator(batch.getSchema, expr, BooleanType.BOOLEAN).eval(batch)
      assert(outputVector.getSize === 87)
      assert(outputVector.getDataType === BooleanType.BOOLEAN)
      Seq.range(0, 87).foreach { rowId =>
        assert(!outputVector.isNullAt(rowId))
        assert(outputVector.getBoolean(rowId) == (expr == ALWAYS_TRUE))
      }
    }
  }

  test("evaluate expression: and, or") {
    val leftColumn = booleanVector(
      Seq[BooleanJ](true, true, false, false, null, true, null, false, null))
    val rightColumn = booleanVector(
      Seq[BooleanJ](true, false, false, true, true, null, false, null, null))
    val expAndOutputVector = booleanVector(
      Seq[BooleanJ](true, false, false, false, null, null, false, false, null))
    val expOrOutputVector = booleanVector(
      Seq[BooleanJ](true, true, false, true, true, true, null, null, null))

    val schema = new StructType()
      .add("left", BooleanType.BOOLEAN)
      .add("right", BooleanType.BOOLEAN)
    val batch = new DefaultColumnarBatch(leftColumn.getSize, schema, Array(leftColumn, rightColumn))

    val left = comparator("=", new Column("left"), Literal.ofBoolean(true))
    val right = comparator("=", new Column("right"), Literal.ofBoolean(true))

    // And
    val andExpression = and(left, right)
    val actAndOutputVector = evaluator(schema, andExpression, BooleanType.BOOLEAN).eval(batch)
    checkBooleanVectors(actAndOutputVector, expAndOutputVector)

    // Or
    val orExpression = or(left, right)
    val actOrOutputVector = evaluator(schema, orExpression, BooleanType.BOOLEAN).eval(batch)
    checkBooleanVectors(actOrOutputVector, expOrOutputVector)
  }

  test("evaluate expression: not") {
    val childColumn = booleanVector(Seq[BooleanJ](true, false, null))

    val schema = new StructType().add("child", BooleanType.BOOLEAN)
    val batch = new DefaultColumnarBatch(childColumn.getSize, schema, Array(childColumn))

    val notExpression = new Predicate(
      "NOT",
      comparator("=", new Column("child"), Literal.ofBoolean(true)))
    val expOutputVector = booleanVector(Seq[BooleanJ](false, true, null))
    val actOutputVector = evaluator(schema, notExpression, BooleanType.BOOLEAN).eval(batch)
    checkBooleanVectors(actOutputVector, expOutputVector)
  }

  test("evaluate expression: is not null") {
    val childColumn = booleanVector(Seq[BooleanJ](true, false, null))

    val schema = new StructType().add("child", BooleanType.BOOLEAN)
    val batch = new DefaultColumnarBatch(childColumn.getSize, schema, Array(childColumn))

    val isNotNullExpression = new Predicate("IS_NOT_NULL", new Column("child"))
    val expOutputVector = booleanVector(Seq[BooleanJ](true, true, false))
    val actOutputVector = evaluator(schema, isNotNullExpression, BooleanType.BOOLEAN).eval(batch)
    checkBooleanVectors(actOutputVector, expOutputVector)
  }

  test("evaluate expression: is null") {
    val childColumn = booleanVector(Seq[BooleanJ](true, false, null))

    val schema = new StructType().add("child", BooleanType.BOOLEAN)
    val batch = new DefaultColumnarBatch(childColumn.getSize, schema, Array(childColumn))

    val isNullExpression = new Predicate("IS_NULL", new Column("child"))
    val expOutputVector = booleanVector(Seq[BooleanJ](false, false, true))
    val actOutputVector = evaluator(schema, isNullExpression, BooleanType.BOOLEAN).eval(batch)
    checkBooleanVectors(actOutputVector, expOutputVector)
  }

  test("evaluate expression: coalesce (boolean columns)") {
    val col1 = booleanVector(Seq[BooleanJ](true, null, null, null))
    val col2 = booleanVector(Seq[BooleanJ](false, false, null, null))
    val col3 = booleanVector(Seq[BooleanJ](true, true, true, null))

    val schema = new StructType()
      .add("col1", BooleanType.BOOLEAN)
      .add("col2", BooleanType.BOOLEAN)
      .add("col3", BooleanType.BOOLEAN)

    val batch = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2, col3))

    val coalesceEpxr1 = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(new Column("col1")))
    val expOutputVector1 = booleanVector(Seq[BooleanJ](true, null, null, null))
    val actOutputVector1 = evaluator(schema, coalesceEpxr1, BooleanType.BOOLEAN).eval(batch)
    checkBooleanVectors(actOutputVector1, expOutputVector1)

    val coalesceEpxr3 = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(
        new Column("col1"),
        new Column("col2"),
        new Column("col3")))
    val expOutputVector3 = booleanVector(Seq[BooleanJ](true, false, true, null))
    val actOutputVector3 = evaluator(schema, coalesceEpxr3, BooleanType.BOOLEAN).eval(batch)
    checkBooleanVectors(actOutputVector3, expOutputVector3)
  }

  test("evaluate expression: coalesce (long columns)") {
    val longCol1 = longVector(Seq(1L, null, null, 4L))
    val longCol2 = longVector(Seq(null, 2L, null, 5L))
    val longCol3 = longVector(Seq(100L, null, 3L, null))
    val longSchema = new StructType()
      .add("longCol1", LongType.LONG)
      .add("longCol2", LongType.LONG)
      .add("longCol3", LongType.LONG)
    val longBatch =
      new DefaultColumnarBatch(longCol1.getSize, longSchema, Array(longCol1, longCol2, longCol3))
    val longCoalesceExpr = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(new Column("longCol1"), new Column("longCol2"), new Column("longCol3")))
    val expLongOutput = longVector(Seq(1L, 2L, 3L, 4L))
    val actLongOutput = evaluator(longSchema, longCoalesceExpr, LongType.LONG).eval(longBatch)
    checkLongVectors(actLongOutput, expLongOutput)
  }

  test("evaluate expression: coalesce (string columns)") {
    val strCol1 = stringVector(Seq("a", null, null, "d"))
    val strCol2 = stringVector(Seq("null", "b", null, null))
    val strCol3 = stringVector(Seq(null, null, "c", "abc"))
    val strSchema = new StructType()
      .add("strCol1", StringType.STRING)
      .add("strCol2", StringType.STRING)
      .add("strCol3", StringType.STRING)
    val strBatch =
      new DefaultColumnarBatch(strCol1.getSize, strSchema, Array(strCol1, strCol2, strCol3))
    val strCoalesceExpr = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(new Column("strCol1"), new Column("strCol2"), new Column("strCol3")))
    val expStrOutput = stringVector(Seq("a", "b", "c", "d"))
    val actStrOutput = evaluator(strSchema, strCoalesceExpr, StringType.STRING).eval(strBatch)
    checkStringVectors(actStrOutput, expStrOutput)
  }

  test("evaluate expression: coalesce (timestamp columns)") {
    val tsCol1 = timestampVector(Seq(1000L, null, null, 4000L))
    val tsCol2 = timestampVector(Seq(null, 2000L, null, 5000L))
    val tsCol3 = timestampVector(Seq(10000L, null, 3000L, null))
    val tsSchema = new StructType()
      .add("tsCol1", TimestampType.TIMESTAMP)
      .add("tsCol2", TimestampType.TIMESTAMP)
      .add("tsCol3", TimestampType.TIMESTAMP)
    val tsBatch =
      new DefaultColumnarBatch(tsCol1.getSize, tsSchema, Array(tsCol1, tsCol2, tsCol3))
    val tsCoalesceExpr = new ScalarExpression(
      "COALESCE",
      util.Arrays.asList(new Column("tsCol1"), new Column("tsCol2"), new Column("tsCol3")))
    val expTsOutput = timestampVector(Seq(1000L, 2000L, 3000L, 4000L))
    val actTsOutput = evaluator(tsSchema, tsCoalesceExpr, TimestampType.TIMESTAMP).eval(tsBatch)
    checkLongVectors(actTsOutput, expTsOutput)
  }

  test("evaluate expression: coalesce (unequal column types)") {
    def checkUnsupportedTypes(
        col1Type: DataType,
        col2Type: DataType,
        messageContains: String): Unit = {
      val schema = new StructType()
        .add("col1", col1Type)
        .add("col2", col2Type)
      val batch = new DefaultColumnarBatch(
        5,
        schema,
        Array(testColumnVector(5, col1Type), testColumnVector(5, col2Type)))
      val e = intercept[UnsupportedOperationException] {
        evaluator(
          schema,
          new ScalarExpression(
            "COALESCE",
            util.Arrays.asList(new Column("col1"), new Column("col2"))),
          col1Type).eval(batch)
      }
      assert(e.getMessage.contains(messageContains))
    }
    // TODO support least-common-type resolution
    checkUnsupportedTypes(
      LongType.LONG,
      IntegerType.INTEGER,
      "Coalesce is only supported for arguments of the same type")
  }

  test("evaluate expression: ADD (column and literal)") {
    val col1 = longVector(Seq(1, 2, 3, 4, null))
    val schema = new StructType().add("col1", LongType.LONG)
    val batch = new DefaultColumnarBatch(col1.getSize, schema, Array(col1))

    // ADD with literal
    val addExpr = new ScalarExpression(
      "ADD",
      util.Arrays.asList(new Column("col1"), Literal.ofLong(10L)))
    val expOutputVector = longVector(Seq(11, 12, 13, 14, null))
    val actOutputVector = evaluator(schema, addExpr, LongType.LONG).eval(batch)
    checkLongVectors(actOutputVector, expOutputVector)
  }

  test("evaluate expression: ADD (column and column)") {
    val col1 = longVector(Seq(1, 2, null, 4, null))
    val col2 = longVector(Seq(null, 20, 30, 40, null))
    val schema = new StructType()
      .add("col1", LongType.LONG)
      .add("col2", LongType.LONG)
    val batch = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2))

    // ADD with two columns
    val addExpr = new ScalarExpression(
      "ADD",
      util.Arrays.asList(new Column("col1"), new Column("col2")))
    val expOutputVector = longVector(Seq(null, 22, null, 44, null))
    val actOutputVector = evaluator(schema, addExpr, LongType.LONG).eval(batch)
    checkLongVectors(actOutputVector, expOutputVector)
  }

  test("evaluate expression: ADD (more than two operands)") {
    val col1 = longVector(Seq(1, 2, null, 4, null))
    val col2 = longVector(Seq(null, 20, 30, 40, null))
    val col3 = longVector(Seq(5, null, 15, null, 25))
    val schema = new StructType()
      .add("col1", LongType.LONG)
      .add("col2", LongType.LONG)
      .add("col3", LongType.LONG)
    val batch = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2, col3))

    // ADD with three columns
    val addExpr = new ScalarExpression(
      "ADD",
      util.Arrays.asList(new Column("col1"), new Column("col2"), new Column("col3")))
    val e =
      intercept[UnsupportedOperationException] {
        evaluator(schema, addExpr, LongType.LONG).eval(batch)
      }
    assert(e.getMessage.contains("ADD requires exactly two arguments: left and right operands"))
  }

  test("evaluate expression: ADD (unequal operand types") {
    val col1 = longVector(Seq(1, 2, null, 4, null))
    val col2 = floatVector(Seq(1.0f, 2.0f, 3.0f, 4.0f, null))
    val schema = new StructType()
      .add("col1", LongType.LONG)
      .add("col2", FloatType.FLOAT)
    val batch = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2))

    // ADD with two columns of different types
    val addExpr = new ScalarExpression(
      "ADD",
      util.Arrays.asList(new Column("col1"), new Column("col2")))
    val e =
      intercept[UnsupportedOperationException] {
        evaluator(schema, addExpr, LongType.LONG).eval(batch)
      }
    assert(e.getMessage.contains("ADD is only supported for arguments of the same type"))
  }

  test("evaluate expression: ADD (unsupported types)") {
    val col1 = stringVector(Seq("a", "b", null, "d", null))
    val col2 = stringVector(Seq("x", "y", "z", "w", null))
    val schema = new StructType()
      .add("col1", StringType.STRING)
      .add("col2", StringType.STRING)
    val batch = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2))

    // ADD with two columns of unsupported types
    val addExpr = new ScalarExpression(
      "ADD",
      util.Arrays.asList(new Column("col1"), new Column("col2")))
    val e =
      intercept[UnsupportedOperationException] {
        evaluator(schema, addExpr, StringType.STRING).eval(batch)
      }
    assert(e.getMessage.contains(
      "ADD is only supported for numeric types: byte, short, int, long, float, double"))
  }

  test("evaluate expression: TIMEADD with TIMESTAMP columns") {
    val timestampColumn = timestampVector(Seq(
      1577836800000000L, // 2020-01-01 00:00:00.000
      1577836800123456L, // 2020-01-01 00:00:00.123456
      -1 // Representing null
    ))

    val durationColumn = longVector(Seq(
      1000, // 1 second in milliseconds
      100, // 0.1 second in milliseconds
      -1))

    val schema = new StructType()
      .add("timestamp", TimestampType.TIMESTAMP)
      .add("duration", LongType.LONG)

    val batch = new DefaultColumnarBatch(
      timestampColumn.getSize,
      schema,
      Array(timestampColumn, durationColumn))

    // TimeAdd expression adds milliseconds to timestamps
    val timeAddExpr = new ScalarExpression(
      "TIMEADD",
      util.Arrays.asList(new Column("timestamp"), new Column("duration")))

    val expOutputVector = timestampVector(Seq(
      1577836801000000L, // 2020-01-01 00:00:01.000
      1577836800123456L + 100000, // 2020-01-01 00:00:00.123556
      -1 // Null should propagate
    ))
    val actOutputVector = evaluator(schema, timeAddExpr, TimestampType.TIMESTAMP).eval(batch)

    checkTimestampVectors(actOutputVector, expOutputVector)
  }

  def checkUnsupportedTimeAddTypes(
      col1Type: DataType,
      col2Type: DataType): Unit = {
    val schema = new StructType()
      .add("timestamp", col1Type)
      .add("duration", col2Type)
    val batch = new DefaultColumnarBatch(
      5,
      schema,
      Array(testColumnVector(5, col1Type), testColumnVector(5, col2Type)))

    val timeAddExpr = new ScalarExpression(
      "TIMEADD",
      util.Arrays.asList(new Column("timestamp"), new Column("duration")))

    val e = intercept[IllegalArgumentException] {
      val evaluator = new DefaultExpressionEvaluator(schema, timeAddExpr, col1Type)
      evaluator.eval(batch)
    }
    assert(e.getMessage.contains("TIMEADD requires a timestamp and a Long"))
  }

  // Test to ensure TIMEADD requires the first argument to be a TimestampType
  // and the second to be a LongType
  test("TIMEADD with unsupported types") {
    // Check invalid timestamp column type
    checkUnsupportedTimeAddTypes(
      IntegerType.INTEGER,
      IntegerType.INTEGER)

    // Check invalid duration column type
    checkUnsupportedTimeAddTypes(
      TimestampType.TIMESTAMP,
      StringType.STRING)

    // Check valid type but with unsupported operations
    checkUnsupportedTimeAddTypes(
      TimestampType.TIMESTAMP,
      FloatType.FLOAT)
  }

  test("evaluate expression: like") {
    val col1 = stringVector(Seq[String](
      null,
      "one",
      "two",
      "three",
      "four",
      null,
      null,
      "seven",
      "eight"))
    val col2 = stringVector(Seq[String](
      null,
      "one",
      "Two",
      "thr%",
      "four%",
      "f",
      null,
      null,
      "%ght"))
    val schema = new StructType()
      .add("col1", StringType.STRING)
      .add("col2", StringType.STRING)
    val input = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2))

    def checkLike(
        input: DefaultColumnarBatch,
        likeExpression: Predicate,
        expOutputSeq: Seq[BooleanJ]): Unit = {
      val actOutputVector =
        new DefaultExpressionEvaluator(
          schema,
          likeExpression,
          BooleanType.BOOLEAN).eval(input)
      val expOutputVector = booleanVector(expOutputSeq);
      checkBooleanVectors(actOutputVector, expOutputVector)
    }

    // check column expressions on both sides
    checkLike(
      input,
      like(new Column("col1"), new Column("col2")),
      Seq[BooleanJ](null, true, false, true, true, null, null, null, true))

    // check column expression against literal
    checkLike(
      input,
      like(new Column("col1"), Literal.ofString("t%")),
      Seq[BooleanJ](null, false, true, true, false, null, null, false, false))

    // ends with checks
    checkLike(
      input,
      like(new Column("col1"), Literal.ofString("%t")),
      Seq[BooleanJ](null, false, false, false, false, null, null, false, true))

    // contains checks
    checkLike(
      input,
      like(new Column("col1"), Literal.ofString("%t%")),
      Seq[BooleanJ](null, false, true, true, false, null, null, false, true))

    val dummyInput = new DefaultColumnarBatch(
      1,
      new StructType().add("dummy", StringType.STRING),
      Array(stringVector(Seq[String](""))))

    def checkLikeLiteral(
        left: String,
        right: String,
        escape: Character = null,
        expOutput: BooleanJ): Unit = {
      val expression = like(Literal.ofString(left), Literal.ofString(right), Option(escape))
      checkLike(dummyInput, expression, Seq[BooleanJ](expOutput))
    }

    // null/empty
    checkLikeLiteral(null, "a", null, null)
    checkLikeLiteral("a", null, null, null)
    checkLikeLiteral(null, null, null, null)
    checkLikeLiteral("", "", null, true)
    checkLikeLiteral("a", "", null, false)
    checkLikeLiteral("", "a", null, false)

    Seq('!', '@', '#').foreach {
      escape =>
        {
          // simple patterns
          checkLikeLiteral("abc", "abc", escape, true)
          checkLikeLiteral("a_%b", s"a${escape}__b", escape, true)
          checkLikeLiteral("abbc", "a_%c", escape, true)
          checkLikeLiteral("abbc", s"a${escape}__c", escape, false)
          checkLikeLiteral("abbc", s"a%${escape}%c", escape, false)
          checkLikeLiteral("a_%b", s"a%${escape}%b", escape, true)
          checkLikeLiteral("abbc", "a%", escape, true)
          checkLikeLiteral("abbc", "**", escape, false)
          checkLikeLiteral("abc", "a%", escape, true)
          checkLikeLiteral("abc", "b%", escape, false)
          checkLikeLiteral("abc", "bc%", escape, false)
          checkLikeLiteral("a\nb", "a_b", escape, true)
          checkLikeLiteral("ab", "a%b", escape, true)
          checkLikeLiteral("a\nb", "a%b", escape, true)
          checkLikeLiteral("a\nb", "ab", escape, false)
          checkLikeLiteral("a\nb", "a\nb", escape, true)
          checkLikeLiteral("a\n\nb", "a\nb", escape, false)
          checkLikeLiteral("a\n\nb", "a\n_b", escape, true)

          // case
          checkLikeLiteral("A", "a%", escape, false)
          checkLikeLiteral("a", "a%", escape, true)
          checkLikeLiteral("a", "A%", escape, false)
          checkLikeLiteral(s"aAa", s"aA_", escape, true)

          // regex
          checkLikeLiteral("a([a-b]{2,4})a", "_([a-b]{2,4})%", null, true)
          checkLikeLiteral("a([a-b]{2,4})a", "_([a-c]{2,6})_", null, false)

          // %/_
          checkLikeLiteral("a%a", s"%${escape}%%", escape, true)
          checkLikeLiteral("a%", s"%${escape}%%", escape, true)
          checkLikeLiteral("a%a", s"_${escape}%_", escape, true)
          checkLikeLiteral("a_a", s"%${escape}_%", escape, true)
          checkLikeLiteral("a_", s"%${escape}_%", escape, true)
          checkLikeLiteral("a_a", s"_${escape}__", escape, true)

          // double-escaping
          checkLikeLiteral(
            s"$escape$escape$escape$escape",
            s"%${escape}${escape}%",
            escape,
            true)
          checkLikeLiteral("%%", "%%", escape, true)
          checkLikeLiteral(s"${escape}__", s"${escape}${escape}${escape}__", escape, true)
          checkLikeLiteral(s"${escape}__", s"%${escape}${escape}%${escape}%", escape, false)
          checkLikeLiteral(s"_${escape}${escape}${escape}%", s"%${escape}${escape}", escape, false)
        }
    }

    // check '_' for escape char
    checkLikeLiteral("abc", "abc", '_', true)
    checkLikeLiteral("a_%b", s"a__%%b", '_', true)
    checkLikeLiteral("abbc", "a__c", '_', false)
    checkLikeLiteral("abbc", "a%%c", '_', true)
    checkLikeLiteral("abbc", s"a___%c", '_', false)
    checkLikeLiteral("abbc", s"a%_%c", '_', false)

    // check '%' for escape char
    checkLikeLiteral("abc", "abc", '%', true)
    checkLikeLiteral("a_%b", s"a__%%b", '%', false)
    checkLikeLiteral("a_%b", s"a_%%b", '%', true)
    checkLikeLiteral("abbc", "a__c", '%', true)
    checkLikeLiteral("abbc", "a%%c", '%', false)
    checkLikeLiteral("abbc", s"a%__c", '%', false)
    checkLikeLiteral("abbc", s"a%_%_c", '%', false)

    def checkUnsupportedTypes(
        col1Type: DataType,
        col2Type: DataType): Unit = {
      val schema = new StructType()
        .add("col1", col1Type)
        .add("col2", col2Type)
      val expr = like(new Column("col1"), new Column("col2"), Option(null))
      val input = new DefaultColumnarBatch(
        5,
        schema,
        Array(testColumnVector(5, col1Type), testColumnVector(5, col2Type)))

      val e = intercept[UnsupportedOperationException] {
        new DefaultExpressionEvaluator(
          schema,
          expr,
          BooleanType.BOOLEAN).eval(input)
      }
      assert(e.getMessage.contains("LIKE is only supported for string type expressions"))
    }
    checkUnsupportedTypes(BooleanType.BOOLEAN, BooleanType.BOOLEAN)
    checkUnsupportedTypes(LongType.LONG, LongType.LONG)
    checkUnsupportedTypes(IntegerType.INTEGER, IntegerType.INTEGER)
    checkUnsupportedTypes(StringType.STRING, BooleanType.BOOLEAN)
    checkUnsupportedTypes(StringType.STRING, IntegerType.INTEGER)
    checkUnsupportedTypes(StringType.STRING, LongType.LONG)
    checkUnsupportedTypes(BooleanType.BOOLEAN, BooleanType.BOOLEAN)

    // input count checks
    val inputCountCheckUserMessage =
      "Invalid number of inputs to LIKE expression. Example usage:"
    val inputCountError1 = intercept[UnsupportedOperationException] {
      val expression = like(List(Literal.ofString("a")))
      checkLike(dummyInput, expression, Seq[BooleanJ](null))
    }
    assert(inputCountError1.getMessage.contains(inputCountCheckUserMessage))

    val inputCountError2 = intercept[UnsupportedOperationException] {
      val expression = like(List(
        Literal.ofString("a"),
        Literal.ofString("b"),
        Literal.ofString("c"),
        Literal.ofString("d")))
      checkLike(dummyInput, expression, Seq[BooleanJ](null))
    }
    assert(inputCountError2.getMessage.contains(inputCountCheckUserMessage))

    // additional escape token checks
    val escapeCharError1 = intercept[UnsupportedOperationException] {
      val expression =
        like(List(Literal.ofString("a"), Literal.ofString("b"), Literal.ofString("~~")))
      checkLike(dummyInput, expression, Seq[BooleanJ](null))
    }
    assert(escapeCharError1.getMessage.contains(
      "LIKE expects escape token to be a single character"))

    val escapeCharError2 = intercept[UnsupportedOperationException] {
      val expression = like(List(Literal.ofString("a"), Literal.ofString("b"), Literal.ofInt(1)))
      checkLike(dummyInput, expression, Seq[BooleanJ](null))
    }
    assert(escapeCharError2.getMessage.contains(
      "LIKE expects escape token expression to be a literal of String type"))

    // empty input checks
    val emptyInput = new DefaultColumnarBatch(
      0,
      new StructType().add("dummy", StringType.STRING),
      Array(stringVector(Seq[String](""))))
    checkLike(
      emptyInput,
      like(Literal.ofString("abc"), Literal.ofString("abc"), Some('_')),
      Seq[BooleanJ]())

    // invalid pattern check
    val invalidPatternError = intercept[IllegalArgumentException] {
      checkLikeLiteral("abbc", "a%%%c", '%', false)
    }
    assert(invalidPatternError.getMessage.contains(
      "LIKE expression has invalid escape sequence"))
  }

  private val SPARK_UTF8_LCASE = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
  test("evaluate expression: starts with") {
    Seq(
      // collation
      None,
      Some(SPARK_UTF8_BINARY)).foreach {
      collationIdentifier =>
        val col1 = stringVector(Seq[String]("one", "two", "t%hree", "four", null, null, "%"))
        val col2 = stringVector(Seq[String]("o", "t", "T", "4", "f", null, null))
        val schema = new StructType()
          .add("col1", StringType.STRING)
          .add("col2", new StringType(SPARK_UTF8_LCASE))
        val input = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2))

        val startsWithExpressionLiteral =
          startsWith(new Column("col1"), Literal.ofString("t%"), collationIdentifier)
        val expOutputVectorLiteral =
          booleanVector(Seq[BooleanJ](false, false, true, false, null, null, false))
        checkBooleanVectors(
          new DefaultExpressionEvaluator(
            schema,
            startsWithExpressionLiteral,
            BooleanType.BOOLEAN).eval(input),
          expOutputVectorLiteral)

        val startsWithExpressionNullLiteral =
          startsWith(new Column("col1"), Literal.ofString(null), collationIdentifier)
        val allNullVector =
          booleanVector(Seq[BooleanJ](null, null, null, null, null, null, null))
        checkBooleanVectors(
          new DefaultExpressionEvaluator(
            schema,
            startsWithExpressionNullLiteral,
            BooleanType.BOOLEAN).eval(input),
          allNullVector)

        // Two literal expressions on both sides
        val startsWithExpressionAlwaysTrue =
          startsWith(Literal.ofString("ABC"), Literal.ofString("A"), collationIdentifier)
        val allTrueVector = booleanVector(Seq[BooleanJ](true, true, true, true, true, true, true))
        checkBooleanVectors(
          new DefaultExpressionEvaluator(
            schema,
            startsWithExpressionAlwaysTrue,
            BooleanType.BOOLEAN).eval(input),
          allTrueVector)

        val startsWithExpressionAlwaysFalse =
          startsWith(Literal.ofString("ABC"), Literal.ofString("_B%"), collationIdentifier)
        val allFalseVector =
          booleanVector(Seq[BooleanJ](false, false, false, false, false, false, false))
        checkBooleanVectors(
          new DefaultExpressionEvaluator(
            schema,
            startsWithExpressionAlwaysFalse,
            BooleanType.BOOLEAN).eval(input),
          allFalseVector)

        // scalastyle:off nonascii
        val colUnicode = stringVector(Seq[String]("中文", "中", "文"))
        val schemaUnicode = new StructType().add("col", StringType.STRING)
        val inputUnicode =
          new DefaultColumnarBatch(colUnicode.getSize, schemaUnicode, Array(colUnicode))
        val startsWithExpressionUnicode =
          startsWith(new Column("col"), Literal.ofString("中"), collationIdentifier)
        val expOutputVectorLiteralUnicode = booleanVector(Seq[BooleanJ](true, true, false))
        checkBooleanVectors(
          new DefaultExpressionEvaluator(
            schemaUnicode,
            startsWithExpressionUnicode,
            BooleanType.BOOLEAN).eval(inputUnicode),
          expOutputVectorLiteralUnicode)

        // scalastyle:off nonascii
        val colSurrogatePair = stringVector(Seq[String]("💕😉💕", "😉💕", "💕"))
        val schemaSurrogatePair = new StructType().add("col", StringType.STRING)
        val inputSurrogatePair =
          new DefaultColumnarBatch(colSurrogatePair.getSize, schemaUnicode, Array(colSurrogatePair))
        val startsWithExpressionSurrogatePair =
          startsWith(new Column("col"), Literal.ofString("💕"), collationIdentifier)
        val expOutputVectorLiteralSurrogatePair = booleanVector(Seq[BooleanJ](true, false, true))
        checkBooleanVectors(
          new DefaultExpressionEvaluator(
            schemaSurrogatePair,
            startsWithExpressionSurrogatePair,
            BooleanType.BOOLEAN).eval(inputSurrogatePair),
          expOutputVectorLiteralSurrogatePair)

        val startsWithExpressionExpression =
          startsWith(new Column("col1"), new Column("col2"), collationIdentifier)
        val e = intercept[UnsupportedOperationException] {
          new DefaultExpressionEvaluator(
            schema,
            startsWithExpressionExpression,
            BooleanType.BOOLEAN).eval(input)
        }
        assert(e.getMessage.contains("'STARTS_WITH' expects literal as the second input"))

        def checkUnsupportedTypes(colType: DataType, literalType: DataType): Unit = {
          val schema = new StructType()
            .add("col", colType)
          val expr = startsWith(new Column("col"), Literal.ofNull(literalType), collationIdentifier)
          val input = new DefaultColumnarBatch(5, schema, Array(testColumnVector(5, colType)))

          val e = intercept[UnsupportedOperationException] {
            new DefaultExpressionEvaluator(
              schema,
              expr,
              BooleanType.BOOLEAN).eval(input)
          }
          assert(e.getMessage.contains("'STARTS_WITH' expects STRING type inputs"))
        }

        checkUnsupportedTypes(BooleanType.BOOLEAN, BooleanType.BOOLEAN)
        checkUnsupportedTypes(LongType.LONG, LongType.LONG)
        checkUnsupportedTypes(IntegerType.INTEGER, IntegerType.INTEGER)
        checkUnsupportedTypes(StringType.STRING, BooleanType.BOOLEAN)
        checkUnsupportedTypes(StringType.STRING, IntegerType.INTEGER)
        checkUnsupportedTypes(StringType.STRING, LongType.LONG)
    }
  }

  test("evaluate expression: starts with (unsupported collations)") {
    Seq(
      Some(SPARK_UTF8_LCASE),
      Some(CollationIdentifier.fromString("ICU.sr_Cyrl_SRB")),
      Some(CollationIdentifier.fromString("ICU.sr_Cyrl_SRB.75.1"))).foreach {
      collationIdentifier =>
        val col1 = stringVector(Seq[String]("one", "two", "t%hree", "four", null, null, "%"))
        val col2 = stringVector(Seq[String]("o", "t", "T", "4", "f", null, null))
        val schema = new StructType()
          .add("col1", new StringType(SPARK_UTF8_LCASE))
          .add("col2", StringType.STRING)
        val input = new DefaultColumnarBatch(col1.getSize, schema, Array(col1, col2))

        val startsWithExpressionLiteral =
          startsWith(new Column("col1"), Literal.ofString("t%"), collationIdentifier)
        checkUnsupportedCollation(
          schema,
          startsWithExpressionLiteral,
          input,
          collationIdentifier.get)

        val startsWithExpressionNullLiteral =
          startsWith(new Column("col1"), Literal.ofString(null), collationIdentifier)
        checkUnsupportedCollation(
          schema,
          startsWithExpressionNullLiteral,
          input,
          collationIdentifier.get)

        // Two literal expressions on both sides
        val startsWithExpressionAlwaysTrue =
          startsWith(Literal.ofString("ABC"), Literal.ofString("A"), collationIdentifier)
        checkUnsupportedCollation(
          schema,
          startsWithExpressionAlwaysTrue,
          input,
          collationIdentifier.get)

        val startsWithExpressionAlwaysFalse =
          startsWith(Literal.ofString("ABC"), Literal.ofString("_B%"), collationIdentifier)
        checkUnsupportedCollation(
          schema,
          startsWithExpressionAlwaysFalse,
          input,
          collationIdentifier.get)

        // scalastyle:off nonascii
        val colUnicode = stringVector(Seq[String]("中文", "中", "文"))
        val schemaUnicode = new StructType().add("col", StringType.STRING)
        val inputUnicode =
          new DefaultColumnarBatch(colUnicode.getSize, schemaUnicode, Array(colUnicode))
        val startsWithExpressionUnicode =
          startsWith(new Column("col"), Literal.ofString("中"), collationIdentifier)
        checkUnsupportedCollation(
          schemaUnicode,
          startsWithExpressionUnicode,
          inputUnicode,
          collationIdentifier.get)

        // scalastyle:off nonascii
        val colSurrogatePair = stringVector(Seq[String]("💕😉💕", "😉💕", "💕"))
        val schemaSurrogatePair = new StructType().add("col", StringType.STRING)
        val inputSurrogatePair =
          new DefaultColumnarBatch(
            colSurrogatePair.getSize,
            schemaSurrogatePair,
            Array(colSurrogatePair))
        val startsWithExpressionSurrogatePair =
          startsWith(new Column("col"), Literal.ofString("💕"), collationIdentifier)
        checkUnsupportedCollation(
          schemaSurrogatePair,
          startsWithExpressionSurrogatePair,
          inputSurrogatePair,
          collationIdentifier.get)
    }
  }

  test("evaluate expression: basic case for in expression") {
    // Test with string values
    val col1 = stringVector(Seq[String]("one", "two", "three", "four", null, "five"))
    val schema = new StructType().add("col1", StringType.STRING)
    val input = new DefaultColumnarBatch(col1.getSize, schema, Array(col1))

    // Basic case for string: col1 IN ("one", "three", "five")
    val inExpressionBasic = in(
      new Column("col1"),
      Literal.ofString("one"),
      Literal.ofString("three"),
      Literal.ofString("five"))
    val expOutputBasic = booleanVector(Seq[BooleanJ](true, false, true, false, null, true))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(
        schema,
        inExpressionBasic,
        BooleanType.BOOLEAN).eval(input),
      expOutputBasic)

    // IN test with no matches: col1 IN ("six", "seven")
    val inExpressionNoMatch = in(
      new Column("col1"),
      Literal.ofString("six"),
      Literal.ofString("seven"))
    val expOutputNoMatch = booleanVector(Seq[BooleanJ](false, false, false, false, null, false))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(
        schema,
        inExpressionNoMatch,
        BooleanType.BOOLEAN).eval(input),
      expOutputNoMatch)

    // IN test with NULL in list: col1 IN ("one", NULL, "three"), returns null if no matches.
    val inExpressionWithNull = in(
      new Column("col1"),
      Literal.ofString("one"),
      Literal.ofString(null),
      Literal.ofString("three"))
    val expOutputWithNull = booleanVector(Seq[BooleanJ](true, null, true, null, null, null))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(
        schema,
        inExpressionWithNull,
        BooleanType.BOOLEAN).eval(input),
      expOutputWithNull)

    // Test with float values: col IN (1.5f, 2.5f, 3.5f)
    val floatCol = floatVector(Seq[FloatJ](1.5f, 2.5f, 3.5f, 4.5f, null))
    val floatSchema = new StructType().add("floatCol", FloatType.FLOAT)
    val floatInput = new DefaultColumnarBatch(floatCol.getSize, floatSchema, Array(floatCol))

    val inExpressionFloat = in(
      new Column("floatCol"),
      Literal.ofFloat(1.5f),
      Literal.ofFloat(2.5f),
      Literal.ofFloat(3.5f))
    val expOutputFloat = booleanVector(Seq[BooleanJ](true, true, true, false, null))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(
        floatSchema,
        inExpressionFloat,
        BooleanType.BOOLEAN).eval(floatInput),
      expOutputFloat)

    // Test with double values: col IN (1.1, 2.2, null)
    val doubleCol = doubleVector(Seq[DoubleJ](1.1, 2.2, 3.3, 4.4, null))
    val doubleSchema = new StructType().add("doubleCol", DoubleType.DOUBLE)
    val doubleInput = new DefaultColumnarBatch(doubleCol.getSize, doubleSchema, Array(doubleCol))

    val inExpressionDouble = in(
      new Column("doubleCol"),
      Literal.ofDouble(1.1),
      Literal.ofDouble(2.2),
      Literal.ofNull(DoubleType.DOUBLE))
    val expOutputDouble = booleanVector(Seq[BooleanJ](true, true, null, null, null))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(
        doubleSchema,
        inExpressionDouble,
        BooleanType.BOOLEAN).eval(doubleInput),
      expOutputDouble)

    // Test with byte values: col IN (0, 1)
    val byteCol = byteVector(Seq[java.lang.Byte](null, 0.toByte, 0.toByte, 1.toByte, 2.toByte))
    val byteSchema = new StructType().add("byteCol", ByteType.BYTE)
    val byteInput = new DefaultColumnarBatch(byteCol.getSize, byteSchema, Array(byteCol))

    val inExpressionByte = in(
      new Column("byteCol"),
      Literal.ofByte(0.toByte),
      Literal.ofByte(1.toByte))
    // Expected: [null, true, true, true, false] for values [null, 0, 0, 1, 2]
    val expOutputByte = booleanVector(Seq[BooleanJ](null, true, true, true, false))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(
        byteSchema,
        inExpressionByte,
        BooleanType.BOOLEAN).eval(byteInput),
      expOutputByte)
  }

  test("evaluate expression: in with incompatible types") {
    // Test error cases - incompatible types
    def checkIncompatibleTypes(valueType: DataType, listElementType: DataType): Unit = {
      val valueSchema = new StructType().add("col", valueType)
      val valueVector = testColumnVector(3, valueType)
      val valueInput = new DefaultColumnarBatch(3, valueSchema, Array(valueVector))

      val incompatibleInExpr = in(
        new Column("col"),
        Literal.ofNull(listElementType))

      val e = intercept[UnsupportedOperationException] {
        new DefaultExpressionEvaluator(
          valueSchema,
          incompatibleInExpr,
          BooleanType.BOOLEAN).eval(valueInput)
      }
      assert(
        e.getMessage.contains("IN expression requires all list elements to match the value type"))
    }

    // Test incompatible type combinations
    checkIncompatibleTypes(StringType.STRING, IntegerType.INTEGER)
    checkIncompatibleTypes(IntegerType.INTEGER, StringType.STRING)
    checkIncompatibleTypes(BooleanType.BOOLEAN, StringType.STRING)
  }

  test("evaluate expression: in with collation") {
    Seq(
      None, // no collation
      Some(SPARK_UTF8_BINARY) // UTF8_BINARY collation
    ).foreach { collationIdentifier =>
      val col1 = stringVector(Seq[String]("Test", "test", "TEST", null))
      val schema = new StructType().add("col1", StringType.STRING)
      val input = new DefaultColumnarBatch(col1.getSize, schema, Array(col1))

      // Test with basic case
      val inExpressionBasic = in(
        new Column("col1"),
        collationIdentifier,
        Literal.ofString("test"),
        Literal.ofString("other"))
      val expectedOutput = booleanVector(Seq[BooleanJ](false, true, false, null))
      checkBooleanVectors(
        new DefaultExpressionEvaluator(
          schema,
          inExpressionBasic,
          BooleanType.BOOLEAN).eval(input),
        expectedOutput)

      // Test with NULL in list
      val inExpressionWithNull = in(
        new Column("col1"),
        collationIdentifier,
        Literal.ofString("Test"),
        Literal.ofString(null))
      val expOutputWithNull = booleanVector(Seq[BooleanJ](true, null, null, null))
      checkBooleanVectors(
        new DefaultExpressionEvaluator(
          schema,
          inExpressionWithNull,
          BooleanType.BOOLEAN).eval(input),
        expOutputWithNull)
    }
  }

  test("evaluate expression: in with unsupported collations") {
    val col1 = stringVector(Seq[String]("Test", "test"))
    val schema = new StructType().add("col1", StringType.STRING)
    val input = new DefaultColumnarBatch(col1.getSize, schema, Array(col1))

    val inExpressionUnsupported = in(
      new Column("col1"),
      Some(SPARK_UTF8_LCASE),
      Literal.ofString("test"))

    checkUnsupportedCollation(
      schema,
      inExpressionUnsupported,
      input,
      SPARK_UTF8_LCASE)
  }

  test("evaluate expression: in with non-literal list elements") {
    val schema = new StructType().add("col1", IntegerType.INTEGER).add("col2", IntegerType.INTEGER)
    val input = new DefaultColumnarBatch(
      2,
      schema,
      Array(
        testColumnVector(2, IntegerType.INTEGER),
        testColumnVector(2, IntegerType.INTEGER)))

    // Try to create IN with non-literal (Column) in the list
    val nonLiteralInExpr = new Predicate(
      "IN",
      List[Expression](
        new Column("col1"),
        new Column("col2"), // This should cause an error
        Literal.ofInt(1)).asJava)

    val e = intercept[UnsupportedOperationException] {
      new DefaultExpressionEvaluator(schema, nonLiteralInExpr, BooleanType.BOOLEAN).eval(input)
    }
    assert(e.getMessage.contains("IN expression requires all list elements to be literals"))
  }

  test("evaluate expression: in expression handling null") {
    val col1 = testColumnVector(6, StringType.STRING) // [null, "1", null, "3", null, "5"]
    val schema = new StructType().add("col1", StringType.STRING)
    val input = new DefaultColumnarBatch(col1.getSize, schema, Array(col1))

    // Test all null semantics scenarios:
    // 1. NULL value with non-null list -> NULL
    // 2. Non-null value matches -> TRUE
    // 3. Non-null value no match, no nulls in list -> FALSE
    // 4. Non-null value no match, but nulls in list -> NULL

    // Case: value IN (match, null) -> [null, true, null, null, null, null]
    val inExprMatchWithNull = in(new Column("col1"), Literal.ofString("1"), Literal.ofString(null))
    val expectedMatchWithNull = booleanVector(Seq[BooleanJ](null, true, null, null, null, null))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(schema, inExprMatchWithNull, BooleanType.BOOLEAN).eval(input),
      expectedMatchWithNull)

    // Case: value IN (no_match1, no_match2) -> [null, false, null, false, null, false]
    val inExprNoMatch = in(new Column("col1"), Literal.ofString("x"), Literal.ofString("y"))
    val expectedNoMatch = booleanVector(Seq[BooleanJ](null, false, null, false, null, false))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(schema, inExprNoMatch, BooleanType.BOOLEAN).eval(input),
      expectedNoMatch)

    // Case: value IN (no_match, null) -> [null, null, null, null, null, null]
    val inExprNoMatchWithNull =
      in(new Column("col1"), Literal.ofString("x"), Literal.ofString(null))
    val expectedNoMatchWithNull = booleanVector(Seq[BooleanJ](null, null, null, null, null, null))
    checkBooleanVectors(
      new DefaultExpressionEvaluator(
        schema,
        inExprNoMatchWithNull,
        BooleanType.BOOLEAN).eval(input),
      expectedNoMatchWithNull)
  }

  test("evaluate expression: comparators (=, <, <=, >, >=, 'IS NOT DISTINCT FROM')") {
    val ASCII_MAX_CHARACTER = '\u007F'
    val UTF8_MAX_CHARACTER = new String(Character.toChars(Character.MAX_CODE_POINT))

    // Literals for each data type from the data type value range, used as inputs to comparator
    // (small, big, small, null)
    val literals = Seq(
      (ofByte(1.toByte), ofByte(2.toByte), ofByte(1.toByte), ofNull(ByteType.BYTE)),
      (ofShort(1.toShort), ofShort(2.toShort), ofShort(1.toShort), ofNull(ShortType.SHORT)),
      (ofInt(1), ofInt(2), ofInt(1), ofNull(IntegerType.INTEGER)),
      (ofLong(1L), ofLong(2L), ofLong(1L), ofNull(LongType.LONG)),
      (ofFloat(1.0f), ofFloat(2.0f), ofFloat(1.0f), ofNull(FloatType.FLOAT)),
      (ofDouble(1.0), ofDouble(2.0), ofDouble(1.0), ofNull(DoubleType.DOUBLE)),
      (ofBoolean(false), ofBoolean(true), ofBoolean(false), ofNull(BooleanType.BOOLEAN)),
      (
        ofTimestamp(343L),
        ofTimestamp(123212312L),
        ofTimestamp(343L),
        ofNull(TimestampType.TIMESTAMP)),
      (
        ofTimestampNtz(323423L),
        ofTimestampNtz(1232123423312L),
        ofTimestampNtz(323423L),
        ofNull(TimestampNTZType.TIMESTAMP_NTZ)),
      (ofDate(-12123), ofDate(123123), ofDate(-12123), ofNull(DateType.DATE)),
      (ofString("apples"), ofString("oranges"), ofString("apples"), ofNull(StringType.STRING)),
      (ofString(""), ofString("a"), ofString(""), ofNull(StringType.STRING)),
      (ofString("abc"), ofString("abc0"), ofString("abc"), ofNull(StringType.STRING)),
      (ofString("abc"), ofString("abcd"), ofString("abc"), ofNull(StringType.STRING)),
      (ofString("abc"), ofString("abd"), ofString("abc"), ofNull(StringType.STRING)),
      (
        ofString("Abcabcabc"),
        ofString("aBcabcabc"),
        ofString("Abcabcabc"),
        ofNull(StringType.STRING)),
      (
        ofString("abcabcabC"),
        ofString("abcabcabc"),
        ofString("abcabcabC"),
        ofNull(StringType.STRING)),
      // scalastyle:off nonascii
      (ofString("abc"), ofString("世界"), ofString("abc"), ofNull(StringType.STRING)),
      (ofString("世界"), ofString("你好"), ofString("世界"), ofNull(StringType.STRING)),
      (ofString("你好122"), ofString("你好123"), ofString("你好122"), ofNull(StringType.STRING)),
      (ofString("A"), ofString("Ā"), ofString("A"), ofNull(StringType.STRING)),
      (ofString("»"), ofString("î"), ofString("»"), ofNull(StringType.STRING)),
      (ofString("�"), ofString("🌼"), ofString("�"), ofNull(StringType.STRING)),
      (
        ofString("abcdef🚀"),
        ofString(s"abcdef$UTF8_MAX_CHARACTER"),
        ofString("abcdef🚀"),
        ofNull(StringType.STRING)),
      (
        ofString("abcde�abcdef�abcdef�abcdef"),
        ofString(s"abcde�$ASCII_MAX_CHARACTER"),
        ofString("abcde�abcdef�abcdef�abcdef"),
        ofNull(StringType.STRING)),
      (
        ofString("abcde�abcdef�abcdef�abcdef"),
        ofString(s"abcde�$ASCII_MAX_CHARACTER"),
        ofString("abcde�abcdef�abcdef�abcdef"),
        ofNull(StringType.STRING)),
      (
        ofString("����"),
        ofString(s"��$UTF8_MAX_CHARACTER"),
        ofString("����"),
        ofNull(StringType.STRING)),
      (
        ofString(s"a${UTF8_MAX_CHARACTER}d"),
        ofString(s"a$UTF8_MAX_CHARACTER$ASCII_MAX_CHARACTER"),
        ofString(s"a${UTF8_MAX_CHARACTER}d"),
        ofNull(StringType.STRING)),
      (
        ofString("abcdefghijklm💞😉💕\n🥀🌹💐🌺🌷🌼🌻🌷🥀"),
        ofString(s"abcdefghijklm💞😉💕\n🥀🌹💐🌺🌷🌼$UTF8_MAX_CHARACTER"),
        ofString("abcdefghijklm💞😉💕\n🥀🌹💐🌺🌷🌼🌻🌷🥀"),
        ofNull(StringType.STRING)),
      // scalastyle:on nonascii
      (
        ofBinary("apples".getBytes()),
        ofBinary("oranges".getBytes()),
        ofBinary("apples".getBytes()),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte]()),
        ofBinary(Array[Byte](5.toByte)),
        ofBinary(Array[Byte]()),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte](0.toByte)), // 00000000
        ofBinary(Array[Byte](-1.toByte)), // 11111111
        ofBinary(Array[Byte](0.toByte)),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte](127.toByte)), // 01111111
        ofBinary(Array[Byte](-1.toByte)), // 11111111
        ofBinary(Array[Byte](127.toByte)),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte](5.toByte, 10.toByte)),
        ofBinary(Array[Byte](6.toByte)),
        ofBinary(Array[Byte](5.toByte, 10.toByte)),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte](5.toByte, 10.toByte)),
        ofBinary(Array[Byte](5.toByte, 100.toByte)),
        ofBinary(Array[Byte](5.toByte, 10.toByte)),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte](5.toByte, 10.toByte, 5.toByte)), // 00000101 00001010 00000101
        ofBinary(Array[Byte](5.toByte, -3.toByte)), // 00000101 11111101
        ofBinary(Array[Byte](5.toByte, 10.toByte, 5.toByte)),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte](5.toByte, -25.toByte, 5.toByte)), // 00000101 11100111 00000101
        ofBinary(Array[Byte](5.toByte, -9.toByte)), // 00000101 11110111
        ofBinary(Array[Byte](5.toByte, -25.toByte, 5.toByte)),
        ofNull(BinaryType.BINARY)),
      (
        ofBinary(Array[Byte](5.toByte, 10.toByte)),
        ofBinary(Array[Byte](5.toByte, 10.toByte, 0.toByte)),
        ofBinary(Array[Byte](5.toByte, 10.toByte)),
        ofNull(BinaryType.BINARY)),
      (
        ofDecimal(BigDecimalJ.valueOf(1.12), 7, 3),
        ofDecimal(BigDecimalJ.valueOf(5233.232), 7, 3),
        ofDecimal(BigDecimalJ.valueOf(1.12), 7, 3),
        ofNull(new DecimalType(7, 3))))

    // Mapping of comparator to expected results for:
    // comparator(small, big)
    // comparator(big, small)
    // comparator(small, small)
    // comparator(small, null)
    // comparator(big, null)
    // comparator(null, null)
    val comparatorToExpResults = Map[String, Seq[BooleanJ]](
      "<" -> Seq(true, false, false, null, null, null),
      "<=" -> Seq(true, false, true, null, null, null),
      ">" -> Seq(false, true, false, null, null, null),
      ">=" -> Seq(false, true, true, null, null, null),
      "=" -> Seq(false, false, true, null, null, null),
      "IS NOT DISTINCT FROM" -> Seq(false, false, true, false, false, true))

    literals.foreach {
      case (small1, big, small2, nullLit) =>
        comparatorToExpResults.foreach {
          case (comparator, expectedResults) =>
            val testCases = Seq(
              (small1, big),
              (big, small1),
              (small1, small2),
              (small1, nullLit),
              (nullLit, big),
              (nullLit, nullLit))
            testCases.zip(expectedResults).foreach { case ((left, right), expected) =>
              testComparator(comparator, left, right, expected)
            }

            // Predicate with collation is supported only for comparisons between StringTypes
            val allStringTypes = Seq(small1, big, small2, nullLit).forall {
              literal => literal.getDataType.isInstanceOf[StringType]
            }
            if (allStringTypes) {
              Seq(
                SPARK_UTF8_BINARY,
                SPARK_UTF8_LCASE,
                CollationIdentifier.fromString("ICU.sr_Cyrl_SRB"),
                CollationIdentifier.fromString("ICU.sr_Cyrl_SRB.75.1")).foreach {
                collationIdentifier =>
                  testCases.zip(expectedResults).foreach { case ((left, right), expected) =>
                    testCollatedComparator(comparator, left, right, expected, collationIdentifier)
                  }
              }
            }
        }
    }
  }

  test("check Predicate with collation comparing invalid types") {
    Seq(
      // predicateName
      "=",
      "<",
      "<=",
      ">",
      ">=",
      "IS NOT DISTINCT FROM",
      "STARTS_WITH").foreach {
      predicateName =>
        Seq(
          // (expr1, expr2, schema)
          (
            Literal.ofString("apple"),
            Literal.ofInt(1),
            new StructType()),
          (
            Literal.ofString("apple"),
            Literal.ofLong(1L),
            new StructType()),
          (
            Literal.ofFloat(2.3f),
            Literal.ofString("apple"),
            new StructType()),
          (
            Literal.ofDouble(2.3),
            Literal.ofBoolean(false),
            new StructType()),
          (
            new Column(Array("col1", "col11")),
            Literal.ofString("apple"),
            new StructType()
              .add(
                "col1",
                new StructType()
                  .add("col11", IntegerType.INTEGER))),
          (
            new Column(Array("col1", "col11")),
            Literal.ofBoolean(false),
            new StructType()
              .add(
                "col1",
                new StructType()
                  .add("col11", StringType.STRING))),
          (
            new Column(Array("col1", "col11")),
            Literal.ofBoolean(false),
            new StructType()
              .add(
                "col1",
                new StructType()
                  .add("col11", new StringType(SPARK_UTF8_LCASE)))),
          (
            new Column(Array("col1", "col11")),
            new Column(Array("col1", "col12")),
            new StructType()
              .add(
                "col1",
                new StructType()
                  .add("col11", DoubleType.DOUBLE)
                  .add("col12", FloatType.FLOAT)))).foreach {
          case (expr1: Expression, expr2: Expression, schema: StructType) =>
            val expr = comparator(
              predicateName,
              expr1,
              expr2,
              Some(SPARK_UTF8_BINARY))
            val input = zeroColumnBatch(rowCount = 1)

            val e = intercept[UnsupportedOperationException] {
              new DefaultExpressionEvaluator(
                schema,
                expr,
                BooleanType.BOOLEAN).eval(input)
            }
            assert(e.getMessage.contains("expects STRING type inputs"))
        }
    }
  }

  // Literals for each data type from the data type value range, used as inputs to comparator
  // (byte, short, int, float, double)
  val literals = Seq(
    ofByte(1.toByte),
    ofShort(223),
    ofInt(-234),
    ofLong(223L),
    ofFloat(-2423423.9f),
    ofNull(DoubleType.DOUBLE))

  test("evaluate expression: substring") {
    // scalastyle:off nonascii
    val data = Seq[String](
      null,
      "one",
      "two",
      "three",
      "four",
      null,
      null,
      "seven",
      "eight",
      "😉",
      "ë")
    val col = stringVector(data)
    val col_name = "str_col"
    val schema = new StructType().add(col_name, StringType.STRING)
    val input = new DefaultColumnarBatch(col.getSize, schema, Array(col))

    def checkSubString(
        input: DefaultColumnarBatch,
        substringExpression: ScalarExpression,
        expOutputSeq: Seq[String]): Unit = {
      val actOutputVector =
        new DefaultExpressionEvaluator(
          schema,
          substringExpression,
          StringType.STRING).eval(input)
      val expOutputVector = stringVector(expOutputSeq);
      checkStringVectors(actOutputVector, expOutputVector)
    }

    checkSubString(
      input,
      substring(new Column(col_name), 0),
      // scalastyle:off nonascii
      Seq[String](null, "one", "two", "three", "four", null, null, "seven", "eight", "😉", "ë"))

    checkSubString(
      input,
      substring(new Column(col_name), 1),
      // scalastyle:off nonascii
      Seq[String](null, "one", "two", "three", "four", null, null, "seven", "eight", "😉", "ë"))

    checkSubString(
      input,
      substring(new Column(col_name), 2),
      Seq[String](null, "ne", "wo", "hree", "our", null, null, "even", "ight", "", "̈"))

    checkSubString(
      input,
      substring(new Column(col_name), -1),
      // scalastyle:off nonascii
      Seq[String](null, "e", "o", "e", "r", null, null, "n", "t", "😉", "̈"))

    checkSubString(
      input,
      substring(new Column(col_name), -1000),
      // scalastyle:off nonascii
      Seq[String](null, "one", "two", "three", "four", null, null, "seven", "eight", "😉", "ë"))

    checkSubString(
      input,
      substring(new Column(col_name), 0, Option(4)),
      // scalastyle:off nonascii
      Seq[String](null, "one", "two", "thre", "four", null, null, "seve", "eigh", "😉", "ë"))

    checkSubString(
      input,
      substring(new Column(col_name), 2, Option(0)),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), 1, Option(1)),
      // scalastyle:off nonascii
      Seq[String](null, "o", "t", "t", "f", null, null, "s", "e", "😉", "e"))

    checkSubString(
      input,
      substring(new Column(col_name), 2, Option(1)),
      Seq[String](null, "n", "w", "h", "o", null, null, "e", "i", "", "̈"))

    checkSubString(
      input,
      substring(new Column(col_name), 2, Option(10000)),
      Seq[String](null, "ne", "wo", "hree", "our", null, null, "even", "ight", "", "̈"))

    checkSubString(
      input,
      substring(new Column(col_name), 1000),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), 1000, Option(10000)),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), 2, Option(-10)),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), -2, Option(1)),
      Seq[String](null, "n", "w", "e", "u", null, null, "e", "h", "", "e"))

    checkSubString(
      input,
      substring(new Column(col_name), -2, Option(2)),
      // scalastyle:off nonascii
      Seq[String](null, "ne", "wo", "ee", "ur", null, null, "en", "ht", "😉", "ë"))

    checkSubString(
      input,
      substring(new Column(col_name), -4, Option(3)),
      Seq[String](null, "on", "tw", "hre", "fou", null, null, "eve", "igh", "", "e"))

    checkSubString(
      input,
      substring(new Column(col_name), -100, Option(95)),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), -100, Option(98)),
      Seq[String](null, "o", "t", "thr", "fo", null, null, "sev", "eig", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), -100, Option(108)),
      // scalastyle:off nonascii
      Seq[String](null, "one", "two", "three", "four", null, null, "seven", "eight", "😉", "ë"))

    checkSubString(
      input,
      substring(new Column(col_name), 2147483647, Option(10000)),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), 2147483647),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), -2147483648, Option(10000)),
      Seq[String](null, "", "", "", "", null, null, "", "", "", ""))

    checkSubString(
      input,
      substring(new Column(col_name), -2147483648),
      // scalastyle:off nonascii
      Seq[String](null, "one", "two", "three", "four", null, null, "seven", "eight", "😉", "ë"))

    val outputVectorForEmptyInput = evaluator(
      schema,
      new ScalarExpression(
        "SUBSTRING",
        util.Arrays.asList(
          new Column(col_name),
          Literal.ofInt(1),
          Literal.ofInt(1))),
      StringType.STRING).eval(new DefaultColumnarBatch(
      /* size= */ 0,
      schema,
      Array(
        testColumnVector( /* size= */ 0, StringType.STRING),
        testColumnVector( /* size= */ 0, BinaryType.BINARY))))
    checkStringVectors(outputVectorForEmptyInput, stringVector(Seq[String]()))

    def checkUnsupportedColumnTypes(colType: DataType): Unit = {
      val schema = new StructType()
        .add(col_name, colType)
      val batch = new DefaultColumnarBatch(5, schema, Array(testColumnVector(5, colType)))
      val e = intercept[UnsupportedOperationException] {
        evaluator(
          schema,
          new ScalarExpression(
            "SUBSTRING",
            util.Arrays.asList(new Column(col_name), Literal.ofInt(1))),
          StringType.STRING).eval(batch)
      }
      assert(
        e.getMessage.contains("Invalid type of first input of SUBSTRING: expects STRING"))
    }

    checkUnsupportedColumnTypes(IntegerType.INTEGER)
    checkUnsupportedColumnTypes(ByteType.BYTE)
    checkUnsupportedColumnTypes(BooleanType.BOOLEAN)
    checkUnsupportedColumnTypes(BinaryType.BINARY)

    val badLiteralSize = intercept[UnsupportedOperationException] {
      evaluator(
        schema,
        new ScalarExpression(
          "SUBSTRING",
          util.Arrays.asList(
            new Column(col_name),
            Literal.ofInt(1),
            Literal.ofInt(1),
            Literal.ofInt(1))),
        StringType.STRING).eval(new DefaultColumnarBatch(
        /* size= */ 5,
        schema,
        Array(testColumnVector( /* size= */ 5, StringType.STRING))))
    }
    assert(
      badLiteralSize.getMessage.contains(
        "Invalid number of inputs to SUBSTRING expression."))

    val badPosType = intercept[UnsupportedOperationException] {
      evaluator(
        schema,
        new ScalarExpression(
          "SUBSTRING",
          util.Arrays.asList(
            new Column("str_col"),
            Literal.ofBoolean(true))),
        StringType.STRING).eval(new DefaultColumnarBatch(
        /* size= */ 5,
        schema,
        Array(testColumnVector( /* size= */ 5, StringType.STRING))))
    }
    assert(badPosType.getMessage.contains("Invalid `pos` argument type for SUBSTRING"))

    val badLenType = intercept[UnsupportedOperationException] {
      evaluator(
        schema,
        new ScalarExpression(
          "SUBSTRING",
          util.Arrays.asList(
            new Column(col_name),
            Literal.ofInt(1),
            Literal.ofBoolean(true))),
        StringType.STRING).eval(new DefaultColumnarBatch(
        /* size= */ 5,
        schema,
        Array(testColumnVector( /* size= */ 5, StringType.STRING))))
    }
    assert(badLenType.getMessage.contains("Invalid `len` argument type for SUBSTRING"))
  }

  test("evaluate expression: comparators `byte` with other implicit types") {
    // Mapping of comparator to expected results for:
    // (byte, short), (byte, int), (byte, long), (byte, float), (byte, double)
    val comparatorToExpResults = Map[String, Seq[BooleanJ]](
      "<" -> Seq(true, false, true, false, null),
      "<=" -> Seq(true, false, true, false, null),
      ">" -> Seq(false, true, false, true, null),
      ">=" -> Seq(false, true, false, true, null),
      "=" -> Seq(false, false, false, false, null))

    // Left operand is first literal in [[literal]] which a byte type
    // Right operands are the remaining literals to the left side of it in [[literal]]
    val right = literals(0)
    Seq.range(1, literals.length).foreach { idx =>
      comparatorToExpResults.foreach {
        case (comparator, expectedResults) =>
          testComparator(comparator, right, literals(idx), expectedResults(idx - 1))
      }
    }
  }

  test("evaluate expression: comparators `short` with other implicit types") {
    // Mapping of comparator to expected results for:
    // (short, int), (short, long), (short, float), (short, double)
    val comparatorToExpResults = Map[String, Seq[BooleanJ]](
      "<" -> Seq(false, false, false, null),
      "<=" -> Seq(false, true, false, null),
      ">" -> Seq(true, false, true, null),
      ">=" -> Seq(true, true, true, null),
      "=" -> Seq(false, true, false, null))

    // Left operand is first literal in [[literal]] which a short type
    // Right operands are the remaining literals to the left side of it in [[literal]]
    val right = literals(1)
    Seq.range(2, literals.length).foreach { idx =>
      comparatorToExpResults.foreach {
        case (comparator, expectedResults) =>
          testComparator(comparator, right, literals(idx), expectedResults(idx - 2))
      }
    }
  }

  test("evaluate expression: comparators `int` with other implicit types") {
    // Mapping of comparator to expected results for: (int, long), (int, float), (int, double)
    val comparatorToExpResults = Map[String, Seq[BooleanJ]](
      "<" -> Seq(true, false, null),
      "<=" -> Seq(true, false, null),
      ">" -> Seq(false, true, null),
      ">=" -> Seq(false, true, null),
      "=" -> Seq(false, false, null))

    // Left operand is first literal in [[literal]] which a int type
    // Right operands are the remaining literals to the left side of it in [[literal]]
    val right = literals(2)
    Seq.range(3, literals.length).foreach { idx =>
      comparatorToExpResults.foreach {
        case (comparator, expectedResults) =>
          testComparator(comparator, right, literals(idx), expectedResults(idx - 3))
      }
    }
  }

  test("evaluate expression: comparators `long` with other implicit types") {
    // Mapping of comparator to expected results for: (long, float), (long, double)
    val comparatorToExpResults = Map[String, Seq[BooleanJ]](
      "<" -> Seq(false, null),
      "<=" -> Seq(false, null),
      ">" -> Seq(true, null),
      ">=" -> Seq(true, null),
      "=" -> Seq(false, null))

    // Left operand is fourth literal in [[literal]] which a long type
    // Right operands are the remaining literals to the left side of it in [[literal]]
    val right = literals(3)
    Seq.range(4, literals.length).foreach { idx =>
      comparatorToExpResults.foreach {
        case (comparator, expectedResults) =>
          testComparator(comparator, right, literals(idx), expectedResults(idx - 4))
      }
    }
  }

  test("evaluate expression: unsupported implicit casts") {
    intercept[UnsupportedOperationException] {
      testComparator("<", ofInt(21), ofDate(123), null)
    }
  }

  test("evaluate expression: comparators `float` with other implicit types") {
    // Comparator results for: (float, double) is always null as one of the operands is null
    val comparatorToExpResults = Seq("<", "<=", ">", ">=", "=")

    // Left operand is fifth literal in [[literal]] which is a float type
    // Right operands are the remaining literals to the left side of it in [[literal]]
    val right = literals(4)
    Seq.range(5, literals.length).foreach { idx =>
      comparatorToExpResults.foreach { comparator =>
        testComparator(comparator, right, literals(idx), null)
      }
    }
  }

  test("evaluate expression: element_at") {
    val nullStr = null.asInstanceOf[String]
    val testMapValues: Seq[Map[AnyRef, AnyRef]] = Seq(
      Map("k0" -> "v00", "k1" -> "v01", "k3" -> nullStr, nullStr -> "v04"),
      Map("k0" -> "v10", "k1" -> nullStr, "k3" -> "v13", nullStr -> "v14"),
      Map("k0" -> nullStr, "k1" -> "v21", "k3" -> "v23", nullStr -> "v24"),
      null)
    val testMapVector = buildMapVector(
      testMapValues,
      new MapType(StringType.STRING, StringType.STRING, true))

    val inputBatch = new DefaultColumnarBatch(
      testMapVector.getSize,
      new StructType().add("partitionValues", testMapVector.getDataType),
      Seq(testMapVector).toArray)
    Seq("k0", "k1", "k2", null).foreach { lookupKey =>
      val expOutput = testMapValues.map(map => {
        if (map == null) null
        else map.getOrElse(lookupKey, null)
      })

      val lookupKeyExpr = if (lookupKey == null) {
        Literal.ofNull(StringType.STRING)
      } else {
        Literal.ofString(lookupKey)
      }
      val elementAtExpr = new ScalarExpression(
        "element_at",
        util.Arrays.asList(new Column("partitionValues"), lookupKeyExpr))

      val outputVector = evaluator(inputBatch.getSchema, elementAtExpr, StringType.STRING)
        .eval(inputBatch)
      assert(outputVector.getSize === testMapValues.size)
      assert(outputVector.getDataType === StringType.STRING)
      Seq.range(0, testMapValues.size).foreach { rowId =>
        val expNull = expOutput(rowId) == null
        assert(outputVector.isNullAt(rowId) == expNull)
        if (!expNull) {
          assert(outputVector.getString(rowId) === expOutput(rowId))
        }
      }
    }
  }

  test("evaluate expression: element_at - unsupported map type input") {
    val inputSchema = new StructType()
      .add("as_map", new MapType(IntegerType.INTEGER, BooleanType.BOOLEAN, true))
    val elementAtExpr = new ScalarExpression(
      "element_at",
      util.Arrays.asList(new Column("as_map"), Literal.ofString("empty")))

    val ex = intercept[UnsupportedOperationException] {
      evaluator(inputSchema, elementAtExpr, StringType.STRING)
    }
    assert(ex.getMessage.contains(
      "ELEMENT_AT(column(`as_map`), empty): Supported only on type map(string, string) input data"))
  }

  test("evaluate expression: element_at - unsupported lookup type input") {
    val inputSchema = new StructType()
      .add("as_map", new MapType(StringType.STRING, StringType.STRING, true))
    val elementAtExpr = new ScalarExpression(
      "element_at",
      util.Arrays.asList(new Column("as_map"), Literal.ofShort(24)))

    val ex = intercept[UnsupportedOperationException] {
      evaluator(inputSchema, elementAtExpr, StringType.STRING)
    }
    assert(ex.getMessage.contains(
      "lookup key type (short) is different from the map key type (string)"))
  }

  test("evaluate expression: partition_value") {
    // (serialized partition value, partition col type, expected deserialized partition value)
    val testCases = Seq(
      ("true", BooleanType.BOOLEAN, true),
      ("false", BooleanType.BOOLEAN, false),
      (null, BooleanType.BOOLEAN, null),
      ("24", ByteType.BYTE, 24.toByte),
      ("null", ByteType.BYTE, null),
      ("876", ShortType.SHORT, 876.toShort),
      ("null", ShortType.SHORT, null),
      ("2342342", IntegerType.INTEGER, 2342342),
      ("null", IntegerType.INTEGER, null),
      ("234234223", LongType.LONG, 234234223L),
      ("null", LongType.LONG, null),
      ("23423.4223", FloatType.FLOAT, 23423.4223f),
      ("null", FloatType.FLOAT, null),
      ("23423.422233", DoubleType.DOUBLE, 23423.422233d),
      ("null", DoubleType.DOUBLE, null),
      ("234.422233", new DecimalType(10, 6), new BigDecimalJ("234.422233")),
      ("null", DoubleType.DOUBLE, null),
      ("string_val", StringType.STRING, "string_val"),
      ("null", StringType.STRING, null),
      ("binary_val", BinaryType.BINARY, "binary_val".getBytes()),
      ("null", BinaryType.BINARY, null),
      ("2021-11-18", DateType.DATE, InternalUtils.daysSinceEpoch(Date.valueOf("2021-11-18"))),
      ("null", DateType.DATE, null),
      (
        "2020-02-18 22:00:10",
        TimestampType.TIMESTAMP,
        InternalUtils.microsSinceEpoch(Timestamp.valueOf("2020-02-18 22:00:10"))),
      (
        "2020-02-18 00:00:10.023",
        TimestampType.TIMESTAMP,
        InternalUtils.microsSinceEpoch(Timestamp.valueOf("2020-02-18 00:00:10.023"))),
      ("null", TimestampType.TIMESTAMP, null))

    val inputBatch = zeroColumnBatch(rowCount = 1)
    testCases.foreach { testCase =>
      val (serializedPartVal, partType, deserializedPartVal) = testCase
      val literalSerializedPartVal = if (serializedPartVal == "null") {
        Literal.ofNull(StringType.STRING)
      } else {
        Literal.ofString(serializedPartVal)
      }
      val expr = new PartitionValueExpression(literalSerializedPartVal, partType)
      val outputVector = evaluator(inputBatch.getSchema, expr, partType).eval(inputBatch)
      assert(outputVector.getSize === 1)
      assert(outputVector.getDataType === partType)
      assert(outputVector.isNullAt(0) === (deserializedPartVal == null))
      if (deserializedPartVal != null) {
        assert(getValueAsObject(outputVector, 0) === deserializedPartVal)
      }
    }
  }

  test("evaluate expression: partition_value - invalid serialize value") {
    val inputBatch = zeroColumnBatch(rowCount = 1)
    val (serializedPartVal, partType) = ("23423sdfsdf", IntegerType.INTEGER)
    val expr = new PartitionValueExpression(Literal.ofString(serializedPartVal), partType)
    val ex = intercept[IllegalArgumentException] {
      val outputVector = evaluator(inputBatch.getSchema, expr, partType).eval(inputBatch)
      outputVector.getInt(0)
    }
    assert(ex.getMessage.contains(serializedPartVal))
  }

  private def evaluator(inputSchema: StructType, expression: Expression, outputType: DataType)
      : DefaultExpressionEvaluator = {
    new DefaultExpressionEvaluator(inputSchema, expression, outputType)
  }

  private def checkUnsupportedCollation(
      schema: StructType,
      expression: Expression,
      input: ColumnarBatch,
      collationIdentifier: CollationIdentifier): Unit = {
    val e = intercept[UnsupportedOperationException] {
      evaluator(schema, expression, BooleanType.BOOLEAN).eval(input)
    }
    assert(e.getMessage.contains(
      s"""Unsupported collation: "$collationIdentifier".
         | Default Engine supports just "$SPARK_UTF8_BINARY"
         | collation.""".stripMargin.replace("\n", "")))

  }

  private def testCollatedComparator(
      comparatorName: String,
      left: Expression,
      right: Expression,
      expResult: BooleanJ,
      collationIdentifier: CollationIdentifier): Unit = {
    val collatedPredicate = comparator(comparatorName, left, right, Some(collationIdentifier))
    val batch = zeroColumnBatch(rowCount = 1)

    if (collationIdentifier.isSparkUTF8BinaryCollation) {
      val outputVector = evaluator(
        batch.getSchema,
        collatedPredicate,
        BooleanType.BOOLEAN).eval(batch)

      assert(outputVector.getSize === 1)
      assert(outputVector.getDataType === BooleanType.BOOLEAN)
      assert(
        outputVector.isNullAt(0) === (expResult == null),
        s"Unexpected null value for Predicate with collation: $collatedPredicate")
      if (expResult != null) {
        assert(
          outputVector.getBoolean(0) === expResult,
          s"""Unexpected value for Predicate with collation: $collatedPredicate,
             | expected: $expResult, actual: ${outputVector.getBoolean(0)}""".stripMargin)
      }
    } else {
      checkUnsupportedCollation(batch.getSchema, collatedPredicate, batch, collationIdentifier)
    }
  }

  private def testComparator(
      comparator: String,
      left: Expression,
      right: Expression,
      expResult: BooleanJ): Unit = {
    val expression = new Predicate(comparator, left, right)
    val batch = zeroColumnBatch(rowCount = 1)
    val outputVector = evaluator(batch.getSchema, expression, BooleanType.BOOLEAN).eval(batch)

    assert(outputVector.getSize === 1)
    assert(outputVector.getDataType === BooleanType.BOOLEAN)
    assert(
      outputVector.isNullAt(0) === (expResult == null),
      s"Unexpected null value: $comparator($left, $right)")
    if (expResult != null) {
      assert(
        outputVector.getBoolean(0) === expResult,
        s"Unexpected value: $comparator($left, $right)")
    }
  }
}
