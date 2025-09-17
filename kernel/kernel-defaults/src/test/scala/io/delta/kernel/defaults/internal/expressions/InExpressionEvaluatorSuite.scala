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
package io.delta.kernel.defaults.internal.expressions

import java.lang.{Boolean => BooleanJ, Double => DoubleJ, Float => FloatJ, Integer => IntegerJ, Long => LongJ}
import java.math.{BigDecimal => BigDecimalJ}
import java.sql.{Date, Timestamp}

import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

/**
 * Unit tests for utility methods in InExpressionEvaluator.
 */
class InExpressionEvaluatorSuite extends AnyFunSuite {

  test("areTypesCompatible - same types") {
    assert(InExpressionEvaluator.areTypesCompatible(IntegerType.INTEGER, IntegerType.INTEGER))
    assert(InExpressionEvaluator.areTypesCompatible(StringType.STRING, StringType.STRING))
    assert(InExpressionEvaluator.areTypesCompatible(BooleanType.BOOLEAN, BooleanType.BOOLEAN))
  }

  test("areTypesCompatible - equivalent types") {
    // Test equivalent types (should use type.equivalent() method)
    // ArrayTypes with different field names but same element type should be equivalent
    val arrayType1 = new ArrayType(
      new StructField("element", IntegerType.INTEGER, true))
    val arrayType2 = new ArrayType(
      new StructField("item", IntegerType.INTEGER, true) // Different field name
    )

    // They should be equivalent but not equal
    assert(arrayType1.equivalent(arrayType2))
    assert(!arrayType1.equals(arrayType2))

    // Test our method uses equivalent() correctly
    assert(InExpressionEvaluator.areTypesCompatible(arrayType1, arrayType2))
  }

  test("areTypesCompatible - numeric type group") {
    // All numeric types should be compatible with each other
    assert(InExpressionEvaluator.areTypesCompatible(ByteType.BYTE, IntegerType.INTEGER))
    assert(InExpressionEvaluator.areTypesCompatible(ShortType.SHORT, LongType.LONG))
    assert(InExpressionEvaluator.areTypesCompatible(IntegerType.INTEGER, FloatType.FLOAT))
    assert(InExpressionEvaluator.areTypesCompatible(LongType.LONG, DoubleType.DOUBLE))
    assert(InExpressionEvaluator.areTypesCompatible(FloatType.FLOAT, DoubleType.DOUBLE))
  }

  // Note: TimestampType and TimestampNTZType are no longer compatible
  // after switching to use ImplicitCastExpression.canCastTo for consistency with "=" comparisons

  test("areTypesCompatible - incompatible types") {
    assert(!InExpressionEvaluator.areTypesCompatible(IntegerType.INTEGER, StringType.STRING))
    assert(!InExpressionEvaluator.areTypesCompatible(BooleanType.BOOLEAN, IntegerType.INTEGER))
    assert(!InExpressionEvaluator.areTypesCompatible(DateType.DATE, TimestampType.TIMESTAMP))
    assert(!InExpressionEvaluator.areTypesCompatible(StringType.STRING, BinaryType.BINARY))
  }

  test("compareValues - null values") {
    assert(!InExpressionEvaluator.compareValues(null, IntegerJ.valueOf(5), IntegerType.INTEGER))
    assert(!InExpressionEvaluator.compareValues(IntegerJ.valueOf(5), null, IntegerType.INTEGER))
    assert(!InExpressionEvaluator.compareValues(null, null, IntegerType.INTEGER))
  }

  test("compareValues - same values") {
    assert(InExpressionEvaluator.compareValues(
      IntegerJ.valueOf(42),
      IntegerJ.valueOf(42),
      IntegerType.INTEGER))
    assert(InExpressionEvaluator.compareValues(
      "hello",
      "hello",
      StringType.STRING))
    assert(InExpressionEvaluator.compareValues(
      BooleanJ.TRUE,
      BooleanJ.TRUE,
      BooleanType.BOOLEAN))
  }

  test("compareValues - different values") {
    assert(!InExpressionEvaluator.compareValues(
      IntegerJ.valueOf(42),
      IntegerJ.valueOf(43),
      IntegerType.INTEGER))
    assert(!InExpressionEvaluator.compareValues(
      "hello",
      "world",
      StringType.STRING))
    assert(!InExpressionEvaluator.compareValues(
      BooleanJ.TRUE,
      BooleanJ.FALSE,
      BooleanType.BOOLEAN))
  }

  test("compareNumericValues - same type") {
    assert(InExpressionEvaluator.compareNumericValues(
      IntegerJ.valueOf(42),
      IntegerJ.valueOf(42),
      IntegerType.INTEGER))
    assert(!InExpressionEvaluator.compareNumericValues(
      IntegerJ.valueOf(42),
      IntegerJ.valueOf(43),
      IntegerType.INTEGER))
  }

  test("compareNumericValues - mixed numeric types") {
    assert(InExpressionEvaluator.compareNumericValues(
      IntegerJ.valueOf(42),
      LongJ.valueOf(42L),
      LongType.LONG))
    // Use exact float values to avoid precision issues
    assert(InExpressionEvaluator.compareNumericValues(
      FloatJ.valueOf(5.0f),
      DoubleJ.valueOf(5.0),
      DoubleType.DOUBLE))
    assert(!InExpressionEvaluator.compareNumericValues(
      IntegerJ.valueOf(42),
      LongJ.valueOf(43L),
      LongType.LONG))
  }

  test("compareNumericValues - precision handling") {
    assert(InExpressionEvaluator.compareNumericValues(
      IntegerJ.valueOf(100),
      FloatJ.valueOf(100.0f),
      FloatType.FLOAT))
    assert(InExpressionEvaluator.compareNumericValues(
      LongJ.valueOf(1000L),
      DoubleJ.valueOf(1000.0),
      DoubleType.DOUBLE))
  }
}
