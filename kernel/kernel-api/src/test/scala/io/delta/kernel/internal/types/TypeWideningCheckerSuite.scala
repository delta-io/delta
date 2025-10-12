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

package io.delta.kernel.internal.types

import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

/** Test suite for the TypeWideningChecker class. */
class TypeWideningCheckerSuite extends AnyFunSuite {

  test("same type is allowed") {
    // Same types should not be considered widening
    assert(TypeWideningChecker.isWideningSupported(IntegerType.INTEGER, IntegerType.INTEGER))
    assert(TypeWideningChecker.isWideningSupported(StringType.STRING, StringType.STRING))
    assert(TypeWideningChecker.isWideningSupported(
      new DecimalType(10, 2),
      new DecimalType(10, 2)))
  }

  test("integer widening is supported") {
    assert(TypeWideningChecker.isWideningSupported(ByteType.BYTE, ShortType.SHORT))
    assert(TypeWideningChecker.isWideningSupported(ByteType.BYTE, IntegerType.INTEGER))
    assert(TypeWideningChecker.isWideningSupported(ByteType.BYTE, LongType.LONG))
    assert(TypeWideningChecker.isWideningSupported(ShortType.SHORT, IntegerType.INTEGER))
    assert(TypeWideningChecker.isWideningSupported(ShortType.SHORT, LongType.LONG))
    assert(TypeWideningChecker.isWideningSupported(IntegerType.INTEGER, LongType.LONG))
  }

  test("integer type narrowing is not supported") {
    assert(!TypeWideningChecker.isWideningSupported(LongType.LONG, IntegerType.INTEGER))
    assert(!TypeWideningChecker.isWideningSupported(IntegerType.INTEGER, ShortType.SHORT))
    assert(!TypeWideningChecker.isWideningSupported(ShortType.SHORT, ByteType.BYTE))
  }

  test("float to double widening") {
    assert(TypeWideningChecker.isWideningSupported(FloatType.FLOAT, DoubleType.DOUBLE))
  }

  test("double to float is not supported") {
    assert(!TypeWideningChecker.isWideningSupported(DoubleType.DOUBLE, FloatType.FLOAT))
  }

  test("integer to double widening supported") {
    Seq(ByteType.BYTE, ShortType.SHORT, IntegerType.INTEGER) foreach { t =>
      assert(TypeWideningChecker.isWideningSupported(t, DoubleType.DOUBLE))
    }
  }

  test("unsupported integral to floating point no supported") {
    // Test invalid integer to double widening
    assert(!TypeWideningChecker.isWideningSupported(LongType.LONG, DoubleType.DOUBLE))
    Seq(ByteType.BYTE, ShortType.SHORT, IntegerType.INTEGER) foreach { t =>
      assert(!TypeWideningChecker.isWideningSupported(t, FloatType.FLOAT))
    }
  }

  test("date to timestamp NTZ widening") {
    // Test Date -> TimestampNTZ widening
    assert(TypeWideningChecker.isWideningSupported(DateType.DATE, TimestampNTZType.TIMESTAMP_NTZ))
  }

  test("decimal widening supported") {
    // Test Decimal(p, s) -> Decimal(p + k1, s + k2) where k1 >= k2 >= 0

    // Same scale, increased precision
    assert(TypeWideningChecker.isWideningSupported(new DecimalType(5, 2), new DecimalType(10, 2)))
    // Increased scale and precision, with precision increase >= scale increase
    assert(TypeWideningChecker.isWideningSupported(new DecimalType(5, 2), new DecimalType(10, 5)))
    assert(TypeWideningChecker.isWideningSupported(new DecimalType(5, 2), new DecimalType(10, 5)))
    assert(TypeWideningChecker.isWideningSupported(new DecimalType(5, 2), new DecimalType(8, 4)))
  }

  test("decimal widening unsupported") {
    // Invalid decimal widening
    assert(!TypeWideningChecker.isWideningSupported(new DecimalType(10, 2), new DecimalType(5, 2)))
    assert(!TypeWideningChecker.isWideningSupported(new DecimalType(10, 2), new DecimalType(10, 1)))
    assert(!TypeWideningChecker.isWideningSupported(new DecimalType(10, 2), new DecimalType(9, 2)))
    assert(!TypeWideningChecker.isWideningSupported(
      new DecimalType(10, 5),
      new DecimalType(12, 8)
    )) // k1 < k2
    assert(!TypeWideningChecker.isWideningSupported(
      new DecimalType(10, 5),
      new DecimalType(10, 3)
    )) // scale decrease
  }

  test("integer to decimal supported widening") {
    // Test Byte, Short, Int -> Decimal(10 + k1, k2) where k1 >= k2 >= 0
    Seq(ByteType.BYTE, ShortType.SHORT, IntegerType.INTEGER) foreach { t =>
      assert(TypeWideningChecker.isWideningSupported(t, new DecimalType(10, 0)))
      assert(TypeWideningChecker.isWideningSupported(t, new DecimalType(11, 0)))
      assert(TypeWideningChecker.isWideningSupported(t, new DecimalType(12, 2)))
      assert(TypeWideningChecker.isWideningSupported(t, new DecimalType(15, 3)))
    }

    // Test Long -> Decimal(20 + k1, k2) where k1 >= k2 >= 0
    assert(TypeWideningChecker.isWideningSupported(LongType.LONG, new DecimalType(20, 0)))
    assert(TypeWideningChecker.isWideningSupported(LongType.LONG, new DecimalType(25, 5)))
  }

  test("integer to Decimal unsupported widening") {
    Seq(ByteType.BYTE, ShortType.SHORT, IntegerType.INTEGER) foreach { t =>
      assert(!TypeWideningChecker.isWideningSupported(
        t,
        new DecimalType(9, 0)
      )) // precision < 10
      assert(!TypeWideningChecker.isWideningSupported(
        t,
        new DecimalType(12, 3)
      )) // k1 < k2
    }
    assert(!TypeWideningChecker.isWideningSupported(
      LongType.LONG,
      new DecimalType(19, 0)
    )) // precision < 20
  }

  test("unsupported widening") {
    // Test unsupported widening operations
    assert(!TypeWideningChecker.isWideningSupported(StringType.STRING, BinaryType.BINARY))
    assert(!TypeWideningChecker.isWideningSupported(IntegerType.INTEGER, StringType.STRING))
    assert(!TypeWideningChecker.isWideningSupported(DateType.DATE, StringType.STRING))
    assert(!TypeWideningChecker.isWideningSupported(DoubleType.DOUBLE, new DecimalType(10, 2)))
    // Test invalid date widening
    assert(!TypeWideningChecker.isWideningSupported(DateType.DATE, TimestampType.TIMESTAMP))
    assert(!TypeWideningChecker.isWideningSupported(TimestampNTZType.TIMESTAMP_NTZ, DateType.DATE))
  }

  test("Iceberg V2 compatible widening") {
    // Test Iceberg V2 compatible widening

    // Integer widening
    assert(TypeWideningChecker.isIcebergV2Compatible(ByteType.BYTE, ShortType.SHORT))
    assert(TypeWideningChecker.isIcebergV2Compatible(ShortType.SHORT, IntegerType.INTEGER))
    assert(TypeWideningChecker.isIcebergV2Compatible(IntegerType.INTEGER, LongType.LONG))

    // Float -> Double widening
    assert(TypeWideningChecker.isIcebergV2Compatible(FloatType.FLOAT, DoubleType.DOUBLE))

    // Decimal precision increase (without scale increase)
    assert(TypeWideningChecker.isIcebergV2Compatible(
      new DecimalType(5, 2),
      new DecimalType(10, 2)))

  }

  test("iceberg V2 unsupported type widening") {
    /////////////////////////////////////////////////////////////////////////////////////
    // Test generally unsupported widening operations
    /////////////////////////////////////////////////////////////////////////////////////
    assert(!TypeWideningChecker.isIcebergV2Compatible(StringType.STRING, BinaryType.BINARY))
    assert(!TypeWideningChecker.isIcebergV2Compatible(IntegerType.INTEGER, StringType.STRING))
    assert(!TypeWideningChecker.isIcebergV2Compatible(DateType.DATE, StringType.STRING))
    assert(!TypeWideningChecker.isIcebergV2Compatible(DoubleType.DOUBLE, new DecimalType(10, 2)))
    assert(!TypeWideningChecker.isIcebergV2Compatible(DateType.DATE, TimestampType.TIMESTAMP))
    assert(!TypeWideningChecker.isIcebergV2Compatible(
      TimestampNTZType.TIMESTAMP_NTZ,
      DateType.DATE))

    ////////////////////////////////////////////////////////////////////////////////////
    // Test invalid widening that are generally supported by Delta but not by Iceberg V2
    ////////////////////////////////////////////////////////////////////////////////////

    // Integer to Double widening (not supported by Iceberg)
    assert(!TypeWideningChecker.isIcebergV2Compatible(ByteType.BYTE, DoubleType.DOUBLE))
    assert(!TypeWideningChecker.isIcebergV2Compatible(ShortType.SHORT, DoubleType.DOUBLE))
    assert(!TypeWideningChecker.isIcebergV2Compatible(IntegerType.INTEGER, DoubleType.DOUBLE))

    // Date to TimestampNTZ widening (not supported by Iceberg)
    assert(!TypeWideningChecker.isIcebergV2Compatible(
      DateType.DATE,
      TimestampNTZType.TIMESTAMP_NTZ))

    // Decimal scale increase (not supported by Iceberg)
    assert(!TypeWideningChecker.isIcebergV2Compatible(
      new DecimalType(5, 2),
      new DecimalType(10, 5)))

    // Integer to Decimal widening (not supported by Iceberg)
    assert(!TypeWideningChecker.isIcebergV2Compatible(ByteType.BYTE, new DecimalType(10, 0)))
    assert(!TypeWideningChecker.isIcebergV2Compatible(ShortType.SHORT, new DecimalType(12, 2)))
    assert(!TypeWideningChecker.isIcebergV2Compatible(
      IntegerType.INTEGER,
      new DecimalType(15, 3)))
    assert(!TypeWideningChecker.isIcebergV2Compatible(LongType.LONG, new DecimalType(20, 0)))
  }
}
