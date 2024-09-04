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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._

/**
 * The set of type changes supported by type widening is different between Spark 3.5 and Spark 4.0.
 * See [[TypeWideningShims]]. This shim splits the test cases into supported and unsupported
 * accordingly for delta on Spark 4.0.
 */
trait TypeWideningTestCasesShims {
  self: TypeWideningTestCases with SQLTestUtils =>

  import testImplicits._

  // Type changes that are supported by all Parquet readers. Byte, Short, Int are all stored as
  // INT32 in parquet so these changes are guaranteed to be supported.
  protected val supportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    SupportedTypeEvolutionTestCase(ByteType, ShortType,
      Seq(1, -1, Byte.MinValue, Byte.MaxValue, null.asInstanceOf[Byte]),
      Seq(4, -4, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short])),
    SupportedTypeEvolutionTestCase(ByteType, IntegerType,
      Seq(1, -1, Byte.MinValue, Byte.MaxValue, null.asInstanceOf[Byte]),
      Seq(4, -4, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int])),
    SupportedTypeEvolutionTestCase(ShortType, IntegerType,
      Seq(1, -1, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short]),
      Seq(4, -4, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int])),
    SupportedTypeEvolutionTestCase(ShortType, LongType,
      Seq(1, -1, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short]),
      Seq(4L, -4L, Long.MinValue, Long.MaxValue, null.asInstanceOf[Long])),
    SupportedTypeEvolutionTestCase(IntegerType, LongType,
      Seq(1, -1, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int]),
      Seq(4L, -4L, Long.MinValue, Long.MaxValue, null.asInstanceOf[Long])),
    SupportedTypeEvolutionTestCase(FloatType, DoubleType,
      Seq(1234.56789f, -0f, 0f, Float.NaN, Float.NegativeInfinity, Float.PositiveInfinity,
        Float.MinPositiveValue, Float.MinValue, Float.MaxValue, null.asInstanceOf[Float]),
      Seq(987654321.987654321d, -0d, 0d, Double.NaN, Double.NegativeInfinity,
        Double.PositiveInfinity, Double.MinPositiveValue, Double.MinValue, Double.MaxValue,
        null.asInstanceOf[Double])),
    SupportedTypeEvolutionTestCase(DateType, TimestampNTZType,
      Seq("2020-01-01", "2024-02-29", "1312-02-27"),
      Seq("2020-03-17 15:23:15.123456", "2058-12-31 23:59:59.999", "0001-01-01 00:00:00")),
    // Larger precision, same physical type.
    SupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS - 2, 2),
      DecimalType(Decimal.MAX_INT_DIGITS, 2),
      Seq(BigDecimal("1.23"), BigDecimal("10.34"), null.asInstanceOf[BigDecimal]),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"),
        null.asInstanceOf[BigDecimal])),
    // Larger precision, different physical type.
    SupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_LONG_DIGITS, 2),
      Seq(BigDecimal("1.23"), BigDecimal("10.34"), null.asInstanceOf[BigDecimal]),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99"),
        null.asInstanceOf[BigDecimal]))
  )

  // Type changes that were only supported during the preview and were removed in the stable version
  // of the feature.
  protected val previewOnlySupportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    SupportedTypeEvolutionTestCase(IntegerType, DoubleType,
      Seq(1, -1, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int]),
      Seq(987654321.987654321d, -0d, 0d, Double.NaN, Double.NegativeInfinity,
        Double.PositiveInfinity, Double.MinPositiveValue, Double.MinValue, Double.MaxValue,
        null.asInstanceOf[Double])),
    SupportedTypeEvolutionTestCase(ByteType, DecimalType(10, 0),
      Seq(1, -1, Byte.MinValue, Byte.MaxValue, null.asInstanceOf[Byte]),
      Seq(BigDecimal("1.23"), BigDecimal("9" * 10), null.asInstanceOf[BigDecimal])),
    SupportedTypeEvolutionTestCase(ShortType, DecimalType(10, 0),
      Seq(1, -1, Short.MinValue, Short.MaxValue, null.asInstanceOf[Short]),
      Seq(BigDecimal("1.23"), BigDecimal("9" * 10), null.asInstanceOf[BigDecimal])),
    SupportedTypeEvolutionTestCase(IntegerType, DecimalType(10, 0),
      Seq(1, -1, Int.MinValue, Int.MaxValue, null.asInstanceOf[Int]),
      Seq(BigDecimal("1.23"), BigDecimal("9" * 10), null.asInstanceOf[BigDecimal])),
    SupportedTypeEvolutionTestCase(LongType, DecimalType(20, 0),
      Seq(1L, -1L, Long.MinValue, Long.MaxValue, null.asInstanceOf[Int]),
      Seq(BigDecimal("1.23"), BigDecimal("9" * 20), null.asInstanceOf[BigDecimal])),
    // Larger precision and scale.
    SupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS - 1, 2),
      DecimalType(Decimal.MAX_INT_DIGITS, 3),
      Seq(BigDecimal("1.23"), BigDecimal("10.34"), null.asInstanceOf[BigDecimal]),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 3) + ".99"),
        null.asInstanceOf[BigDecimal]))
  )

  // Test type changes that aren't supported.
  protected val unsupportedTestCases: Seq[TypeEvolutionTestCase] = Seq(
    UnsupportedTypeEvolutionTestCase(IntegerType, ByteType,
      Seq(1, 2, Int.MinValue)),
    UnsupportedTypeEvolutionTestCase(LongType, IntegerType,
      Seq(4, 5, Long.MaxValue)),
    UnsupportedTypeEvolutionTestCase(DoubleType, FloatType,
      Seq(987654321.987654321d, Double.NaN, Double.NegativeInfinity,
        Double.PositiveInfinity, Double.MinPositiveValue,
        Double.MinValue, Double.MaxValue)),
    UnsupportedTypeEvolutionTestCase(ByteType, DecimalType(2, 0),
      Seq(1, -1, Byte.MinValue)),
    UnsupportedTypeEvolutionTestCase(ShortType, DecimalType(4, 0),
      Seq(1, -1, Short.MinValue)),
    UnsupportedTypeEvolutionTestCase(IntegerType, DecimalType(9, 0),
      Seq(1, -1, Int.MinValue)),
    UnsupportedTypeEvolutionTestCase(LongType, DecimalType(19, 0),
      Seq(1, -1, Long.MinValue)),
    UnsupportedTypeEvolutionTestCase(TimestampNTZType, DateType,
      Seq("2020-03-17 15:23:15", "2023-12-31 23:59:59", "0001-01-01 00:00:00")),
    // Reduce scale
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS, 3),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Reduce precision
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 2),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Reduce precision & scale
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_LONG_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS - 1, 1),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99"))),
    // Increase scale more than precision
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_INT_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS + 1, 4),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_INT_DIGITS - 2) + ".99"))),
    // Smaller scale and larger precision.
    UnsupportedTypeEvolutionTestCase(DecimalType(Decimal.MAX_LONG_DIGITS, 2),
      DecimalType(Decimal.MAX_INT_DIGITS + 3, 1),
      Seq(BigDecimal("-67.89"), BigDecimal("9" * (Decimal.MAX_LONG_DIGITS - 2) + ".99")))
  )
}
