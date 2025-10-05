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

import io.delta.kernel.data.ColumnVector
import io.delta.kernel.defaults.internal.expressions.ImplicitCastExpression.canCastTo
import io.delta.kernel.defaults.utils.DefaultKernelTestUtils.getValueAsObject
import io.delta.kernel.defaults.utils.TestUtils
import io.delta.kernel.expressions.Column
import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ImplicitCastExpressionSuite extends AnyFunSuite with TestUtils {
  private val allowedCasts: Set[(DataType, DataType)] = Set(
    (ByteType.BYTE, ShortType.SHORT),
    (ByteType.BYTE, IntegerType.INTEGER),
    (ByteType.BYTE, LongType.LONG),
    (ByteType.BYTE, FloatType.FLOAT),
    (ByteType.BYTE, DoubleType.DOUBLE),
    (ShortType.SHORT, IntegerType.INTEGER),
    (ShortType.SHORT, LongType.LONG),
    (ShortType.SHORT, FloatType.FLOAT),
    (ShortType.SHORT, DoubleType.DOUBLE),
    (IntegerType.INTEGER, LongType.LONG),
    (IntegerType.INTEGER, FloatType.FLOAT),
    (IntegerType.INTEGER, DoubleType.DOUBLE),
    (LongType.LONG, FloatType.FLOAT),
    (LongType.LONG, DoubleType.DOUBLE),
    (FloatType.FLOAT, DoubleType.DOUBLE))

  test("can cast to") {
    ALL_TYPES.foreach { fromType =>
      ALL_TYPES.foreach { toType =>
        assert(canCastTo(fromType, toType) ===
          allowedCasts.contains((fromType, toType)))
      }
    }
  }

  allowedCasts.foreach { castPair =>
    test(s"eval cast expression: ${castPair._1} -> ${castPair._2}") {
      val fromType = castPair._1
      val toType = castPair._2
      val inputVector = testData(87, fromType, (rowId) => rowId % 7 == 0)
      val outputVector = new ImplicitCastExpression(new Column("id"), toType)
        .eval(inputVector)
      checkCastOutput(inputVector, toType, outputVector)
    }
  }

  def testData(size: Int, dataType: DataType, nullability: (Int) => Boolean): ColumnVector = {
    new ColumnVector {
      override def getDataType: DataType = dataType
      override def getSize: Int = size
      override def close(): Unit = {}
      override def isNullAt(rowId: Int): Boolean = nullability(rowId)

      override def getByte(rowId: Int): Byte = {
        assert(dataType === ByteType.BYTE)
        generateValue(rowId).toByte
      }

      override def getShort(rowId: Int): Short = {
        assert(dataType === ShortType.SHORT)
        generateValue(rowId).toShort
      }

      override def getInt(rowId: Int): Int = {
        assert(dataType === IntegerType.INTEGER)
        generateValue(rowId).toInt
      }

      override def getLong(rowId: Int): Long = {
        assert(dataType === LongType.LONG)
        generateValue(rowId).toLong
      }

      override def getFloat(rowId: Int): Float = {
        assert(dataType === FloatType.FLOAT)
        generateValue(rowId).toFloat
      }

      override def getDouble(rowId: Int): Double = {
        assert(dataType === DoubleType.DOUBLE)
        generateValue(rowId)
      }
    }
  }

  // Utility method to generate a value based on the rowId. Returned value is a double
  // which the callers can cast to appropriate numerical type.
  private def generateValue(rowId: Int): Double = rowId * 2.76 + 7623

  private def checkCastOutput(input: ColumnVector, toType: DataType, output: ColumnVector): Unit = {
    assert(input.getSize === output.getSize)
    assert(toType === output.getDataType)
    Seq.range(0, input.getSize).foreach { rowId =>
      assert(input.isNullAt(rowId) === output.isNullAt(rowId))
      assert(getValueAsObject(input, rowId) === getValueAsObject(output, rowId))
    }
  }

  // Tests for investigating float-to-double precision issue (Task 2)
  test("float to double precision loss investigation") {
    // The problematic value from the task description
    val originalValue = 84.08f

    // Direct cast to double (this is what FloatUpConverter.getDouble() does)
    val convertedValue = originalValue.toDouble

    // Document the precision loss
    println("=== Float to Double Precision Investigation ===")
    println(s"Original float value: $originalValue")
    println(s"Converted double value: $convertedValue")
    println(s"Precision loss: ${convertedValue - originalValue}")

    // Verify that precision loss occurs as described in the task
    assert(convertedValue === 84.08000183105469)

    // Verify that this is different from the original value
    assert(convertedValue != originalValue.toDouble)
  }

  test("multiple float to double precision issues") {
    val problematicValues = Array(84.08f, 12.34f, 56.78f, 99.99f, 0.1f, 0.2f, 0.3f)

    println("\n=== Multiple Float to Double Precision Analysis ===")

    problematicValues.foreach { originalValue =>
      val convertedValue = originalValue.toDouble
      val directDouble = originalValue.toString.toDouble
      val precisionLoss = convertedValue - directDouble

      println(
        f"Float: $originalValue%f -> Double: $convertedValue%.15f (Expected: $directDouble%.15f, Loss: $precisionLoss%.15f)")

      // This demonstrates that the precision loss is inherent to IEEE 754 representation
      // The float value cannot exactly represent the decimal, so when cast to double,
      // the imprecise float representation is extended with zeros
    }
  }

  test("IEEE 754 float representation analysis") {
    val originalValue = 84.08f

    // Show the binary representation
    val floatBits = java.lang.Float.floatToIntBits(originalValue)
    val doubleBits = java.lang.Double.doubleToLongBits(originalValue.toDouble)

    println("\n=== IEEE 754 Representation Analysis ===")
    println(s"Original float: $originalValue")
    println(s"Float bits (hex): 0x${floatBits.toHexString}")
    println(s"Float bits (binary): ${floatBits.toBinaryString}")
    println(s"Double cast: ${originalValue.toDouble}")
    println(s"Double bits (hex): 0x${doubleBits.toHexString}")

    // The issue is that 84.08 cannot be exactly represented in IEEE 754 float format
    // When cast to double, the imprecise float representation is extended with zeros

    // Demonstrate that the issue exists at the float level, not in the casting
    val directDouble = 84.08
    val floatFromDouble = directDouble.toFloat
    val backToDouble = floatFromDouble.toDouble

    println(s"Direct double: $directDouble")
    println(s"Float from double: $floatFromDouble")
    println(s"Back to double: $backToDouble")

    // This shows the precision loss happens during float representation, not casting
    assert(directDouble != backToDouble)

    // The FloatUpConverter.getDouble() method simply does: return inputVector.getFloat(rowId);
    // This is equivalent to a direct cast, which is the source of the precision issue
    assert(originalValue.toDouble === backToDouble)
  }

  test("precision handling solutions") {
    val floatValue = 84.08f
    val expectedDouble = 84.08
    val actualDouble = floatValue.toDouble

    println("\n=== Precision Handling Solutions ===")
    println(s"Float value: $floatValue")
    println(s"Expected double: $expectedDouble")
    println(s"Actual double: $actualDouble")

    // Solution 1: Epsilon comparison
    val epsilon = 1e-6 // Reasonable epsilon for float precision
    val epsilonEqual = math.abs(expectedDouble - actualDouble) < epsilon
    println(s"Epsilon comparison (1e-6): $epsilonEqual")
    assert(epsilonEqual, "Epsilon comparison should work for float precision")

    // Solution 2: Convert both to float precision for comparison
    val expectedAsFloat = expectedDouble.toFloat
    val floatPrecisionEqual = floatValue == expectedAsFloat
    println(s"Float precision comparison: $floatPrecisionEqual")
    assert(floatPrecisionEqual, "Float precision comparison should work")

    // Solution 3: Use BigDecimal for exact decimal representation (not practical for performance)
    // This is mentioned for completeness but not recommended for the IN expression

    println("Recommended approach: Use epsilon comparison for float-double operations")
  }

  test("FloatUpConverter behavior analysis") {
    println("\n=== FloatUpConverter.getDouble() Analysis ===")

    // The FloatUpConverter.getDouble() method implementation:
    // public double getDouble(int rowId) {
    //   return inputVector.getFloat(rowId);
    // }

    val testValues = Array(84.08f, 12.34f, 0.1f, 99.99f)

    testValues.foreach { value =>
      val converted = value.toDouble // This is what FloatUpConverter does

      println(f"Float: $value%f -> Double via cast: $converted%.15f")

      // Show that this is identical to what FloatUpConverter.getDouble() would return
      // The precision loss is inherent to the IEEE 754 float->double conversion
    }

    println("\nRoot Cause: FloatUpConverter.getDouble() performs a direct cast")
    println("from float to double, which preserves the imprecise float representation")
    println("and extends it with zeros in the additional double precision bits.")
  }

  test("recommended approach for refactoring") {
    println("\n=== Recommended Approach for IN Expression Refactoring ===")

    val floatValue = 84.08f
    val doubleValue = 84.08

    // Current behavior: exact comparison fails due to precision loss
    val exactMatch = floatValue.toDouble == doubleValue
    println(s"Exact comparison: $exactMatch")

    // Recommended approach 1: Epsilon-based comparison
    val epsilon = 1e-6 // Appropriate for float precision
    val epsilonMatch = math.abs(floatValue.toDouble - doubleValue) < epsilon
    println(s"Epsilon comparison (1e-6): $epsilonMatch")

    // Recommended approach 2: Convert both to the narrower type for comparison
    val floatPrecisionMatch = floatValue == doubleValue.toFloat
    println(s"Float precision comparison: $floatPrecisionMatch")

    println("\nRecommendation for refactored IN expression:")
    println("1. Use epsilon comparison when comparing float-derived doubles")
    println("2. Document the precision limitation in error messages")
    println("3. Consider using the narrower type precision for comparisons")

    assert(epsilonMatch, "Epsilon comparison should be used for float precision")
    assert(floatPrecisionMatch, "Float precision comparison should work")
  }
}
