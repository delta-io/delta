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

import scala.jdk.CollectionConverters._

import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class TypeResolverSuite extends AnyFunSuite {

  test("findCommonType - same types") {
    // Test with identical types
    val intType = IntegerType.INTEGER
    val listTypes = List[DataType](intType, intType).asJava

    val result = TypeResolver.findCommonType(intType, listTypes)
    assert(result === intType)
  }

  test("findCommonType - empty list") {
    // Test with empty list - should return value type
    val intType = IntegerType.INTEGER
    val emptyList = List.empty[DataType].asJava

    val result = TypeResolver.findCommonType(intType, emptyList)
    assert(result === intType)
  }

  test("findCommonType - numeric upcast") {
    // Test byte -> int upcast
    val byteType = ByteType.BYTE
    val intType = IntegerType.INTEGER
    val listTypes = List[DataType](intType).asJava

    val result = TypeResolver.findCommonType(byteType, listTypes)
    assert(result === intType)
  }

  test("findCommonType - multiple numeric types") {
    // Test byte, short, int -> int
    val byteType = ByteType.BYTE
    val listTypes = List[DataType](ShortType.SHORT, IntegerType.INTEGER).asJava

    val result = TypeResolver.findCommonType(byteType, listTypes)
    assert(result === IntegerType.INTEGER)
  }

  test("findCommonType - float to double upcast") {
    // Test float -> double upcast
    val floatType = FloatType.FLOAT
    val doubleType = DoubleType.DOUBLE
    val listTypes = List[DataType](doubleType).asJava

    val result = TypeResolver.findCommonType(floatType, listTypes)
    assert(result === doubleType)
  }

  test("findCommonType - complex numeric hierarchy") {
    // Test byte, short, int, long, float, double -> double
    val byteType = ByteType.BYTE
    val listTypes = List[DataType](
      ShortType.SHORT,
      IntegerType.INTEGER,
      LongType.LONG,
      FloatType.FLOAT,
      DoubleType.DOUBLE).asJava

    val result = TypeResolver.findCommonType(byteType, listTypes)
    assert(result === DoubleType.DOUBLE)
  }

  test("findCommonType - incompatible types") {
    // Test string and int - should throw exception
    val stringType = StringType.STRING
    val intType = IntegerType.INTEGER
    val listTypes = List[DataType](intType).asJava

    assertThrows[IllegalArgumentException] {
      TypeResolver.findCommonType(stringType, listTypes)
    }
  }

  test("findCommonType - non-numeric types") {
    // Test boolean and string - should throw exception
    val boolType = BooleanType.BOOLEAN
    val stringType = StringType.STRING
    val listTypes = List[DataType](stringType).asJava

    assertThrows[IllegalArgumentException] {
      TypeResolver.findCommonType(boolType, listTypes)
    }
  }

  test("isNumericType") {
    // Test all numeric types
    assert(TypeResolver.isNumericType(ByteType.BYTE))
    assert(TypeResolver.isNumericType(ShortType.SHORT))
    assert(TypeResolver.isNumericType(IntegerType.INTEGER))
    assert(TypeResolver.isNumericType(LongType.LONG))
    assert(TypeResolver.isNumericType(FloatType.FLOAT))
    assert(TypeResolver.isNumericType(DoubleType.DOUBLE))

    // Test non-numeric types
    assert(!TypeResolver.isNumericType(BooleanType.BOOLEAN))
    assert(!TypeResolver.isNumericType(StringType.STRING))
    assert(!TypeResolver.isNumericType(BinaryType.BINARY))
    assert(!TypeResolver.isNumericType(DateType.DATE))
    assert(!TypeResolver.isNumericType(TimestampType.TIMESTAMP))
  }

  test("findCompatibleType - exact match") {
    val intType = IntegerType.INTEGER
    val targetTypes = List[DataType](ByteType.BYTE, intType, LongType.LONG).asJava

    val result = TypeResolver.findCompatibleType(intType, targetTypes)
    assert(result.isPresent)
    assert(result.get() === intType)
  }

  test("findCompatibleType - cast compatible") {
    val byteType = ByteType.BYTE
    val targetTypes = List[DataType](
      StringType.STRING, // Not compatible
      IntegerType.INTEGER, // Compatible via cast
      BooleanType.BOOLEAN // Not compatible
    ).asJava

    val result = TypeResolver.findCompatibleType(byteType, targetTypes)
    assert(result.isPresent)
    assert(result.get() === IntegerType.INTEGER)
  }

  test("findCompatibleType - no match") {
    val stringType = StringType.STRING
    val targetTypes = List[DataType](ByteType.BYTE, IntegerType.INTEGER, BooleanType.BOOLEAN).asJava

    val result = TypeResolver.findCompatibleType(stringType, targetTypes)
    assert(!result.isPresent)
  }

  test("findCompatibleType - empty list") {
    val intType = IntegerType.INTEGER
    val emptyList = List.empty[DataType].asJava

    val result = TypeResolver.findCompatibleType(intType, emptyList)
    assert(!result.isPresent)
  }

  test("numeric type precedence") {
    // Test that wider types are chosen correctly
    val shortType = ShortType.SHORT
    val longType = LongType.LONG
    val listTypes = List[DataType](longType).asJava

    val result = TypeResolver.findCommonType(shortType, listTypes)
    assert(result === longType)

    // Test reverse order
    val result2 = TypeResolver.findCommonType(longType, List[DataType](shortType).asJava)
    assert(result2 === longType)
  }

  test("date time types - not supported") {
    // Date types should not be considered numeric
    assert(!TypeResolver.isNumericType(DateType.DATE))
    assert(!TypeResolver.isNumericType(TimestampType.TIMESTAMP))
    assert(!TypeResolver.isNumericType(TimestampNTZType.TIMESTAMP_NTZ))
  }

  test("decimal type - not supported") {
    // Decimal types are not currently supported in the numeric hierarchy
    val decimalType = new DecimalType(10, 2)
    val intType = IntegerType.INTEGER
    val listTypes = List[DataType](intType).asJava

    assertThrows[IllegalArgumentException] {
      TypeResolver.findCommonType(decimalType, listTypes)
    }
  }
}
