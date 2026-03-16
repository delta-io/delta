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
package io.delta.kernel.types

import java.util.Arrays

import org.scalatest.funsuite.AnyFunSuite

class TypesSuite extends AnyFunSuite {
  test("isNested - false") {
    // All primitive types should return false for isNested
    val primitiveTypes = Seq(
      BinaryType.BINARY,
      BooleanType.BOOLEAN,
      ByteType.BYTE,
      DateType.DATE,
      new DecimalType(10, 2),
      DoubleType.DOUBLE,
      FloatType.FLOAT,
      IntegerType.INTEGER,
      LongType.LONG,
      ShortType.SHORT,
      StringType.STRING,
      TimestampType.TIMESTAMP,
      TimestampNTZType.TIMESTAMP_NTZ,
      VariantType.VARIANT)

    primitiveTypes.foreach { dataType =>
      assert(!dataType.isNested(), s"Expected $dataType to not be nested")
    }
  }

  test("isNested - nested types") {
    // Create instances of nested types
    val structFields = Arrays.asList(
      new StructField("field1", IntegerType.INTEGER, true),
      new StructField("field2", StringType.STRING, true))
    val structType = new StructType(structFields)

    val arrayType = new ArrayType(
      new StructField("element", IntegerType.INTEGER, true))

    val mapType = new MapType(
      new StructField("key", StringType.STRING, false),
      new StructField("value", IntegerType.INTEGER, true))

    // All nested types should return true for isNested
    val nestedTypes = Seq(structType, arrayType, mapType)

    nestedTypes.foreach { dataType =>
      assert(dataType.isNested(), s"Expected $dataType to be nested")
    }
  }

  test("MapType rejects collated StringType keys") {
    val collatedString = new StringType("SPARK.UTF8_LCASE")

    // 3-arg constructor should reject collated key
    val ex1 = intercept[IllegalArgumentException] {
      new MapType(collatedString, IntegerType.INTEGER, false)
    }
    assert(ex1.getMessage.contains("does not support collated string types as keys"))

    // 2-arg StructField constructor should also reject collated key
    val ex2 = intercept[IllegalArgumentException] {
      new MapType(
        new StructField("key", collatedString, false),
        new StructField("value", IntegerType.INTEGER, true))
    }
    assert(ex2.getMessage.contains("does not support collated string types as keys"))
  }

  test("MapType accepts default collation StringType keys") {
    // Default StringType.STRING should be accepted
    val map1 = new MapType(StringType.STRING, IntegerType.INTEGER, false)
    assert(map1.getKeyType === StringType.STRING)

    // Explicit UTF8_BINARY collation should also be accepted
    val utf8BinaryString = new StringType("SPARK.UTF8_BINARY")
    val map2 = new MapType(utf8BinaryString, IntegerType.INTEGER, false)
    assert(map2.getKeyType === utf8BinaryString)

    // Non-string key types should continue to work
    val map3 = new MapType(IntegerType.INTEGER, StringType.STRING, true)
    assert(map3.getKeyType === IntegerType.INTEGER)
  }
}
