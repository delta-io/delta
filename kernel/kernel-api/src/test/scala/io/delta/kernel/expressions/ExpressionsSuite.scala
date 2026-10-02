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
package io.delta.kernel.expressions

import io.delta.kernel.types._

import org.scalatest.funsuite.AnyFunSuite

class ExpressionsSuite extends AnyFunSuite {
  test("expressions: unsupported literal data types") {
    val ex1 = intercept[IllegalArgumentException] {
      Literal.ofNull(new ArrayType(IntegerType.INTEGER, true))
    }
    assert(ex1.getMessage.contains("array[integer] is an invalid data type for Literal."))

    val ex2 = intercept[IllegalArgumentException] {
      Literal.ofNull(new MapType(IntegerType.INTEGER, IntegerType.INTEGER, true))
    }
    assert(ex2.getMessage.contains("map[integer, integer] is an invalid data type for Literal."))

    val ex3 = intercept[IllegalArgumentException] {
      Literal.ofNull(new StructType().add("s1", BooleanType.BOOLEAN))
    }
    assert(ex3.getMessage.matches("struct.* is an invalid data type for Literal."))
  }

  test("ofDecimal: adjusts precision when scale exceeds caller-provided precision") {
    // Java's BigDecimal.precision() returns the count of significant digits in the
    // unscaled value, not the SQL precision. For example, BigDecimal.valueOf(0, 18) has
    // precision=1 and scale=18. A naive caller passing bd.precision() as the precision
    // argument would create DecimalType(1, 18) which is invalid. ofDecimal should
    // adjust precision upward to at least scale.
    val bd = java.math.BigDecimal.valueOf(0, 18)
    assert(bd.precision() == 1)
    assert(bd.scale() == 18)
    val lit = Literal.ofDecimal(bd, bd.precision(), bd.scale())
    val dt = lit.getDataType.asInstanceOf[DecimalType]
    assert(dt.getPrecision == 18)
    assert(dt.getScale == 18)
  }

  test("ofDecimal: normal case with precision >= scale is unchanged") {
    val bd = new java.math.BigDecimal("123.45")
    val lit = Literal.ofDecimal(bd, 10, 2)
    val dt = lit.getDataType.asInstanceOf[DecimalType]
    assert(dt.getPrecision == 10)
    assert(dt.getScale == 2)
    assert(lit.getValue.asInstanceOf[java.math.BigDecimal].compareTo(bd) == 0)
  }

  test("ofDecimal: rejects scale exceeding DecimalType max precision (38)") {
    val bd = java.math.BigDecimal.valueOf(0, 39) // scale=39 > MAX_PRECISION
    val ex = intercept[IllegalArgumentException] {
      Literal.ofDecimal(bd, bd.precision(), bd.scale())
    }
    assert(ex.getMessage.contains("Invalid precision and scale combo"))
  }

  test("ofDecimal: rejects value that exceeds adjusted precision") {
    // BigDecimal "99999.99" has 7 significant digits, so precision=5, scale=2 is too small
    val bd = new java.math.BigDecimal("99999.99")
    val ex = intercept[IllegalArgumentException] {
      Literal.ofDecimal(bd, 5, 2)
    }
    assert(ex.getMessage.contains("exceeds max precision"))
  }

  test("equals and hashCode: equal literals produce equal hash codes") {
    assert(Literal.ofInt(5) == Literal.ofInt(5))
    assert(Literal.ofInt(5).hashCode() == Literal.ofInt(5).hashCode())

    // Identity hashing would place two equal literals in different buckets.
    val set = new java.util.HashSet[Literal]()
    set.add(Literal.ofInt(5))
    set.add(Literal.ofInt(5))
    assert(set.size() == 1)

    val map = new java.util.HashMap[Literal, String]()
    map.put(Literal.ofString("a"), "v")
    assert(map.get(Literal.ofString("a")) == "v")
  }

  test("equals and hashCode: binary literals compare by value") {
    val lit1 = Literal.ofBinary(Array[Byte](1, 2, 3))
    val lit2 = Literal.ofBinary(Array[Byte](1, 2, 3))

    assert(lit1 == lit2)
    assert(lit1.hashCode() == lit2.hashCode())
    assert(lit1 != Literal.ofBinary(Array[Byte](1, 2, 4)))
    assert(lit1 != Literal.ofBinary(Array[Byte](1, 2)))

    val map = new java.util.HashMap[Literal, String]()
    map.put(lit1, "v")
    assert(map.get(lit2) == "v")
  }

  test("equals and hashCode: collated string literals compare by collation") {
    val collation1 = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
    val collation2 = CollationIdentifier.fromString("SPARK.UTF8_LCASE")
    val lit1 = Literal.ofString("a", collation1)
    val lit2 = Literal.ofString("a", collation2)

    assert(lit1 == lit2)
    assert(lit1.hashCode() == lit2.hashCode())
    assert(lit1 != Literal.ofString("a"))
  }

  test("equals and hashCode: differing value or data type are not equal") {
    assert(Literal.ofInt(5) != Literal.ofInt(6))
    assert(Literal.ofInt(5) != Literal.ofLong(5L))
    // Integer(5) and Long(5L) both hash to 5, so the data type must be part of the hash.
    assert(Literal.ofInt(5).hashCode() != Literal.ofLong(5L).hashCode())
  }

  test("equals and hashCode: null literals are equal and hash without throwing") {
    val nullInt1 = Literal.ofNull(IntegerType.INTEGER)
    val nullInt2 = Literal.ofNull(IntegerType.INTEGER)

    assert(nullInt1 == nullInt2)
    assert(nullInt1.hashCode() == nullInt2.hashCode())
    assert(nullInt1 != Literal.ofNull(LongType.LONG))
  }
}
