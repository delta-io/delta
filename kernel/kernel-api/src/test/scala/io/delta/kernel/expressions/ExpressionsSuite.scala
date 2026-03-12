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
    assert(dt.getPrecision >= dt.getScale)
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

  test("ofDecimal: rejects value that exceeds adjusted precision") {
    // BigDecimal "99999.99" has 7 significant digits, so precision=5, scale=2 is too small
    val bd = new java.math.BigDecimal("99999.99")
    val ex = intercept[IllegalArgumentException] {
      Literal.ofDecimal(bd, 5, 2)
    }
    assert(ex.getMessage.contains("exceeds max precision"))
  }
}
