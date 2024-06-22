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
}
