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

import org.scalatest.funsuite.AnyFunSuite

class StructTypeSuite extends AnyFunSuite {

  test("get works as expected") {
    val structField = new StructField("fieldA", IntegerType.INTEGER, true)
    val struct = new StructType()
      .add(structField)
    assert(struct.get("fieldA") == structField)

    assert(intercept[IllegalArgumentException] {
      struct.get("fieldB")
    }.getMessage.contains("Field with name 'fieldB' not found in schema"))
  }
}
