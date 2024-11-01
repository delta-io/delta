/*
 * Copyright (2024) The Delta Lake Project Authors.
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

package io.delta.kernel

import org.scalatest.funsuite.AnyFunSuite

class TableIdentifierSuite extends AnyFunSuite {

  test("TableIdentifier should throw IllegalArgumentException for null or empty namespace") {
    assertThrows[IllegalArgumentException] {
      new TableIdentifier(null, "table")
    }
    assertThrows[IllegalArgumentException] {
      new TableIdentifier(Array(), "table")
    }
  }

  test("TableIdentifier should throw NullPointerException for null table name") {
    assertThrows[NullPointerException] {
      new TableIdentifier(Array("catalog", "schema"), null)
    }
  }

  test("TableIdentifier should return the correct namespace and name") {
    val namespace = Array("catalog", "schema")
    val name = "testTable"
    val tid = new TableIdentifier(namespace, name)

    assert(tid.getNamespace.sameElements(namespace))
    assert(tid.getName == name)
  }

  test("TableIdentifiers with same namespace and name should be equal") {
    val tid1 = new TableIdentifier(Array("catalog", "schema"), "table")
    val tid2 = new TableIdentifier(Array("catalog", "schema"), "table")

    assert(tid1 == tid2)
    assert(tid1.hashCode == tid2.hashCode)
  }

  test("TableIdentifiers with different namespace or name should not be equal") {
    val tid1 = new TableIdentifier(Array("catalog", "schema1"), "table1")
    val tid2 = new TableIdentifier(Array("catalog", "schema2"), "table1")
    val tid3 = new TableIdentifier(Array("catalog", "schema1"), "table2")

    assert(tid1 != tid2)
    assert(tid1 != tid3)
  }

  test("TableIdentifier toString") {
    // Normal case
    val tidNormal = new TableIdentifier(Array("catalog", "schema"), "table")
    val expectedNormal = "TableIdentifier{catalog.schema.table}"
    assert(tidNormal.toString == expectedNormal)

    // Special case: should escape backticks
    val tidSpecial = new TableIdentifier(Array("catalog", "sche`ma"), "tab`le")
    val expectedSpecial = "TableIdentifier{catalog.sche``ma.tab``le}"
    assert(tidSpecial.toString == expectedSpecial)
  }
}
