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
package io.delta.kernel.types

import org.scalatest.funsuite.AnyFunSuite

class StringTypeSuite extends AnyFunSuite {
  test("check equals") {
    // Testcase: (instance1, instance2, expected value for `instance1 == instance2`)
    Seq(
      (
        StringType.STRING,
        StringType.STRING,
        true),
      (
        StringType.STRING,
        new StringType("sPark.UTF8_bINary"),
        true),
      (
        StringType.STRING,
        new StringType("SPARK.UTF8_LCASE"),
        false),
      (
        new StringType("ICU.UNICODE"),
        new StringType("SPARK.UTF8_LCASE"),
        false),
      (
        new StringType("ICU.UNICODE"),
        new StringType("ICU.UNICODE_CI"),
        false),
      (
        new StringType("ICU.UNICODE_CI"),
        new StringType("icU.uniCODe_Ci"),
        true)).foreach {
      case (st1, st2, expResult) =>
        assert(st1.equals(st2) == expResult)
    }
  }

  test("isUTF8BinaryCollated") {
    assert(StringType.STRING.isUTF8BinaryCollated)
    assert(new StringType("sPark.UTF8_bINary").isUTF8BinaryCollated)
    assert(!new StringType("SPARK.UTF8_LCASE").isUTF8BinaryCollated)
    assert(!new StringType("ICU.UNICODE.72.2").isUTF8BinaryCollated)
    assert(!new StringType("ICU.UNICODE_CI").isUTF8BinaryCollated)
  }

  test("toString") {
    assert(StringType.STRING.toString == "string")
    assert(new StringType("sPark.UTF8_bINary").toString == "string")
    assert(new StringType("SPARK.UTF8_LCASE").toString == "string collate UTF8_LCASE")
    assert(new StringType("ICU.uNICoDE.72.2").toString == "string collate UNICODE")
    assert(new StringType("ICU.UNICODE_CI").toString == "string collate UNICODE_CI")
  }
}
