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

class DataTypeSuite extends AnyFunSuite {

  test("test equivalentIgnoreCollations") {
    val utf8LcaseString = new StringType("SPARK.UTF8_LCASE")
    val unicodeString = new StringType("ICU.UNICODE")

    val testCases = Seq(
      (StringType.STRING, StringType.STRING, true),
      (StringType.STRING, utf8LcaseString, true),
      (IntegerType.INTEGER, StringType.STRING, false),
      (utf8LcaseString, unicodeString, true),
      (
        new ArrayType(StringType.STRING, true),
        new ArrayType(utf8LcaseString, true),
        true),
      (
        new ArrayType(unicodeString, false),
        new ArrayType(StringType.STRING, false),
        true),
      (
        new ArrayType(StringType.STRING, true),
        new ArrayType(utf8LcaseString, false),
        false),
      (
        new MapType(StringType.STRING, IntegerType.INTEGER, false),
        new MapType(utf8LcaseString, IntegerType.INTEGER, false),
        true),
      (
        new MapType(unicodeString, IntegerType.INTEGER, false),
        new MapType(utf8LcaseString, IntegerType.INTEGER, false),
        true),
      (
        new MapType(unicodeString, IntegerType.INTEGER, false),
        new MapType(utf8LcaseString, IntegerType.INTEGER, true),
        false),
      (
        new StructType()
          .add("name", StringType.STRING)
          .add("age", IntegerType.INTEGER),
        new StructType()
          .add("name", utf8LcaseString)
          .add("age", IntegerType.INTEGER),
        true),
      (
        new StructType()
          .add("name", StringType.STRING)
          .add("details", new StructType().add("address", StringType.STRING)),
        new StructType()
          .add("name", unicodeString)
          .add("details", new StructType().add("address", utf8LcaseString)),
        true),
      (
        new StructType()
          .add("c1", new ArrayType(unicodeString, true))
          .add("c2", new MapType(StringType.STRING, utf8LcaseString, false)),
        new StructType()
          .add("c1", new ArrayType(StringType.STRING, true))
          .add("c2", new MapType(utf8LcaseString, unicodeString, false)),
        true),
      (
        new StructType()
          .add("c1", new ArrayType(unicodeString, false))
          .add("c2", new MapType(StringType.STRING, utf8LcaseString, false)),
        new StructType()
          .add("c1", new ArrayType(StringType.STRING, true))
          .add("c2", new MapType(utf8LcaseString, unicodeString, false)),
        false),
      (
        new StructType()
          .add("c1", new ArrayType(IntegerType.INTEGER, true))
          .add("c2", new MapType(StringType.STRING, utf8LcaseString, false)),
        new StructType()
          .add("c1", new ArrayType(StringType.STRING, true))
          .add("c2", new MapType(utf8LcaseString, unicodeString, false)),
        false),
      (
        new ArrayType(
          new StructType().add("c1", new MapType(utf8LcaseString, StringType.STRING, true), true),
          true),
        new ArrayType(
          new StructType().add("c1", new MapType(StringType.STRING, utf8LcaseString, true), true),
          true),
        true),
      (
        new ArrayType(
          new StructType().add("c1", new MapType(utf8LcaseString, StringType.STRING, true), true),
          true),
        new ArrayType(
          new StructType().add("c2", new MapType(StringType.STRING, utf8LcaseString, true), true),
          true),
        false),
      (
        new ArrayType(
          new StructType().add("c1", new MapType(utf8LcaseString, StringType.STRING, true), false),
          true),
        new ArrayType(
          new StructType().add("c1", new MapType(StringType.STRING, utf8LcaseString, true), true),
          true),
        false),
      (
        new MapType(
          new StructType().add("c1", utf8LcaseString),
          new ArrayType(utf8LcaseString, false),
          true),
        new MapType(
          new StructType().add("c1", StringType.STRING),
          new ArrayType(utf8LcaseString, false),
          true),
        true),
      (
        new MapType(
          new StructType().add("c1", utf8LcaseString),
          new ArrayType(utf8LcaseString, false),
          false),
        new MapType(
          new StructType().add("c1", StringType.STRING),
          new ArrayType(utf8LcaseString, false),
          true),
        false),
      (
        new MapType(new StructType().add("c1", utf8LcaseString), StringType.STRING, false),
        new MapType(new StructType().add("c1", StringType.STRING), utf8LcaseString, true),
        false))

    testCases.foreach { case (dt1, dt2, expected) =>
      assert(dt1.equivalentIgnoreCollations(dt2) == expected)
    }
  }
}
