/*
 * Copyright (2020) The Delta Lake Project Authors.
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

package io.delta.standalone.internal

import java.util.Locale

import io.delta.standalone.exceptions.DeltaStandaloneException
import io.delta.standalone.types.{ArrayType, IntegerType, MapType, StringType, StructType}
import io.delta.standalone.internal.util.SchemaUtils
import io.delta.standalone.internal.util.SchemaMergingUtils.checkColumnNameDuplication

import org.scalatest.FunSuite

class SchemaUtilsSuite extends FunSuite {

  private def expectFailure(shouldContain: String*)(f: => Unit): Unit = {
    val e = intercept[DeltaStandaloneException] {
      f
    }
    val msg = e.getMessage.toLowerCase(Locale.ROOT)
    assert(shouldContain.map(_.toLowerCase(Locale.ROOT)).forall(msg.contains),
      s"Error message '$msg' didn't contain: $shouldContain")
  }

  ///////////////////////////////////////////////////////////////////////////
  // Duplicate Column Checks
  ///////////////////////////////////////////////////////////////////////////

  test("duplicate column name in top level") {
    val schema = new StructType()
      .add("dupColName", new IntegerType())
      .add("b", new IntegerType())
      .add("dupColName", new StringType())
    expectFailure("dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in top level - case sensitivity") {
    val schema = new StructType()
      .add("dupColName", new IntegerType())
      .add("b", new IntegerType())
      .add("dupCOLNAME", new StringType())
    expectFailure("dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name for nested column + non-nested column") {
    val schema = new StructType()
      .add("dupColName", new StructType()
        .add("a", new IntegerType())
        .add("b", new IntegerType()))
      .add("dupColName", new IntegerType())
    expectFailure("dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name for nested column + non-nested column - case sensitivity") {
    val schema = new StructType()
      .add("dupColName", new StructType()
        .add("a", new IntegerType())
        .add("b", new IntegerType()))
      .add("dupCOLNAME", new IntegerType())
    expectFailure("dupCOLNAME") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in nested level") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("dupColName", new IntegerType())
        .add("b", new IntegerType())
        .add("dupColName", new StringType())
      )
    expectFailure("top.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in nested level - case sensitivity") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("dupColName", new IntegerType())
        .add("b", new IntegerType())
        .add("dupCOLNAME", new StringType())
      )
    expectFailure("top.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in double nested level") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", new StructType()
          .add("dupColName", new StringType())
          .add("c", new IntegerType())
          .add("dupColName", new StringType()))
        .add("d", new IntegerType())
      )
    expectFailure("top.b.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in double nested array") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", new ArrayType(
          new ArrayType(new StructType()
            .add("dupColName", new StringType())
            .add("c", new IntegerType())
            .add("dupColName", new StringType()),
          true),
        true))
        .add("d", new IntegerType())
      )
    expectFailure("top.b.element.element.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in double nested map") {
    val keyType = new StructType()
      .add("dupColName", new IntegerType())
      .add("d", new StringType())
    expectFailure("top.b.key.dupColName") {
      val schema = new StructType()
        .add("top", new StructType()
          .add("b", new MapType(keyType.add("dupColName", new StringType()), keyType, true))
        )
      checkColumnNameDuplication(schema, "")
    }
    expectFailure("top.b.value.dupColName") {
      val schema = new StructType()
        .add("top", new StructType()
          .add("b", new MapType(keyType, keyType.add("dupColName", new StringType()), true))
        )
      checkColumnNameDuplication(schema, "")
    }
    // This is okay
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", new MapType(keyType, keyType, true))
      )
    checkColumnNameDuplication(schema, "")
  }

  test("duplicate column name in nested array") {
    val schema = new StructType()
      .add("top", new ArrayType(new StructType()
        .add("dupColName", new IntegerType())
        .add("b", new IntegerType())
        .add("dupColName", new StringType()), true)
      )
    expectFailure("top.element.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in nested array - case sensitivity") {
    val schema = new StructType()
      .add("top", new ArrayType(new StructType()
        .add("dupColName", new IntegerType())
        .add("b", new IntegerType())
        .add("dupCOLNAME", new StringType()), true)
      )
    expectFailure("top.element.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("non duplicate column because of back tick") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("a", new IntegerType())
        .add("b", new IntegerType()))
      .add("top.a", new IntegerType())
    checkColumnNameDuplication(schema, "")
  }

  test("non duplicate column because of back tick - nested") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("top", new StructType()
          .add("a", new IntegerType())
          .add("b", new IntegerType()))
        .add("top.a", new IntegerType()))
    checkColumnNameDuplication(schema, "")
  }

  test("duplicate column with back ticks - nested") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("top.a", new StringType())
        .add("b", new IntegerType())
        .add("top.a", new IntegerType()))
    expectFailure("first.`top.a`") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column with back ticks - nested and case sensitivity") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("TOP.a", new StringType())
        .add("b", new IntegerType())
        .add("top.a", new IntegerType()))
    expectFailure("first.`top.a`") { checkColumnNameDuplication(schema, "") }
  }

  ///////////////////////////////////////////////////////////////////////////
  // checkFieldNames
  ///////////////////////////////////////////////////////////////////////////

  test("check non alphanumeric column characters") {
    val badCharacters = " ,;{}()\n\t="
    val goodCharacters = "#.`!@$%^&*~_<>?/:"

    badCharacters.foreach { char =>
      Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString).foreach { name =>
        val e = intercept[DeltaStandaloneException] {
          SchemaUtils.checkFieldNames(Seq(name))
        }
        assert(e.getMessage.contains("invalid character"))
      }
    }

    goodCharacters.foreach { char =>
      // no issues here
      SchemaUtils.checkFieldNames(Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString))
    }
  }

}
