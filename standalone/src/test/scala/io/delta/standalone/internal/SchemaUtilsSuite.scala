/*
 * Copyright (2020-present) The Delta Lake Project Authors.
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

import org.scalatest.FunSuite

import io.delta.standalone.exceptions.DeltaStandaloneException
import io.delta.standalone.types._

import io.delta.standalone.internal.util.SchemaMergingUtils.checkColumnNameDuplication
import io.delta.standalone.internal.util.SchemaUtils._

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
          checkFieldNames(Seq(name))
        }
        assert(e.getMessage.contains("invalid character"))
      }
    }

    goodCharacters.foreach { char =>
      // no issues here
      checkFieldNames(Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Write Compatibility Checks
  ///////////////////////////////////////////////////////////////////////////

  /**
   * Tests change of datatype within a schema.
   *  - the make() function is a "factory" function to create schemas that vary only by the
   *    given datatype in a specific position in the schema.
   *  - other tests will call this method with different make() functions to test datatype
   *    incompatibility in all the different places within a schema (in a top-level struct,
   *    in a nested struct, as the element type of an array, etc.)
   */
  def testDatatypeChange(scenario: String)(make: DataType => StructType): Unit = {
    val schemas = Map(
      ("int", make(new IntegerType())),
      ("string", make(new StringType())),
      ("struct", make(new StructType().add("a", new StringType()))),
      ("array", make(new ArrayType(new IntegerType(), true))), // containsNull
      ("map", make(new MapType(new StringType(), new FloatType(), true))) // valueContainsNull
    )
    test(s"change of datatype should fail write compatibility - $scenario") {
      for (a <- schemas.keys; b <- schemas.keys if a != b) {
        assert(!isWriteCompatible(schemas(a), schemas(b)),
          s"isWriteCompatible should have failed for: ${schemas(a)}, ${schemas(b)}")
      }
    }
  }

  /**
   * Tests change of nullability within a schema.
   * - ALLOWED: making a non-nullable field nullable
   * - NOT ALLOWED: making a nullable field non-nullable
   *
   * Implementation details:
   *  - the make() function is a "factory" function to create schemas that vary only by the
   *    nullability (of a field, array element, or map values) in a specific position in the schema.
   *  - other tests will call this method with different make() functions to test nullability
   *    incompatibility in all the different places within a schema (in a top-level struct,
   *    in a nested struct, for the element type of an array, etc.)
   */
  def testNullability (scenario: String)(make: Boolean => StructType): Unit = {
    val nullable = make(true)
    val nonNullable = make(false)

    // restricted: nullable=true ==> nullable=false
    test(s"restricted nullability should fail write compatibility - $scenario") {
      assert(!isWriteCompatible(nullable, nonNullable))
    }

    // relaxed: nullable=false ==> nullable=true
    test(s"relaxed nullability should not fail write compatibility - $scenario") {
      assert(isWriteCompatible(nonNullable, nullable))
    }
  }

  /**
   * Tests for fields of a struct: adding/dropping fields, changing nullability, case variation
   *  - The make() function is a "factory" method to produce schemas. It takes a function that
   *    mutates a struct (for example, but adding a column, or it could just not make any change).
   *  - Following tests will call this method with different factory methods, to mutate the
   *    various places where a struct can appear (at the top-level, nested in another struct,
   *    within an array, etc.)
   *  - This allows us to have one shared code to test compatibility of a struct field in all the
   *    different places where it may occur.
   */
  def testColumnVariations(scenario: String)
    (make: (StructType => StructType) => StructType): Unit = {

    // generate one schema without extra column, one with, one nullable, and one with mixed case
    val withoutExtra = make(struct => struct) // produce struct WITHOUT extra field
    val withExtraNullable = make(struct => struct.add("extra", new StringType()))
    val withExtraMixedCase = make(struct => struct.add("eXtRa", new StringType()))
    val withExtraNonNullable =
      make(struct => struct.add("extra", new StringType(), false)) // nullable = false

    test(s"dropping a field should fail write compatibility - $scenario") {
      assert(!isWriteCompatible(withExtraNullable, withoutExtra))
    }
    test(s"adding a nullable field should not fail write compatibility - $scenario") {
      assert(isWriteCompatible(withoutExtra, withExtraNullable))
    }
    test(s"adding a non-nullable field should not fail write compatibility - $scenario") {
      assert(isWriteCompatible(withoutExtra, withExtraNonNullable))
    }
    test(s"case variation of field name should fail write compatibility - $scenario") {
      assert(!isWriteCompatible(withExtraNullable, withExtraMixedCase))
    }

    testNullability(scenario) { nullable =>
      make(struct => struct.add("extra", new StringType(), nullable))
    }
    testDatatypeChange(scenario) { datatype =>
      make(struct => struct.add("extra", datatype))
    }
  }

  // --------------------------------------------------------------------
  // tests for all kinds of places where a field can appear in a struct
  // --------------------------------------------------------------------

  testColumnVariations("top level")(
    f => f(new StructType().add("a", new IntegerType())))

  testColumnVariations("nested struct")(
    f => new StructType()
      .add("a", f(new StructType().add("b", new IntegerType()))))

  testColumnVariations("nested in array")(
    f => new StructType()
      .add("array", new ArrayType(
        f(new StructType().add("b", new IntegerType())), true) // containsNull
      )
  )

  testColumnVariations("nested in map key")(
    f => new StructType()
      .add("map", new MapType(
        f(new StructType().add("b", new IntegerType())),
        new StringType(), true) // valueContainsNull
      )
  )

  testColumnVariations("nested in map value")(
    f => new StructType()
      .add("map", new MapType(
        new StringType(),
        f(new StructType().add("b", new IntegerType())), true) // valueContainsNull
      )
  )

  // --------------------------------------------------------------------
  // tests for data type change in places other than struct
  // --------------------------------------------------------------------

  testDatatypeChange("array element")(
    datatype => new StructType()
      .add("array", new ArrayType(datatype, true))) // containsNull

  testDatatypeChange("map key")(
    datatype => new StructType()
      .add("map", new MapType(datatype, new StringType(), true))) // valueContainsNull

  testDatatypeChange("map value")(
    datatype => new StructType()
      .add("map", new MapType(new StringType(), datatype, true))) // valueContainsNull

  // --------------------------------------------------------------------
  // tests for nullability change in places other than struct
  // --------------------------------------------------------------------

  testNullability("array contains null")(
    containsNull => new StructType()
      .add("array", new ArrayType(new StringType(), containsNull)))

  testNullability("map contains null values")(
    valueContainsNull => new StructType()
      .add("map", new MapType(new IntegerType(), new StringType(), valueContainsNull)))

  testNullability("map nested in array")(
    valueContainsNull => new StructType()
      .add("map", new ArrayType(
        new MapType(new IntegerType(), new StringType(), valueContainsNull), true))) // containsNull

  testNullability("array nested in map")(
    containsNull => new StructType()
      .add("map", new MapType(
        new IntegerType(),
        new ArrayType(new StringType(), containsNull), true))) // valueContainsNull
}
