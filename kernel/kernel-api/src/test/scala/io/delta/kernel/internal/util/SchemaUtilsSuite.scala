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
package io.delta.kernel.internal.util

import io.delta.kernel.exceptions.KernelException
import io.delta.kernel.internal.util.SchemaUtils.validateSchema
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.StringType.STRING
import io.delta.kernel.types.DoubleType.DOUBLE
import io.delta.kernel.types.{ArrayType, MapType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Arrays
import java.util.Locale

class SchemaUtilsSuite extends AnyFunSuite {
  private def expectFailure(shouldContain: String*)(f: => Unit): Unit = {
    val e = intercept[KernelException] {
      f
    }
    val msg = e.getMessage.toLowerCase(Locale.ROOT)
    assert(shouldContain.map(_.toLowerCase(Locale.ROOT)).forall(msg.contains),
      s"Error message '$msg' didn't contain: $shouldContain")
  }

  //////////////////////
  // isSuperset tests //
  //////////////////////

  private val field_a = new StructField("a", INTEGER, true)
  private val field_b_1 = new StructField("1", INTEGER, true)
  private val field_b_2 = new StructField("2", STRING, true)
  private val field_b =
    new StructField("b", new StructType(Arrays.asList(field_b_1, field_b_2)), true)
  private val field_c_foobar_3 = new StructField("3", INTEGER, true)
  private val field_c_foobar =
    new StructField("foo.bar", new StructType(Arrays.asList(field_c_foobar_3)), true)
  private val field_c = new StructField("c", new StructType(Arrays.asList(field_c_foobar)), true)
  private val field_d = new StructField("d", new ArrayType(STRING, true), true)
  private val field_e = new StructField("e", new MapType(INTEGER, STRING, true), true)
  private val field_f_element_4 = new StructField("4", INTEGER, true)
  private val field_f =
    new StructField("f",
      new ArrayType(new StructType(Arrays.asList(field_f_element_4)), true), true)
  private val field_g_key_5 = new StructField("5", INTEGER, true)
  private val field_g_value_zipzap_6 = new StructField("6", INTEGER, true)
  private val field_g_value_zipzap_7 = new StructField("7", INTEGER, true)
  private val field_g_value_zipzap =
    new StructField("zip.zap",
      new StructType(Arrays.asList(field_g_value_zipzap_6, field_g_value_zipzap_7)), true)
  private val field_g = new StructField("g", new MapType(
    new StructType(Arrays.asList(field_g_key_5)),
    new StructType(Arrays.asList(field_g_value_zipzap)), true), true)
  private val complexSchema =
    new StructType(Arrays.asList(field_a, field_b, field_c, field_d, field_e, field_f, field_g))

  test("isSuperset -- simple cases") {
    {
      // is subset, empty
      assert(SchemaUtils.isSuperset(complexSchema, new StructType()))
    }
    {
      // is subset, simple
      val schema = new StructType(Arrays.asList(field_a))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is subset, itself
      assert(SchemaUtils.isSuperset(complexSchema, complexSchema))
    }
    {
      // is subset, different order
      val schema = new StructType(Arrays.asList(field_e, field_d, field_c, field_b, field_a))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, field doesn't exist
      val schema = new StructType().add("new_field", INTEGER)
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, field has different type
      val schema = new StructType().add("a", STRING)
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
  }

  test("isSuperset -- struct cases") {
    {
      // is subset, an entire nested struct
      val schema = new StructType(Arrays.asList(field_b))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is subset, several entire nested structs
      val schema = new StructType(Arrays.asList(field_b, field_c))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is subset, only some nested fields of a struct. here we select b.1 but not b.2.
      val b_1_nested = new StructField("b", new StructType(Arrays.asList(field_b_1)), true)
      val schema = new StructType(Arrays.asList(b_1_nested))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT a subset, nested field doesn't exist
      val field_b_99 = new StructField("99", STRING, true)
      val b_99_nested = new StructField("b", new StructType(Arrays.asList(field_b_99)), true)
      val schema = new StructType(Arrays.asList(b_99_nested))
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
  }

  test("isSuperset -- simple array cases") {
    {
      // is subset, array
      val schema = new StructType(Arrays.asList(field_d))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, array has wrong element type (DOUBLE instead of STRING)
      val field_d_bad_type = new StructField("d", new ArrayType(DOUBLE, true), true)
      val schema = new StructType(Arrays.asList(field_d_bad_type))
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
  }

  test("isSuperset -- simple map cases") {
    {
      // is subset, an entire map
      val schema = new StructType(Arrays.asList(field_e))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, map has wrong key type (DOUBLE instead of INTEGER)
      val field_e_bad_type = new StructField("e", new MapType(DOUBLE, STRING, true), true)
      val schema = new StructType(Arrays.asList(field_e_bad_type))
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, map has wrong value type (DOUBLE instead of STRING)
      val field_e_bad_type = new StructField("e", new MapType(INTEGER, DOUBLE, true), true)
      val schema = new StructType(Arrays.asList(field_e_bad_type))
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
  }

  test("isSuperset -- array of complex type cases") {
    {
      // is subset, an entire array whose element is a struct
      val schema = new StructType(Arrays.asList(field_f))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, array of struct has wrong element type (DOUBLE instead of INTEGER)
      val field_f_element_4_bad_type = new StructField("4", DOUBLE, true)
      val field_f_bad_type = new StructField("f",
        new ArrayType(new StructType(Arrays.asList(field_f_element_4_bad_type)), true), true)
      val schema = new StructType(Arrays.asList(field_f_bad_type))
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
  }

  test("isSuperset -- map of complex type cases") {
    {
      // is subset, an entire map whose keys and values are structs
      val schema = new StructType(Arrays.asList(field_g))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is subset, only some of the nested fields of the struct which is the map's value type
      val field_g_value_zipzap_excluding_7 =
        new StructField("zip.zap", new StructType(Arrays.asList(field_g_value_zipzap_6)), true)
      val field_g_with_subset_value = new StructField("g", new MapType(
        new StructType(Arrays.asList(field_g_key_5)),
        new StructType(Arrays.asList(field_g_value_zipzap_excluding_7)), // <-- value has bad field
        true), true)
      val schema = new StructType(Arrays.asList(field_g_with_subset_value))
      assert(SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, non-existent nested field in the struct which is the maps's value type
      val field_g_value_zipzap_8 = new StructField("8", INTEGER, true)
      val field_g_value_zipzap_with_extra =
        new StructField("zip.zap",
          new StructType(
            Arrays.asList(field_g_value_zipzap_6, field_g_value_zipzap_7, field_g_value_zipzap_8)
          ), true)
      val field_g_with_bad_value = new StructField("g", new MapType(
        new StructType(Arrays.asList(field_g_key_5)),
        new StructType(Arrays.asList(field_g_value_zipzap_with_extra)), // <-- value has bad field
        true), true)
      val schema = new StructType(Arrays.asList(field_g_with_bad_value))
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
    {
      // is NOT subset, nested field with wrong type in the struct which is the maps's key type
      val field_g_key_5_bad_type = new StructField("5", DOUBLE, true)
      val field_g_with_bad_value = new StructField("g", new MapType(
        new StructType(Arrays.asList(field_g_key_5_bad_type)), // <-- bad key type
        new StructType(Arrays.asList(field_g_value_zipzap)), true), true)
      val schema = new StructType(Arrays.asList(field_g_with_bad_value))
      assert(!SchemaUtils.isSuperset(complexSchema, schema))
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Duplicate Column Checks
  ///////////////////////////////////////////////////////////////////////////

  test("duplicate column name in top level") {
    val schema = new StructType()
      .add("dupColName", INTEGER)
      .add("b", INTEGER)
      .add("dupColName", StringType.STRING)
    expectFailure("dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name in top level - case sensitivity") {
    val schema = new StructType()
      .add("dupColName", INTEGER)
      .add("b", INTEGER)
      .add("dupCOLNAME", StringType.STRING)
    expectFailure("dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name for nested column + non-nested column") {
    val schema = new StructType()
      .add("dupColName", new StructType()
        .add("a", INTEGER)
        .add("b", INTEGER))
      .add("dupColName", INTEGER)
    expectFailure("dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name for nested column + non-nested column - case sensitivity") {
    val schema = new StructType()
      .add("dupColName", new StructType()
        .add("a", INTEGER)
        .add("b", INTEGER))
      .add("dupCOLNAME", INTEGER)
    expectFailure("dupCOLNAME") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name in nested level") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("dupColName", INTEGER)
        .add("b", INTEGER)
        .add("dupColName", StringType.STRING)
      )
    expectFailure("top.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name in nested level - case sensitivity") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("dupColName", INTEGER)
        .add("b", INTEGER)
        .add("dupCOLNAME", StringType.STRING)
      )
    expectFailure("top.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name in double nested level") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", new StructType()
          .add("dupColName", StringType.STRING)
          .add("c", INTEGER)
          .add("dupColName", StringType.STRING))
        .add("d", INTEGER)
      )
    expectFailure("top.b.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name in double nested array") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", new ArrayType(
          new ArrayType(new StructType()
            .add("dupColName", StringType.STRING)
            .add("c", INTEGER)
            .add("dupColName", StringType.STRING),
            true),
          true))
        .add("d", INTEGER)
      )
    expectFailure("top.b.element.element.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("only duplicate columns are listed in the error message") {
    val schema = new StructType()
      .add("top",
        new StructType().add("a", INTEGER).add("b", INTEGER).add("c", INTEGER)
      ).add("top",
        new StructType().add("b", INTEGER).add("c", INTEGER).add("d", INTEGER)
      ).add("bottom",
        new StructType().add("b", INTEGER).add("c", INTEGER).add("d", INTEGER)
      )

    val e = intercept[KernelException] {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
    assert(e.getMessage.contains("Schema contains duplicate columns: top, top.b, top.c"))
  }

  test("duplicate column name in double nested map") {
    val keyType = new StructType()
      .add("dupColName", INTEGER)
      .add("d", StringType.STRING)
    expectFailure("top.b.key.dupColName") {
      val schema = new StructType()
        .add("top", new StructType()
          .add("b", new MapType(keyType.add("dupColName", StringType.STRING), keyType, true))
        )
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
    expectFailure("top.b.value.dupColName") {
      val schema = new StructType()
        .add("top", new StructType()
          .add("b", new MapType(keyType, keyType.add("dupColName", StringType.STRING), true))
        )
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
    // This is okay
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", new MapType(keyType, keyType, true))
      )
    validateSchema(schema, false /* isColumnMappingEnabled */)
  }

  test("duplicate column name in nested array") {
    val schema = new StructType()
      .add("top", new ArrayType(new StructType()
        .add("dupColName", INTEGER)
        .add("b", INTEGER)
        .add("dupColName", StringType.STRING), true)
      )
    expectFailure("top.element.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column name in nested array - case sensitivity") {
    val schema = new StructType()
      .add("top", new ArrayType(new StructType()
        .add("dupColName", INTEGER)
        .add("b", INTEGER)
        .add("dupCOLNAME", StringType.STRING), true)
      )
    expectFailure("top.element.dupColName") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("non duplicate column because of back tick") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("a", INTEGER)
        .add("b", INTEGER))
      .add("top.a", INTEGER)
    validateSchema(schema, false /* isColumnMappingEnabled */)
  }

  test("non duplicate column because of back tick - nested") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("top", new StructType()
          .add("a", INTEGER)
          .add("b", INTEGER))
        .add("top.a", INTEGER))
    validateSchema(schema, false /* isColumnMappingEnabled */)
  }

  test("duplicate column with back ticks - nested") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("top.a", StringType.STRING)
        .add("b", INTEGER)
        .add("top.a", INTEGER))
    expectFailure("first.`top.a`") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  test("duplicate column with back ticks - nested and case sensitivity") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("TOP.a", StringType.STRING)
        .add("b", INTEGER)
        .add("top.a", INTEGER))
    expectFailure("first.`top.a`") {
      validateSchema(schema, false /* isColumnMappingEnabled */)
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // checkFieldNames
  ///////////////////////////////////////////////////////////////////////////
  test("check non alphanumeric column characters") {
    val badCharacters = " ,;{}()\n\t="
    val goodCharacters = "#.`!@$%^&*~_<>?/:"

    badCharacters.foreach { char =>
      Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString).foreach { name =>
        val schema = new StructType().add(name, INTEGER)
        val e = intercept[KernelException] {
          validateSchema(schema, false /* isColumnMappingEnabled */)
        }

        if (char != '\n') {
          // with column mapping disabled this should be a valid name
          validateSchema(schema, true /* isColumnMappingEnabled */)
        }

        assert(e.getMessage.contains("contains one of the unsupported"))
      }
    }

    goodCharacters.foreach { char =>
      // no issues here
      Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString).foreach { name =>
        val schema = new StructType().add(name, INTEGER);
        validateSchema(schema, false /* isColumnMappingEnabled */)
        validateSchema(schema, true /* isColumnMappingEnabled */)
      }
    }
  }
}
