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
  private val field_f_4 = new StructField("4", INTEGER, true)
  private val field_f =
    new StructField("f", new ArrayType(new StructType(Arrays.asList(field_f_4)), true), true)
  private val field_g_5 = new StructField("5", INTEGER, true)
  private val field_g_zipzap = new StructField("zip.zap", STRING, true)
  private val field_g = new StructField("g", new MapType(
    new StructType(Arrays.asList(field_g_5)),
    new StructType(Arrays.asList(field_g_zipzap)), true), true)
  private val complexSchema =
    new StructType(Arrays.asList(field_a, field_b, field_c, field_d, field_e, field_f, field_g))

  //////////////////////
  // isSuperset tests //
  //////////////////////

  test("isSuperset") {
    {
      // is subset, empty
      assert(SchemaUtils.isSuperset(complexSchema, new StructType()), "case 'empty' failed")
    }
    {
      // is subset, simple
      val schema = new StructType(Arrays.asList(field_a))
      assert(SchemaUtils.isSuperset(complexSchema, schema), "case 'simple' failed")
    }
    {
      // is subset, itself
      assert(SchemaUtils.isSuperset(complexSchema, complexSchema), "case 'itself' failed")
    }
    {
      // is subset, nested
      val schema = new StructType(Arrays.asList(field_b))
      assert(SchemaUtils.isSuperset(complexSchema, schema), "case 'nested' failed")
    }
    {
      // is subset, array
      val schema = new StructType(Arrays.asList(field_f))
      assert(SchemaUtils.isSuperset(complexSchema, schema), "case 'array' failed")
    }
    {
      // is subset, map
      val schema = new StructType(Arrays.asList(field_g))
      assert(SchemaUtils.isSuperset(complexSchema, schema), "case 'map' failed")
    }
    {
      // is subset, different order
      val schema = new StructType(Arrays.asList(field_e, field_d, field_c, field_b, field_a))
      assert(SchemaUtils.isSuperset(complexSchema, schema), "case 'different order' failed")
    }
    {
      // is not subset, new field
      val schema = new StructType().add("new_field", INTEGER)
      assert(!SchemaUtils.isSuperset(complexSchema, schema), "case 'new field' failed")
    }
    {
      // is not subset, different type
      val schema = new StructType().add("a", STRING)
      assert(!SchemaUtils.isSuperset(complexSchema, schema), "case 'different type' failed")
    }
  }

  ///////////////////////////////
  // flattenNestedFields tests //
  ///////////////////////////////

  test("flattenNestedFields") {
    val result = SchemaUtils.flattenNestedFields(complexSchema)

    val expected = java.util.Arrays.asList(
      new Tuple2("a", field_a),
      new Tuple2("b", field_b),
      new Tuple2("b.1", field_b_1),
      new Tuple2("b.2", field_b_2),
      new Tuple2("c", field_c),
      new Tuple2("c.`foo.bar`", field_c_foobar),
      new Tuple2("c.`foo.bar`.3", field_c_foobar_3),
      new Tuple2("d", field_d),
      new Tuple2("e", field_e),
      new Tuple2("f", field_f),
      new Tuple2("f.element.4", field_f_4),
      new Tuple2("g", field_g),
      new Tuple2("g.key.5", field_g_5),
      new Tuple2("g.value.`zip.zap`", field_g_zipzap)
    )

    assert(result === expected)
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
