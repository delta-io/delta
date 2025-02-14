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
import io.delta.kernel.internal.util.SchemaUtils.{filterRecursively, validateSchema}
import io.delta.kernel.types.IntegerType.INTEGER
import io.delta.kernel.types.LongType.LONG
import io.delta.kernel.types.TimestampType.TIMESTAMP
import io.delta.kernel.types.{ArrayType, MapType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite

import java.util.Locale
import scala.collection.JavaConverters._

class SchemaUtilsSuite extends AnyFunSuite {
  private def expectFailure(shouldContain: String*)(f: => Unit): Unit = {
    val e = intercept[KernelException] {
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

  ///////////////////////////////////////////////////////////////////////////
  // filterRecursively
  ///////////////////////////////////////////////////////////////////////////
  val testSchema = new StructType()
    .add("a", INTEGER)
    .add("b", INTEGER)
    .add("c", LONG)
    .add("s", new StructType()
      .add("a", TIMESTAMP)
      .add("e", INTEGER)
      .add("f", LONG)
      .add("g", new StructType()
        .add("a", INTEGER)
        .add("b", TIMESTAMP)
        .add("c", LONG)
      ).add("h", new MapType(
        new StructType().add("a", TIMESTAMP),
        new StructType().add("b", INTEGER),
        true)
      ).add("i", new ArrayType(
        new StructType().add("b", TIMESTAMP),
        true)
      )
    ).add("d", new MapType(
      new StructType().add("b", TIMESTAMP),
      new StructType().add("a", INTEGER),
      true)
    ).add("e", new ArrayType(
      new StructType()
        .add("f", TIMESTAMP)
        .add("b", INTEGER),
      true)
    )
  val flattenedTestSchema = {
    SchemaUtils.filterRecursively(
      testSchema,
      /* visitListMapTypes = */ true,
      /* stopOnFirstMatch = */ false,
      (v1: StructField) => true
    ).asScala.map(f => f._1.asScala.mkString(".") -> f._2).toMap
  }
  Seq(
    // Format: (testPrefix, visitListMapTypes, stopOnFirstMatch, filter, expectedColumns)
    ("Filter by name 'b', stop on first match",
      true, true, (field: StructField) => field.getName == "b", Seq("b")),
    ("Filter by name 'b', visit all matches",
      false, false, (field: StructField) => field.getName == "b",
      Seq("b", "s.g.b")),
    ("Filter by name 'b', visit all matches including nested structures",
      true, false, (field: StructField) => field.getName == "b",
      Seq(
        "b",
        "s.g.b",
        "s.h.value.b",
        "s.i.element.b",
        "d.key.b",
        "e.element.b"
      )),
    ("Filter by TIMESTAMP type, stop on first match",
      false, true, (field: StructField) => field.getDataType == TIMESTAMP,
      Seq("s.a")),
    ("Filter by TIMESTAMP type, visit all matches including nested structures",
      true, false, (field: StructField) => field.getDataType == TIMESTAMP,
      Seq(
        "s.a",
        "s.g.b",
        "s.h.key.a",
        "s.i.element.b",
        "d.key.b",
        "e.element.f"
      )),
    ("Filter by TIMESTAMP type and name 'f', visit all matches", true, false,
      (field: StructField) => field.getDataType == TIMESTAMP && field.getName == "f",
      Seq("e.element.f")),
    ("Filter by non-existent field name 'z'",
      true, false, (field: StructField) => field.getName == "z", Seq())
  ).foreach {
    case (testDescription, visitListMapTypes, stopOnFirstMatch, filter, expectedColumns) =>
      test(s"filterRecursively - $testDescription | " +
        s"visitListMapTypes=$visitListMapTypes, stopOnFirstMatch=$stopOnFirstMatch") {

        val results =
          filterRecursively(testSchema, visitListMapTypes, stopOnFirstMatch,
              (v1: StructField) => filter(v1))
            // convert to map of column path concatenated with '.' and the StructField
            .asScala.map(f => (f._1.asScala.mkString("."), f._2)).toMap

        // Assert that the number of results matches the expected columns
        assert(results.size === expectedColumns.size)
        assert(results === flattenedTestSchema.filterKeys(expectedColumns.contains))
      }
  }
}
