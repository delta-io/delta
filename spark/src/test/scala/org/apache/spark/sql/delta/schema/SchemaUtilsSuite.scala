/*
 * Copyright (2021) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta.schema

// scalastyle:off import.ordering.noEmptyLine
import java.util.Locale
import java.util.regex.Pattern

import scala.annotation.tailrec

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaLog, DeltaTestUtils}
import org.apache.spark.sql.delta.RowCommitVersion
import org.apache.spark.sql.delta.RowId
import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.schema.SchemaMergingUtils._
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import io.delta.tables.DeltaTable
import org.scalatest.GivenWhenThen

import org.apache.spark.sql.{AnalysisException, Column, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class SchemaUtilsSuite extends QueryTest
  with SharedSparkSession
  with GivenWhenThen
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest {
  import SchemaUtils._
  import testImplicits._

  private def expectFailure(shouldContain: String*)(f: => Unit): Unit = {
    val e = intercept[AnalysisException] {
      f
    }
    val msg = e.getMessage.toLowerCase(Locale.ROOT)
    assert(shouldContain.map(_.toLowerCase(Locale.ROOT)).forall(msg.contains),
      s"Error message '$msg' didn't contain: $shouldContain")
  }

  private def expectFailurePattern(shouldContainPatterns: String*)(f: => Unit): Unit = {
    val e = intercept[AnalysisException] {
      f
    }
    val patterns =
      shouldContainPatterns.map(regex => Pattern.compile(regex, Pattern.CASE_INSENSITIVE))
    assert(patterns.forall(_.matcher(e.getMessage).find()),
      s"Error message '${e.getMessage}' didn't contain the patterns: $shouldContainPatterns")
  }

  private def expectAnalysisErrorClass(errorClass: String, params: Map[String, String])
                                      (f: => Unit): Unit = {
    val e = intercept[AnalysisException] {
      f
    }

    @tailrec
    def getError(ex: Throwable): Option[DeltaAnalysisException] = ex match {
      case e: DeltaAnalysisException if e.getErrorClass() == errorClass => Some(e)
      case e: AnalysisException => getError(e.getCause)
      case _ => None
    }

    val err = getError(e)
    assert(err.isDefined, "exception with the error class not found")
    checkError(
      exception = err.get,
      errorClass = errorClass,
      parameters = params,
      matchPVals = true)
  }

  /////////////////////////////
  // Duplicate Column Checks
  /////////////////////////////

  test("duplicate column name in top level") {
    val schema = new StructType()
      .add("dupColName", IntegerType)
      .add("b", IntegerType)
      .add("dupColName", StringType)
    expectFailure("dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in top level - case sensitivity") {
    val schema = new StructType()
      .add("dupColName", IntegerType)
      .add("b", IntegerType)
      .add("dupCOLNAME", StringType)
    expectFailure("dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name for nested column + non-nested column") {
    val schema = new StructType()
      .add("dupColName", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType))
      .add("dupColName", IntegerType)
    expectFailure("dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name for nested column + non-nested column - case sensitivity") {
    val schema = new StructType()
      .add("dupColName", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType))
      .add("dupCOLNAME", IntegerType)
    expectFailure("dupCOLNAME") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in nested level") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("dupColName", IntegerType)
        .add("b", IntegerType)
        .add("dupColName", StringType)
      )
    expectFailure("top.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in nested level - case sensitivity") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("dupColName", IntegerType)
        .add("b", IntegerType)
        .add("dupCOLNAME", StringType)
      )
    expectFailure("top.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in double nested level") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", new StructType()
          .add("dupColName", StringType)
          .add("c", IntegerType)
          .add("dupColName", StringType))
        .add("d", IntegerType)
      )
    expectFailure("top.b.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in double nested array") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", ArrayType(ArrayType(new StructType()
          .add("dupColName", StringType)
          .add("c", IntegerType)
          .add("dupColName", StringType))))
        .add("d", IntegerType)
      )
    expectFailure("top.b.element.element.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in double nested map") {
    val keyType = new StructType()
      .add("dupColName", IntegerType)
      .add("d", StringType)
    expectFailure("top.b.key.dupColName") {
      val schema = new StructType()
        .add("top", new StructType()
          .add("b", MapType(keyType.add("dupColName", StringType), keyType))
        )
      checkColumnNameDuplication(schema, "")
    }
    expectFailure("top.b.value.dupColName") {
      val schema = new StructType()
        .add("top", new StructType()
          .add("b", MapType(keyType, keyType.add("dupColName", StringType)))
        )
      checkColumnNameDuplication(schema, "")
    }
    // This is okay
    val schema = new StructType()
      .add("top", new StructType()
        .add("b", MapType(keyType, keyType))
      )
    checkColumnNameDuplication(schema, "")
  }

  test("duplicate column name in nested array") {
    val schema = new StructType()
      .add("top", ArrayType(new StructType()
        .add("dupColName", IntegerType)
        .add("b", IntegerType)
        .add("dupColName", StringType))
      )
    expectFailure("top.element.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column name in nested array - case sensitivity") {
    val schema = new StructType()
      .add("top", ArrayType(new StructType()
        .add("dupColName", IntegerType)
        .add("b", IntegerType)
        .add("dupCOLNAME", StringType))
      )
    expectFailure("top.element.dupColName") { checkColumnNameDuplication(schema, "") }
  }

  test("non duplicate column because of back tick") {
    val schema = new StructType()
      .add("top", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType))
      .add("top.a", IntegerType)
    checkColumnNameDuplication(schema, "")
  }

  test("non duplicate column because of back tick - nested") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("top", new StructType()
          .add("a", IntegerType)
          .add("b", IntegerType))
        .add("top.a", IntegerType))
    checkColumnNameDuplication(schema, "")
  }

  test("duplicate column with back ticks - nested") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("top.a", StringType)
        .add("b", IntegerType)
        .add("top.a", IntegerType))
    expectFailure("first.`top.a`") { checkColumnNameDuplication(schema, "") }
  }

  test("duplicate column with back ticks - nested and case sensitivity") {
    val schema = new StructType()
      .add("first", new StructType()
        .add("TOP.a", StringType)
        .add("b", IntegerType)
        .add("top.a", IntegerType))
    expectFailure("first.`top.a`") { checkColumnNameDuplication(schema, "") }
  }

  /////////////////////////////
  // Read Compatibility Checks
  /////////////////////////////

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
      ("int", make(IntegerType)),
      ("string", make(StringType)),
      ("struct", make(new StructType().add("a", StringType))),
      ("array", make(ArrayType(IntegerType))),
      ("map", make(MapType(StringType, FloatType)))
    )
    test(s"change of datatype should fail read compatibility - $scenario") {
      for (a <- schemas.keys; b <- schemas.keys if a != b) {
        assert(!isReadCompatible(schemas(a), schemas(b)),
          s"isReadCompatible should have failed for: ${schemas(a)}, ${schemas(b)}")
      }
    }
  }

  /**
   * Tests change of nullability within a schema (making a field nullable is not allowed,
   * but making a nullable field non-nullable is ok).
   *  - the make() function is a "factory" function to create schemas that vary only by the
   *    nullability (of a field, array element, or map values) in a specific position in the schema.
   *  - other tests will call this method with different make() functions to test nullability
   *    incompatibility in all the different places within a schema (in a top-level struct,
   *    in a nested struct, for the element type of an array, etc.)
   */
  def testNullability(scenario: String)(make: Boolean => StructType): Unit = {
    val nullable = make(true)
    val nonNullable = make(false)
    Seq(true, false).foreach { forbidTightenNullability =>
      val (blockedCase, blockedExisting, blockedRead) = if (forbidTightenNullability) {
        (s"tighten nullability should fail read compatibility " +
          s"(forbidTightenNullability=$forbidTightenNullability) - $scenario",
          nullable, nonNullable)
      } else {
        (s"relax nullability should fail read compatibility " +
          s"(forbidTightenNullability=$forbidTightenNullability) - $scenario",
          nonNullable, nullable)
      }
      val (allowedCase, allowedExisting, allowedRead) = if (forbidTightenNullability) {
        (s"relax nullability should not fail read compatibility " +
          s"(forbidTightenNullability=$forbidTightenNullability) - $scenario",
          nonNullable, nullable)
      } else {
        (s"tighten nullability should not fail read compatibility " +
          s"(forbidTightenNullability=$forbidTightenNullability) - $scenario",
          nullable, nonNullable)
      }
      test(blockedCase) {
        assert(!isReadCompatible(blockedExisting, blockedRead, forbidTightenNullability))
      }
      test(allowedCase) {
        assert(isReadCompatible(allowedExisting, allowedRead, forbidTightenNullability))
      }
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
    val withExtraNullable = make(struct => struct.add("extra", StringType))
    val withExtraMixedCase = make(struct => struct.add("eXtRa", StringType))
    val withExtraNonNullable = make(struct => struct.add("extra", StringType, nullable = false))

    test(s"dropping a field should fail read compatibility - $scenario") {
      assert(!isReadCompatible(withExtraNullable, withoutExtra))
    }
    test(s"adding a nullable field should not fail read compatibility - $scenario") {
      assert(isReadCompatible(withoutExtra, withExtraNullable))
    }
    test(s"adding a non-nullable field should not fail read compatibility - $scenario") {
      assert(isReadCompatible(withoutExtra, withExtraNonNullable))
    }
    test(s"case variation of field name should fail read compatibility - $scenario") {
      assert(!isReadCompatible(withExtraNullable, withExtraMixedCase))
    }
    testNullability(scenario)(b => make(struct => struct.add("extra", StringType, nullable = b)))
    testDatatypeChange(scenario)(datatype => make(struct => struct.add("extra", datatype)))
  }

  // --------------------------------------------------------------------
  // tests for all kinds of places where a field can appear in a struct
  // --------------------------------------------------------------------

  testColumnVariations("top level")(
    f => f(new StructType().add("a", IntegerType)))

  testColumnVariations("nested struct")(
    f => new StructType()
      .add("a", f(new StructType().add("b", IntegerType))))

  testColumnVariations("nested in array")(
    f => new StructType()
      .add("array", ArrayType(
        f(new StructType().add("b", IntegerType)))))

  testColumnVariations("nested in map key")(
    f => new StructType()
      .add("map", MapType(
        f(new StructType().add("b", IntegerType)),
        StringType)))

  testColumnVariations("nested in map value")(
    f => new StructType()
      .add("map", MapType(
        StringType,
        f(new StructType().add("b", IntegerType)))))

  // --------------------------------------------------------------------
  // tests for data type change in places other than struct
  // --------------------------------------------------------------------

  testDatatypeChange("array element")(
    datatype => new StructType()
      .add("array", ArrayType(datatype)))

  testDatatypeChange("map key")(
    datatype => new StructType()
      .add("map", MapType(datatype, StringType)))

  testDatatypeChange("map value")(
    datatype => new StructType()
      .add("map", MapType(StringType, datatype)))

  // --------------------------------------------------------------------
  // tests for nullability change in places other than struct
  // --------------------------------------------------------------------

  testNullability("array contains null")(
    b => new StructType()
      .add("array", ArrayType(StringType, containsNull = b)))

  testNullability("map contains null values")(
    b => new StructType()
      .add("map", MapType(IntegerType, StringType, valueContainsNull = b)))

  testNullability("map nested in array")(
    b => new StructType()
      .add("map", ArrayType(
        MapType(IntegerType, StringType, valueContainsNull = b))))

  testNullability("array nested in map")(
    b => new StructType()
      .add("map", MapType(
        IntegerType,
        ArrayType(StringType, containsNull = b))))

  ////////////////////////////
  // reportDifference
  ////////////////////////////

  /**
   * @param existing the existing schema to compare to
   * @param specified the new specified schema
   * @param expected an expected list of messages, each describing a schema difference.
   *                 Every expected message is actually a regex patterns that is matched
   *                 against all diffs that are returned. This is necessary to tolerate
   *                 variance in ordering of field names, for example in a message such as
   *                 "Specified schema has additional field(s): x, y", we cannot predict
   *                 the order of x and y.
   */
  def testReportDifferences(testName: String)
    (existing: StructType, specified: StructType, expected: String*): Unit = {
    test(testName) {
      val differences = SchemaUtils.reportDifferences(existing, specified)
      // make sure every expected difference is reported
      expected foreach ((exp: String) =>
        assert(differences.exists(message => exp.r.findFirstMatchIn(message).isDefined),
          s"""Difference not reported.
             |Expected:
             |- $exp
             |Reported: ${differences.mkString("\n- ", "\n- ", "")}
            """.stripMargin))
      // make sure there are no extra differences reported
      assert(expected.size == differences.size,
        s"""Too many differences reported.
           |Expected: ${expected.mkString("\n- ", "\n- ", "")}
           |Reported: ${differences.mkString("\n- ", "\n- ", "")}
          """.stripMargin)
    }
  }

  testReportDifferences("extra columns should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", StringType),
    expected = "additional field[(]s[)]: b"
  )

  testReportDifferences("missing columns should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", StringType),
    specified = new StructType()
      .add("a", IntegerType),
    expected = "missing field[(]s[)]: b"
  )

  testReportDifferences("making a column nullable should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", StringType, nullable = true),
    specified = new StructType()
      .add("a", IntegerType, nullable = true)
      .add("b", StringType, nullable = true),
    expected = "a is nullable in specified schema but non-nullable in existing schema"
  )

  testReportDifferences("making a column non-nullable should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", StringType, nullable = true),
    specified = new StructType()
      .add("a", IntegerType, nullable = false)
      .add("b", StringType, nullable = false),
    expected = "b is non-nullable in specified schema but nullable in existing schema"
  )

  testReportDifferences("change in column metadata should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType, nullable = true, new MetadataBuilder().putString("x", "1").build())
      .add("b", StringType),
    specified = new StructType()
      .add("a", IntegerType, nullable = true, new MetadataBuilder().putString("x", "2").build())
      .add("b", StringType),
    expected = "metadata for field a is different"
  )

  testReportDifferences("change in generation expression for generated columns")(
    existing = new StructType()
      .add("a", IntegerType, nullable = true,
        new MetadataBuilder()
          .putString(GENERATION_EXPRESSION_METADATA_KEY, "b + 1")
          .putString("x", "1").build())
      .add("b", StringType),
    specified = new StructType()
      .add("a", IntegerType, nullable = true, new MetadataBuilder()
        .putString(GENERATION_EXPRESSION_METADATA_KEY, "1 + b")
        .putString("x", "1").build())
      .add("b", StringType),
    // Regex flags: DOTALL and MULTILINE
    expected = "(?sm)generation expression for field a is different" +
      // Not include
      "(?!.*metadata for field a is different)"
  )

  testReportDifferences("change in column metadata for generated columns")(
    existing = new StructType()
      .add("a", IntegerType, nullable = true,
        new MetadataBuilder()
          .putString(GENERATION_EXPRESSION_METADATA_KEY, "b + 1")
          .putString("x", "1").build())
      .add("b", StringType),
    specified = new StructType()
      .add("a", IntegerType, nullable = true, new MetadataBuilder()
        .putString(GENERATION_EXPRESSION_METADATA_KEY, "b + 1")
        .putString("x", "2").build())
      .add("b", StringType),
    expected = "metadata for field a is different"
  )

  testReportDifferences("change in generation expression and metadata for generated columns")(
    existing = new StructType()
      .add("a", IntegerType, nullable = true,
        new MetadataBuilder()
          .putString(GENERATION_EXPRESSION_METADATA_KEY, "b + 1")
          .putString("x", "1").build())
      .add("b", StringType),
    specified = new StructType()
      .add("a", IntegerType, nullable = true, new MetadataBuilder()
        .putString(GENERATION_EXPRESSION_METADATA_KEY, "b + 2")
        .putString("x", "2").build())
      .add("b", StringType),
    // Regex flags: DOTALL and MULTILINE
    expected = "(?sm)generation expression for field a is different" +
      ".*metadata for field a is different"
  )

  testReportDifferences("change of column type should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", StringType),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", new ArrayType(
        StringType, containsNull = false)),
    expected = "type for b is different"
  )

  testReportDifferences("change of array nullability should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", new ArrayType(
        new StructType().add("x", LongType), containsNull = true)),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", new ArrayType(
        new StructType().add("x", LongType), containsNull = false)),
    expected = "b\\[\\] can not contain null in specified schema but can in existing"
  )

  testReportDifferences("change of element type should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", new ArrayType(LongType, containsNull = true)),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", new ArrayType(StringType, containsNull = true)),
    expected = "type for b\\[\\] is different"
  )

  testReportDifferences("change of element struct type should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", new ArrayType(
        new StructType()
          .add("x", LongType),
        containsNull = true)),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", new ArrayType(
        new StructType()
          .add("x", StringType),
        containsNull = true)),
    expected = "type for b\\[\\].x is different"
  )

  testReportDifferences("change of map value nullability should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", new MapType(
        StringType,
        new StructType().add("x", LongType), valueContainsNull = true)),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", new MapType(
        StringType,
        new StructType().add("x", LongType), valueContainsNull = false)),
    expected = "b can not contain null values in specified schema but can in existing"
  )

  testReportDifferences("change of map key type should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", new MapType(LongType, StringType, valueContainsNull = true)),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", new MapType(StringType, StringType, valueContainsNull = true)),
    expected = "type for b\\[key\\] is different"
  )

  testReportDifferences("change of value struct type should be reported as a difference")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", new MapType(
        StringType,
        new StructType().add("x", LongType),
        valueContainsNull = true)),
    specified = new StructType()
      .add("a", IntegerType)
      .add("b", new MapType(
        StringType,
        new StructType().add("x", FloatType),
        valueContainsNull = true)),
    expected = "type for b\\[value\\].x is different"
  )

  testReportDifferences("nested extra columns should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", StringType)
        .add("c", LongType)),
    expected = "additional field[(]s[)]: (x.b, x.c|x.c, x.b)"
  )

  testReportDifferences("nested missing columns should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", StringType)
        .add("c", FloatType)),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)),
    expected = "missing field[(]s[)]: (x.b, x.c|x.c, x.b)"
  )

  testReportDifferences("making a nested column nullable should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType, nullable = false)
        .add("b", StringType, nullable = true)),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType, nullable = true)
        .add("b", StringType, nullable = true)),
    expected = "x.a is nullable in specified schema but non-nullable in existing schema"
  )

  testReportDifferences("making a nested column non-nullable should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType, nullable = false)
        .add("b", StringType, nullable = true)),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType, nullable = false)
        .add("b", StringType, nullable = false)),
    expected = "x.b is non-nullable in specified schema but nullable in existing schema"
  )

  testReportDifferences("change in nested column metadata should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType, nullable = true, new MetadataBuilder().putString("x", "1").build())
        .add("b", StringType)),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType, nullable = true, new MetadataBuilder().putString("x", "2").build())
        .add("b", StringType)),
    expected = "metadata for field x.a is different"
  )

  testReportDifferences("change of nested column type should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", StringType)),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", new ArrayType(
          StringType, containsNull = false))),
    expected = "type for x.b is different"
  )

  testReportDifferences("change of nested array nullability should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", new ArrayType(
          new StructType()
            .add("x", LongType),
          containsNull = true))),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", new ArrayType(
          new StructType()
            .add("x", LongType),
          containsNull = false))),
    expected = "x.b\\[\\] can not contain null in specified schema but can in existing"
  )

  testReportDifferences("change of nested element type should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", new ArrayType(LongType, containsNull = true))),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", new ArrayType(StringType, containsNull = true))),
    expected = "type for x.b\\[\\] is different"
  )

  testReportDifferences("change of nested element struct type should be reported as a difference")(
    existing = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", new ArrayType(
          new StructType()
            .add("x", LongType),
          containsNull = true))),
    specified = new StructType()
      .add("x", new StructType()
        .add("a", IntegerType)
        .add("b", new ArrayType(
          new StructType()
            .add("x", StringType),
          containsNull = true))),
    expected = "type for x.b\\[\\].x is different"
  )

  private val piiTrue = new MetadataBuilder().putBoolean("pii", value = true).build()
  private val piiFalse = new MetadataBuilder().putBoolean("pii", value = false).build()

  testReportDifferences("multiple differences should be reported")(
    existing = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
      .add("c", BinaryType)
      .add("f", LongType, nullable = true, piiTrue)
      .add("g", new MapType(
        IntegerType,
        new StructType()
          .add("a", IntegerType, nullable = false, piiFalse)
          .add("b", StringType)
          .add("d", new ArrayType(
            LongType,
            containsNull = false
          )),
        valueContainsNull = true))
      .add("h", new MapType(
        LongType,
        StringType,
        valueContainsNull = true)),
    specified = new StructType()
      .add("a", FloatType)
      .add("d", StringType)
      .add("e", LongType)
      .add("f", LongType, nullable = false, piiFalse)
      .add("g", new MapType(
        StringType,
        new StructType()
          .add("a", LongType, nullable = true)
          .add("c", StringType)
          .add("d", new ArrayType(
            BooleanType,
            containsNull = true
          )),
        valueContainsNull = false))
      .add("h", new MapType(
        LongType,
        new ArrayType(IntegerType, containsNull = false),
        valueContainsNull = true)),
    "type for a is different",
    "additional field[(]s[)]: (d, e|e, d)",
    "missing field[(]s[)]: (b, c|c, b)",
    "f is non-nullable in specified schema but nullable",
    "metadata for field f is different",
    "type for g\\[key\\] is different",
    "g can not contain null values in specified schema but can in existing",
    "additional field[(]s[)]: g\\[value\\].c",
    "missing field[(]s[)]: g\\[value\\].b",
    "type for g\\[value\\].a is different",
    "g\\[value\\].a is nullable in specified schema but non-nullable in existing",
    "metadata for field g\\[value\\].a is different",
    "field g\\[value\\].d\\[\\] can contain null in specified schema but can not in existing",
    "type for g\\[value\\].d\\[\\] is different",
    "type for h\\[value\\] is different"
  )

  ////////////////////////////
  // findColumnPosition
  ////////////////////////////

  test("findColumnPosition") {
    val schema = new StructType()
      .add("struct", new StructType()
        .add("a", IntegerType)
        .add("b", IntegerType))
      .add("array", ArrayType(new StructType()
        .add("c", IntegerType)
        .add("d", IntegerType)))
      .add("field", StringType)
      .add("map", MapType(
        new StructType()
          .add("e", IntegerType),
        new StructType()
          .add("f", IntegerType)))
      .add("mapStruct", MapType(
        IntegerType,
        new StructType()
          .add("g", new StructType()
          .add("h", IntegerType))))
      .add("arrayMap", ArrayType(
        MapType(
          new StructType()
            .add("i", IntegerType),
          new StructType()
            .add("j", IntegerType))))

    val List(structIdx, arrayIdx, fieldIdx, mapIdx, mapStructIdx, arrayMapIdx) = (0 to 5).toList
    val ARRAY_ELEMENT_INDEX = 0
    val MAP_KEY_INDEX = 0
    val MAP_VALUE_INDEX = 1

    def checkPosition(column: Seq[String], position: Seq[Int]): Unit =
      assert(SchemaUtils.findColumnPosition(column, schema) === position)

    checkPosition(Seq("struct"), Seq(structIdx))
    checkPosition(Seq("STRucT"), Seq(structIdx))
    expectFailure("Couldn't find", schema.treeString) {
      SchemaUtils.findColumnPosition(Seq("struct", "array"), schema)
    }
    checkPosition(Seq("struct", "a"), Seq(structIdx, 0))
    checkPosition(Seq("STRucT", "a"), Seq(structIdx, 0))
    checkPosition(Seq("struct", "A"), Seq(structIdx, 0))
    checkPosition(Seq("STRucT", "A"), Seq(structIdx, 0))
    checkPosition(Seq("struct", "b"), Seq(structIdx, 1))
    checkPosition(Seq("array"), Seq(arrayIdx))
    checkPosition(Seq("array", "element", "C"), Seq(arrayIdx, ARRAY_ELEMENT_INDEX, 0))
    checkPosition(Seq("array", "element", "d"), Seq(arrayIdx, ARRAY_ELEMENT_INDEX, 1))
    checkPosition(Seq("field"), Seq(fieldIdx))
    checkPosition(Seq("map"), Seq(mapIdx))
    checkPosition(Seq("map", "key", "e"), Seq(mapIdx, MAP_KEY_INDEX, 0))
    checkPosition(Seq("map", "value", "f"), Seq(mapIdx, MAP_VALUE_INDEX, 0))
    checkPosition(Seq("map", "value", "F"), Seq(mapIdx, MAP_VALUE_INDEX, 0))
    checkPosition(Seq("mapStruct", "key"), Seq(mapStructIdx, MAP_KEY_INDEX))
    checkPosition(Seq("mapStruct", "value", "g"), Seq(mapStructIdx, MAP_VALUE_INDEX, 0))
    checkPosition(Seq("mapStruct", "key"), Seq(mapStructIdx, MAP_KEY_INDEX))
    checkPosition(Seq("mapStruct", "value"), Seq(mapStructIdx, MAP_VALUE_INDEX))
    checkPosition(Seq("arrayMap"), Seq(arrayMapIdx))
    checkPosition(Seq("arrayMap", "element"), Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX))
    checkPosition(
      Seq("arrayMap", "element", "key"),
      Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_KEY_INDEX))
    checkPosition(
      Seq("arrayMap", "element", "value"),
      Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_VALUE_INDEX))
    checkPosition(
      Seq("arrayMap", "element", "key", "i"),
      Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_KEY_INDEX, 0))
    checkPosition(
      Seq("arrayMap", "element", "value", "j"),
      Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_VALUE_INDEX, 0))

    val resolver = org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
    Seq(Seq("STRucT", "b"), Seq("struct", "B"), Seq("array", "element", "C"),
        Seq("map", "key", "E")).foreach { column =>
      expectFailure("Couldn't find", schema.treeString) {
        SchemaUtils.findColumnPosition(column, schema, resolver)
      }
    }
  }

  test("findColumnPosition that doesn't exist") {
    val schema = new StructType()
      .add("a", IntegerType)
      .add("b", MapType(StringType, StringType))
      .add("c", ArrayType(IntegerType))
    expectFailure("Couldn't find", schema.treeString) {
      SchemaUtils.findColumnPosition(Seq("d"), schema)
    }
    expectFailure("A MapType was found", "mapType", schema.treeString) {
      SchemaUtils.findColumnPosition(Seq("b", "c"), schema)
    }
    expectFailure("An ArrayType was found", "arrayType", schema.treeString) {
      SchemaUtils.findColumnPosition(Seq("c", "b"), schema)
    }
  }

  ////////////////////////////
  // getNestedFieldFromPosition
  ////////////////////////////

  test("getNestedFieldFromPosition") {
    val a = StructField("a", IntegerType)
    val b = StructField("b", IntegerType)
    val c = StructField("c", IntegerType)
    val d = StructField("d", IntegerType)
    val e = StructField("e", IntegerType)
    val f = StructField("f", IntegerType)
    val g = StructField("g", IntegerType)

    val field = StructField("field", StringType)
    val struct = StructField("struct", new StructType().add(a).add(b))
    val arrayElement = StructField("element", new StructType().add(c))
    val array = StructField("array", ArrayType(arrayElement.dataType))
    val mapKey = StructField("key", new StructType().add(d))
    val mapValue = StructField("value", new StructType().add(e))
    val map = StructField("map", MapType(
      keyType = mapKey.dataType,
      valueType = mapValue.dataType))
    val arrayMapKey = StructField("key", new StructType().add(f))
    val arrayMapValue = StructField("value", new StructType().add(g))
    val arrayMapElement = StructField("element", MapType(
      keyType = arrayMapKey.dataType,
      valueType = arrayMapValue.dataType))
    val arrayMap = StructField("arrayMap", ArrayType(arrayMapElement.dataType))

    val root = StructField("root", StructType(Seq(field, struct, array, map, arrayMap)))

    val List(fieldIdx, structIdx, arrayIdx, mapIdx, arrayMapIdx) = (0 to 4).toList
    val ARRAY_ELEMENT_INDEX = 0
    val MAP_KEY_INDEX = 0
    val MAP_VALUE_INDEX = 1

    def checkField(position: Seq[Int], expected: StructField): Unit =
      assert(getNestedFieldFromPosition(root, position) === expected)

    checkField(Seq.empty, root)
    checkField(Seq(fieldIdx), field)
    checkField(Seq(structIdx), struct)
    checkField(Seq(structIdx, 0), a)
    checkField(Seq(structIdx, 1), b)
    checkField(Seq(arrayIdx), array)
    checkField(Seq(arrayIdx, ARRAY_ELEMENT_INDEX), arrayElement)
    checkField(Seq(arrayIdx, ARRAY_ELEMENT_INDEX, 0), c)
    checkField(Seq(mapIdx), map)
    checkField(Seq(mapIdx, MAP_KEY_INDEX), mapKey)
    checkField(Seq(mapIdx, MAP_VALUE_INDEX), mapValue)
    checkField(Seq(mapIdx, MAP_KEY_INDEX, 0), d)
    checkField(Seq(mapIdx, MAP_VALUE_INDEX, 0), e)
    checkField(Seq(arrayMapIdx), arrayMap)
    checkField(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX), arrayMapElement)
    checkField(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_KEY_INDEX), arrayMapKey)
    checkField(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_VALUE_INDEX), arrayMapValue)
    checkField(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_KEY_INDEX, 0), f)
    checkField(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_VALUE_INDEX, 0), g)

    def checkError(position: Seq[Int]): Unit =
      assertThrows[IllegalArgumentException] {
        getNestedFieldFromPosition(root, position)
      }

    checkError(Seq(-1))
    checkError(Seq(fieldIdx, 0))
    checkError(Seq(structIdx, -1))
    checkError(Seq(structIdx, 2))
    checkError(Seq(arrayIdx, ARRAY_ELEMENT_INDEX - 1))
    checkError(Seq(arrayIdx, ARRAY_ELEMENT_INDEX + 1))
    checkError(Seq(mapIdx, MAP_KEY_INDEX - 1))
    checkError(Seq(mapIdx, MAP_VALUE_INDEX + 1))
    checkError(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX - 1))
    checkError(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX + 1))
    checkError(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_KEY_INDEX - 1))
    checkError(Seq(arrayMapIdx, ARRAY_ELEMENT_INDEX, MAP_VALUE_INDEX + 1))
    checkError(Seq(arrayMapIdx + 1))
  }

  test("getNestedTypeFromPosition") {
    val schema = new StructType().add("a", IntegerType)
    assert(getNestedTypeFromPosition(schema, Seq.empty) === schema)
    assert(getNestedTypeFromPosition(schema, Seq(0)) === IntegerType)
    assertThrows[IllegalArgumentException] {
      getNestedTypeFromPosition(schema, Seq(-1))
    }
    assertThrows[IllegalArgumentException] {
      getNestedTypeFromPosition(schema, Seq(1))
    }
  }

  ////////////////////////////
  // addColumn
  ////////////////////////////

  test("addColumn - simple") {
    val a = StructField("a", IntegerType)
    val b = StructField("b", StringType)
    val schema = new StructType().add(a).add(b)

    val x = StructField("x", LongType)
    assert(SchemaUtils.addColumn(schema, x, Seq(0)) === new StructType().add(x).add(a).add(b))
    assert(SchemaUtils.addColumn(schema, x, Seq(1)) === new StructType().add(a).add(x).add(b))
    assert(SchemaUtils.addColumn(schema, x, Seq(2)) === new StructType().add(a).add(b).add(x))

    expectFailure("Index -1", "lower than 0") {
      SchemaUtils.addColumn(schema, x, Seq(-1))
    }
    expectFailure("Index 3", "larger than struct length: 2") {
      SchemaUtils.addColumn(schema, x, Seq(3))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, 0))
    }
  }

  test("addColumn - nested struct") {
    val a = StructField("a", IntegerType)
    val b = StructField("b", StringType)
    val first = StructField("first", new StructType().add(a).add(b))
    val middle = StructField("middle", new StructType().add(a).add(b))
    val last = StructField("last", new StructType().add(a).add(b))
    val schema = new StructType().add(first).add(middle).add(last)

    val x = StructField("x", LongType)
    assert(SchemaUtils.addColumn(schema, x, Seq(0)) ===
      new StructType().add(x).add(first).add(middle).add(last))
    assert(SchemaUtils.addColumn(schema, x, Seq(1)) ===
      new StructType().add(first).add(x).add(middle).add(last))
    assert(SchemaUtils.addColumn(schema, x, Seq(2)) ===
      new StructType().add(first).add(middle).add(x).add(last))
    assert(SchemaUtils.addColumn(schema, x, Seq(3)) ===
      new StructType().add(first).add(middle).add(last).add(x))

    assert(SchemaUtils.addColumn(schema, x, Seq(0, 2)) ===
      new StructType().add("first", new StructType().add(a).add(b).add(x)).add(middle).add(last))
    assert(SchemaUtils.addColumn(schema, x, Seq(0, 1)) ===
      new StructType().add("first", new StructType().add(a).add(x).add(b)).add(middle).add(last))
    assert(SchemaUtils.addColumn(schema, x, Seq(0, 0)) ===
      new StructType().add("first", new StructType().add(x).add(a).add(b)).add(middle).add(last))
    assert(SchemaUtils.addColumn(schema, x, Seq(1, 0)) ===
      new StructType().add(first).add("middle", new StructType().add(x).add(a).add(b)).add(last))
    assert(SchemaUtils.addColumn(schema, x, Seq(2, 0)) ===
      new StructType().add(first).add(middle).add("last", new StructType().add(x).add(a).add(b)))

    expectFailure("Index -1", "lower than 0") {
      SchemaUtils.addColumn(schema, x, Seq(0, -1))
    }
    expectFailure("Index 3", "larger than struct length: 2") {
      SchemaUtils.addColumn(schema, x, Seq(0, 3))
    }
    expectFailure("Struct not found at position 2") {
      SchemaUtils.addColumn(schema, x, Seq(0, 2, 0))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, 0, 0))
    }
  }

  test("addColumn - nested map") {
    val k = StructField("k", IntegerType)
    val v = StructField("v", StringType)
    val schema = new StructType().add("m", MapType(
      keyType = new StructType().add(k),
      valueType = new StructType().add(v)))

    val MAP_KEY_INDEX = 0
    val MAP_VALUE_INDEX = 1

    val x = StructField("x", LongType)
    assert(SchemaUtils.addColumn(schema, x, Seq(0, MAP_KEY_INDEX, 0)) ===
      new StructType().add("m", MapType(
        keyType = new StructType().add(x).add(k),
        valueType = new StructType().add(v))))

    assert(SchemaUtils.addColumn(schema, x, Seq(0, MAP_KEY_INDEX, 1)) ===
      new StructType().add("m", MapType(
        keyType = new StructType().add(k).add(x),
        valueType = new StructType().add(v))))

    assert(SchemaUtils.addColumn(schema, x, Seq(0, MAP_VALUE_INDEX, 0)) ===
      new StructType().add("m", MapType(
        keyType = new StructType().add(k),
        valueType = new StructType().add(x).add(v))))

    assert(SchemaUtils.addColumn(schema, x, Seq(0, MAP_VALUE_INDEX, 1)) ===
      new StructType().add("m", MapType(
        keyType = new StructType().add(k),
        valueType = new StructType().add(v).add(x))))

    // Adding to map key/value.
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, MAP_KEY_INDEX))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, MAP_VALUE_INDEX))
    }
    // Invalid map access.
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, MAP_KEY_INDEX - 1, 0))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, MAP_VALUE_INDEX + 1, 0))
    }
  }

  test("addColumn - nested maps") {
    // Helper method to create a 2-level deep nested map of structs. The tests below each cover
    // adding a field to one of the leaf struct.
    def schema(
        kk: StructType = new StructType().add("kk", IntegerType),
        kv: StructType = new StructType().add("kv", IntegerType),
        vk: StructType = new StructType().add("vk", IntegerType),
        vv: StructType = new StructType().add("vv", IntegerType))
      : StructType = new StructType().add("m", MapType(
        keyType = MapType(
          keyType = kk,
          valueType = kv),
        valueType = MapType(
          keyType = vk,
          valueType = vv)))

    val MAP_KEY_INDEX = 0
    val MAP_VALUE_INDEX = 1

    val x = StructField("x", LongType)
    // Add field `x` at the front of each leaf struct.
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX, 0)) ===
      schema(kk = new StructType().add(x).add("kk", IntegerType)))
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_VALUE_INDEX, MAP_KEY_INDEX, 0)) ===
      schema(vk = new StructType().add(x).add("vk", IntegerType)))
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX, 0)) ===
      schema(kv = new StructType().add(x).add("kv", IntegerType)))
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_VALUE_INDEX, MAP_VALUE_INDEX, 0)) ===
      schema(vv = new StructType().add(x).add("vv", IntegerType)))

    // Add field `x` at the back of each leaf struct.
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX, 1)) ===
      schema(kk = new StructType().add("kk", IntegerType).add(x)))
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_VALUE_INDEX, MAP_KEY_INDEX, 1)) ===
      schema(vk = new StructType().add("vk", IntegerType).add(x)))
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX, 1)) ===
      schema(kv = new StructType().add("kv", IntegerType).add(x)))
    assert(SchemaUtils.addColumn(schema(), x, Seq(0, MAP_VALUE_INDEX, MAP_VALUE_INDEX, 1)) ===
      schema(vv = new StructType().add("vv", IntegerType).add(x)))

    // Adding to map key/value.
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX))
    }
    // Invalid map access.
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX - 1, 0))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX - 1, MAP_KEY_INDEX, 0))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema(), x, Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX + 1, 0))
    }
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema(), x, Seq(0, MAP_VALUE_INDEX + 1, MAP_KEY_INDEX, 0))
    }
  }

  test("addColumn - nested array") {
    val e = StructField("e", IntegerType)
    val schema = new StructType().add("a", ArrayType(new StructType().add(e)))
    val x = StructField("x", LongType)

    val ARRAY_ELEMENT_INDEX = 0

    // Add field `x` at the front of the leaf struct.
    assert(SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX, 0)) ===
      new StructType().add("a", ArrayType(new StructType().add(x).add(e))))
    // Add field `x` at the back of the leaf struct.
    assert(SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX, 1)) ===
      new StructType().add("a", ArrayType(new StructType().add(e).add(x))))

    // Adding to array element.
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX))
    }
    // Invalid array access.
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX - 1, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX + 1, 0))
    }
  }

  test("addColumn - nested arrays") {
    val e = StructField("e", IntegerType)
    val schema = new StructType().add("a", ArrayType(ArrayType(new StructType().add(e))))
    val x = StructField("x", LongType)

    val ARRAY_ELEMENT_INDEX = 0

    // Add field `x` at the front of the leaf struct.
    assert(SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX, 0)) ===
      new StructType().add("a", ArrayType(ArrayType(new StructType().add(x).add(e)))))
    // Add field `x` at the back of the leaf struct.
    assert(SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX, 1)) ===
      new StructType().add("a", ArrayType(ArrayType(new StructType().add(e).add(x)))))

    // Adding to array element.
    expectFailure("parent is not a structtype") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX))
    }
    // Invalid array access.
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX - 1, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX - 1, ARRAY_ELEMENT_INDEX, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX + 1, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.addColumn(schema, x, Seq(0, ARRAY_ELEMENT_INDEX + 1, ARRAY_ELEMENT_INDEX, 0))
    }
  }

  ////////////////////////////
  // dropColumn
  ////////////////////////////

  test("dropColumn - simple") {
    val a = StructField("a", IntegerType)
    val b = StructField("b", StringType)
    val schema = new StructType().add(a).add(b)

    assert(SchemaUtils.dropColumn(schema, Seq(0)) === ((new StructType().add(b), a)))
    assert(SchemaUtils.dropColumn(schema, Seq(1)) === ((new StructType().add(a), b)))

    expectFailure("Index -1", "lower than 0") {
      SchemaUtils.dropColumn(schema, Seq(-1))
    }
    expectFailure("Index 2", "equals to or is larger than struct length: 2") {
      SchemaUtils.dropColumn(schema, Seq(2))
    }
    expectFailure("Can only drop nested columns from StructType") {
      SchemaUtils.dropColumn(schema, Seq(0, 0))
    }
  }

  test("dropColumn - nested struct") {
    val a = StructField("a", IntegerType)
    val b = StructField("b", StringType)
    val c = StructField("c", StringType)
    val first = StructField("first", new StructType().add(a).add(b).add(c))
    val middle = StructField("middle", new StructType().add(a).add(b).add(c))
    val last = StructField("last", new StructType().add(a).add(b).add(c))
    val schema = new StructType().add(first).add(middle).add(last)

    assert(SchemaUtils.dropColumn(schema, Seq(0)) ===
      new StructType().add(middle).add(last) -> first)
    assert(SchemaUtils.dropColumn(schema, Seq(1)) ===
      new StructType().add(first).add(last) -> middle)
    assert(SchemaUtils.dropColumn(schema, Seq(2)) ===
      new StructType().add(first).add(middle) -> last)

    assert(SchemaUtils.dropColumn(schema, Seq(0, 2)) ===
      new StructType().add("first", new StructType().add(a).add(b)).add(middle).add(last) -> c)
    assert(SchemaUtils.dropColumn(schema, Seq(0, 1)) ===
      new StructType().add("first", new StructType().add(a).add(c)).add(middle).add(last) -> b)
    assert(SchemaUtils.dropColumn(schema, Seq(0, 0)) ===
      new StructType().add("first", new StructType().add(b).add(c)).add(middle).add(last) -> a)
    assert(SchemaUtils.dropColumn(schema, Seq(1, 0)) ===
      new StructType().add(first).add("middle", new StructType().add(b).add(c)).add(last) -> a)
    assert(SchemaUtils.dropColumn(schema, Seq(2, 0)) ===
      new StructType().add(first).add(middle).add("last", new StructType().add(b).add(c)) -> a)

    expectFailure("Index -1", "lower than 0") {
      SchemaUtils.dropColumn(schema, Seq(0, -1))
    }
    expectFailure("Index 3", "equals to or is larger than struct length: 3") {
      SchemaUtils.dropColumn(schema, Seq(0, 3))
    }
    expectFailure("Can only drop nested columns from StructType") {
      SchemaUtils.dropColumn(schema, Seq(0, 0, 0))
    }
  }

  test("dropColumn - nested map") {
    val a = StructField("a", IntegerType)
    val b = StructField("b", StringType)
    val c = StructField("c", LongType)
    val d = StructField("d", DateType)
    val schema = new StructType().add("m", MapType(
      keyType = new StructType().add(a).add(b),
      valueType = new StructType().add(c).add(d)))

    val MAP_KEY_INDEX = 0
    val MAP_VALUE_INDEX = 1

    assert(SchemaUtils.dropColumn(schema, Seq(0, MAP_KEY_INDEX, 0)) ===
      (new StructType().add("m", MapType(
        keyType = new StructType().add(b),
        valueType = new StructType().add(c).add(d))),
      a))

    assert(SchemaUtils.dropColumn(schema, Seq(0, MAP_KEY_INDEX, 1)) ===
      (new StructType().add("m", MapType(
        keyType = new StructType().add(a),
        valueType = new StructType().add(c).add(d))),
      b))

    assert(SchemaUtils.dropColumn(schema, Seq(0, MAP_VALUE_INDEX, 0)) ===
      (new StructType().add("m", MapType(
        keyType = new StructType().add(a).add(b),
        valueType = new StructType().add(d))),
      c))

    assert(SchemaUtils.dropColumn(schema, Seq(0, MAP_VALUE_INDEX, 1)) ===
      (new StructType().add("m", MapType(
        keyType = new StructType().add(a).add(b),
        valueType = new StructType().add(c))),
      d))

    // Dropping map key/value.
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema, Seq(0, MAP_KEY_INDEX))
    }
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema, Seq(0, MAP_VALUE_INDEX))
    }
    // Invalid map access.
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema, Seq(0, MAP_KEY_INDEX - 1, 0))
    }
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema, Seq(0, MAP_VALUE_INDEX + 1, 0))
    }
  }

  test("dropColumn - nested maps") {
    // Helper method to create a 2-level deep nested map of structs. The tests below each cover
    // dropping a field to one of the leaf struct. Each test adds an extra field `a` at a specific
    // position then drops it to end up with the default schema returned by `schema()`
    def schema(
        kk: StructType = new StructType().add("kk", IntegerType),
        kv: StructType = new StructType().add("kv", IntegerType),
        vk: StructType = new StructType().add("vk", IntegerType),
        vv: StructType = new StructType().add("vv", IntegerType))
      : StructType = new StructType().add("m", MapType(
        keyType = MapType(
          keyType = kk,
          valueType = kv),
        valueType = MapType(
          keyType = vk,
          valueType = vv)))

    val a = StructField("a", LongType)

    val MAP_KEY_INDEX = 0
    val MAP_VALUE_INDEX = 1

    def checkDrop(initialSchema: StructType, position: Seq[Int]): Unit =
      assert(SchemaUtils.dropColumn(initialSchema, position) === (schema(), a))
    // Drop field `a` from the front of each leaf struct.
    checkDrop(
      initialSchema = schema(kk = new StructType().add(a).add("kk", IntegerType)),
      position = Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX, 0))

    checkDrop(
      initialSchema = schema(kv = new StructType().add(a).add("kv", IntegerType)),
      position = Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX, 0))

    checkDrop(
      initialSchema = schema(vk = new StructType().add(a).add("vk", IntegerType)),
      position = Seq(0, MAP_VALUE_INDEX, MAP_KEY_INDEX, 0))

    checkDrop(
      initialSchema = schema(vv = new StructType().add(a).add("vv", IntegerType)),
      position = Seq(0, MAP_VALUE_INDEX, MAP_VALUE_INDEX, 0))

    // Drop field `a` from the back of each leaf struct.
    checkDrop(
      initialSchema = schema(kk = new StructType().add("kk", IntegerType).add(a)),
      position = Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX, 1))

    checkDrop(
      initialSchema = schema(kv = new StructType().add("kv", IntegerType).add(a)),
      position = Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX, 1))

    checkDrop(
      initialSchema = schema(vk = new StructType().add("vk", IntegerType).add(a)),
      position = Seq(0, MAP_VALUE_INDEX, MAP_KEY_INDEX, 1))

    checkDrop(
      initialSchema = schema(vv = new StructType().add("vv", IntegerType).add(a)),
      position = Seq(0, MAP_VALUE_INDEX, MAP_VALUE_INDEX, 1))

    // Dropping map key/value.
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema(), Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX))
    }
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema(), Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX))
    }
    // Invalid map access.
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema(), Seq(0, MAP_KEY_INDEX, MAP_KEY_INDEX - 1, 0))
    }
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema(), Seq(0, MAP_KEY_INDEX - 1, MAP_KEY_INDEX, 0))
    }
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema(), Seq(0, MAP_KEY_INDEX, MAP_VALUE_INDEX + 1, 0))
    }
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema(), Seq(0, MAP_VALUE_INDEX + 1, MAP_KEY_INDEX, 0))
    }
  }

  test("dropColumn - nested array") {
    val e = StructField("e", IntegerType)
    val f = StructField("f", IntegerType)
    val schema = new StructType().add("a", ArrayType(new StructType().add(e).add(f)))

    val ARRAY_ELEMENT_INDEX = 0

    // Drop field from the front of the leaf struct.
    assert(SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX, 0)) ===
      (new StructType().add("a", ArrayType(new StructType().add(f))), e))
    // Drop field from the back of the leaf struct.
    assert(SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX, 1)) ===
      (new StructType().add("a", ArrayType(new StructType().add(e))), f))

    // Dropping array element.
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX))
    }
    // Invalid array access.
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX - 1, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX + 1, 0))
    }
  }

  test("dropColumn - nested arrays") {
    val e = StructField("e", IntegerType)
    val f = StructField("f", IntegerType)
    val schema = new StructType().add("a", ArrayType(ArrayType(new StructType().add(e).add(f))))

    val ARRAY_ELEMENT_INDEX = 0

    // Drop field `x` from the front of the leaf struct.
    assert(SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX, 0)) ===
      (new StructType().add("a", ArrayType(ArrayType(new StructType().add(f)))), e))
    // Drop field `x` from the back of the leaf struct.
    assert(SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX, 1)) ===
      (new StructType().add("a", ArrayType(ArrayType(new StructType().add(e)))), f))

    // Dropping array element.
    expectFailure("can only drop nested columns from structtype") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX))
    }
    // Invalid array access.
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX - 1, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX - 1, ARRAY_ELEMENT_INDEX, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX, ARRAY_ELEMENT_INDEX + 1, 0))
    }
    expectFailure("Incorrectly accessing an ArrayType") {
      SchemaUtils.dropColumn(schema, Seq(0, ARRAY_ELEMENT_INDEX + 1, ARRAY_ELEMENT_INDEX, 0))
    }
  }

  /////////////////////////////////
  // normalizeColumnNamesInDataType
  /////////////////////////////////

  private def checkNormalizedColumnNamesInDataType(
      sourceDataType: DataType,
      tableDataType: DataType,
      expectedDataType: DataType): Unit = {
    assert(normalizeColumnNamesInDataType(
      deltaLog = null,
      sourceDataType,
      tableDataType,
      sourceParentFields = Seq.empty,
      tableSchema = new StructType()) == expectedDataType)
  }

  test("normalize column names in data type - atomic types") {
    val source = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
    val table = new StructType()
      .add("B", StringType)
      .add("A", IntegerType)
    val expected = new StructType()
      .add("A", IntegerType)
      .add("B", StringType)
    checkNormalizedColumnNamesInDataType(source, table, expected)
  }

  test("normalize column names in data type - incompatible atomic types") {
    val source = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
    val table = new StructType()
      .add("B", StringType)
      .add("A", StringType) // StringType != IntegerType
    val exception = intercept[AssertionError] {
      normalizeColumnNamesInDataType(
        deltaLog = null,
        source,
        table,
        sourceParentFields = Seq.empty,
        tableSchema = new StructType())
    }
    assert(exception.getMessage.contains("Types without nesting should match"))
  }

  test("normalize column names in data type - different integral types") {
    val source = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
    val table = new StructType()
      .add("B", StringType)
      .add("A", LongType) // LongType != IntegerType
    val expected = new StructType()
      .add("A", IntegerType)
      .add("B", StringType)
    checkNormalizedColumnNamesInDataType(source, table, expected)
  }

  test("normalize column names in data type - nested structs") {
    val source = new StructType()
      .add("a1", IntegerType)
      .add("a2", new StructType()
        .add("b1", IntegerType)
        .add("b2", new StructType()
          .add("c1", IntegerType)
          .add("c2", LongType)
        )
        .add("b3", LongType)
      )
      .add("a3", new StructType()
        .add("d1", IntegerType)
        .add("d2", LongType)
      )
    val table = new StructType()
      .add("A3", new StructType()
        .add("D2", LongType)
        .add("D3x", StringType)
        .add("D1", IntegerType)
      )
      .add("A2", new StructType()
        .add("B3", LongType)
        .add("B4x", IntegerType)
        .add("B1", IntegerType)
        .add("B2", new StructType()
          .add("C3", LongType)
          .add("C2", LongType)
          .add("C1", IntegerType)
        )
      )
      .add("A4x", StringType)
      .add("A1", IntegerType)
    val expected = new StructType()
      .add("A1", IntegerType)
      .add("A2", new StructType()
        .add("B1", IntegerType)
        .add("B2", new StructType()
          .add("C1", IntegerType)
          .add("C2", LongType))
        .add("B3", LongType)
      )
      .add("A3", new StructType()
        .add("D1", IntegerType)
        .add("D2", LongType)
      )
    checkNormalizedColumnNamesInDataType(source, table, expected)
  }

  test("normalize column names in data type - incompatible types in a struct") {
    val source = new StructType()
      .add("a", new StructType()
      .add("b", new StructType()
      .add("c", MapType(StringType, IntegerType))))
    val table = new StructType()
      .add("A", new StructType()
      .add("B", new StructType()
      .add("C", MapType(IntegerType, StringType))))
    val expected = new StructType()
      .add("A", new StructType()
      .add("B", new StructType()
      .add("C", MapType(StringType, IntegerType))))
    assertThrows[AssertionError] {
      checkNormalizedColumnNamesInDataType(source, table, expected)
    }
  }

  test("normalize column names in data type - arrays, maps, structs") {
    val source = MapType(
      new StructType()
        .add("aa", IntegerType)
        .add("bb", StringType),
      ArrayType(new StructType()
        .add("aa", IntegerType)
        .add("bb", StringType)))
    val table = MapType(
      new StructType()
        .add("aA", IntegerType)
        .add("bB", StringType),
      ArrayType(new StructType()
        .add("Cc", IntegerType)
        .add("Aa", IntegerType)
        .add("Bb", StringType)))
    val expected = MapType(
      new StructType()
        .add("aA", IntegerType)
        .add("bB", StringType),
      ArrayType(new StructType()
        .add("Aa", IntegerType)
        .add("Bb", StringType)))
    checkNormalizedColumnNamesInDataType(source, table, expected)
  }

  test("normalize column names in data type - missing column") {
    val source = ArrayType(
      new StructType()
        .add("aa", IntegerType)
        .add("bb", StringType)
    )
    val target = ArrayType(
      new StructType()
        .add("AA", IntegerType)
        .add("CC", StringType) // "bb" != "CC"
    )
    val exception = intercept[DeltaAnalysisException] {
      normalizeColumnNamesInDataType(deltaLog = null, source, target,
        Seq("x", "Y"), new StructType())
    }
    checkError(
      exception = exception,
      errorClass = "DELTA_CANNOT_RESOLVE_COLUMN",
      sqlState = "42703",
      parameters = Map("columnName" -> "x.Y.bb", "schema" -> "root\n")
    )
  }

  test("normalize column names in data type - preserve nullability and comments") {
    val source = new StructType()
      .add("a1", IntegerType, nullable = true)
      .add("a2", new StructType()
        .add("b1", IntegerType, nullable = false)
        .add("b2", ArrayType(IntegerType, containsNull = true),
          nullable = true, comment = "comment for b2")
        .add("b3", MapType(IntegerType, StringType, valueContainsNull = false),
          nullable = true, comment = "comment for b3"),
        nullable = false, comment = "comment for a2"
      )
    val table = new StructType()
      .add("A1", IntegerType, nullable = false, "comment for A1")
      .add("A2", new StructType()
        .add("B1", IntegerType, nullable = true)
        .add("B2", ArrayType(IntegerType, containsNull = false),
          nullable = false, comment = "comment for B2")
        .add("B3", MapType(IntegerType, StringType, valueContainsNull = true),
          nullable = false, comment = "comment for B3"),
        nullable = true
      )
    val expected = new StructType()
      .add("A1", IntegerType, nullable = true)
      .add("A2", new StructType()
        .add("B1", IntegerType, nullable = false)
        .add("B2", ArrayType(IntegerType, containsNull = true),
          nullable = true, comment = "comment for b2")
        .add("B3", MapType(IntegerType, StringType, valueContainsNull = false),
          nullable = true, comment = "comment for b3"),
        nullable = false, comment = "comment for a2"
      )
    checkNormalizedColumnNamesInDataType(source, table, expected)
  }

  test("normalize column names in data type - empty source struct") {
    val source = new StructType()
    val table = new StructType().add("a", IntegerType)
    val expected = new StructType()
    checkNormalizedColumnNamesInDataType(source, table, expected)
  }

  ////////////////////////////
  // normalizeColumnNames
  ////////////////////////////

  /**
   * SchemaUtils.normalizeColumnNames() introduces a Project operator where for each of the
   * top-level columns:
   * - If a top-level field name differs from the table schema, we correct it using an Alias.
   * - If a nested field name differs from the table schema, we correct it using a Cast.
   * This function verifies that the Casts are only introduced for the correct subset of top-level
   * columns.
   */
  private def verifyColumnsWithCasts(df: DataFrame, columnsWithCasts: Seq[String]): Unit = {
    @tailrec def isCast(expression: Expression): Boolean = expression match {
      case _: Cast => true
      case Alias(child, _) => isCast(child)
      case _ => false
    }

    val plan = df.queryExecution.analyzed
    val projections = plan.asInstanceOf[Project].projectList
    for (projection <- projections) {
      val expectedIsCast = columnsWithCasts.contains(projection.name)
      val actualIsCast = isCast(projection)
      assert(expectedIsCast === actualIsCast, s"Verifying cast for ${projection.name}")
    }
  }

  test("normalize column names - different top-level ordering") {
    val df = Seq((1, 2, 3)).toDF("def", "gHi", "abC")
    val tableSchema = new StructType()
      .add("abc", IntegerType)
      .add("Def", IntegerType)
      .add("ghi", IntegerType)
      // Add an extra column to the table schema to make sure it is not added, and does not cause
      // an error.
      .add("jkl", StringType)
    val expectedSchema = new StructType()
      .add("Def", IntegerType, false)
      .add("ghi", IntegerType, false)
      .add("abc", IntegerType, false)
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq.empty)
    assert(normalized.schema == expectedSchema)
  }

  test("normalize column names - dots in the name") {
    val df = spark.read.json(Seq("""{"a.b":1,"c.d":{"x.y":2, "y.z":1}}""").toDS())
    val tableSchema = new StructType()
      .add("c.D", new StructType()
        .add("y.z", LongType)
        .add("x.Y", LongType)
      )
      .add("a.B", LongType)
    val expectedSchema = new StructType()
      .add("a.B", LongType, nullable = true)
      .add("c.D", new StructType()
        .add("x.Y", LongType, nullable = true)
        .add("y.z", LongType, nullable = true),
        nullable = true
      )
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq("c.D"))
    assert(normalized.schema === expectedSchema)
  }

  test("normalize column names - different case in struct") {
    // JSON schema inference does not preserve the order of columns, so we need an explicit schema.
    val jsonSchema = new StructType()
      .add("b", new StructType()
        .add("x", LongType)
        .add("y", new StructType()
          .add("T", LongType)
          .add("s", LongType)
        )
      )
      .add("a", LongType)
    val df = spark.read.schema(jsonSchema)
      .json(Seq("""{"b":{"x":1,"y":{"T":2, "s":1}}, "a":1}""").toDS())
    val tableSchema = new StructType()
      .add("a", LongType)
      .add("b", new StructType()
        .add("x", LongType)
        .add("y", new StructType()
          .add("s", LongType)
          .add("t", LongType)
        )
      )
    val expectedSchema = new StructType()
      .add("b", new StructType()
        .add("x", LongType)
        .add("y", new StructType()
          .add("t", LongType)
          .add("s", LongType)
        )
      )
      .add("a", LongType)
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq("b"))
    assert(normalized.schema === expectedSchema)
  }

  test("normalize column names - different case in array") {
    val df = spark.read.json(Seq("""{"X":1,"y":[{"Z": "alpha"},{"Z":"beta"}]}""").toDS())
    val tableSchema = new StructType()
      .add("x", LongType)
      .add("y", ArrayType(new StructType().add("z", StringType)))
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq("y"))
    assert(normalized.schema == tableSchema)
  }

  test("normalize column names - different case in map") {
    val sourceMapType = MapType(StringType, new StructType().add("Z", StringType))
    val df = spark.range(1).toDF("X")
      .withColumn("y", lit(null).cast(sourceMapType))
      .select(col("y"), col("X"))
    val tableSchema = new StructType()
      .add("x", LongType)
      .add("y", MapType(StringType, new StructType()
        .add("z", StringType)
        // Add an extra nested column to the table schema to make sure it is not added.
        .add("v", IntegerType)))
    val expectedSchema = new StructType()
      .add("y", MapType(StringType, new StructType().add("z", StringType)))
      .add("x", LongType, nullable = false)
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq("y"))
    assert(normalized.schema === expectedSchema)
  }

  test("normalize column names - maintain nested column order") {
    val sourceStructColumnNames =
      Seq("the", "quick", "brown", "fox", "jumps", "over", "them", "lazy", "dog")
    val sourceStructType = new StructType(
      sourceStructColumnNames.map(StructField(_, IntegerType)).toArray)

    // Nested columns in the table are all name in upper case, and listed in reverse order.
    val tableStructColumnNames = Seq("LOOK") ++
      sourceStructColumnNames.reverse.map(_.toUpperCase(Locale.ROOT))
    val tableSchema = new StructType()
      .add("s", new StructType(
        tableStructColumnNames.map(StructField(_, IntegerType)).toArray))

    // We expect the columns to maintain the same order as the source.
    val expectedStructColumnNames = sourceStructColumnNames.map(_.toUpperCase(Locale.ROOT))
    val expectedSchema = new StructType()
      .add("s", new StructType(
        expectedStructColumnNames.map(StructField(_, IntegerType)).toArray))

    val df = spark.range(1).toDF("id")
      .select(lit(null).cast(sourceStructType).as("s"))
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq("s"))
    assert(normalized.schema === expectedSchema)
  }

  test("normalize column names - only top-level names of complex columns differ") {
    val structType = new StructType()
      .add("a", IntegerType)
      .add("b", IntegerType)
    val mapType = MapType(structType, structType)
    val arrayType = ArrayType(structType)

    val df = spark.range(1).toDF("id")
      .select(lit(null).cast(structType).as("x"),
        lit(null).cast(mapType).as("y"),
        lit(null).cast(arrayType).as("z"))
    val tableSchema = new StructType()
      .add("X", structType)
      .add("Y", mapType)
      .add("Z", arrayType)
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    // If only top-level names differ, there is no need to cast complex types.
    verifyColumnsWithCasts(normalized, Seq.empty)
    assert(normalized.schema === tableSchema)
  }

  test("normalize column names - unmatched top-level column") {
    val df = spark.range(1).toDF("id")
      .select(lit(1L).as("one"), lit(2L).as("two"))
    val tableSchema = new StructType()
      .add("ONE", LongType)
      .add("THREE", LongType)
    val exception = intercept[DeltaAnalysisException] {
      normalizeColumnNames(
        deltaLog = null,
        tableSchema,
        df
      )
    }
    checkError(
      exception = exception,
      errorClass = "DELTA_CANNOT_RESOLVE_COLUMN",
      sqlState = "42703",
      parameters = Map("columnName" -> "two", "schema" -> tableSchema.treeString)
    )
  }

  test("normalize column names - unmatched nested column") {
    val sourceStructType = new StructType()
      .add("one", LongType)
      .add("two", LongType)
    val df = spark.range(1).toDF("id")
      .select(lit(null).cast(sourceStructType).as("s"))
    val tableSchema = new StructType()
      .add("S", new StructType()
        .add("ONE", LongType)
        .add("THREE", LongType)
      )
    val exception = intercept[DeltaAnalysisException] {
      normalizeColumnNames(
        deltaLog = null,
        tableSchema,
        df
      )
    }
    checkError(
      exception = exception,
      errorClass = "DELTA_CANNOT_RESOLVE_COLUMN",
      sqlState = "42703",
      parameters = Map("columnName" -> "s.two", "schema" -> tableSchema.treeString)
    )
  }

  test("normalize column names - deeply nested schema") {
    // The only difference is the case of the most deeply nested column.
    val structTypes = Seq("z", "Z").map { finalColumnName =>
      new StructType()
        .add("a", IntegerType)
        .add("b", MapType(StringType, new StructType()
          .add("c", IntegerType)
          .add("d", new StructType()
            .add("e", IntegerType)
            .add("f", IntegerType)
            .add("g", ArrayType(new StructType()
              .add("h", IntegerType)
              .add("i", IntegerType)
              .add("j", new StructType()
                .add("k", MapType(StringType, new StructType()
                  .add("l", ArrayType(new StructType()
                    .add("m", IntegerType)
                    .add(finalColumnName, IntegerType)
      ))))))))))
    }.toArray

    val sourceStructType = structTypes(0)
    val df = spark.range(1).toDF("id")
      .select(lit(null).cast(sourceStructType).as("s"))
    val tableStructType = structTypes(1)
    val tableSchema = new StructType()
      .add("s", tableStructType)
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq("s"))
    assert(normalized.schema === tableSchema)
  }

  test("normalize column names - can normalize row id column") {
    withTable("src") {
      spark.range(3).toDF("id").write
        .format("delta")
        .mode("overwrite")
        .option("delta.enableRowTracking", "true")
        .saveAsTable("src")

      val df = spark.read.format("delta").table("src")
        .select(
          col("*"),
          col("_metadata.row_id").as("row_id")
        )
        .withMetadata("row_id", RowId.RowIdMetadataStructField.metadata("name"))

      val tableSchema = new StructType().add("id", LongType)
      val normalized =
        normalizeColumnNames(deltaLog = null, tableSchema, df)
        assert(normalized.schema.fieldNames === Seq("id", "row_id"))
    }
  }

  test("normalize column names - can normalize both row id and commit version columns") {
    withTable("src") {
      spark.range(3).toDF("id").write
        .format("delta")
        .mode("overwrite")
        .option("delta.enableRowTracking", "true")
        .saveAsTable("src")

      val df = spark.read.format("delta").table("src")
        .select(
          col("*"),
          col("_metadata.row_id").as("row_id"),
          col("_metadata.row_commit_version").as("row_commit_version")
        )
        .withMetadata("row_id", RowId.RowIdMetadataStructField.metadata("name"))
        .withMetadata("row_commit_version", RowCommitVersion.MetadataStructField.metadata("name"))

      val tableSchema = new StructType().add("id", LongType)
        val normalized =
        normalizeColumnNames(deltaLog = null, tableSchema, df)
        assert(normalized.schema.fieldNames === Seq("id", "row_id", "row_commit_version"))
    }
  }

  test("normalize column names - can normalize CDC type column") {
    val df = Seq((1, 2, 3, 4)).toDF("Abc", "def", "gHi", CDCReader.CDC_TYPE_COLUMN_NAME)
    val tableSchema = new StructType()
      .add("abc", IntegerType)
      .add("Def", IntegerType)
      .add("ghi", IntegerType)
    val normalized = normalizeColumnNames(
      deltaLog = null,
      tableSchema,
      df
    )
    verifyColumnsWithCasts(normalized, Seq.empty)
    assert(normalized.schema.fieldNames ===
      tableSchema.fieldNames :+ CDCReader.CDC_TYPE_COLUMN_NAME)
  }

  private def checkLatestStatsForOneRowFile(
      tableName: String,
      expectedStats: Map[String, Option[Any]]): Unit = {
    val snapshot = DeltaLog.forTable(spark, TableIdentifier(tableName)).update()
    val fileStats = snapshot.allFiles
      .orderBy(desc("modificationTime"))
      .limit(1)
      .withColumn("stats", from_json(col("stats"), snapshot.statsSchema))
      .select("stats.*")

    val assertions = Seq(assert_true(col("numRecords") === lit(1L))) ++
        expectedStats.flatMap { case (columnName, columnValue) =>
      columnValue match {
        case Some(value) => Seq(
          assert_true(col("minValues." + columnName) === lit(value)),
          assert_true(col("maxValues." + columnName) === lit(value)),
          assert_true(col("nullCount." + columnName) === lit(0L)))
        case None => Seq(
          assert_true(col("minValues." + columnName).isNull),
          assert_true(col("maxValues." + columnName).isNull),
          assert_true(col("nullCount." + columnName) === lit(1L)))
      }
    }
    fileStats.select(assertions: _*).collect()
  }

  for (caseSensitive <- DeltaTestUtils.BOOLEAN_DOMAIN) {
    test(s"normalize column names - e2e nested struct (caseSensitive=$caseSensitive)") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val sourceData = Seq((105L, "foo", 205L, "bar",
            Struct1("James", 11, "Smith", 3000, "Correct")))
        val sourceDf = sourceData.toDF("long1", "str1", "long2", "str2", "struct1")
        val sourceSchema = new StructType()
          .add("long1", LongType, nullable = false)
          .add("str1", StringType, nullable = true)
          .add("long2", LongType, nullable = false)
          .add("str2", StringType, nullable = true)
          .add("struct1", new StructType()
            .add("firstname", StringType, nullable = true)
            .add("numberone", LongType, nullable = false)
            .add("lastname", StringType, nullable = true)
            .add("numbertwo", LongType, nullable = false)
            .add("CorrectCase", StringType, nullable = true),
            nullable = true
          )
        assert(sourceDf.schema === sourceSchema)
        val createTableCommand =
          """ CREATE TABLE t (
            |  Long2 LONG, Str2 STRING, Long1 LONG, Str1 STRING, Int1 INT,
            |  Struct1 STRUCT<LastName: STRING, NumberTwo: LONG, FirstName: STRING,
            |    NumberOne: LONG, MissingNested: INT, CorrectCase: STRING>
            | ) USING delta
            |""".stripMargin

        withTable("t") {
          sql(createTableCommand)
          sourceDf.write.format("delta").mode("append").saveAsTable("t")

          // Make sure all the values were inserted into the right columns, and columns missing in
          // the source were set to null.
          spark.table("t")
            .select(
              assert_true(col("Long2") === 205L),
              assert_true(col("Str2") === "bar"),
              assert_true(col("Long1") === 105L),
              assert_true(col("Str1") === "foo"),
              assert_true(col("Int1").isNull),
              assert_true(col("Struct1.LastName") === "Smith"),
              assert_true(col("Struct1.NumberTwo") === 3000L),
              assert_true(col("Struct1.FirstName") === "James"),
              assert_true(col("Struct1.NumberOne") === 11L),
              assert_true(col("Struct1.MissingNested").isNull),
              assert_true(col("Struct1.CorrectCase") === "Correct")
            ).collect()

          // Make sure each of the columns stats was computed correctly.
          checkLatestStatsForOneRowFile("t", Map(
            "Long2" -> Some(205L),
            "Str2" -> Some("bar"),
            "Long1" -> Some(105L),
            "Str1" -> Some("foo"),
            "Int1" -> None,
            "Struct1.LastName" -> Some("Smith"),
            "Struct1.NumberTwo" -> Some(3000L),
            "Struct1.FirstName" -> Some("James"),
            "Struct1.NumberOne" -> Some(11L),
            "Struct1.MissingNested" -> None,
            "Struct1.CorrectCase" -> Some("Correct")
          ))
        }
      }
    }
  }

  ////////////////////////////
  // mergeSchemas
  ////////////////////////////

  test("mergeSchemas: missing columns in df") {
    val base = new StructType().add("a", IntegerType).add("b", IntegerType)
    val write = new StructType().add("a", IntegerType)
    assert(mergeSchemas(base, write) === base)
  }

  test("mergeSchemas: missing columns in df - case sensitivity") {
    val base = new StructType().add("a", IntegerType).add("b", IntegerType)
    val write = new StructType().add("A", IntegerType)
    assert(mergeSchemas(base, write) === base)
  }

  test("new columns get added to the tail of the schema") {
    val base = new StructType().add("a", IntegerType)
    val write = new StructType().add("a", IntegerType).add("b", IntegerType)
    val write2 = new StructType().add("b", IntegerType).add("a", IntegerType)
    assert(mergeSchemas(base, write) === write)
    assert(mergeSchemas(base, write2) === write)
  }

  test("new columns get added to the tail of the schema - nested") {
    val base = new StructType()
      .add("regular", StringType)
      .add("struct", new StructType()
        .add("a", IntegerType))

    val write = new StructType()
      .add("other", StringType)
      .add("struct", new StructType()
        .add("b", DateType)
        .add("a", IntegerType))
      .add("this", StringType)

    val expected = new StructType()
      .add("regular", StringType)
      .add("struct", new StructType()
        .add("a", IntegerType)
        .add("b", DateType))
      .add("other", StringType)
      .add("this", StringType)
    assert(mergeSchemas(base, write) === expected)
  }

  test("schema merging of incompatible types") {
    val base = new StructType()
      .add("top", StringType)
      .add("struct", new StructType()
        .add("a", IntegerType))
      .add("array", ArrayType(new StructType()
        .add("b", DecimalType(18, 10))))
      .add("map", MapType(StringType, StringType))

    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DATATYPE",
      Map("currentDataType" -> "StringType", "updateDataType" -> "IntegerType")) {
      mergeSchemas(base, new StructType().add("top", IntegerType))
    }
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DATATYPE",
      Map("currentDataType" -> "IntegerType", "updateDataType" -> "DateType")) {
      mergeSchemas(base, new StructType()
        .add("struct", new StructType().add("a", DateType)))
    }
    // StructType's toString is different between Scala 2.12 and 2.13.
    // - In Scala 2.12, it extends `scala.collection.Seq` which returns
    //   `StructType(StructField(a,IntegerType,true))`.
    // - In Scala 2.13, it extends `scala.collection.immutable.Seq` which returns
    //   `Seq(StructField(a,IntegerType,true))`.
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DATATYPE",
      Map("currentDataType" -> "(StructType|Seq)\\(.*", "updateDataType" -> "MapType\\(.*")) {
      mergeSchemas(base, new StructType()
        .add("struct", MapType(StringType, IntegerType)))
    }
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DATATYPE",
      Map("currentDataType" -> "DecimalType\\(.*", "updateDataType" -> "DoubleType")) {
      mergeSchemas(base, new StructType()
        .add("array", ArrayType(new StructType().add("b", DoubleType))))
    }
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DECIMAL_TYPE",
      Map("decimalRanges" -> "scale.*")) {
      mergeSchemas(base, new StructType()
        .add("array", ArrayType(new StructType().add("b", DecimalType(18, 12)))))
    }
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DECIMAL_TYPE",
      Map("decimalRanges" -> "precision.*")) {
      mergeSchemas(base, new StructType()
        .add("array", ArrayType(new StructType().add("b", DecimalType(16, 10)))))
    }
    // See the above comment about `StructType`
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DATATYPE",
      Map("currentDataType" -> "MapType\\(.*", "updateDataType" -> "(StructType|Seq)\\(.*")) {
      mergeSchemas(base, new StructType()
        .add("map", new StructType().add("b", StringType)))
    }
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DATATYPE",
      Map("currentDataType" -> "StringType", "updateDataType" -> "IntegerType")) {
      mergeSchemas(base, new StructType()
        .add("map", MapType(StringType, IntegerType)))
    }
    expectAnalysisErrorClass("DELTA_MERGE_INCOMPATIBLE_DATATYPE",
      Map("currentDataType" -> "StringType", "updateDataType" -> "IntegerType")) {
      mergeSchemas(base, new StructType()
        .add("map", MapType(IntegerType, StringType)))
    }
  }

  test("schema merging should pick current nullable and metadata") {
    val m = new MetadataBuilder().putDouble("a", 0.2).build()
    val base = new StructType()
      .add("top", StringType, nullable = false, m)
      .add("struct", new StructType()
        .add("a", IntegerType, nullable = false, m))
      .add("array", ArrayType(new StructType()
        .add("b", DecimalType(18, 10))), nullable = false, m)
      .add("map", MapType(StringType, StringType), nullable = false, m)

    assert(mergeSchemas(base, new StructType().add("top", StringType)) === base)
    assert(mergeSchemas(base, new StructType().add("struct", new StructType()
      .add("a", IntegerType))) === base)
    assert(mergeSchemas(base, new StructType().add("array", ArrayType(new StructType()
      .add("b", DecimalType(18, 10))))) === base)
    assert(mergeSchemas(base, new StructType()
      .add("map", MapType(StringType, StringType))) === base)
  }

  test("schema merging null type") {
    val base = new StructType().add("top", NullType)
    val update = new StructType().add("top", StringType)

    assert(mergeSchemas(base, update) === update)
    assert(mergeSchemas(update, base) === update)
  }

  test("schema merging performs upcast between ByteType, ShortType, and IntegerType") {
    val byteType = new StructType().add("top", ByteType)
    val shortType = new StructType().add("top", ShortType)
    val intType = new StructType().add("top", IntegerType)

    assert(mergeSchemas(byteType, shortType) === shortType)
    assert(mergeSchemas(byteType, intType) === intType)
    assert(mergeSchemas(shortType, intType) === intType)
    assert(mergeSchemas(shortType, byteType) === shortType)
    assert(mergeSchemas(intType, shortType) === intType)
    assert(mergeSchemas(intType, byteType) === intType)

    val structInt = new StructType().add("top", new StructType().add("leaf", IntegerType))
    val structShort = new StructType().add("top", new StructType().add("leaf", ShortType))
    assert(mergeSchemas(structInt, structShort) === structInt)

    val map1 = new StructType().add("top", new MapType(IntegerType, ShortType, true))
    val map2 = new StructType().add("top", new MapType(ShortType, IntegerType, true))
    val mapMerged = new StructType().add("top", new MapType(IntegerType, IntegerType, true))
    assert(mergeSchemas(map1, map2) === mapMerged)

    val arrInt = new StructType().add("top", new ArrayType(IntegerType, true))
    val arrShort = new StructType().add("top", new ArrayType(ShortType, true))
    assert(mergeSchemas(arrInt, arrShort) === arrInt)
  }

  test("schema merging allows upcasting to LongType with allowImplicitConversions") {
    val byteType = new StructType().add("top", ByteType)
    val shortType = new StructType().add("top", ShortType)
    val intType = new StructType().add("top", IntegerType)
    val longType = new StructType().add("top", LongType)

    Seq(byteType, shortType, intType).foreach { sourceType =>
      assert(
        longType === mergeSchemas(
          longType, sourceType, allowImplicitConversions = true))
      val e = intercept[DeltaAnalysisException] {
          mergeSchemas(longType, sourceType)
        }
      checkError(
        exception = e.getCause.asInstanceOf[AnalysisException],
        errorClass = "DELTA_MERGE_INCOMPATIBLE_DATATYPE",
        parameters = Map("currentDataType" -> "LongType",
          "updateDataType" -> sourceType.head.dataType.toString))
    }
  }

  test("Upcast between ByteType, ShortType and IntegerType is OK for parquet") {
    import org.apache.spark.sql.functions._
    def testParquetUpcast(): Unit = {
      withTempDir { dir =>
        val tempDir = dir.getCanonicalPath
        spark.range(1.toByte).select(col("id") cast ByteType).write.save(tempDir + "/byte")
        spark.range(1.toShort).select(col("id") cast ShortType).write.save(tempDir + "/short")
        spark.range(1).select(col("id") cast IntegerType).write.save(tempDir + "/int")

        val shortSchema = new StructType().add("id", ShortType)
        val intSchema = new StructType().add("id", IntegerType)

        spark.read.schema(shortSchema).parquet(tempDir + "/byte").collect() === Seq(Row(1.toShort))
        spark.read.schema(intSchema).parquet(tempDir + "/short").collect() === Seq(Row(1))
        spark.read.schema(intSchema).parquet(tempDir + "/byte").collect() === Seq(Row(1))
      }
    }

    testParquetUpcast()

  }
  ////////////////////////////
  // transformColumns
  ////////////////////////////

  test("transform columns - simple") {
    val base = new StructType()
      .add("a", IntegerType)
      .add("b", StringType)
    val update = new StructType()
      .add("c", IntegerType)
      .add("b", StringType)

    // Identity.
    var visitedFields = 0
    val res1 = SchemaMergingUtils.transformColumns(base) {
      case (Seq(), field, _) =>
        visitedFields += 1
        field
    }
    assert(visitedFields === 2)
    assert(base === res1)

    // Rename a -> c
    visitedFields = 0
    val res2 = SchemaMergingUtils.transformColumns(base) {
      case (Seq(), field, _) =>
        visitedFields += 1
        val name = field.name
        field.copy(name = if (name == "a") "c" else name)
    }
    assert(visitedFields === 2)
    assert(update === res2)

    // Rename a -> c; using input map.
    visitedFields = 0
    val res3 = transformColumns(base, (Seq("A"), "c") :: Nil) {
      case (Seq(), field, Seq((_, newName))) =>
        visitedFields += 1
        field.copy(name = newName)
    }
    assert(visitedFields === 1)
    assert(update === res3)
  }

  test("transform element field type") {
    val base = new StructType()
      .add("a", new StructType()
        .add("element", StringType))

    val update = new StructType()
      .add("a", new StructType()
        .add("element", IntegerType))

    // Update type
    var visitedFields = 0
    val res = SchemaMergingUtils.transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.name match {
        case Seq("a", "element") => IntegerType
        case _ => field.dataType
      }
      field.copy(dataType = dataType)
    }
    assert(visitedFields === 2)
    assert(update === res)
  }

  test("transform array nested field type") {
    val nested = new StructType()
      .add("s1", IntegerType)
      .add("s2", LongType)
    val base = new StructType()
      .add("arr", ArrayType(nested))

    val updatedNested = new StructType()
      .add("s1", StringType)
      .add("s2", LongType)
    val update = new StructType()
      .add("arr", ArrayType(updatedNested))

    // Update type
    var visitedFields = 0
    val res = SchemaMergingUtils.transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.name match {
        case Seq("arr", "element", "s1") => StringType
        case _ => field.dataType
      }
      field.copy(dataType = dataType)
    }
    assert(visitedFields === 3)
    assert(update === res)
  }

  test("transform map nested field type") {
    val nested = new StructType()
      .add("s1", IntegerType)
      .add("s2", LongType)
    val base = new StructType()
      .add("m", MapType(StringType, nested))

    val updatedNested = new StructType()
      .add("s1", StringType)
      .add("s2", LongType)
    val update = new StructType()
      .add("m", MapType(StringType, updatedNested))

    // Update type
    var visitedFields = 0
    val res = SchemaMergingUtils.transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.name match {
        case Seq("m", "value", "s1") => StringType
        case _ => field.dataType
      }
      field.copy(dataType = dataType)
    }
    assert(visitedFields === 3)
    assert(update === res)
  }

  test("transform map type") {
    val base = new StructType()
      .add("m", MapType(StringType, IntegerType))
    val update = new StructType()
      .add("m", MapType(StringType, StringType))

    // Update type
    var visitedFields = 0
    val res = SchemaMergingUtils.transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val dataType = path :+ field.name match {
        case Seq("m") => MapType(field.dataType.asInstanceOf[MapType].keyType, StringType)
        case _ => field.dataType
      }
      field.copy(dataType = dataType)
    }
    assert(visitedFields === 1)
    assert(update === res)
  }

  test("transform columns - nested") {
    val nested = new StructType()
      .add("s1", IntegerType)
      .add("s2", LongType)
    val base = new StructType()
      .add("nested", nested)
      .add("arr", ArrayType(nested))
      .add("kvs", MapType(nested, nested))
    val update = new StructType()
      .add("nested",
        new StructType()
          .add("t1", IntegerType)
          .add("s2", LongType))
      .add("arr", ArrayType(
        new StructType()
          .add("s1", IntegerType)
          .add("a2", LongType)))
      .add("kvs", MapType(
        new StructType()
          .add("k1", IntegerType)
          .add("s2", LongType),
        new StructType()
          .add("s1", IntegerType)
          .add("v2", LongType)))

    // Identity.
    var visitedFields = 0
    val res1 = SchemaMergingUtils.transformColumns(base) {
      case (_, field, _) =>
        visitedFields += 1
        field
    }
    assert(visitedFields === 11)
    assert(base === res1)

    // Rename
    visitedFields = 0
    val res2 = SchemaMergingUtils.transformColumns(base) { (path, field, _) =>
      visitedFields += 1
      val name = path :+ field.name match {
        case Seq("nested", "s1") => "t1"
        case Seq("arr", "element", "s2") => "a2"
        case Seq("kvs", "key", "s1") => "k1"
        case Seq("kvs", "value", "s2") => "v2"
        case _ => field.name
      }
      field.copy(name = name)
    }
    assert(visitedFields === 11)
    assert(update === res2)

    // Rename; using map
    visitedFields = 0
    val mapping = Seq(
      Seq("nested", "s1") -> "t1",
      Seq("arr", "element", "s2") -> "a2",
      Seq("kvs", "key", "S1") -> "k1",
      Seq("kvs", "value", "s2") -> "v2")
    val res3 = transformColumns(base, mapping) {
      case (_, field, Seq((_, name))) =>
        visitedFields += 1
        field.copy(name = name)
    }
    assert(visitedFields === 4)
    assert(update === res3)
  }

  ////////////////////////////
  // checkFieldNames
  ////////////////////////////

  test("check non alphanumeric column characters") {
    val badCharacters = " ,;{}()\n\t="
    val goodCharacters = "#.`!@$%^&*~_<>?/:"

    badCharacters.foreach { char =>
      Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString).foreach { name =>
        checkError(
          exception = intercept[AnalysisException] {
            SchemaUtils.checkFieldNames(Seq(name))
          },
          errorClass = "DELTA_INVALID_CHARACTERS_IN_COLUMN_NAME",
          parameters = Map("columnName" -> s"$name")
        )
      }
    }

    goodCharacters.foreach { char =>
      // no issues here
      SchemaUtils.checkFieldNames(Seq(s"a${char}b", s"${char}ab", s"ab${char}", char.toString))
    }
  }

  test("fieldToColumn") {
    assert(SchemaUtils.fieldToColumn(StructField("a", IntegerType)).expr ==
      new UnresolvedAttribute("a" :: Nil))
    // Dot in the column name should be converted correctly
    assert(SchemaUtils.fieldToColumn(StructField("a.b", IntegerType)).expr ==
      new UnresolvedAttribute("a.b" :: Nil))
  }

  ////////////////////////////
  // findNestedFieldIgnoreCase
  ////////////////////////////

  test("complex schema access") {
    val st = StringType
    val it = IntegerType
    def m(a: DataType, b: DataType): MapType = MapType(a, b)
    def a(el: DataType): ArrayType = ArrayType(el)
    def struct(el: DataType): StructType = new StructType().add("f1", el)

    val schema = new StructType()
      .add("a", it)
      .add("b", struct(st))
      .add("c", struct(struct(struct(st))))
      .add("d", a(it))
      .add("e", a(a(it)))
      .add("f", a(a(struct(st))))
      .add("g", m(m(st, it), m(st, it)))
      .add("h", m(a(st), a(it)))
      .add("i", m(a(struct(st)), a(struct(st))))
      .add("j", m(m(struct(st), struct(it)), m(struct(st), struct(it))))
      .add("k", m(struct(a(a(struct(a(struct(st)))))),
                m(m(struct(st), struct(it)), m(struct(st), struct(it)))))

    def find(names: Seq[String]): Option[StructField] =
      SchemaUtils.findNestedFieldIgnoreCase(schema, names, true)

    val checks = Map(
      "a" -> it,
      "b" -> struct(st),
      "b.f1" -> st,
      "c.f1.f1.f1" -> st,
      "d.element" -> it,
      "e.element.element" -> it,
      "f.element.element.f1" -> st,
      "g.key.key" -> st,
      "g.key.value" -> it,
      "g.value.key" -> st,
      "g.value.value" -> it,
      "h.key.element" -> st,
      "h.value.element" -> it,
      "i.key.element.f1" -> st,
      "i.value.element.f1" -> st,
      "j.key.key.f1" -> st,
      "j.key.value.f1" -> it,
      "j.value.key.f1" -> st,
      "j.value.value.f1" -> it,
      "k.key.f1.element.element.f1.element.f1" -> st,
      "k.value.key.key.f1" -> st,
      "k.value.key.value.f1" -> it,
      "k.value.value.key.f1" -> st,
      "k.value.value.value.f1" -> it
    )

    checks.foreach { pair =>
      val (key, t) = pair
      val path = key.split('.')
      val f = find(path)
      assert(f.isDefined, s"cannot find $key")
      assert(f.get.name == path.last && f.get.dataType == t)
    }

    val negativeChecks = Seq(
      "x",
      "b.f2",
      "c.f1.f2",
      "c.f1.f1.f2",
      "d.f1",
      "d.element.f1",
      "e.element.element.f1",
      "f.element.key.f1",
      "g.key.element",
      "g.key.keyy",
      "g.key.valuee",
      "h.key.element.f1",
      "k.key.f1.element.element.f2.element.f1",
      "k.value.value.f1"
    )

    negativeChecks.foreach { key =>
      val path = key.split('.')
      val f = find(path)
      assert(f.isEmpty, s"$key should be empty")
    }

  }

  test("findUnsupportedDataTypes") {
    def assertUnsupportedDataType(
        dataType: DataType,
        expected: Seq[UnsupportedDataTypeInfo]): Unit = {
      val schema = StructType(Seq(StructField("col", dataType)))
      assert(findUnsupportedDataTypes(schema) == expected)
    }

    assertUnsupportedDataType(NullType, Nil)
    assertUnsupportedDataType(BooleanType, Nil)
    assertUnsupportedDataType(ByteType, Nil)
    assertUnsupportedDataType(ShortType, Nil)
    assertUnsupportedDataType(IntegerType, Nil)
    assertUnsupportedDataType(LongType, Nil)
    assertUnsupportedDataType(
      YearMonthIntervalType.DEFAULT,
      Seq(UnsupportedDataTypeInfo("col", YearMonthIntervalType.DEFAULT)))
    assertUnsupportedDataType(
      DayTimeIntervalType.DEFAULT,
      Seq(UnsupportedDataTypeInfo("col", DayTimeIntervalType.DEFAULT)))
    assertUnsupportedDataType(FloatType, Nil)
    assertUnsupportedDataType(DoubleType, Nil)
    assertUnsupportedDataType(StringType, Nil)
    assertUnsupportedDataType(DateType, Nil)
    assertUnsupportedDataType(TimestampType, Nil)
    assertUnsupportedDataType(
      CalendarIntervalType,
      Seq(UnsupportedDataTypeInfo("col", CalendarIntervalType)))
    assertUnsupportedDataType(BinaryType, Nil)
    assertUnsupportedDataType(DataTypes.createDecimalType(), Nil)
    assertUnsupportedDataType(
      UnsupportedDataType,
      Seq(UnsupportedDataTypeInfo("col", UnsupportedDataType)))

    // array
    assertUnsupportedDataType(ArrayType(IntegerType, true), Nil)
    assertUnsupportedDataType(
      ArrayType(UnsupportedDataType, true),
      Seq(UnsupportedDataTypeInfo("col[]", UnsupportedDataType)))

    // map
    assertUnsupportedDataType(MapType(IntegerType, IntegerType, true), Nil)
    assertUnsupportedDataType(
      MapType(UnsupportedDataType, IntegerType, true),
      Seq(UnsupportedDataTypeInfo("col[key]", UnsupportedDataType)))
    assertUnsupportedDataType(
      MapType(IntegerType, UnsupportedDataType, true),
      Seq(UnsupportedDataTypeInfo("col[value]", UnsupportedDataType)))
    assertUnsupportedDataType(
      MapType(UnsupportedDataType, UnsupportedDataType, true),
      Seq(
        UnsupportedDataTypeInfo("col[key]", UnsupportedDataType),
        UnsupportedDataTypeInfo("col[value]", UnsupportedDataType)))

    // struct
    assertUnsupportedDataType(StructType(StructField("f", LongType) :: Nil), Nil)
    assertUnsupportedDataType(
      StructType(StructField("a", LongType) :: StructField("dot.name", UnsupportedDataType) :: Nil),
      Seq(UnsupportedDataTypeInfo("col.`dot.name`", UnsupportedDataType)))
    val nestedStructType = StructType(Seq(
      StructField("a", LongType),
      StructField("b", StructType(Seq(
        StructField("c", LongType),
        StructField("d", UnsupportedDataType)
      ))),
      StructField("e", StructType(Seq(
        StructField("f", LongType),
        StructField("g", UnsupportedDataType)
      )))
    ))
    assertUnsupportedDataType(
      nestedStructType,
      Seq(
        UnsupportedDataTypeInfo("col.b.d", UnsupportedDataType),
        UnsupportedDataTypeInfo("col.e.g", UnsupportedDataType)))

    // udt
    assertUnsupportedDataType(new PointUDT, Nil)
    assertUnsupportedDataType(
      new UnsupportedUDT,
      Seq(UnsupportedDataTypeInfo("col", UnsupportedDataType)))
  }

  test("findUndefinedTypes: basic types") {
    val schema = StructType(Seq(
      StructField("c1", NullType),
      StructField("c2", BooleanType),
      StructField("c3", ByteType),
      StructField("c4", ShortType),
      StructField("c5", IntegerType),
      StructField("c6", LongType),
      StructField("c7", FloatType),
      StructField("c8", DoubleType),
      StructField("c9", StringType),
      StructField("c10", DateType),
      StructField("c11", TimestampType),
      StructField("c12", BinaryType),
      StructField("c13", DataTypes.createDecimalType()),
      // undefined types
      StructField("c14", TimestampNTZType),
      StructField("c15", YearMonthIntervalType.DEFAULT),
      StructField("c16", DayTimeIntervalType.DEFAULT),
      StructField("c17", new PointUDT) // UserDefinedType
    ))
    val udts = findUndefinedTypes(schema)
    assert(udts.map(_.getClass.getName.stripSuffix("$")) ==
      Seq(
        classOf[TimestampNTZType],
        classOf[YearMonthIntervalType],
        classOf[DayTimeIntervalType],
        classOf[PointUDT]
      ).map(_.getName.stripSuffix("$"))
    )
  }

  test("findUndefinedTypes: complex types") {
    val schema = StructType(Seq(
      StructField("c1", new PointUDT),
      StructField("c2", ArrayType(new PointUDT, true)),
      StructField("c3", MapType(new PointUDT, new PointUDT, true)),
      StructField("c4", StructType(Seq(
        StructField("c1", new PointUDT),
        StructField("c2", ArrayType(new PointUDT, true)),
        StructField("c3", MapType(new PointUDT, new PointUDT, true))
      )))
    ))
    val udts = findUndefinedTypes(schema)
    assert(udts.size == 8)
    assert(udts.map(_.getClass.getName).toSet == Set(classOf[PointUDT].getName))
  }

}

object UnsupportedDataType extends DataType {
  override def defaultSize: Int = throw new UnsupportedOperationException("defaultSize")
  override def asNullable: DataType = throw new UnsupportedOperationException("asNullable")
  override def toString: String = "UnsupportedDataType"
}

@SQLUserDefinedType(udt = classOf[PointUDT])
case class Point(x: Int, y: Int)

class PointUDT extends UserDefinedType[Point] {
  override def sqlType: DataType = StructType(Array(
    StructField("x", IntegerType, nullable = false),
    StructField("y", IntegerType, nullable = false)))

  override def serialize(obj: Point): Any = InternalRow(obj.x, obj.y)

  override def deserialize(datum: Any): Point = datum match {
    case row: InternalRow => Point(row.getInt(0), row.getInt(1))
  }

  override def userClass: Class[Point] = classOf[Point]

  override def toString: String = "PointUDT"
}

class UnsupportedUDT extends PointUDT {
  override def sqlType: DataType = UnsupportedDataType
}
case class Struct1(
    firstname: String,
    numberone: Long,
    lastname: String,
    numbertwo: Long,
    CorrectCase: String)
