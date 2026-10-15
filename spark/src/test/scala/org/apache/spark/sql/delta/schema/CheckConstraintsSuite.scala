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

import java.util.regex.Pattern

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.{AllowedUserProvidedExpressions, DeltaConfigs, DeltaLog, DeltaTableProvider, DeltaTestUtils}
import org.apache.spark.sql.delta.constraints.CharVarcharConstraint
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.sources.DeltaSQLConf.ValidateCheckConstraintsMode
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BooleanType, CharType, IntegerType, MapType, MetadataBuilder, StringType, StructField, StructType}

class CheckConstraintsSuite extends QueryTest
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils
    with DeltaTableProvider {


  private def withTestTable(thunk: String => Unit): Unit = {
    withTable("checkConstraintsTest") {
      spark.sql(
        s"""CREATE TABLE checkConstraintsTest (num INT, text STRING)
           |USING $tableProvider
           |""".stripMargin)

      spark.sql(
        """INSERT INTO checkConstraintsTest
          |VALUES (1, "a"),
          |       (2, "b"),
          |       (3, "c"),
          |       (4, "d"),
          |       (5, "e"),
          |       (6, "f")
          |""".stripMargin)

      thunk("checkConstraintsTest")
    }
  }

  test("can't add unparseable constraint") {
    withTestTable { table =>
      val e = intercept[SparkThrowable] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT lessThan5 CHECK (id <)")
      }
      // Make sure we're still getting a useful parse error, even though we do some complicated
      // internal stuff to persist the constraint. Unfortunately this test may be a bit fragile.
      checkError(
        exception = e,
        condition = "PARSE_SYNTAX_ERROR",
        sqlState = Some("42601"),
        parameters = Map("hint" -> "", "error" -> "end of input")
      )
    }
  }

  test("Checking incorrect constraints added through table property in CREATE TABLE errors out") {
    val tableName = "test_tbl"
    withTable(tableName) {
      sql(createTableSQL(
        tableName,
        "id INT, event_date DATE",
        props = Map("delta.constraints.ch" -> "event_date < 2025-06-12")))

      val e = intercept[SparkThrowable] {
        sql(s"INSERT INTO $tableName VALUES(1, '2025-06-11')")
      }
      checkError(
        exception = e,
        condition = "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES",
        sqlState = Some("42K09"),
        parameters = Map(
          "sqlExpr" -> "\"(event_date < ((2025 - 6) - 12))\"",
          "left" -> "\"DATE\"",
          "right" -> "\"INT\""),
        queryContext = Array(ExpectedContext("event_date < 2025-06-12", 0, 22)))
    }
  }

  test("CREATE TABLE with check constraint referencing non-existent column fails at create time") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
        ValidateCheckConstraintsMode.ASSERT.toString) {
      val tableName = "test_create_invalid_constraint"
      withTable(tableName) {
        checkError(
          exception = intercept[SparkThrowable] {
            sql(createTableSQL(
              tableName,
              "id INT, value STRING",
              props = Map("delta.constraints.invalid" -> "non_existent_column > 0")))
          },
          "DELTA_INVALID_CHECK_CONSTRAINT_REFERENCES",
          parameters = Map("colName" -> "`non_existent_column`"))
      }
    }
  }

  test("CREATE TABLE with non-boolean check constraint fails at create time") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
        ValidateCheckConstraintsMode.ASSERT.toString) {
      val tableName = "test_create_non_boolean_constraint"
      withTable(tableName) {
        checkError(
          exception = intercept[SparkThrowable] {
            sql(createTableSQL(
              tableName,
              "id INT, value STRING",
              props = Map("delta.constraints.nonbool" -> "id + 1")))
          },
          "DELTA_NON_BOOLEAN_CHECK_CONSTRAINT",
          parameters = Map(
            "name" -> "nonbool",
            "expr" -> "(id + 1)"))
      }
    }
  }

  test("CREATE TABLE with valid check constraint succeeds") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
        ValidateCheckConstraintsMode.ASSERT.toString) {
      val tableName = "test_create_valid_constraint"
      withTable(tableName) {
        sql(createTableSQL(
          tableName,
          "id INT, value STRING",
          props = Map("delta.constraints.positive_id" -> "id > 0")))
      }
    }
  }

  test("constraint must be boolean") {
    withTestTable { table =>
      checkError(
        exception = intercept[SparkThrowable] {
          sql(s"ALTER TABLE $table ADD CONSTRAINT integerVal CHECK (3)")
        },
        "DELTA_NON_BOOLEAN_CHECK_CONSTRAINT",
        parameters = Map(
          "name" -> "integerVal",
          "expr" -> "3"
        )
      )
    }
  }

  test("can't add constraint referencing non-existent columns") {
    withTestTable { table =>
      checkError(
        intercept[SparkThrowable] {
          sql(s"ALTER TABLE $table ADD CONSTRAINT c CHECK (does_not_exist)")
        },
        "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`does_not_exist`",
          "proposal" -> "`text`, `num`"
        )
      )
    }
  }

  test("can't add constraint with duplicate name") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT trivial CHECK (true)")
      val e = intercept[SparkThrowable] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT trivial CHECK (true)")
      }
      checkError(
        exception = e,
        condition = "DELTA_CONSTRAINT_ALREADY_EXISTS",
        sqlState = Some("42710"),
        parameters = Map(
          "oldConstraint" -> "true",
          "constraintName" -> "trivial"
        )
      )
    }
  }

  test("can't add constraint with names that are reserved for internal usage") {
    withTestTable { table =>
      val e = intercept[SparkThrowable] {
        sql(
          s"ALTER TABLE $table ADD CONSTRAINT ${CharVarcharConstraint.INVARIANT_NAME} CHECK (true)")
      }
      checkError(
        exception = e,
        condition = "DELTA_INVALID_CONSTRAINT_NAME",
        sqlState = Some("42939"),
        parameters = Map("name" -> CharVarcharConstraint.INVARIANT_NAME)
      )
    }
  }

  test("duplicate constraint check is case insensitive") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT trivial CHECK (true)")
      val e = intercept[SparkThrowable] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT TRIVIAL CHECK (true)")
      }
      checkError(
        exception = e,
        condition = "DELTA_CONSTRAINT_ALREADY_EXISTS",
        sqlState = Some("42710"),
        parameters = Map(
          "oldConstraint" -> "true",
          "constraintName" -> "TRIVIAL"
        )
      )
    }
  }

  testQuietly("can't add already violated constraint") {
    withTestTable { table =>
      val e = intercept[SparkThrowable] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT lessThan5 CHECK (num < 5 and text < 'd')")
      }
      checkError(
        exception = e,
        condition = "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
        sqlState = Some("23512"),
        parameters = Map(
          "numRows" -> "3",
          "checkConstraint" -> "num < 5 and text < 'd'",
          "tableName" -> "spark_catalog.default.checkconstraintstest"))
    }
  }

  testQuietly("can't add row violating constraint") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT lessThan10 CHECK (num < 10 and text < 'g')")
      sql(s"INSERT INTO $table VALUES (5, 'a')")
      val e = intercept[SparkThrowable] {
        sql(s"INSERT INTO $table VALUES (11, 'a')")
      }
      checkError(
        exception = e,
        condition = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
        sqlState = Some("23001"),
        parameters = Map(
          "constraintName" -> "lessthan10",
          "expression" -> "((num < 10) AND (text < 'g'))",
          "values" -> Seq(
            " - num : 11",
            " - text : a"
          ).mkString("\n")
        )
      )
    }
  }

  test("drop constraint that doesn't exist throws an exception") {
    withTestTable { table =>
      checkError(
        exception = intercept[SparkThrowable] {
          sql(s"ALTER TABLE $table DROP CONSTRAINT myConstraint")
        },
        condition = "DELTA_CONSTRAINT_DOES_NOT_EXIST",
        sqlState = Some("42704"),
        parameters = Map(
          "constraintName" -> "myConstraint",
          "tableName" -> "`default`.`checkConstraintsTest`",
          "config" -> "spark.databricks.delta.constraints.assumesDropIfExists.enabled",
          "confValue" -> "true"
        )
      )
    }

    withSQLConf((DeltaSQLConf.DELTA_ASSUMES_DROP_CONSTRAINT_IF_EXISTS.key, "false")) {
      withTestTable { table =>
        val e = intercept[SparkThrowable] {
          sql(s"ALTER TABLE $table DROP CONSTRAINT myConstraint")
        }
        checkError(
          exception = e,
          condition = "DELTA_CONSTRAINT_DOES_NOT_EXIST",
          sqlState = Some("42704"),
          parameters = Map(
            "confValue" -> "true",
            "constraintName" -> "myConstraint",
            "config" -> "spark.databricks.delta.constraints.assumesDropIfExists.enabled",
            "tableName" -> "`default`.`checkConstraintsTest`"
          )
        )
      }
    }
  }

  test("can drop constraint that doesn't exist with IF EXISTS") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table DROP CONSTRAINT IF EXISTS myConstraint")
    }

    withSQLConf((DeltaSQLConf.DELTA_ASSUMES_DROP_CONSTRAINT_IF_EXISTS.key, "true")) {
      withTestTable { table =>
        sql(s"ALTER TABLE $table DROP CONSTRAINT myConstraint")
      }
    }
  }


  test("drop constraint is case insensitive") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT myConstraint CHECK (true)")
      sql(s"ALTER TABLE $table DROP CONSTRAINT MYCONSTRAINT")
    }
  }

  testQuietly("add row violating constraint after it's dropped") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT lessThan10 CHECK (num < 10 and text < 'g')")
      intercept[SparkThrowable] {
        sql(s"INSERT INTO $table VALUES (11, 'a')")
      }
      sql(s"ALTER TABLE $table DROP CONSTRAINT lessThan10")
      sql(s"INSERT INTO $table VALUES (11, 'a')")
      checkAnswer(
        sql(s"SELECT num FROM $table"),
        Seq(Row(1), Row(2), Row(3), Row(4), Row(5), Row(6), Row(11))
      )
    }
  }

  test("see constraints in table properties") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT toBeDropped CHECK (text < 'n')")
      sql(s"ALTER TABLE $table ADD CONSTRAINT trivial CHECK (true)")
      sql(s"ALTER TABLE $table ADD CONSTRAINT numLimit CHECK (num < 10)")
      sql(s"ALTER TABLE $table ADD CONSTRAINT combo CHECK (concat(num, text) != '9i')")
      sql(s"ALTER TABLE $table DROP CONSTRAINT toBeDropped")
      val props =
        sql(s"DESCRIBE DETAIL $table").selectExpr("properties").head().getMap[String, String](0)
      // We've round-tripped through the parser, so the text of the constraints stored won't exactly
      // match what was originally given.
      assert(props == Map(
        "delta.constraints.trivial" -> "true",
        "delta.constraints.numlimit" -> "num < 10",
        "delta.constraints.combo" -> "concat ( num , text ) != '9i'"
      ))
    }
  }

  test("delta history for constraints") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT lessThan10 CHECK (num < 10)")
      checkAnswer(
        sql(s"DESCRIBE HISTORY $table")
          .where("operation = 'ADD CONSTRAINT'")
          .selectExpr("operation", "operationParameters"),
        Seq(Row("ADD CONSTRAINT", Map("name" -> "lessThan10", "expr" -> "num < 10"))))

      sql(s"ALTER TABLE $table DROP CONSTRAINT IF EXISTS lessThan10")
      checkAnswer(
        sql(s"DESCRIBE HISTORY $table")
          .where("operation = 'DROP CONSTRAINT'")
          .selectExpr("operation", "operationParameters"),
        Seq(Row(
          "DROP CONSTRAINT",
          Map("name" -> "lessThan10", "expr" -> "num < 10", "existed" -> "true")
        )))
      sql(s"ALTER TABLE $table DROP CONSTRAINT IF EXISTS lessThan10")
        checkAnswer(
          sql(s"DESCRIBE HISTORY $table")
            .where("operation = 'DROP CONSTRAINT'")
            .selectExpr("operation", "operationParameters"),
          Seq(
            Row("DROP CONSTRAINT",
              Map("name" -> "lessThan10", "expr" -> "num < 10", "existed" -> "true")),
            Row("DROP CONSTRAINT",
              Map("name" -> "lessThan10", "existed" -> "false"))
          ))
    }
  }

  testQuietly("constraint on builtin methods") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT textSize CHECK (LENGTH(text) < 10)")
      sql(s"INSERT INTO $table VALUES (11, 'abcdefg')")
      val e = intercept[SparkThrowable] {
        sql(s"INSERT INTO $table VALUES (12, 'abcdefghijklmnop')")
      }
      checkError(
        exception = e,
        condition = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
        sqlState = Some("23001"),
        parameters = Map(
          "constraintName" -> "textsize",
          "expression" -> "(LENGTH(text) < 10)",
          "values" -> " - text : abcdefghijklmnop"))
    }
  }

  testQuietly("constraint with implicit casts") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT maxWithImplicitCast CHECK (num < '10')")
      val e = intercept[SparkThrowable] {
        sql(s"INSERT INTO $table VALUES (11, 'data')")
      }
      checkError(
        exception = e,
        condition = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
        sqlState = Some("23001"),
        parameters = Map(
          "constraintName" -> "maxwithimplicitcast",
          "expression" -> "(num < '10')",
          "values" -> " - num : 11"))
    }
  }

  testQuietly("constraint with nested parentheses") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT maxWithParens " +
        s"CHECK (( (num < '10') AND ((LENGTH(text)) < 100) ))")
      val e = intercept[SparkThrowable] {
        sql(s"INSERT INTO $table VALUES (11, 'data')")
      }
      checkError(
        exception = e,
        condition = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
        sqlState = Some("23001"),
        parameters = Map(
          "constraintName" -> "maxwithparens",
          "expression" -> "((num < '10') AND (LENGTH(text) < 100))",
          "values" -> Seq(
            " - num : 11",
            " - text : data"
          ).mkString("\n")
        )
      )
    }
  }

  for (expression <- Seq("year(current_date())", "unix_timestamp()"))
  testQuietly(s"constraint with analyzer-evaluated expressions. Expression: $expression") {
    // Explicitly block constraint validation since both functions are nondeterministic.
    val disabled = ValidateCheckConstraintsMode.OFF.toString
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key -> disabled) {
      withTestTable { table =>
        // We use current_timestamp()/current_date() as the most convenient
        // analyzer-evaluated expressions - of course in a realistic use case
        // it'd probably not be right to add a constraint on a
        // nondeterministic expression.
        sql(s"ALTER TABLE $table ADD CONSTRAINT maxWithAnalyzerEval " +
          s"CHECK (num < $expression)")
        val e = intercept[SparkThrowable] {
          sql(s"INSERT INTO $table VALUES (${Int.MaxValue}, 'data')")
        }
        checkError(
          exception = e,
          condition = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
          sqlState = Some("23001"),
          parameters = Map(
            "constraintName" -> "maxwithanalyzereval",
            "expression" -> s"(num < $expression)",
            "values" -> s" - num : ${Int.MaxValue}"))
      }
    }
  }

  testQuietly("constraints with nulls") {
    withTable("checkConstraintsTest") {
      val schema = new StructType(Array(
        StructField("id", IntegerType),
        StructField("text", StringType),
        StructField("nested", new StructType(Array(
          StructField("constant", StringType),
          StructField("m", MapType(IntegerType, IntegerType, valueContainsNull = true)),
          StructField("arr", ArrayType(IntegerType, containsNull = true)))))))

      sql(
        s"""CREATE TABLE checkConstraintsTest (${schema.toDDL})
           |USING $tableProvider
           |""".stripMargin)

      sql(
        """INSERT INTO checkConstraintsTest
          |VALUES (0, null, struct('constantWithinStruct', map(0, 0), array(0, null,  2))),
          |       (1, null, struct('constantWithinStruct', map(1, 1), array(1, null,  3))),
          |       (2, null, struct('constantWithinStruct', map(2, 2), array(2, null,  4))),
          |       (3, null, struct('constantWithinStruct', map(3, 3), array(3, null,  5))),
          |       (4, null, struct('constantWithinStruct', map(4, 4), array(4, null,  6))),
          |       (5, null, struct('constantWithinStruct', map(5, 5), array(5, null,  7))),
          |       (6, null, struct('constantWithinStruct', map(6, 6), array(6, null,  8))),
          |       (7, null, struct('constantWithinStruct', map(7, 7), array(7, null,  9))),
          |       (8, null, struct('constantWithinStruct', map(8, 8), array(8, null,  10))),
          |       (9, null, struct('constantWithinStruct', map(9, 9), array(9, null, 11)))
          |""".stripMargin)

      // Constraints checking for a null value should work.
      sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT textNull CHECK (text IS NULL)")
      sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arr1Null " +
        "CHECK (nested.arr[1] IS NULL)")

      // Constraints incompatible with a null value will of course fail, but they should fail with
      // the same clear error as normal.
      val e = intercept[SparkThrowable] {
        sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arrLessThan5 " +
          "CHECK (nested.arr[1] < 5)")
      }
      checkError(
        exception = e,
        condition = "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
        sqlState = Some("23512"),
        parameters = Map(
          "numRows" -> "10",
          "checkConstraint" -> "nested . arr [ 1 ] < 5",
          "tableName" -> "spark_catalog.default.checkconstraintstest"
        )
      )

      // Adding a null value into a constraint should fail similarly, even if it's null
      // because a parent field is null.
      sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arr0 " +
        "CHECK (nested.arr[0] < 100)")
      val newRows = Seq(
        "10, null, struct('c', map(10, null), array(null, null, 12))",
        "11, null, struct('c', map(11, null), null)",
        "12, null, null"
      )
      // Regex patterns for the reported values. For the first row the array itself is not null
      // (only its first element is), so its unstable string representation is reported.
      val expectedValuePatterns = Seq(
        " - nested.arr : \\S*UnsafeArrayData@\\w+",
        " - nested.arr : null",
        " - nested.arr : null")

      newRows.zip(expectedValuePatterns).foreach { case (r, expectedValuePattern) =>
        val e = intercept[SparkThrowable] {
          spark.sql(s"INSERT INTO checkConstraintsTest VALUES ($r)")
        }
        checkError(
          exception = e,
          condition = "DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
          sqlState = Some("23001"),
          parameters = Map(
            "constraintName" -> "arr0",
            "expression" -> Pattern.quote("(nested.arr[0] < 100)"),
            "values" -> expectedValuePattern),
          matchPVals = true)
      }

      // On the other hand, existing constraints like arr1Null which do allow null values should
      // permit new rows even if the value's parent is null.
      sql("ALTER TABLE checkConstraintsTest DROP CONSTRAINT arr0")
      newRows.foreach { r =>
        spark.sql(s"INSERT INTO checkConstraintsTest VALUES ($r)")
      }
      checkAnswer(
        spark.read.format(writeFormat).table("checkConstraintsTest").select("id"),
        (0 to 12).map(Row(_)))
    }

  }

  testQuietly("complex constraints") {
    withTable("checkConstraintsTest") {
      val schema = new StructType(Array(
        StructField("id", IntegerType),
        StructField("text", CharType(2)),
        StructField("nested", new StructType(Array(
          StructField("constant", StringType),
          StructField("m", MapType(IntegerType, IntegerType, valueContainsNull = false)),
          StructField("arr", ArrayType(IntegerType, containsNull = false)))))))

      sql(
        s"""CREATE TABLE checkConstraintsTest (${schema.toDDL})
           |USING $tableProvider
           |""".stripMargin)

      sql(
        """INSERT INTO checkConstraintsTest
          |VALUES (0, "a", struct('constantWithinStruct', map(0, 0), array(0, 1,  2))),
          |       (1, "b", struct('constantWithinStruct', map(1, 1), array(1, 2,  3))),
          |       (2, "c", struct('constantWithinStruct', map(2, 2), array(2, 3,  4))),
          |       (3, "d", struct('constantWithinStruct', map(3, 3), array(3, 4,  5))),
          |       (4, "e", struct('constantWithinStruct', map(4, 4), array(4, 5,  6))),
          |       (5, "f", struct('constantWithinStruct', map(5, 5), array(5, 6,  7))),
          |       (6, "g", struct('constantWithinStruct', map(6, 6), array(6, 7,  8))),
          |       (7, "h", struct('constantWithinStruct', map(7, 7), array(7, 8,  9))),
          |       (8, "i", struct('constantWithinStruct', map(8, 8), array(8, 9,  10))),
          |       (9, "j", struct('constantWithinStruct', map(9, 9), array(9, 10, 11)))
          |""".stripMargin)

      sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arrLen CHECK (SIZE(nested.arr) = 3)")
      sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT mapIntegrity " +
        "CHECK (nested.m[id] = id)")
      val e = intercept[SparkThrowable] {
        sql(s"ALTER TABLE checkConstraintsTest ADD CONSTRAINT violated " +
          s"CHECK (nested.arr[0] < id)")
      }
      checkError(
        exception = e,
        condition = "DELTA_NEW_CHECK_CONSTRAINT_VIOLATION",
        sqlState = Some("23512"),
        parameters = Map(
          "numRows" -> "10",
          "checkConstraint" -> "nested . arr [ 0 ] < id",
          "tableName" -> "spark_catalog.default.checkconstraintstest"
        )
      )
    }
  }


  // TODO: https://github.com/delta-io/delta/issues/831
  test("SET NOT NULL constraint fails") {
    withTable("my_table") {
      sql(createTableSQL("my_table", "id INT"))
      sql("INSERT INTO my_table VALUES (1);")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE my_table CHANGE COLUMN id SET NOT NULL;")
      }
      checkError(
        exception = e,
        condition = "_LEGACY_ERROR_TEMP_2330",
        parameters = Map("fieldName" -> "id"),
        queryContext =
          Array(ExpectedContext("ALTER TABLE my_table CHANGE COLUMN id SET NOT NULL", 0, 49))
      )
    }
  }

  testQuietly("ending semi-colons no longer makes ADD, DROP constraint commands fail") {
    withTable("my_table") {
      sql(createTableSQL("my_table", "birthday DATE"))
      sql("INSERT INTO my_table VALUES ('2021-11-11');")

      sql("ALTER TABLE my_table ADD CONSTRAINT aaa CHECK (birthday > '1900-01-01')")
      sql("ALTER TABLE my_table ADD CONSTRAINT bbb CHECK (birthday > '1900-02-02')")
      sql("ALTER TABLE my_table ADD CONSTRAINT ccc CHECK (birthday > '1900-03-03');") // semi-colon

      sql("ALTER TABLE my_table DROP CONSTRAINT aaa")
      sql("ALTER TABLE my_table DROP CONSTRAINT bbb;") // semi-colon
    }
  }

  test("validate check constraints on table with char/varchar columns") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
        ValidateCheckConstraintsMode.ASSERT.toString,
      SQLConf.READ_SIDE_CHAR_PADDING.key -> "true") {
      withTable("charVarcharConstraintTest") {
        sql(createTableSQL(
          "charVarcharConstraintTest",
          "id INT, name VARCHAR(50), code CHAR(10)",
          props = Map("delta.constraints.positive_id" -> "id > 0")))
        sql("INSERT INTO charVarcharConstraintTest VALUES (1, 'test', 'ABC')")
        checkAnswer(
          sql("SELECT id, name, code FROM charVarcharConstraintTest"),
          Seq(Row(1, "test", "ABC       ")))
      }
    }
  }

  test("constraint induced by varchar") {
    withTable("table") {
      sql(createTableSQL("table", "id INT, value VARCHAR(12)"))
      sql("INSERT INTO table VALUES (1, 'short string')")
      val exception = intercept[SparkThrowable] {
        sql("INSERT INTO table VALUES (2, 'a very long string')")
      }
      checkError(
        exception,
        "DELTA_EXCEED_CHAR_VARCHAR_LIMIT",
        parameters = Map(
          "value" -> "a very long string",
          "expr" -> "((value IS NULL) OR (length(value) <= 12))"
        )
      )
    }
  }

  test("drop table feature") {
    withSQLConf(
        DeltaConfigs.ENABLE_DELETION_VECTORS_CREATION.defaultTablePropertyKey -> false.toString) {
      withTable("table") {
        sql(createTableSQL("table", "a INT, b INT",
          props = Map("delta.feature.checkConstraints" -> "supported")))
        sql("ALTER TABLE table ADD CONSTRAINT c1 CHECK (a > 0)")
        sql("ALTER TABLE table ADD CONSTRAINT c2 CHECK (b > 0)")

        val error1 = intercept[SparkThrowable] {
          sql("ALTER TABLE table DROP FEATURE checkConstraints")
        }
        checkError(
          error1,
          "DELTA_CANNOT_DROP_CHECK_CONSTRAINT_FEATURE",
          parameters = Map("constraints" -> "`c1`, `c2`")
        )
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier("table"))
        val featureNames1 =
          deltaLog.update().protocol.implicitlyAndExplicitlySupportedFeatures.map(_.name)
        assert(featureNames1.contains("checkConstraints"))

        sql("ALTER TABLE table DROP CONSTRAINT c1")
        val error2 = intercept[SparkThrowable] {
          sql("ALTER TABLE table DROP FEATURE checkConstraints")
        }
        checkError(
          error2,
          "DELTA_CANNOT_DROP_CHECK_CONSTRAINT_FEATURE",
          parameters = Map("constraints" -> "`c2`")
        )
        val featureNames2 =
          deltaLog.update().protocol.implicitlyAndExplicitlySupportedFeatures.map(_.name)
        assert(featureNames2.contains("checkConstraints"))

        sql("ALTER TABLE table DROP CONSTRAINT c2")
        sql("ALTER TABLE table DROP FEATURE checkConstraints")
        val featureNames3 =
          deltaLog.update().protocol.implicitlyAndExplicitlySupportedFeatures.map(_.name)
        assert(!featureNames3.contains("checkConstraints"))
      }
    }
  }

  for (expression <- Seq("startsWith", "endsWith", "contains")) {
    test(s"Creating constraints with expressions in the allowList should work for: $expression") {
      withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
        ValidateCheckConstraintsMode.ASSERT.toString) {
        val testTable = "tbl"
        withTable(testTable) {
          sql(createTableSQL(testTable, "id STRING, value BOOLEAN",
            props = Map("delta.feature.checkConstraints" -> "supported")))
          sql(s"ALTER TABLE $testTable ADD CONSTRAINT c1 CHECK (value == $expression(id, 'A'))")
          sql(s"INSERT INTO $testTable VALUES ('ABA', true), ('DEF', false)")
        }
      }
    }
  }

  test("check constraints with LIKE ANY/ALL and NOT LIKE ANY/ALL expressions") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
      ValidateCheckConstraintsMode.ASSERT.toString) {
      val testTable = "like_any_all_test"
      withTable(testTable) {
        sql(createTableSQL(testTable, "id INT, name STRING, code STRING",
          props = Map("delta.feature.checkConstraints" -> "supported")))
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_like_any " +
          "CHECK (name LIKE ANY ('%test%', '%prod%'))")
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_not_like_any " +
          "CHECK (name NOT LIKE ANY ('%forbidden%', '%blocked%'))")
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_like_all " +
          "CHECK (code LIKE ALL ('%A%', '%B%'))")
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_not_like_all " +
          "CHECK (code NOT LIKE ALL ('%X%', '%Y%'))")
        sql(s"INSERT INTO $testTable VALUES (1, 'test_data', 'AB')")
        checkAnswer(
          sql(s"SELECT * FROM $testTable"),
          Seq(Row(1, "test_data", "AB")))
      }
    }
  }

  test("check constraints with timestamp + interval (TimestampAddInterval) expression") {
    assume(
      DeltaTestUtils.sparkVersionBucket(spark) == "4.2+",
      "TimestampAddInterval is only allowlisted in Spark 4.2")
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
      ValidateCheckConstraintsMode.ASSERT.toString) {
      val testTable = "time_add_test"
      withTable(testTable) {
        sql(createTableSQL(testTable, "id INT, event_ts TIMESTAMP",
          props = Map("delta.feature.checkConstraints" -> "supported")))
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_time_add " +
          "CHECK (event_ts > CAST('2020-01-01' AS TIMESTAMP) + INTERVAL 1 DAY)")
        sql(s"INSERT INTO $testTable VALUES (1, '2025-06-15 10:00:00')")
        checkAnswer(
          sql(s"SELECT id FROM $testTable"),
          Seq(Row(1)))
      }
    }
  }

  test("CREATE TABLE with a NULLIF check constraint succeeds") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
      ValidateCheckConstraintsMode.ASSERT.toString) {
      val tableName = "test_create_nullif_constraint"
      withTable(tableName) {
        sql(createTableSQL(
          tableName,
          "id INT, value STRING",
          props = Map("delta.constraints.nullif_value" -> "NULLIF(value, value) IS NULL")))
        sql(s"INSERT INTO $tableName VALUES (1, '')")
      }
    }
  }

  test("check constraints with array_size and array_compact expressions") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
      ValidateCheckConstraintsMode.ASSERT.toString) {
      val testTable = "array_funcs_test"
      withTable(testTable) {
        sql(createTableSQL(testTable, "id INT, tags ARRAY<STRING>",
          props = Map("delta.feature.checkConstraints" -> "supported")))
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_array_size " +
          "CHECK (array_size(tags) > 0)")
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_array_compact " +
          "CHECK (array_size(array_compact(tags)) > 0)")
        sql(s"INSERT INTO $testTable VALUES (1, array('a', 'b'))")
        checkAnswer(
          sql(s"SELECT id FROM $testTable"),
          Seq(Row(1)))
      }
    }
  }

  test("check constraints with array_append, array_prepend, array_insert expressions") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
      ValidateCheckConstraintsMode.ASSERT.toString) {
      val testTable = "array_modify_funcs_test"
      withTable(testTable) {
        sql(createTableSQL(testTable, "id INT, tags ARRAY<STRING>",
          props = Map("delta.feature.checkConstraints" -> "supported")))
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_array_append " +
          "CHECK (array_size(array_append(tags, 'x')) > 1)")
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_array_prepend " +
          "CHECK (array_size(array_prepend(tags, 'y')) > 1)")
        sql(s"ALTER TABLE $testTable ADD CONSTRAINT c_array_insert " +
          "CHECK (array_size(array_insert(tags, 1, 'z')) > 1)")
        sql(s"INSERT INTO $testTable VALUES (1, array('a', 'b'))")
        checkAnswer(
          sql(s"SELECT id FROM $testTable"),
          Seq(Row(1)))
      }
    }
  }

  test("Creating constraints with expressions not in the allowList should throw an error") {
    withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key ->
      ValidateCheckConstraintsMode.ASSERT.toString) {
      val testTable = "tbl"
      withTable(testTable) {
        sql(createTableSQL(testTable, "id INT",
          props = Map("delta.feature.checkConstraints" -> "supported")))
        checkError(
          exception = intercept[SparkThrowable] {
            sql(s"ALTER TABLE $testTable ADD CONSTRAINT c1 " +
              s"CHECK (id > (SELECT max(id) FROM $testTable))")
          },
          "DELTA_UNSUPPORTED_EXPRESSION_CHECK_CONSTRAINT",
          parameters = Map("expression" -> "scalarsubquery()")
        )
      }
    }
  }

  for (isEnabled <- Seq(ValidateCheckConstraintsMode.OFF, ValidateCheckConstraintsMode.ASSERT)) {
    test(s"Reject CHECK constraints with external UDF calls when validation is ${isEnabled}") {
      withSQLConf(DeltaSQLConf.VALIDATE_CHECK_CONSTRAINTS.key -> isEnabled.toString) {
        val testTable = "check_external_udf_test"
        withTable(testTable) {
          withUserDefinedFunction("external_udf" -> true) {
            sql(createTableSQL(testTable, "id INT, value INT",
              props = Map("delta.feature.checkConstraints" -> "supported")))

            spark.udf.register("external_udf", (x: Int) => x > 0)

            val sqlText = s"ALTER TABLE $testTable ADD CONSTRAINT check_external_udf " +
              "CHECK (external_udf(value))"

            if (isEnabled == ValidateCheckConstraintsMode.ASSERT) {
              checkError(
                exception = intercept[SparkThrowable] {
                  sql(sqlText)
                },
                "DELTA_UDF_IN_CHECK_CONSTRAINT",
                parameters = Map("expr" -> "external_udf(knownnotnull(value))")
              )
            } else {
              sql(sqlText)
            }
          }
        }
      }
    }
  }

}
