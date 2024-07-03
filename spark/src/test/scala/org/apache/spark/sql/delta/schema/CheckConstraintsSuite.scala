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

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.constraints.CharVarcharConstraint
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, IntegerType, MapType, StringType, StructField, StructType}

class CheckConstraintsSuite extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest
    with DeltaSQLTestUtils {


  import testImplicits._

  private def withTestTable(thunk: String => Unit) = {
    withSQLConf(
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "3") {
      withTable("checkConstraintsTest") {
        Seq(
          (1, "a"), (2, "b"), (3, "c"),
          (4, "d"), (5, "e"), (6, "f")
        ).toDF("num", "text").write.format("delta").saveAsTable("checkConstraintsTest")
        thunk("checkConstraintsTest")
      }
    }
  }

  private def errorContains(errMsg: String, str: String): Unit = {
    errMsg.contains(str)
  }

  test("can't add unparseable constraint") {
    withTestTable { table =>
      val e = intercept[ParseException] {
        sql(s"ALTER TABLE $table\nADD CONSTRAINT lessThan5 CHECK (id <)")
      }
      // Make sure we're still getting a useful parse error, even though we do some complicated
      // internal stuff to persist the constraint. Unfortunately this test may be a bit fragile.
      errorContains(e.getMessage, "Syntax error at or near end of input")
      errorContains(e.getMessage,
        """
          |== SQL ==
          |id <
          |----^^^
          |""".stripMargin)
    }
  }

  test("constraint must be boolean") {
    withTestTable { table =>
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $table ADD CONSTRAINT integerVal CHECK (3)")
        },
        errorClass = "DELTA_NON_BOOLEAN_CHECK_CONSTRAINT",
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
        exception = intercept[AnalysisException] {
          sql(s"ALTER TABLE $table ADD CONSTRAINT c CHECK (does_not_exist)")
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
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
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT trivial CHECK (true)")
      }
      errorContains(e.getMessage,
        s"Constraint 'trivial' already exists as a CHECK constraint. Please delete the " +
          s"old constraint first.\nOld constraint:\ntrue")
    }
  }

  test("can't add constraint with names that are reserved for internal usage") {
    withTestTable { table =>
      val reservedName = CharVarcharConstraint.INVARIANT_NAME
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT $reservedName CHECK (true)")
      }
      errorContains(e.getMessage, s"Cannot use '$reservedName' as the name of a CHECK constraint")
    }
  }

  test("duplicate constraint check is case insensitive") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT trivial CHECK (true)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT TRIVIAL CHECK (true)")
      }
      errorContains(e.getMessage,
        s"Constraint 'TRIVIAL' already exists as a CHECK constraint. Please delete the " +
          s"old constraint first.\nOld constraint:\ntrue")
    }
  }

  testQuietly("can't add already violated constraint") {
    withTestTable { table =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $table ADD CONSTRAINT lessThan5 CHECK (num < 5 and text < 'd')")
      }
      errorContains(e.getMessage,
        s"violate the new CHECK constraint (num < 5 and text < 'd')")
    }
  }

  testQuietly("can't add row violating constraint") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT lessThan10 CHECK (num < 10 and text < 'g')")
      sql(s"INSERT INTO $table VALUES (5, 'a')")
      val e = intercept[InvariantViolationException] {
        sql(s"INSERT INTO $table VALUES (11, 'a')")
      }
      errorContains(e.getMessage,
        s"CHECK constraint lessthan10 ((num < 10) AND (text < 'g')) violated")
    }
  }

  test("drop constraint that doesn't exist throws an exception") {
    withTestTable { table =>
      intercept[AnalysisException] {
        sql(s"ALTER TABLE $table DROP CONSTRAINT myConstraint")
      }
    }

    withSQLConf((DeltaSQLConf.DELTA_ASSUMES_DROP_CONSTRAINT_IF_EXISTS.key, "false")) {
      withTestTable { table =>
        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE $table DROP CONSTRAINT myConstraint")
        }
        assert(e.getErrorClass == "DELTA_CONSTRAINT_DOES_NOT_EXIST")
        errorContains(e.getMessage,
          "nonexistent constraint myconstraint from table `default`.`checkconstraintstest`")
        errorContains(e.getMessage,
          "databricks.spark.delta.constraints.assumesDropIfExists.enabled to true")
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
      intercept[InvariantViolationException] {
        sql(s"INSERT INTO $table VALUES (11, 'a')")
      }
      sql(s"ALTER TABLE $table DROP CONSTRAINT lessThan10")
      sql(s"INSERT INTO $table VALUES (11, 'a')")
      checkAnswer(sql(s"SELECT num FROM $table"), Seq(1, 2, 3, 4, 5, 6, 11).toDF())
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
        Seq(("ADD CONSTRAINT", Map("name" -> "lessThan10", "expr" -> "num < 10"))).toDF())

      sql(s"ALTER TABLE $table DROP CONSTRAINT IF EXISTS lessThan10")
      checkAnswer(
        sql(s"DESCRIBE HISTORY $table")
          .where("operation = 'DROP CONSTRAINT'")
          .selectExpr("operation", "operationParameters"),
        Seq((
          "DROP CONSTRAINT",
          Map("name" -> "lessThan10", "expr" -> "num < 10", "existed" -> "true")
        )).toDF())
      sql(s"ALTER TABLE $table DROP CONSTRAINT IF EXISTS lessThan10")
        checkAnswer(
          sql(s"DESCRIBE HISTORY $table")
            .where("operation = 'DROP CONSTRAINT'")
            .selectExpr("operation", "operationParameters"),
          Seq(
            ("DROP CONSTRAINT",
              Map("name" -> "lessThan10", "expr" -> "num < 10", "existed" -> "true")),
            ("DROP CONSTRAINT",
              Map("name" -> "lessThan10", "existed" -> "false"))
          ).toDF())
    }
  }

  testQuietly("constraint on builtin methods") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT textSize CHECK (LENGTH(text) < 10)")
      sql(s"INSERT INTO $table VALUES (11, 'abcdefg')")
      val e = intercept[InvariantViolationException] {
        sql(s"INSERT INTO $table VALUES (12, 'abcdefghijklmnop')")
      }
      errorContains(e.getMessage, "constraint textsize (LENGTH(text) < 10) violated by row")
    }
  }

  testQuietly("constraint with implicit casts") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT maxWithImplicitCast CHECK (num < '10')")
      val e = intercept[InvariantViolationException] {
        sql(s"INSERT INTO $table VALUES (11, 'data')")
      }
      errorContains(e.getMessage, "constraint maxwithimplicitcast (num < '10') violated by row")
    }
  }

  testQuietly("constraint with nested parentheses") {
    withTestTable { table =>
      sql(s"ALTER TABLE $table ADD CONSTRAINT maxWithParens " +
        s"CHECK (( (num < '10') AND ((LENGTH(text)) < 100) ))")
      val e = intercept[InvariantViolationException] {
        sql(s"INSERT INTO $table VALUES (11, 'data')")
      }
      errorContains(e.getMessage,
        "constraint maxwithparens ((num < '10') AND (LENGTH(text) < 100)) violated by row")
    }
  }

  for (expression <- Seq("year(current_date())", "unix_timestamp()"))
  testQuietly(s"constraint with analyzer-evaluated expressions. Expression: $expression") {
    withTestTable { table =>
      // We use current_timestamp()/current_date() as the most convenient
      // analyzer-evaluated expressions - of course in a realistic use case
      // it'd probably not be right to add a constraint on a
      // nondeterministic expression.
      sql(s"ALTER TABLE $table ADD CONSTRAINT maxWithAnalyzerEval " +
        s"CHECK (num < $expression)")
      val e = intercept[InvariantViolationException] {
        sql(s"INSERT INTO $table VALUES (${Int.MaxValue}, 'data')")
      }
      errorContains(e.getMessage,
        s"maxwithanalyzereval (num < $expression) violated by row")
    }
  }

  testQuietly("constraints with nulls") {
    withSQLConf(
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "3") {
      withTable("checkConstraintsTest") {
        val rows = Range(0, 10).map { i =>
          Row(
            i,
            null,
            Row("constantWithinStruct", Map(i -> i), Array(i, null, i + 2)))
        }

        val schema = new StructType(Array(
          StructField("id", IntegerType),
          StructField("text", StringType),
          StructField("nested", new StructType(Array(
            StructField("constant", StringType),
            StructField("m", MapType(IntegerType, IntegerType, valueContainsNull = true)),
            StructField("arr", ArrayType(IntegerType, containsNull = true)))))))
        spark.createDataFrame(rows.toList.asJava, schema)
          .write.format("delta").saveAsTable("checkConstraintsTest")

        // Constraints checking for a null value should work.
        sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT textNull CHECK (text IS NULL)")
        sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arr1Null " +
          "CHECK (nested.arr[1] IS NULL)")

        // Constraints incompatible with a null value will of course fail, but they should fail with
        // the same clear error as normal.
        var e: Exception = intercept[AnalysisException] {
          sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arrLessThan5 " +
            "CHECK (nested.arr[1] < 5)")
        }
        errorContains(e.getMessage,
          s"10 rows in default.checkconstraintstest violate the new CHECK constraint " +
            s"(nested . arr [ 1 ] < 5)")

        // Adding a null value into a constraint should fail similarly, even if it's null
        // because a parent field is null.
        sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arr0 " +
          "CHECK (nested.arr[0] < 100)")
        val newRows = Seq(
          Row(10, null, Row("c", Map(10 -> null), Array(null, null, 12))),
          Row(11, null, Row("c", Map(11 -> null), null)),
          Row(12, null, null))
        newRows.foreach { r =>
          e = intercept[InvariantViolationException] {
            spark.createDataFrame(List(r).asJava, schema)
              .write.format("delta").mode("append").saveAsTable("checkConstraintsTest")
          }
          errorContains(e.getMessage,
            "CHECK constraint arr0 (nested.arr[0] < 100) violated by row")
        }

        // On the other hand, existing constraints like arr1Null which do allow null values should
        // permit new rows even if the value's parent is null.
        sql("ALTER TABLE checkConstraintsTest DROP CONSTRAINT arr0")
        newRows.foreach { r =>
          spark.createDataFrame(List(r).asJava, schema)
            .write.format("delta").mode("append").saveAsTable("checkConstraintsTest")
        }
        checkAnswer(
          spark.read.format("delta").table("checkConstraintsTest").select("id"),
          (0 to 12).toDF("id"))
      }
    }
  }

  testQuietly("complex constraints") {
    withSQLConf(
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
      DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "3") {
      withTable("checkConstraintsTest") {
        val rows = Range(0, 10).map { i =>
          Row(
            i,
            ('a' + i).toString,
            Row("constantWithinStruct", Map(i -> i), Array(i, i + 1, i + 2)))
        }
        val schema = new StructType(Array(
          StructField("id", IntegerType),
          StructField("text", StringType),
          StructField("nested", new StructType(Array(
            StructField("constant", StringType),
            StructField("m", MapType(IntegerType, IntegerType, valueContainsNull = false)),
            StructField("arr", ArrayType(IntegerType, containsNull = false)))))))
        spark.createDataFrame(rows.toList.asJava, schema)
          .write.format("delta").saveAsTable("checkConstraintsTest")
        sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT arrLen CHECK (SIZE(nested.arr) = 3)")
        sql("ALTER TABLE checkConstraintsTest ADD CONSTRAINT mapIntegrity " +
          "CHECK (nested.m[id] = id)")
        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE checkConstraintsTest ADD CONSTRAINT violated " +
            s"CHECK (nested.arr[0] < id)")
        }
        errorContains(e.getMessage,
          s"violate the new CHECK constraint (nested . arr [ 0 ] < id)")
      }
    }
  }


  // TODO: https://github.com/delta-io/delta/issues/831
  test("SET NOT NULL constraint fails") {
    withTable("my_table") {
      sql("CREATE TABLE my_table (id INT) USING DELTA;")
      sql("INSERT INTO my_table VALUES (1);")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE my_table CHANGE COLUMN id SET NOT NULL;")
      }.getMessage()
      assert(e.contains("Cannot change nullable column to non-nullable"))
    }
  }

  testQuietly("ending semi-colons no longer makes ADD, DROP constraint commands fail") {
    withTable("my_table") {
      sql("CREATE TABLE my_table (birthday DATE) USING DELTA;")
      sql("INSERT INTO my_table VALUES ('2021-11-11');")

      sql("ALTER TABLE my_table ADD CONSTRAINT aaa CHECK (birthday > '1900-01-01')")
      sql("ALTER TABLE my_table ADD CONSTRAINT bbb CHECK (birthday > '1900-02-02')")
      sql("ALTER TABLE my_table ADD CONSTRAINT ccc CHECK (birthday > '1900-03-03');") // semi-colon

      sql("ALTER TABLE my_table DROP CONSTRAINT aaa")
      sql("ALTER TABLE my_table DROP CONSTRAINT bbb;") // semi-colon
    }
  }

  test("constraint induced by varchar") {
    withTable("table") {
      sql("CREATE TABLE table (id INT, value VARCHAR(12)) USING DELTA")
      sql("INSERT INTO table VALUES (1, 'short string')")
      val exception = intercept[DeltaInvariantViolationException] {
        sql("INSERT INTO table VALUES (2, 'a very long string')")
      }
      checkError(
        exception,
        errorClass = "DELTA_EXCEED_CHAR_VARCHAR_LIMIT",
        parameters = Map(
          "value" -> "a very long string",
          "expr" -> "((value IS NULL) OR (length(value) <= 12))"
        )
      )
    }
  }

  test("drop table feature") {
    withTable("table") {
      sql("CREATE TABLE table (a INT, b INT) USING DELTA " +
        "TBLPROPERTIES ('delta.feature.checkConstraints' = 'supported')")
      sql("ALTER TABLE table ADD CONSTRAINT c1 CHECK (a > 0)")
      sql("ALTER TABLE table ADD CONSTRAINT c2 CHECK (b > 0)")

      val error1 = intercept[AnalysisException] {
        sql("ALTER TABLE table DROP FEATURE checkConstraints")
      }
      checkError(
        error1,
        errorClass = "DELTA_CANNOT_DROP_CHECK_CONSTRAINT_FEATURE",
        parameters = Map("constraints" -> "`c1`, `c2`")
      )
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("table"))
      assert(deltaLog.update().protocol === Protocol(1, 3))

      sql("ALTER TABLE table DROP CONSTRAINT c1")
      val error2 = intercept[AnalysisException] {
        sql("ALTER TABLE table DROP FEATURE checkConstraints")
      }
      checkError(
        error2,
        errorClass = "DELTA_CANNOT_DROP_CHECK_CONSTRAINT_FEATURE",
        parameters = Map("constraints" -> "`c2`")
      )
      assert(deltaLog.update().protocol === Protocol(1, 3))

      sql("ALTER TABLE table DROP CONSTRAINT c2")
      sql("ALTER TABLE table DROP FEATURE checkConstraints")
      assert(deltaLog.update().protocol === Protocol(1, 2))
    }
  }
}
