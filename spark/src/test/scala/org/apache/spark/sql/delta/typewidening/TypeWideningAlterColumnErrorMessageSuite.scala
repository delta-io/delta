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

package org.apache.spark.sql.delta.typewidening

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/**
 * Tests that ALTER TABLE CHANGE COLUMN TYPE, when rejected on a table that does not have the
 * type widening table feature enabled, surfaces the actionable
 * [[DeltaErrors.alterColumnTypeWideningNotEnabledException]] whenever the requested change is on
 * the supported [[TypeWidening.isTypeChangeSupported]] allow-list — and continues to surface the
 * pre-existing DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP for genuinely unsupported changes.
 *
 * Deliberately does NOT mix in [[TypeWideningTestMixin]] — that mixin enables type widening by
 * default at the SparkConf level, and every case here needs to start from the default protocol
 * (reader=1, writer=2) with type widening off.
 */
class TypeWideningAlterColumnErrorMessageSuite
  extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  private def withDeltaTable(schema: String)(body: String => Unit): Unit = {
    withTable("t") {
      sql(s"CREATE TABLE t ($schema) USING delta")
      body("t")
    }
  }

  /** Widening pairs from [[TypeWidening.isTypeChangeSupported]] that should surface the new hint. */
  private val supportedWideningPairs: Seq[(String, DataType, DataType)] = Seq(
    ("byte -> short",           ByteType,             ShortType),
    ("byte -> int",             ByteType,             IntegerType),
    ("byte -> long",            ByteType,             LongType),
    ("short -> int",            ShortType,            IntegerType),
    ("short -> long",           ShortType,            LongType),
    ("int -> long",             IntegerType,          LongType),
    ("float -> double",         FloatType,            DoubleType),
    ("date -> timestamp_ntz",   DateType,             TimestampNTZType),
    ("int -> double",           IntegerType,          DoubleType),
    ("decimal(5,2) -> decimal(10,2)", DecimalType(5, 2), DecimalType(10, 2)))

  for ((label, from, to) <- supportedWideningPairs) {
    test(s"hint fires for supported widening $label when feature is disabled") {
      withDeltaTable(s"c ${from.sql}") { t =>
        checkError(
          intercept[DeltaAnalysisException] {
            sql(s"ALTER TABLE $t CHANGE COLUMN c TYPE ${to.sql}")
          },
          "DELTA_ALTER_COLUMN_TYPE_WIDENING_NOT_ENABLED",
          parameters = Map(
            "fieldPath" -> "c",
            "fromType"  -> from.sql,
            "toType"    -> to.sql,
            "tableName" -> "spark_catalog.default.t"))
      }
    }
  }

  test("original DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP is preserved for non-widening " +
      "changes that Delta rejects post-analysis") {
    // INT -> STRING is an upcast (so Spark's analyzer allows it through to Delta) but is NOT in the
    // TypeWidening allow-list — must fall through to the pre-existing generic error.
    withDeltaTable("c INT") { t =>
      checkError(
        intercept[DeltaAnalysisException] {
          sql(s"ALTER TABLE $t CHANGE COLUMN c TYPE STRING")
        },
        "DELTA_UNSUPPORTED_ALTER_TABLE_CHANGE_COL_OP",
        parameters = Map(
          "fieldPath" -> "c",
          "oldField"  -> "INT",
          "newField"  -> "STRING"))
    }
  }

  test("ALTER succeeds when type widening is already enabled — no hint fires") {
    withTable("t") {
      sql("CREATE TABLE t (c INT) USING delta " +
            "TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
      sql("ALTER TABLE t CHANGE COLUMN c TYPE BIGINT")
      val df = sql("DESCRIBE TABLE t")
      // The column must now be BIGINT.
      assert(spark.table("t").schema("c").dataType === LongType)
      // A subsequent write of a value that would have overflowed INT succeeds.
      sql("INSERT INTO t VALUES (9999999999)")
      checkAnswer(sql("SELECT c FROM t"), Row(9999999999L) :: Nil)
    }
  }

  test("hint uses the logical column name when column mapping is enabled") {
    withTable("t") {
      sql(
        """
          |CREATE TABLE t (my_col INT) USING delta
          |TBLPROPERTIES (
          |  'delta.columnMapping.mode' = 'name',
          |  'delta.minReaderVersion' = '2',
          |  'delta.minWriterVersion' = '5'
          |)
          |""".stripMargin)
      checkError(
        intercept[DeltaAnalysisException] {
          sql("ALTER TABLE t CHANGE COLUMN my_col TYPE BIGINT")
        },
        "DELTA_ALTER_COLUMN_TYPE_WIDENING_NOT_ENABLED",
        parameters = Map(
          "fieldPath" -> "my_col",
          "fromType"  -> "INT",
          "toType"    -> "BIGINT",
          "tableName" -> "spark_catalog.default.t"))
    }
  }

  test("hint message body names the exact remediation SQL") {
    withDeltaTable("c INT") { t =>
      val ex = intercept[DeltaAnalysisException] {
        sql(s"ALTER TABLE $t CHANGE COLUMN c TYPE BIGINT")
      }
      assert(ex.getErrorClass === "DELTA_ALTER_COLUMN_TYPE_WIDENING_NOT_ENABLED")
      val msg = ex.getMessage
      // Guardrail: the remediation SQL fragment must appear verbatim so users can copy-paste it.
      assert(
        msg.contains("SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')"),
        s"expected remediation SQL in message, got:\n$msg")
      // And the exact from -> to pair renders correctly.
      assert(msg.contains("INT"), s"expected fromType INT in message, got:\n$msg")
      assert(msg.contains("BIGINT"), s"expected toType BIGINT in message, got:\n$msg")
    }
  }
}
