/*
 * Copyright (2026) The Delta Lake Project Authors.
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

package org.apache.spark.sql.delta

import java.util.UUID

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkException
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.functions.expr

class DeltaBooleanReplaceWhereSuite extends QueryTest with DeltaSQLCommandTest {
  private object ReplaceWhereApi extends Enumeration {
    val Sql = Value("INSERT REPLACE WHERE")
    val DataFrameWriter = Value("DataFrameWriter.option(replaceWhere)")
    val DataFrameWriterV2 = Value("DataFrameWriterV2.overwrite")
  }

  for {
    writeApi <- ReplaceWhereApi.values
    flagEnabled <- Seq(false, true)
  } {
    test("Overwrite: boolean REPLACE WHERE b <=> true, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString,
          DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${UUID.randomUUID()}".replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING delta")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b <=> true"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format("delta").mode("overwrite")
                  .option("replaceWhere", condition).saveAsTable(table)
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(2, false), Row(3, null), Row(4, true)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    test("Overwrite: boolean REPLACE WHERE true <=> b, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString,
          DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${UUID.randomUUID()}".replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING delta")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "true <=> b"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format("delta").mode("overwrite")
                  .option("replaceWhere", condition).saveAsTable(table)
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(2, false), Row(3, null), Row(4, true)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    test("Overwrite: boolean REPLACE WHERE b <=> false, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString,
          DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${UUID.randomUUID()}".replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING delta")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b <=> false"
          val source = "SELECT 4 AS id, false AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format("delta").mode("overwrite")
                  .option("replaceWhere", condition).saveAsTable(table)
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(3, null), Row(4, false)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    test("Overwrite: boolean REPLACE WHERE NOT (b <=> true), " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString,
          DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${UUID.randomUUID()}".replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING delta")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "NOT (b <=> true)"
          val source = "SELECT 4 AS id, false AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format("delta").mode("overwrite")
                  .option("replaceWhere", condition).saveAsTable(table)
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(4, false)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    test("Overwrite: boolean REPLACE WHERE b IN (true, false), " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString,
          DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${UUID.randomUUID()}".replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING delta")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b IN (true, false)"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format("delta").mode("overwrite")
                  .option("replaceWhere", condition).saveAsTable(table)
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            overwrite()
            checkAnswer(spark.table(table), Seq(Row(3, null), Row(4, true)))
          } else {
            val error = intercept[SparkException] {
              overwrite()
            }
            assert(error.getMessage.contains("Table does not support overwrite by expression"))
            checkAnswer(spark.table(table), Seq(Row(1, true), Row(2, false), Row(3, null)))
          }
        }
      }
    }

    test("Overwrite: boolean REPLACE WHERE b <=> true OR id = 4, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString,
          DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${UUID.randomUUID()}".replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING delta")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "b <=> true OR id = 4"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          def overwrite(): Unit = {
            writeApi match {
              case ReplaceWhereApi.Sql =>
                sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
              case ReplaceWhereApi.DataFrameWriter =>
                replacement.write.format("delta").mode("overwrite")
                  .option("replaceWhere", condition).saveAsTable(table)
              case ReplaceWhereApi.DataFrameWriterV2 =>
                replacement.writeTo(table).overwrite(expr(condition))
            }
          }

          overwrite()
          if (flagEnabled || writeApi == ReplaceWhereApi.DataFrameWriter) {
            checkAnswer(spark.table(table), Seq(Row(2, false), Row(3, null), Row(4, true)))
          } else {
            checkAnswer(
              spark.table(table),
              Seq(Row(1, true), Row(2, false), Row(3, null), Row(4, true)))
          }
        }
      }
    }

    test("Overwrite: boolean REPLACE WHERE true, " +
        s"$writeApi, flagEnabled=$flagEnabled") {
      withSQLConf(
          DeltaSQLConf.V2_EXPRESSION_BUILDER_PRESERVE_BOOLEAN_LITERALS_ENABLED.key ->
            flagEnabled.toString,
          DeltaSQLConf.V2_ENABLE_MODE.key -> "NONE") {
        val table = s"$SESSION_CATALOG_NAME.default.rw_${UUID.randomUUID()}".replace("-", "_")
        withTable(table) {
          sql(s"CREATE TABLE $table (id INT, b BOOLEAN) USING delta")
          sql(s"INSERT INTO $table VALUES (1, true), (2, false), (3, NULL)")
          val condition = "true"
          val source = "SELECT 4 AS id, true AS b"
          val replacement = sql(source)

          writeApi match {
            case ReplaceWhereApi.Sql =>
              sql(s"INSERT INTO $table REPLACE WHERE $condition $source")
            case ReplaceWhereApi.DataFrameWriter =>
              replacement.write.format("delta").mode("overwrite")
                .option("replaceWhere", condition).saveAsTable(table)
            case ReplaceWhereApi.DataFrameWriterV2 =>
              replacement.writeTo(table).overwrite(expr(condition))
          }
          checkAnswer(spark.table(table), Seq(Row(4, true)))
        }
      }
    }
  }
}
