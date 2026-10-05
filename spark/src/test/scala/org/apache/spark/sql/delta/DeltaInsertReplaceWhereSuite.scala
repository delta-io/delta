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

import java.util.Collections

import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.sources.{DeltaSourceUtils, DeltaSQLConf}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, SupportsOverwrite, V1Write}
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DeltaInsertReplaceWhereSuite extends QueryTest with DeltaSQLCommandTest {

  private object OverwriteApi extends Enumeration {
    val SQL, DataFrameWriterV2 = Value
  }

  private val testTableName = "delta_overwrite"

  private def overwrite(
      api: OverwriteApi.Value,
      predicate: String,
      replacement: DataFrame): Unit = {
    api match {
      case OverwriteApi.SQL =>
        withTempView("replacement") {
          replacement.createOrReplaceTempView("replacement")
          spark.sql(
            s"INSERT INTO $testTableName REPLACE WHERE $predicate SELECT * FROM replacement")
        }
      case OverwriteApi.DataFrameWriterV2 =>
        replacement.writeTo(testTableName).overwrite(expr(predicate))
    }
  }

  private def assertUnsupportedOverwrite(f: => Unit): Unit = {
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
    val version = deltaLog.update().version
    val rows = spark.table(testTableName).collect().toSeq
    checkError(
      exception = intercept[SparkException] { f },
      condition = "_LEGACY_ERROR_TEMP_2245",
      parameters = Map("table" -> "(?s).*"),
      matchPVals = true)
    assert(deltaLog.update().version == version)
    checkAnswer(spark.table(testTableName), rows)
  }

  for {
    enabledFix <- Seq(false, true)
    api <- OverwriteApi.values
  } {
    test(s"V2 conditional overwrite with nested OR and null rows - " +
        s"enabledFix=$enabledFix, api=$api") {
      withSQLConf(
        DeltaSQLConf.REPLACE_WHERE_V2_PREDICATE_CONVERSION_ENABLED.key -> enabledFix.toString) {
        withTable(testTableName) {
          spark.sql(s"CREATE TABLE $testTableName (a INT, b INT) USING delta")
          spark.sql(s"""INSERT INTO $testTableName VALUES
             |(1, 1), (2, 3), (-3, 3), (4, 5), (NULL, 6), (7, NULL), (NULL, NULL)
             |""".stripMargin)
          def runOverwrite(): Unit = {
            overwrite(api, "(a = b OR 2 = a) AND b > 0", spark.sql("SELECT 2 AS a, 2 AS b"))
          }
          if (enabledFix) {
            assertUnsupportedOverwrite { runOverwrite() }
          } else {
            runOverwrite()
            checkAnswer(spark.table(testTableName), Seq(
              Row(1, 1), Row(2, 2), Row(-3, 3), Row(4, 5),
              Row(null, 6), Row(7, null), Row(null, null)))
          }
        }
      }
    }

    test(s"V2 REPLACE WHERE with function and column comparisons - " +
        s"enabledFix=$enabledFix, api=$api") {
      withSQLConf(
        DeltaSQLConf.REPLACE_WHERE_V2_PREDICATE_CONVERSION_ENABLED.key -> enabledFix.toString) {
        withTable(testTableName) {
          spark.sql(s"CREATE TABLE $testTableName (a INT, b INT) USING delta")
          spark.sql(s"INSERT INTO $testTableName VALUES (1, 1), (2, 3), (-3, 3), (4, 5)")
          assertUnsupportedOverwrite {
            overwrite(api, "a = b OR abs(a) = 3", spark.sql("SELECT 3 AS a, 3 AS b"))
          }
        }
      }
    }

    test(s"V2 REPLACE WHERE with quoted and nested partitioned columns - " +
        s"enabledFix=$enabledFix, api=$api") {
      withSQLConf(
        DeltaSQLConf.REPLACE_WHERE_V2_PREDICATE_CONVERSION_ENABLED.key -> enabledFix.toString) {
        withTable(testTableName) {
          spark.sql(s"""CREATE TABLE $testTableName
             |(`a.b` INT, s STRUCT<`odd.name`: INT>, `tick``name` STRING)
             |USING delta PARTITIONED BY (`a.b`)""".stripMargin)
          spark.sql(s"""INSERT INTO $testTableName VALUES
             |(1, named_struct('odd.name', 1), 'match'),
             |(2, named_struct('odd.name', 3), 'replace'),
             |(4, named_struct('odd.name', 5), 'keep')""".stripMargin)
          def runOverwrite(): Unit = {
            overwrite(
              api,
              "`a.b` = s.`odd.name` OR `tick``name` = 'replace'",
              spark.sql("SELECT 2 AS `a.b`, named_struct('odd.name', 2) AS s, " +
                "'replace' AS `tick``name`"))
          }
          if (enabledFix) {
            assertUnsupportedOverwrite { runOverwrite() }
          } else {
            runOverwrite()
            checkAnswer(spark.table(testTableName), Seq(
              Row(1, Row(1), "match"), Row(2, Row(2), "replace"), Row(4, Row(5), "keep")))
          }
        }
      }
    }

    test(s"V2 REPLACE WHERE with supported predicates - enabledFix=$enabledFix, api=$api") {
      withSQLConf(
        DeltaSQLConf.REPLACE_WHERE_V2_PREDICATE_CONVERSION_ENABLED.key -> enabledFix.toString) {
        withTable(testTableName) {
          spark.sql(s"CREATE TABLE $testTableName (a INT, b INT) USING delta")
          spark.sql(s"INSERT INTO $testTableName VALUES (1, 1), (2, 3), (4, 5)")
          overwrite(api, "a = 2 OR a = 4", spark.sql("SELECT 2 AS a, 2 AS b"))
          checkAnswer(spark.table(testTableName), Seq(Row(1, 1), Row(2, 2)))
        }
      }
    }

    test(s"V2 REPLACE WHERE preserves supported OR branches and null matches - " +
        s"enabledFix=$enabledFix, api=$api") {
      withSQLConf(
        DeltaSQLConf.REPLACE_WHERE_V2_PREDICATE_CONVERSION_ENABLED.key -> enabledFix.toString) {
        withTable(testTableName) {
          spark.sql(s"CREATE TABLE $testTableName (a INT, b INT) USING delta")
          spark.sql(s"INSERT INTO $testTableName VALUES (1, 1), (2, 3), (4, 5), (NULL, 6)")
          overwrite(api, "a = 1 OR a = 2 OR a IS NULL", spark.sql("SELECT 2 AS a, 2 AS b"))
          checkAnswer(spark.table(testTableName), Seq(Row(2, 2), Row(4, 5)))
        }
      }
    }
  }

  for (enabledFix <- Seq(false, true)) {
    test(s"V1 conditional overwrite preserves both OR branches - enabledFix=$enabledFix") {
      withSQLConf(
        DeltaSQLConf.REPLACE_WHERE_V2_PREDICATE_CONVERSION_ENABLED.key -> enabledFix.toString) {
        withTable(testTableName) {
          spark.sql(s"CREATE TABLE $testTableName (a INT, b INT) USING delta")
          spark.sql(s"INSERT INTO $testTableName VALUES (1, 1), (2, 3), (4, 5)")
          spark.sql("SELECT 2 AS a, 2 AS b").write.format("delta").mode("overwrite")
            .option("replaceWhere", "a = b OR a = 2").saveAsTable(testTableName)
          checkAnswer(spark.table(testTableName), Seq(Row(2, 2), Row(4, 5)))
        }
      }
    }
  }

  private def predicate(name: String, children: Expression*): Predicate = {
    new Predicate(name, children.toArray)
  }

  private val supported = predicate("=", FieldReference("a"), LiteralValue(2, IntegerType))
  private val unsupported = predicate("=", FieldReference("a"), FieldReference("b"))

  test("strict predicate conversion preserves complete boolean trees") {
    val predicates = Array[Predicate](
      new V2And(
        new V2Or(supported, predicate("IS_NULL", FieldReference("a"))),
        new V2Not(predicate("=", FieldReference("b"), LiteralValue(0, IntegerType)))))
    assert(DeltaSourceUtils.toV1Strict(predicates).toSeq == Seq(
      sources.And(
        sources.Or(sources.EqualTo("a", 2), sources.IsNull("a")),
        sources.Not(sources.EqualTo("b", 0)))))
    assert(DeltaSourceUtils.toV1Strict(Array.empty[Predicate]).isEmpty)
  }

  for ((name, incomplete) <- Seq(
      "unsupported leaf" -> unsupported,
      "unsupported left OR branch" -> new V2Or(unsupported, supported),
      "unsupported right OR branch" -> new V2Or(supported, unsupported),
      "nested OR under AND" -> new V2And(supported, new V2Or(unsupported, supported)),
      "nested OR under NOT" -> new V2Not(new V2Or(unsupported, supported)))) {
    test(s"strict predicate conversion rejects $name") {
      assert(DeltaSourceUtils.toV1Strict(Array[Predicate](incomplete)).isEmpty)
      assert(DeltaSourceUtils.toV1Strict(Array[Predicate](supported, incomplete)).toSeq ==
        Seq(sources.EqualTo("a", 2)))
    }
  }

  test("V2 write builder rejects incomplete predicates before configuring overwrite") {
    withSQLConf(DeltaSQLConf.REPLACE_WHERE_V2_PREDICATE_CONVERSION_ENABLED.key -> "true") {
      withTable(testTableName) {
        spark.sql(s"CREATE TABLE $testTableName (a INT, b INT) USING delta")
        spark.sql(s"INSERT INTO $testTableName VALUES (1, 1), (2, 3), (4, 5)")
        val table = DeltaTableV2(spark, TableIdentifier(testTableName), "REPLACE WHERE")
        val info = new LogicalWriteInfo {
          override def queryId(): String = "strict-overwrite"
          override def schema(): StructType = table.schema()
          override def options(): CaseInsensitiveStringMap =
            new CaseInsensitiveStringMap(Collections.emptyMap[String, String]())
        }
        val builder = table.newWriteBuilder(info).asInstanceOf[SupportsOverwrite]
        for (predicates <- Seq(
            Array[Predicate](new V2Or(unsupported, supported)),
            Array[Predicate](supported, unsupported),
            Array[Predicate](new V2Not(new V2Or(unsupported, supported))))) {
          assert(!builder.canOverwrite(predicates))
          assertUnsupportedOverwrite { builder.overwrite(predicates) }
        }
        builder.build().asInstanceOf[V1Write].toInsertableRelation()
          .insert(spark.sql("SELECT 3 AS a, 3 AS b"), overwrite = false)
        checkAnswer(spark.table(testTableName), Seq(Row(1, 1), Row(2, 3), Row(3, 3), Row(4, 5)))
        assert(builder.canOverwrite(Array[Predicate](supported)))
        assert(builder.canOverwrite(Array.empty[Predicate]))
        assert(builder.overwrite(Array[Predicate](supported)) eq builder)
      }
    }
  }
}
