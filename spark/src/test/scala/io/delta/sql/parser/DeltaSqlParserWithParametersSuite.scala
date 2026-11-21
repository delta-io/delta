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

package io.delta.sql.parser

import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.shims.ParameterizedQueryShim

import org.apache.spark.sql.Row

/**
 * Session-level regression tests for parameterized SQL (`spark.sql(text, args)`) with the Delta
 * extension installed.
 */
class DeltaSqlParserWithParametersSuite extends DeltaSQLCommandTest {

  test("spark.sql with named parameters works with the Delta extension") {
    assert(spark.sql("SELECT :x AS v", Map("x" -> 1)).collect().toSeq === Seq(Row(1)))
  }

  test("spark.sql with positional parameters works with the Delta extension") {
    assert(spark.sql("SELECT ? AS v, ? AS w", Array[Any](1, "hello")).collect().toSeq ===
      Seq(Row(1, "hello")))
  }

  test("spark.sql creates a clustered table with variables and parameters") {
    assume(ParameterizedQueryShim.supportsParserParameterSubstitution)
    withTable("parser_clustered_table") {
      withConf("parser.table" -> "parser_clustered_table", "parser.column" -> "a",
          "parser.literal" -> "EXPANDED") {
        spark.sql(
          "CREATE TABLE ${parser.table} USING DELTA COMMENT :comment " +
            "CLUSTER BY (${parser.column}) AS SELECT :value AS a",
          Map("comment" -> "parameterized table", "value" -> "${parser.literal}"))
        assert(spark.table("parser_clustered_table").collect().toSeq ===
          Seq(Row("${parser.literal}")))
        assert(spark.sql("DESCRIBE DETAIL parser_clustered_table")
          .select("clusteringColumns").collect().toSeq === Seq(Row(Seq("a"))))
      }
    }
  }

  test("named parameter in an IDENTIFIER clause creates a database") {
    assume(ParameterizedQueryShim.supportsParserParameterSubstitution)
    try {
      spark.sql("CREATE DATABASE IF NOT EXISTS IDENTIFIER(:name)", Map("name" -> "parser_test_db"))
      assert(spark.catalog.databaseExists("parser_test_db"))
    } finally {
      spark.sql("DROP DATABASE IF EXISTS parser_test_db")
    }
  }

  test("clustered CTAS expands SQL variables only once") {
    withTable("parser_variable_table") {
      withConf("parser.dollar" -> "$", "parser.name" -> "EXPANDED") {
        spark.sql("CREATE TABLE parser_variable_table USING DELTA CLUSTER BY (a) " +
          "AS SELECT '${parser.dollar}{parser.name}' AS a")
        assert(spark.table("parser_variable_table").collect().toSeq ===
          Seq(Row("${parser.name}")))
      }
    }
  }

  test("parameter values containing variable references are not expanded") {
    withConf("review.param" -> "EXPANDED") {
      assert(spark.sql("SELECT :x AS v", Map("x" -> "${review.param}")).collect().toSeq ===
        Seq(Row("${review.param}")))
    }
  }

  test("legacy parameter substitution mode still works") {
    withConf("spark.sql.legacy.parameterSubstitution.constantsOnly" -> "true") {
      assert(spark.sql("SELECT :x AS v", Map("x" -> 1)).collect().toSeq === Seq(Row(1)))
    }
  }

  test("Delta commands with ':' in paths work through the empty-argument path") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      assert(spark.sql(s"DESCRIBE DETAIL 'file://${dir.getCanonicalPath}'").collect().nonEmpty)
    }
  }
}
