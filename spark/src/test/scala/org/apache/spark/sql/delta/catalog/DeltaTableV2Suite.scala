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

package org.apache.spark.sql.delta.catalog

import java.util.{HashMap => JHashMap}

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DummySessionCatalog}
import org.apache.spark.sql.delta.util.CatalogTableTestUtils

/**
 * Tests for [[DeltaTableV2]] behavior that must not regress for security and debuggability.
 */
class DeltaTableV2Suite extends QueryTest with DeltaSQLCommandTest {

  test("DeltaTableV2.toString does not include CatalogTable storage properties or secrets") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val storageProps = new JHashMap[String, String]()
      storageProps.put("fs.unitycatalog.sensitive.token", "secret-token-must-not-appear")
      storageProps.put("fs.s3a.access.key", "AKIA_TEST_KEY")
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "sensitive_tbl",
        storageProperties = storageProps,
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(spark, path, catalogTable = Some(catalogTable))
      val str = table.toString
      assert(!str.contains("secret-token-must-not-appear"))
      assert(!str.contains("AKIA_TEST_KEY"))
      assert(!str.contains("fs.unitycatalog"))
      assert(!str.contains("fs.s3a"))
      assert(!str.contains("Storage Properties"))
    }
  }

  test("DeltaTableV2.toString does not include SparkSession or raw options") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "opts_tbl",
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(
        spark,
        path,
        catalogTable = Some(catalogTable),
        options = Map("option.fs.leaked" -> "bad-option-value-should-not-appear"))
      val str = table.toString
      assert(!str.contains("SparkSession"))
      assert(!str.contains("bad-option-value-should-not-appear"))
      assert(!str.contains("option.fs"))
    }
  }

  test("DeltaTableV2.toString includes table name and path") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "named_table",
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(spark, path, catalogTable = Some(catalogTable))
      val str = table.toString
      assert(str.startsWith("DeltaTableV2("))
      assert(str.contains("named_table"))
      assert(str.contains(path.toString))
    }
  }

  test("DeltaTableV2.toString for path-only table (no catalogTable)") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      val path = new Path(dir.toURI)
      val table = DeltaTableV2(spark, path)
      val str = table.toString
      assert(str.startsWith("DeltaTableV2("))
      assert(str.contains(path.toString))
      assert(!str.contains("CatalogTable("))
      assert(!str.contains("SparkSession"))
    }
  }

  test("DeltaTableV2.toString for tableIdentifier-only " +
      "(no catalogTable)") {
    withTempDir { dir =>
      spark.range(1).write.format("delta")
        .save(dir.getCanonicalPath)
      val path = new Path(dir.toURI)
      val table = DeltaTableV2(
        spark,
        path,
        catalogTable = None,
        tableIdentifier = Some("myIdent"))
      val str = table.toString
      assert(str.startsWith("DeltaTableV2("))
      assert(str.contains("myIdent"))
      assert(str.contains(path.toString))
      assert(!str.contains("CatalogTable("))
      assert(!str.contains("SparkSession"))
    }
  }

  // Defense-in-depth: EXPLAIN plans stringify CatalogTable
  // via Spark's stringArgsForCatalogTable (identifier-only).
  // This test validates that no plan node in the EXPLAIN
  // output leaks fs.* storage properties, complementing
  // the SHOW CREATE TABLE test which directly exercises
  // DeltaTableV2.toString via ExtendedAnalysisException.
  test("EXPLAIN output does not leak storage credentials") {
    spark.sessionState.catalogManager.reset()
    try {
      withSQLConf(
        "spark.sql.catalog.spark_catalog" ->
          classOf[DummySessionCatalog].getName) {
        withTable("t") {
          withTempPath { path =>
            spark.range(10).write.format("delta")
              .save(path.getCanonicalPath)
            sql(s"CREATE TABLE t (id LONG) USING delta " +
              s"LOCATION '${path.getCanonicalPath}'")
            val explained = sql("EXPLAIN EXTENDED SELECT * FROM t")
              .collect().map(_.getString(0)).mkString("\n")
            assert(!explained.contains("fs.myKey"), explained)
            assert(!explained.contains("CatalogTable("), explained)
            assert(
              !explained.contains("Storage Properties"),
              explained)
          }
        }
      }
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }

  /**
   * Spark may append the analyzed plan to `AnalysisException.getMessage` (see
   * `ExtendedAnalysisException`). With catalog-injected `fs.myKey` on the table (see
   * `DummySessionCatalog` in tests), the plan text must not dump secrets.
   */
  test("SHOW CREATE TABLE exception message does not leak CatalogTable storage / fs.* props") {
    spark.sessionState.catalogManager.reset()
    try {
      withSQLConf("spark.sql.catalog.spark_catalog" -> classOf[DummySessionCatalog].getName) {
        withTable("t") {
          withTempPath { path =>
            spark.range(10).write.format("delta").save(path.getCanonicalPath)
            sql(s"CREATE TABLE t (id LONG) USING delta LOCATION '${path.getCanonicalPath}'")
            val e = intercept[AnalysisException] {
              sql("SHOW CREATE TABLE t").collect()
            }
            val msg = e.getMessage
            assert(
              msg.contains("SHOW CREATE TABLE") && msg.contains("not supported"),
              s"expected unsupported SHOW CREATE TABLE in message: $msg")
            assert(!msg.contains("fs.myKey"), msg)
            assert(!msg.contains("Storage Properties"), msg)
            assert(!msg.contains("CatalogTable("), msg)
            assert(!e.toString.contains("fs.myKey"), e.toString)
            assert(!e.toString.contains("CatalogTable("), e.toString)
            assert(!e.toString.contains("Storage Properties"), e.toString)
          }
        }
      }
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }
}
