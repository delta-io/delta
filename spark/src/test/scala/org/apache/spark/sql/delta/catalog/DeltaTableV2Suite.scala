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
 * Tests for [[DeltaTableV2.toString]] credential filtering behavior.
 *
 * The toString override filters `fs.*` storage properties from the embedded CatalogTable to
 * prevent catalog-injected credentials from leaking into exception messages, EXPLAIN output,
 * and logs. It reuses [[DeltaTableV2.HIDDEN_STORAGE_PROPERTY_PREFIXES]], the same constant
 * that guards [[DeltaTableV2.properties()]].
 */
class DeltaTableV2Suite extends QueryTest with DeltaSQLCommandTest {

  // ---------------------------------------------------------------------------
  // Negative tests: fs.* credential keys and values must NOT appear
  // ---------------------------------------------------------------------------

  test("toString filters fs.* storage property keys and values") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val storageProps = new JHashMap[String, String]()
      storageProps.put("fs.unitycatalog.sensitive.token", "secret-token-must-not-appear")
      storageProps.put("fs.s3a.access.key", "AKIA_TEST_KEY")
      storageProps.put("fs.s3a.init.session.token", "session-token-value")
      storageProps.put("fs.unitycatalog.uri", "https://internal.endpoint/")
      storageProps.put("fs.unitycatalog.table.id", "table-uuid-123")
      storageProps.put("fs.s3a.init.credential.expired.time", "1774050215000")
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "sensitive_tbl",
        storageProperties = storageProps,
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(spark, path, catalogTable = Some(catalogTable))
      val str = table.toString

      assert(!str.contains("secret-token-must-not-appear"),
        "fs.* credential values must not appear in toString")
      assert(!str.contains("AKIA_TEST_KEY"),
        "fs.s3a access key value must not appear in toString")
      assert(!str.contains("session-token-value"),
        "fs.s3a session token value must not appear in toString")
      assert(!str.contains("https://internal.endpoint/"),
        "fs.unitycatalog.uri value must not appear in toString")
      assert(!str.contains("table-uuid-123"),
        "fs.unitycatalog.table.id value must not appear in toString")
      assert(!str.contains("1774050215000"),
        "fs.s3a credential expiry value must not appear in toString")
      assert(!str.contains("fs.unitycatalog"),
        "fs.unitycatalog.* keys must not appear in toString")
      assert(!str.contains("fs.s3a"),
        "fs.s3a.* keys must not appear in toString")
    }
  }

  test("toString filters fs.* but not non-fs storage properties") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val storageProps = new JHashMap[String, String]()
      storageProps.put("fs.s3a.access.key", "AKIA_SHOULD_BE_HIDDEN")
      storageProps.put("delta.minReaderVersion", "3")
      storageProps.put("io.unitycatalog.tableId", "safe-table-id")
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "mixed_props_tbl",
        storageProperties = storageProps,
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(spark, path, catalogTable = Some(catalogTable))
      val str = table.toString

      assert(!str.contains("AKIA_SHOULD_BE_HIDDEN"),
        "fs.* values must be filtered")
      assert(!str.contains("fs.s3a"),
        "fs.* keys must be filtered")
      assert(str.contains("delta.minReaderVersion"),
        "non-fs storage properties must be preserved")
      assert(str.contains("io.unitycatalog.tableId"),
        "io.unitycatalog.* (not fs.*) storage properties must be preserved")
      assert(str.contains("safe-table-id"),
        "non-fs storage property values must be preserved")
    }
  }

  // ---------------------------------------------------------------------------
  // Positive tests: structural elements that SHOULD appear
  // ---------------------------------------------------------------------------

  test("toString preserves CatalogTable metadata (identifier, type, provider)") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val storageProps = new JHashMap[String, String]()
      storageProps.put("fs.secret", "hidden")
      storageProps.put("safe.key", "visible")
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "named_table",
        storageProperties = storageProps,
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(spark, path, catalogTable = Some(catalogTable))
      val str = table.toString

      assert(str.startsWith("DeltaTableV2("))
      assert(str.contains("named_table"),
        "table name must appear in toString")
      assert(str.contains(path.toString),
        "path must appear in toString")
      assert(str.contains("CatalogTable"),
        "CatalogTable structure must be preserved (only fs.* keys are removed)")
      assert(str.contains("safe.key"),
        "non-fs storage properties must remain")
      assert(!str.contains("fs.secret"),
        "fs.* storage properties must be removed")
    }
  }

  test("toString includes all six original parameters") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "all_params_tbl",
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(
        spark,
        path,
        catalogTable = Some(catalogTable),
        tableIdentifier = Some("myIdent"),
        options = Map("userOpt" -> "userVal"))
      val str = table.toString

      assert(str.contains("SparkSession"),
        "SparkSession must appear (original format preserved)")
      assert(str.contains(path.toString),
        "path must appear")
      assert(str.contains("all_params_tbl"),
        "catalogTable identifier must appear")
      assert(str.contains("myIdent"),
        "tableIdentifier must appear")
      assert(str.contains("userOpt"),
        "options must appear")
    }
  }

  test("toString for path-only table (no catalogTable)") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      val path = new Path(dir.toURI)
      val table = DeltaTableV2(spark, path)
      val str = table.toString

      assert(str.startsWith("DeltaTableV2("))
      assert(str.contains(path.toString))
      assert(str.contains("None"),
        "absent catalogTable should render as None")
    }
  }

  test("toString for tableIdentifier-only (no catalogTable)") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
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
    }
  }

  // ---------------------------------------------------------------------------
  // Options filtering: fs.* in the options map must also be hidden
  // ---------------------------------------------------------------------------

  test("toString filters fs.* keys from options map") {
    withTempDir { dir =>
      spark.range(1).write.format("delta").save(dir.getCanonicalPath)
      val path = new Path(dir.toURI)
      val table = DeltaTableV2(
        spark,
        path,
        catalogTable = None,
        options = Map(
          "fs.s3a.access.key" -> "AKIA_OPTIONS_SECRET",
          "fs.unitycatalog.auth.token" -> "options-token-value",
          "userOption" -> "safe-user-value"))
      val str = table.toString

      assert(!str.contains("AKIA_OPTIONS_SECRET"),
        "fs.* values in options must not appear in toString")
      assert(!str.contains("options-token-value"),
        "fs.unitycatalog.* values in options must not appear in toString")
      assert(!str.contains("fs.s3a.access.key"),
        "fs.s3a.* keys in options must not appear in toString")
      assert(!str.contains("fs.unitycatalog.auth.token"),
        "fs.unitycatalog.* keys in options must not appear in toString")
      assert(str.contains("userOption"),
        "non-fs options keys must be preserved")
      assert(str.contains("safe-user-value"),
        "non-fs options values must be preserved")
    }
  }

  test("toString filters fs.* from both CatalogTable storage and options simultaneously") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val storageProps = new JHashMap[String, String]()
      storageProps.put("fs.s3a.secret.key", "storage-secret")
      storageProps.put("safe.storage.prop", "storage-visible")
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "dual_filter_tbl",
        storageProperties = storageProps,
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(
        spark,
        path,
        catalogTable = Some(catalogTable),
        options = Map(
          "fs.s3a.access.key" -> "options-secret",
          "safe.option" -> "options-visible"))
      val str = table.toString

      assert(!str.contains("storage-secret"),
        "fs.* values from storage properties must be filtered")
      assert(!str.contains("options-secret"),
        "fs.* values from options must be filtered")
      assert(str.contains("storage-visible"),
        "non-fs storage properties must be preserved")
      assert(str.contains("options-visible"),
        "non-fs options must be preserved")
      assert(str.contains("dual_filter_tbl"),
        "table name must be preserved")
    }
  }

  // ---------------------------------------------------------------------------
  // Edge case: no storage properties at all
  // ---------------------------------------------------------------------------

  test("toString works when CatalogTable has empty storage properties") {
    withTempDir { dir =>
      val path = new Path(dir.toURI)
      val catalogTable = CatalogTableTestUtils.createCatalogTable(
        tableName = "empty_storage_tbl",
        locationUri = Some(dir.toURI))
      val table = DeltaTableV2(spark, path, catalogTable = Some(catalogTable))
      val str = table.toString

      assert(str.startsWith("DeltaTableV2("))
      assert(str.contains("empty_storage_tbl"))
      assert(!str.contains("fs."))
    }
  }

  // ---------------------------------------------------------------------------
  // Integration: EXPLAIN and exception paths
  // ---------------------------------------------------------------------------

  test("EXPLAIN output does not leak fs.* storage credentials") {
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
            assert(!explained.contains("fs.myKey"),
              s"EXPLAIN must not contain fs.myKey injected by DummySessionCatalog: $explained")
          }
        }
      }
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }

  /**
   * Spark appends the analyzed plan to `AnalysisException.getMessage` (via
   * `ExtendedAnalysisException`). With `DummySessionCatalog` injecting `fs.myKey` into
   * storage properties, the plan text must not leak that key.
   */
  test("SHOW CREATE TABLE exception does not leak fs.* storage properties") {
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
              s"expected unsupported SHOW CREATE TABLE error: $msg")
            assert(!msg.contains("fs.myKey"),
              s"exception message must not contain fs.myKey: $msg")
            assert(!e.toString.contains("fs.myKey"),
              s"exception toString must not contain fs.myKey: ${e.toString}")
          }
        }
      }
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }
}
