/*
 * Copyright (2025) The Delta Lake Project Authors.
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

import java.io.File
import java.net.URI
import java.nio.file.{Files, Path}

import org.apache.spark.sql.delta.catalog.InMemorySparkTable

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests that DML operations can be executed through the DSv2 code path using
 * [[InMemorySparkTable]] as the backing table implementation.
 *
 * Uses [[InMemoryDeltaCatalog]] as the session catalog so that:
 * 1. CREATE TABLE still creates real Delta tables (for schema resolution)
 * 2. Subsequent table loads return [[InMemorySparkTable]] (supports V2 DML)
 * 3. DML operations flow through Spark's V2 execution path
 */
class V2DmlInMemoryTableSuite
    extends DeltaSQLInMemoryTestUtils
    with SharedSparkSession {

  test("catalog returns InMemorySparkTable when InMemoryDeltaCatalog is configured") {
    val tableName = "v2_dml_test_catalog"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG, value STRING) USING delta")

      val catalog = spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[TableCatalog]
      val table = catalog.loadTable(Identifier.of(Array("default"), tableName))

      assert(table.isInstanceOf[InMemorySparkTable],
        s"Expected InMemorySparkTable, got ${table.getClass.getName}")
    }
  }

  test("INSERT via DSv2 InMemoryTable") {
    val tableName = "v2_dml_test_insert"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG, value STRING) USING delta")

      sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b')")
      QueryTest.checkAnswer(
        sql(s"SELECT id, value FROM $tableName ORDER BY id"),
        Seq(Row(1L, "a"), Row(2L, "b")))
    }
  }

  test("INSERT OVERWRITE via DSv2 InMemoryTable") {
    val tableName = "v2_dml_test_overwrite"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id LONG, value STRING) USING delta")

      sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b')")
      sql(s"INSERT OVERWRITE $tableName VALUES (3, 'c')")
      QueryTest.checkAnswer(
        sql(s"SELECT id, value FROM $tableName ORDER BY id"),
        Seq(Row(3L, "c")))
    }
  }

  test("DELETE via DSv2 InMemoryTable") {
    val tableName = "v2_dml_test_delete"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (pk INT NOT NULL, value STRING) USING delta")
      sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      sql(s"DELETE FROM $tableName WHERE pk = 2")
      QueryTest.checkAnswer(
        sql(s"SELECT pk, value FROM $tableName ORDER BY pk"),
        Seq(Row(1, "a"), Row(3, "c")))
    }
  }

  test("UPDATE via DSv2 InMemoryTable") {
    val tableName = "v2_dml_test_update"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (pk INT NOT NULL, value STRING) USING delta")
      sql(s"INSERT INTO $tableName VALUES (1, 'a'), (2, 'b'), (3, 'c')")

      sql(s"UPDATE $tableName SET value = 'updated' WHERE pk >= 2")
      QueryTest.checkAnswer(
        sql(s"SELECT pk, value FROM $tableName ORDER BY pk"),
        Seq(Row(1, "a"), Row(2, "updated"), Row(3, "updated")))
    }
  }

  test("MERGE via DSv2 InMemoryTable") {
    val targetTable = "v2_dml_test_merge_target"
    withTable(targetTable) {
      sql(s"CREATE TABLE $targetTable (pk INT NOT NULL, value STRING) USING delta")
      sql(s"INSERT INTO $targetTable VALUES (1, 'a'), (2, 'b')")

      withTempView("source") {
        sql("CREATE TEMP VIEW source AS SELECT * FROM VALUES (1, 'updated'), (3, 'c') " +
            "AS t(pk, value)")

        sql(
          s"""MERGE INTO $targetTable t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET t.value = s.value
             |WHEN NOT MATCHED THEN INSERT (pk, value) VALUES (s.pk, s.value)
             |""".stripMargin)

        QueryTest.checkAnswer(
          sql(s"SELECT pk, value FROM $targetTable ORDER BY pk"),
          Seq(Row(1, "updated"), Row(2, "b"), Row(3, "c")))
      }
    }
  }

  test("should just throw, but is DSv2Incompatible", DSv2Incompatible("explicit skip")) {
    assert(false, "this test should have been skipped")
  }

  test("should just throw, but is DSv2TemporarilyIncompatible",
      DSv2TemporarilyIncompatible("explicit skip")) {
    assert(false, "this test should have been skipped")
  }

  test("should just throw, but checks physical Delta plan", ChecksPhysicalDeltaPlan()) {
    assert(false, "this test should have been skipped")
  }

  test("should just throw, but checks Delta internals", ChecksDeltaInternals()) {
    assert(false, "this test should have been skipped")
  }
}
