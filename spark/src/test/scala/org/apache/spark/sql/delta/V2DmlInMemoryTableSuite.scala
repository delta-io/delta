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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{TableCatalog, Identifier}
import org.apache.spark.sql.delta.catalog.{InMemoryDeltaCatalog, InMemorySparkTable}
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

/**
 * Tests that DML operations can be executed through the DSv2 code path using
 * [[InMemorySparkTable]] as the backing table implementation.
 *
 * Uses [[InMemoryDeltaCatalog]] as the session catalog so that:
 * 1. CREATE TABLE still creates real Delta tables (for schema resolution)
 * 2. Subsequent table loads return [[InMemorySparkTable]] (supports V2 DML)
 * 3. DML operations flow through Spark's V2 execution path
 */
class V2DmlInMemoryTableSuite extends QueryTest with DeltaSQLCommandTest {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.spark_catalog", classOf[InMemoryDeltaCatalog].getName)

  override protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    super.withTable(tableNames: _*) {
      f
      tableNames.foreach(assertNoParquetFiles)
    }
  }

  override def afterEach(): Unit = {
    try {
      InMemoryDeltaCatalog.reset()
    } finally {
      super.afterEach()
    }
  }

  /**
   * Asserts that no physical parquet data files exist under the table's location.
   * This validates that DML operations went through the in-memory V2 path and
   * did not fall back to the V1 connector (which would write actual parquet files).
   */
  private def assertNoParquetFiles(tableName: String): Unit = {
    val catalogTable = spark.sessionState.catalog.getTableMetadata(
      TableIdentifier(tableName))
    val dataPath = new File(new URI(catalogTable.location.toString))
    if (dataPath.exists()) {
      val stream = Files.walk(dataPath.toPath)
      try {
        val parquetFiles = stream
          .filter(Files.isRegularFile(_))
          .filter(_.toString.endsWith(".parquet"))
          .toArray.map(_.asInstanceOf[Path].toString).toSeq
        assert(parquetFiles.isEmpty,
          s"Physical parquet files found under '$dataPath' while V2 in-memory mode is enabled. " +
          s"DML may have fallen back to V1. Files: $parquetFiles")
      } finally {
        stream.close()
      }
    }
  }

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
      checkAnswer(
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
      checkAnswer(
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
      checkAnswer(
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
      checkAnswer(
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
        sql("CREATE TEMP VIEW source AS SELECT * FROM VALUES (1, 'updated'), (3, 'c') AS t(pk, value)")

        sql(
          s"""MERGE INTO $targetTable t
             |USING source s
             |ON t.pk = s.pk
             |WHEN MATCHED THEN UPDATE SET t.value = s.value
             |WHEN NOT MATCHED THEN INSERT (pk, value) VALUES (s.pk, s.value)
             |""".stripMargin)

        checkAnswer(
          sql(s"SELECT pk, value FROM $targetTable ORDER BY pk"),
          Seq(Row(1, "updated"), Row(2, "b"), Row(3, "c")))
      }
    }
  }
}
