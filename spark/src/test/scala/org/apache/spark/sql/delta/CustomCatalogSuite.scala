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

package org.apache.spark.sql.delta


// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands._
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DummyCatalog, DummySessionCatalog, DummySessionCatalogInner}
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, SetTableProperties, UnaryNode, UnsetTableProperties}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2RelationShim
import org.apache.spark.sql.test.SharedSparkSession

class CustomCatalogSuite extends QueryTest with SharedSparkSession
  with DeltaSQLCommandTest with DescribeDeltaDetailSuiteBase {

  override def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.dummy", classOf[DummyCatalog].getName)

  test("CatalogTable exists in DeltaTableV2 if use table identifier") {
    def catalogTableExists(sqlCmd: String): Unit = {
      val plan = spark.sql(sqlCmd).queryExecution.analyzed
      val catalogTable = plan match {
        case cmd: UnaryNode with DeltaCommand =>
          cmd.getDeltaTable(cmd.child, "dummy").catalogTable
        case AppendData(DataSourceV2RelationShim(table: DeltaTableV2, _, _, _, _), _, _, _, _, _) =>
          table.catalogTable
        case cmd: DeleteCommand =>
          cmd.catalogTable
        case cmd: DescribeDeltaHistoryCommand =>
          cmd.table.catalogTable
        case cmd: MergeIntoCommand =>
          cmd.catalogTable
        case cmd: RestoreTableCommand =>
          cmd.sourceTable.catalogTable
        case SetTableProperties(ResolvedTable(_, _, table: DeltaTableV2, _), _) =>
          table.catalogTable
        case UnsetTableProperties(ResolvedTable(_, _, table: DeltaTableV2, _), _, _) =>
          table.catalogTable
        case cmd: UpdateCommand =>
          cmd.catalogTable
        case cmd: WriteIntoDelta =>
          cmd.catalogTableOpt
      }
      assert(catalogTable.nonEmpty)
    }

    val mergeSrcTable = "merge_src_table"
    val tableName = "delta_commands_table"

    withTable(tableName, mergeSrcTable) {
      sql(f"CREATE TABLE $tableName (c1 int, c2 int) USING delta PARTITIONED BY (c1)")
      // DQL
      catalogTableExists(s"DESCRIBE DETAIL $tableName")
      catalogTableExists(s"DESCRIBE HISTORY $tableName")

      // DDL
      catalogTableExists(s"ALTER TABLE $tableName SET TBLPROPERTIES ('a' = 'b') ")
      catalogTableExists(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('a') ")

      // DML insert
      catalogTableExists(s"INSERT INTO $tableName VALUES (1, 1) ")

      // DML merge
      sql(s"CREATE TABLE $mergeSrcTable (c1 int, c2 int) USING delta PARTITIONED BY (c1)")
      sql(s"INSERT INTO $mergeSrcTable VALUES (1, 1) ")
      catalogTableExists(s"MERGE INTO $tableName USING $mergeSrcTable " +
        s"ON ${mergeSrcTable}.c1 = ${tableName}.c1 WHEN MATCHED THEN DELETE")

      // DML update
      catalogTableExists(s"UPDATE $tableName SET c1 = 4 WHERE true ")

      // DML delete
      catalogTableExists(s"DELETE FROM $tableName WHERE true ")

      // optimize
      sql(s"INSERT INTO $tableName VALUES (1, 1) ")
      sql(s"INSERT INTO $tableName VALUES (1, 1) ")
      catalogTableExists(s"OPTIMIZE $tableName")

      // vacuum
      catalogTableExists(s"VACUUM $tableName")
    }
  }

  test("DESC DETAIL a delta table from DummyCatalog") {
    val tableName = "desc_detail_table"
    withTable(tableName) {
      val dummyCatalog =
        spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
      val tablePath = dummyCatalog.getTablePath(tableName)
      sql("SET CATALOG dummy")
      sql(f"CREATE TABLE $tableName (id bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      // To make it simple, here we insert data directly into the table path.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")
      sql("SET CATALOG dummy")
      // Test simple desc detail command under the dummy catalog
      checkResult(
        sql(f"DESC DETAIL $tableName"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
      // Test 3-part identifier
      checkResult(
        sql(f"DESC DETAIL dummy.default.$tableName"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
      // Test table path
      checkResult(
        sql(f"DESC DETAIL delta.`$tablePath`"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
      // Test 3-part identifier when the current catalog is not dummy catalog
      sql("SET CATALOG spark_catalog")
      checkResult(
        sql(f"DESC DETAIL dummy.default.$tableName"),
        Seq("delta", 1),
        Seq("format", "numFiles"))
    }
  }

  test("RESTORE a table from DummyCatalog") {
    val dummyCatalog =
      spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
    val tableName = "restore_table"
    val tablePath = dummyCatalog.getTablePath(tableName)
    withTable(tableName) {
      sql("SET CATALOG dummy")
      sql(f"CREATE TABLE $tableName (id bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      // To make it simple, here we insert data directly into the table path.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")
      sql(f"INSERT INTO delta.`$tablePath` VALUES (1)")
      // Test 3-part identifier when the current catalog is the default catalog
      sql(f"RESTORE TABLE dummy.default.$tableName VERSION AS OF 1")
      checkAnswer(spark.table(f"dummy.default.$tableName"), spark.range(1).toDF())

      sql("SET CATALOG dummy")
      sql(f"RESTORE TABLE $tableName VERSION AS OF 0")
      checkAnswer(spark.table(tableName), Nil)
      sql(f"RESTORE TABLE $tableName VERSION AS OF 1")
      checkAnswer(spark.table(tableName), spark.range(1).toDF())
      // Test 3-part identifier
      sql(f"RESTORE TABLE dummy.default.$tableName VERSION AS OF 2")
      checkAnswer(spark.table(tableName), spark.range(2).toDF())
      // Test file path table
      sql(f"RESTORE TABLE delta.`$tablePath` VERSION AS OF 1")
      checkAnswer(spark.table(tableName), spark.range(1).toDF())
    }
  }

  test("Shallow Clone a table with time travel") {
    val srcTable = "shallow_clone_src_table"
    val destTable1 = "shallow_clone_dest_table_1"
    val destTable2 = "shallow_clone_dest_table_2"
    val destTable3 = "shallow_clone_dest_table_3"
    val destTable4 = "shallow_clone_dest_table_4"
    val dummyCatalog =
      spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
    val tablePath = dummyCatalog.getTablePath(srcTable)
    withTable(srcTable) {
      sql("SET CATALOG dummy")
      sql(f"CREATE TABLE $srcTable (id bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      // To make it simple, here we insert data directly into the table path.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")
      sql(f"INSERT INTO delta.`$tablePath` VALUES (1)")
      withTable(destTable1) {
        // Test 3-part identifier when the current catalog is the default catalog
        sql(f"CREATE TABLE $destTable1 SHALLOW CLONE dummy.default.$srcTable VERSION AS OF 1")
        checkAnswer(spark.table(destTable1), spark.range(1).toDF())
      }

      sql("SET CATALOG dummy")
      Seq(true, false).foreach { createTableInDummy =>
        val (dest2, dest3, dest4) = if (createTableInDummy) {
          (destTable2, destTable3, destTable4)
        } else {
          val prefix = "spark_catalog.default"
          (s"$prefix.$destTable2", s"$prefix.$destTable3", s"$prefix.$destTable4")
        }
        withTable(dest2, dest3, dest4) {
          // Test simple shallow clone command under the dummy catalog
          sql(f"CREATE TABLE $dest2 SHALLOW CLONE $srcTable")
          checkAnswer(spark.table(dest2), spark.range(2).toDF())
          // Test time travel on the src table
          sql(f"CREATE TABLE $dest3 SHALLOW CLONE dummy.default.$srcTable VERSION AS OF 1")
          checkAnswer(spark.table(dest3), spark.range(1).toDF())
          // Test time travel on the src table delta path
          sql(f"CREATE TABLE $dest4 SHALLOW CLONE delta.`$tablePath` VERSION AS OF 1")
          checkAnswer(spark.table(dest4), spark.range(1).toDF())
        }
      }
    }
  }

  test("DESCRIBE HISTORY a delta table from DummyCatalog") {
    val tableName = "desc_history_table"
    withTable(tableName) {
      sql("SET CATALOG dummy")
      val dummyCatalog =
        spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
      val tablePath = dummyCatalog.getTablePath(tableName)
      sql(f"CREATE TABLE $tableName (column1 bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")

      sql("SET CATALOG dummy")
      // Test simple desc detail command under the dummy catalog
      var result = sql(s"DESCRIBE HISTORY $tableName").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
      // Test 3-part identifier
      result = sql(f"DESCRIBE HISTORY dummy.default.$tableName").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
      // Test table path
      sql(f"DESC DETAIL delta.`$tablePath`").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
      // Test 3-part identifier when the current catalog is not dummy catalog
      sql("SET CATALOG spark_catalog")
      result = sql(s"DESCRIBE HISTORY dummy.default.$tableName").collect()
      assert(result.length == 2)
      assert(result(0).getAs[Long]("version") == 1)
    }
  }

  test("SELECT Table Changes from DummyCatalog") {
    val dummyTableName = "dummy_table"
    val sparkTableName = "spark_catalog.default.spark_table"
    withTable(dummyTableName, sparkTableName) {
      sql("SET CATALOG spark_catalog")
      sql(f"CREATE TABLE $sparkTableName (id bigint, s string) USING delta" +
        f" TBLPROPERTIES(delta.enableChangeDataFeed=true)")
      sql(f"INSERT INTO $sparkTableName VALUES (0, 'a')")
      sql(f"INSERT INTO $sparkTableName VALUES (1, 'b')")
      sql("SET CATALOG dummy")
      // Since the dummy catalog doesn't pass through the TBLPROPERTIES 'delta.enableChangeDataFeed'
      // here we clone a table with the same schema as the spark table to test the table changes.
      sql(f"CREATE TABLE $dummyTableName SHALLOW CLONE $sparkTableName")
      // table_changes() should be able to read the table changes from the dummy catalog
      Seq(dummyTableName, f"dummy.default.$dummyTableName").foreach { name =>
        val rows = sql(f"SELECT * from table_changes('$name', 1)").collect()
        assert(rows.length == 2)
      }
    }
  }

  test("custom catalog that adds additional table storage properties") {
    // Reset catalog manager so that the new `spark_catalog` implementation can apply.
    spark.sessionState.catalogManager.reset()
    withSQLConf("spark.sql.catalog.spark_catalog" -> classOf[DummySessionCatalog].getName) {
      withTable("t") {
        withTempPath { path =>
          spark.range(10).write.format("delta").save(path.getCanonicalPath)
          sql(s"CREATE TABLE t (id LONG) USING delta LOCATION '${path.getCanonicalPath}'")
          val t = spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[TableCatalog]
            .loadTable(Identifier.of(Array("default"), "t")).asInstanceOf[DeltaTableV2]
          assert(t.deltaLog.options("fs.myKey") == "val")
        }
      }
    }
  }

  test("custom catalog that generates location for managed tables") {
    // Reset catalog manager so that the new `spark_catalog` implementation can apply.
    spark.sessionState.catalogManager.reset()
    withSQLConf("spark.sql.catalog.spark_catalog" -> classOf[DummySessionCatalog].getName) {
      withTable("t") {
        withTempPath { path =>
          sql(s"CREATE TABLE t (id LONG) USING delta TBLPROPERTIES (fakeLoc='$path')")
          val t = spark.sessionState.catalogManager.v2SessionCatalog.asInstanceOf[TableCatalog]
            .loadTable(Identifier.of(Array("default"), "t"))
          // It should be a managed table.
          assert(!t.properties().containsKey(TableCatalog.PROP_EXTERNAL))
        }
      }
    }
  }
}
