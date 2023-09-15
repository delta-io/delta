/*
 * Copyright (2023) The Delta Lake Project Authors.
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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCatalog, TableChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

class CustomCatalogSuite extends QueryTest with SharedSparkSession
  with DeltaSQLCommandTest {

  override def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.dummy", classOf[DummyCatalog].getName)

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
    val destTable1 = "spark_catalog.default.shallow_clone_dest_table_1"
    val destTable2 = "spark_catalog.default.shallow_clone_dest_table_2"
    val destTable3 = "spark_catalog.default.shallow_clone_dest_table_3"
    val destTable4 = "spark_catalog.default.shallow_clone_dest_table_4"
    val dummyCatalog =
      spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
    val tablePath = dummyCatalog.getTablePath(srcTable)
    withTable(srcTable, destTable1, destTable2, destTable3, destTable4) {
      sql("SET CATALOG dummy")
      sql(f"CREATE TABLE $srcTable (id bigint) USING delta")
      sql("SET CATALOG spark_catalog")
      // Insert some data into the table in the dummy catalog.
      // To make it simple, here we insert data directly into the table path.
      sql(f"INSERT INTO delta.`$tablePath` VALUES (0)")
      sql(f"INSERT INTO delta.`$tablePath` VALUES (1)")
      // Test 3-part identifier when the current catalog is the default catalog
      sql(f"CREATE TABLE $destTable1 SHALLOW CLONE dummy.default.$srcTable VERSION AS OF 1")
      checkAnswer(spark.table(destTable1), spark.range(1).toDF())

      sql("SET CATALOG dummy")
      // Test simple shallow clone command under the dummy catalog
      sql(f"CREATE TABLE $destTable2 SHALLOW CLONE $srcTable")
      checkAnswer(spark.table(destTable2), spark.range(2).toDF())
      // Test time travel on the src table
      sql(f"CREATE TABLE $destTable3 SHALLOW CLONE dummy.default.$srcTable VERSION AS OF 1")
      checkAnswer(spark.table(destTable3), spark.range(1).toDF())
      // Test time travel on the src table delta path
      sql(f"CREATE TABLE $destTable4 SHALLOW CLONE delta.`$tablePath` VERSION AS OF 1")
      checkAnswer(spark.table(destTable4), spark.range(1).toDF())
    }
  }
}

class DummyCatalog extends TableCatalog {
  private val spark: SparkSession = SparkSession.active
  private val tempDir: Path = new Path(Utils.createTempDir().getAbsolutePath)
  // scalastyle:off deltahadoopconfiguration
  private val fs: FileSystem =
    tempDir.getFileSystem(spark.sessionState.newHadoopConf())
  // scalastyle:on deltahadoopconfiguration

  override def name: String = "dummy"

  def getTablePath(tableName: String): Path = {
    new Path(tempDir.toString + "/" + tableName)
  }
  override def defaultNamespace(): Array[String] = Array("default")

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    val status = fs.listStatus(tempDir)
    status.filter(_.isDirectory).map { dir =>
      Identifier.of(namespace, dir.getPath.getName)
    }
  }

  override def tableExists(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.name())
    fs.exists(tablePath)
  }
  override def loadTable(ident: Identifier): Table = {
    if (!tableExists(ident)) {
      throw new NoSuchTableException("")
    }
    val tablePath = getTablePath(ident.name())
    DeltaTableV2(spark, tablePath)
  }

  override def createTable(
    ident: Identifier,
    schema: StructType,
    partitions: Array[Transform],
    properties: java.util.Map[String, String]): Table = {
    val tablePath = getTablePath(ident.name())
    // Create an empty Delta table on the tablePath
    spark.range(0).write.format("delta").save(tablePath.toString)
    DeltaTableV2(spark, tablePath)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    throw new UnsupportedOperationException("Alter table operation is not supported.")
  }

  override def dropTable(ident: Identifier): Boolean = {
    val tablePath = getTablePath(ident.name())
    try {
      fs.delete(tablePath, true)
      true
    } catch {
      case _: Exception => false
    }
  }

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = {
    throw new UnsupportedOperationException("Rename table operation is not supported.")
  }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    // Initialize tempDir here
    if (!fs.exists(tempDir)) {
      fs.mkdirs(tempDir)
    }
  }
}
