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

import io.delta.tables.execution.VacuumTableCommand
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
import org.apache.spark.sql.delta.commands.VacuumCommand.getDeltaTable

class CustomCatalogSuite extends QueryTest with SharedSparkSession
  with DeltaSQLCommandTest {

  override def sparkConf: SparkConf =
    super.sparkConf.set("spark.sql.catalog.dummy", classOf[DummyCatalog].getName)

  private def verifyVacuumPath(query: String, expected: Path): Unit = {
    val plan = sql(query).queryExecution.analyzed
    assert(plan.isInstanceOf[VacuumTableCommand])
    val path = getDeltaTable(plan.asInstanceOf[VacuumTableCommand].child, "VACUUM").path
    assert(path == expected)
  }

  test("Vacuum a table from DummyCatalog") {
    val tableName = "vacuum_table"
    withTable(tableName) {
      sql("SET CATALOG dummy")
      val dummyCatalog =
        spark.sessionState.catalogManager.catalog("dummy").asInstanceOf[DummyCatalog]
      sql(f"CREATE TABLE $tableName (id bigint) USING delta")
      val tablePath = dummyCatalog.getTablePath(tableName)
      verifyVacuumPath(s"VACUUM $tableName", tablePath)
      verifyVacuumPath(s"VACUUM delta.`$tablePath`", tablePath)

      sql("SET CATALOG spark_catalog")
      verifyVacuumPath(s"VACUUM dummy.default.$tableName", tablePath)
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
    new Path(tempDir, tableName)
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
