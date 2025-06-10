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

package io.delta.tables

import scala.collection.mutable

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DummyCatalogWithNamespace}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogNotFoundException
import org.apache.spark.sql.test.SharedSparkSession

class DeltaTableForNameSuite extends QueryTest
  with DeltaSQLCommandTest
  with SharedSparkSession {
  private val sparkCatalog = "spark_catalog"
  private val catalogName = "test_catalog"
  private val defaultSchema = "default"
  private val nonDefaultSchema = "non_default"
  private val commonTblName = "tbl"
  private val defaultSchemaUniqueTbl = "default_tbl"
  private val nonDefaultSchemaUniqueTbl = "non_default_tbl"
  private val nonSessionCatalogNonDefaultSchema = "non_default_session_schema"
  private val tableNameToId = mutable.Map.empty[String, String]

  override def sparkConf: SparkConf =
    super.sparkConf
      .set(s"spark.sql.catalog.$catalogName", classOf[DummyCatalogWithNamespace].getName)

  override def beforeAll(): Unit = {
    super.beforeAll()
    // General Test setup
    sql(s"CREATE SCHEMA $nonDefaultSchema;")
    sql(s"CREATE SCHEMA $catalogName.$defaultSchema;")
    sql(s"CREATE SCHEMA $catalogName.$nonSessionCatalogNonDefaultSchema;")
    // Setup custom catalog default schema tables
    sql(s"SET CATALOG $catalogName")
    setUpTable(catalogName, defaultSchema, commonTblName)
    setUpTable(catalogName, defaultSchema, defaultSchemaUniqueTbl)
    // Setup custom catalog non default schema tables
    setUpTable(catalogName, nonSessionCatalogNonDefaultSchema, commonTblName)
    setUpTable(catalogName, nonSessionCatalogNonDefaultSchema, nonDefaultSchemaUniqueTbl)
    // Setup session catalog default schema tables
    sql(s"SET CATALOG $sparkCatalog")
    setUpTable(sparkCatalog, defaultSchema, commonTblName)
    setUpTable(sparkCatalog, defaultSchema, defaultSchemaUniqueTbl)
    // Setup session catalog non default schema tables
    setUpTable(sparkCatalog, nonDefaultSchema, commonTblName)
    setUpTable(sparkCatalog, nonDefaultSchema, nonDefaultSchemaUniqueTbl)
  }

  override def afterAll(): Unit = {
    sql(s"DROP SCHEMA $sparkCatalog.$nonDefaultSchema CASCADE;")
    sql(s"DROP SCHEMA $catalogName.$defaultSchema CASCADE;")
    sql(s"DROP SCHEMA $catalogName.$nonSessionCatalogNonDefaultSchema CASCADE;")
    super.afterAll()
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    sql(s"SET CATALOG $sparkCatalog")
  }

  private def getTablePath(tableName: String): Path = {
    new Path(DummyCatalogWithNamespace.catalogDir + s"/$tableName")
  }

  private def setUpTable(catalog: String, schema: String, table: String): Unit = {
    val tableName = s"$catalog.$schema.$table"
    val path = getTablePath(tableName)
    sql(s"CREATE OR REPLACE TABLE $tableName (id int) USING delta LOCATION '$path';")
    tableNameToId += (tableName -> DeltaLog.forTable(spark,
      getTablePath(tableName)).tableId)
  }

  private def validateForNameTableId(
      tableName: String,
      expectedResult: Option[String] = None): Unit = {
    val table = DeltaTable.forName(spark, tableName)
    checkAnswer(table.detail().select("id"), Seq(Row(expectedResult.getOrElse(
      tableNameToId(tableName)
    ))))
  }

  test(s"forName resolves fully qualified tables in session catalog correctly") {
    validateForNameTableId(s"$sparkCatalog.$defaultSchema.$commonTblName")
    validateForNameTableId(s"$sparkCatalog.$defaultSchema.$defaultSchemaUniqueTbl")

    validateForNameTableId(s"$sparkCatalog.$nonDefaultSchema.$commonTblName")
    validateForNameTableId(s"$sparkCatalog.$nonDefaultSchema.$nonDefaultSchemaUniqueTbl")
  }

  test(s"forName resolves partially qualified tables in session catalog correctly") {
    validateForNameTableId(s"$defaultSchema.$commonTblName",
      Some(tableNameToId(s"$sparkCatalog.$defaultSchema.$commonTblName")))
    validateForNameTableId(s"$defaultSchema.$defaultSchemaUniqueTbl",
      Some(tableNameToId(s"$sparkCatalog.$defaultSchema.$defaultSchemaUniqueTbl")))

    validateForNameTableId(s"$nonDefaultSchema.$commonTblName",
      Some(tableNameToId(s"$sparkCatalog.$nonDefaultSchema.$commonTblName")))
    validateForNameTableId(s"$nonDefaultSchema.$nonDefaultSchemaUniqueTbl",
      Some(tableNameToId(s"$sparkCatalog.$nonDefaultSchema.$nonDefaultSchemaUniqueTbl")))
  }

  test(s"forName resolves fully qualified tables in non session catalog correctly") {
    validateForNameTableId(s"$catalogName.$defaultSchema.$commonTblName")
    validateForNameTableId(s"$catalogName.$defaultSchema.$defaultSchemaUniqueTbl")

    validateForNameTableId(s"$catalogName.$nonSessionCatalogNonDefaultSchema.$commonTblName")
    validateForNameTableId(
      s"$catalogName.$nonSessionCatalogNonDefaultSchema.$nonDefaultSchemaUniqueTbl")
  }

  for (table <- Seq(commonTblName, nonDefaultSchemaUniqueTbl))
  test(s"forName fails for partially " +
    s"qualified tables in non session catalog with table=$table") {
    sql(s"SET CATALOG $catalogName")
    val e = intercept[AnalysisException] {
      DeltaTable.forName(spark, s"$nonSessionCatalogNonDefaultSchema.$table")
    }
    checkError(exception = e, "DELTA_MISSING_DELTA_TABLE",
      parameters = Map("tableName" -> s"`$nonSessionCatalogNonDefaultSchema`.`$table`"))
  }

  // forName currently doesn't resolve unqualified tables correctly for non session catalogs.
  // in this test, it resolves to the table `spark_catalog.default.tbl` but it adds an incorrect
  // identifier on top.
  test(s"forName resolves partially qualified tables in non session catalog incorrectly") {
    sql(s"SET CATALOG $catalogName")
    validateForNameTableId(s"$defaultSchema.$commonTblName",
      Some(tableNameToId(s"$catalogName.$defaultSchema.$commonTblName")))
    validateForNameTableId(s"$defaultSchema.$defaultSchemaUniqueTbl",
      Some(tableNameToId(s"$catalogName.$defaultSchema.$defaultSchemaUniqueTbl")))
  }

  test("forName with invalid non session catalog") {
    intercept[CatalogNotFoundException] {
      DeltaTable.forName(spark, "invalid_catalog.default.tbl")
    }
  }

  test("forName with unqualified non session catalog") {
    sql(s"SET CATALOG $sparkCatalog")
    val e = intercept[AnalysisException] {
      DeltaTable.forName(spark, s"$nonSessionCatalogNonDefaultSchema.$commonTblName")
    }
    checkError(exception = e, "DELTA_MISSING_DELTA_TABLE",
      parameters = Map("tableName" -> s"`$nonSessionCatalogNonDefaultSchema`.`$commonTblName`"))
  }

  test("forName fails with fully qualified non existent table") {
    val e = intercept[AnalysisException] {
      DeltaTable.forName(spark, s"$catalogName.$defaultSchema.invalid_table")
    }
    checkError(exception = e, "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map("relationName" -> s"`$catalogName`.`$defaultSchema`.`invalid_table`"))
  }
}
