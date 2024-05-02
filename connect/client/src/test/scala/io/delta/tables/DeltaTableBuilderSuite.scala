/*
 * Copyright (2024) The Delta Lake Project Authors.
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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.test.{QueryTest, SQLHelper}
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructType}

class DeltaTableBuilderSuite extends QueryTest with SQLHelper {

  // Define the information for a default test table used by many tests.
  private val defaultTestTableSchema = "c1 int, c2 int, c3 string"
  private val defaultTestTableGeneratedColumns = Map("c2" -> "c1 + 10")
  private val defaultTestTablePartitionColumns = Seq("c1")
  private val defaultTestTableColumnComments = Map("c1" -> "foo", "c3" -> "bar")
  private val defaultTestTableComment = "tbl comment"
  private val defaultTestTableNullableCols = Set("c1", "c3")
  private val defaultTestTableProperty = Map("foo" -> "bar")
  private val defaultTestTableClusteringColumns = Seq("c1", "c3")


  /**
   * Verify if the table metadata matches the test table. We use this to verify DDLs
   * write correct table metadata into the transaction logs.
   * If clusteringCols are not empty, it verifies the clustering columns for liquid.
   */
  protected def verifyTestTableMetadata(
      table: String,
      schemaString: String,
      generatedColumns: Map[String, String] = Map.empty,
      colComments: Map[String, String] = Map.empty,
      colNullables: Set[String] = Set.empty,
      tableComment: Option[String] = None,
      expectedPartitionCols: Seq[String] = Seq.empty,
      expectedTableProperties: Map[String, String] = Map.empty,
      expectedClusteringCols: Seq[String] = Seq.empty): Unit = {
    val session = spark
    import session.implicits._

    val expectedSchema = StructType(StructType.fromDDL(schemaString).map { field =>
      val newMetadata = new MetadataBuilder().withMetadata(field.metadata)
      if (colComments.contains(field.name)) {
        newMetadata.putString("comment", colComments(field.name))
      }
      field.copy(
        nullable = colNullables.contains(field.name),
        metadata = newMetadata.build())
    })

    val deltaTable = DeltaTable.forName(spark, table)
    assert(deltaTable.toDF.schema == expectedSchema)

    val (description, partitionColumns, properties, clusteringColumns) =
      deltaTable.detail()
        .select("description", "partitionColumns", "properties", "clusteringColumns")
        .as[(String, Seq[String], Map[String, String], Seq[String])]
        .head()

    assert(description == tableComment.orNull)
    assert(partitionColumns == expectedPartitionCols)
    // It may contain other properties other than the ones we added.
    expectedTableProperties.foreach {
      prop => properties.contains(prop._1) && properties.get(prop._1).get == prop._2
    }
    assert(clusteringColumns == expectedClusteringCols)

    val createTableCommandLines =
      spark.sql(s"SHOW CREATE TABLE $table").as[String].head().split("\n").map(_.trim)
    generatedColumns.foreach { case (col, expr) =>
      createTableCommandLines.exists { line =>
        line.startsWith(col) && line.endsWith(s"GENERATED ALWAYS AS ($expr)")
      }
    }
  }

  protected def testCreateTable(testName: String)(createFunc: String => Unit): Unit = {
    test(testName) {
      withTable(testName) {
        createFunc(testName)
        verifyTestTableMetadata(
          testName, defaultTestTableSchema, defaultTestTableGeneratedColumns,
          defaultTestTableColumnComments, defaultTestTableNullableCols,
          Some(defaultTestTableComment), defaultTestTablePartitionColumns,
          defaultTestTableProperty
        )
      }
    }
  }

  protected def testCreateTableWithNameAndLocation(
      testName: String)(createFunc: (String, String) => Unit): Unit = {
    test(testName + ": external - with location and name") {
      withTempPath { path =>
        withTable(testName) {
          createFunc(testName, path.getCanonicalPath)
          verifyTestTableMetadata(
            testName,
            defaultTestTableSchema, defaultTestTableGeneratedColumns,
            defaultTestTableColumnComments, defaultTestTableNullableCols,
            Some(defaultTestTableComment), defaultTestTablePartitionColumns,
            defaultTestTableProperty
          )
        }
      }
    }
  }

  protected def testCreateTableWithLocationOnly(
      testName: String)(createFunc: String => Unit): Unit = {
    test(testName + ": external - location only") {
      withTempPath { path =>
        withTable(testName) {
          createFunc(path.getCanonicalPath)
          verifyTestTableMetadata(
            s"delta.`${path.getCanonicalPath}`",
            defaultTestTableSchema, defaultTestTableGeneratedColumns,
            defaultTestTableColumnComments, defaultTestTableNullableCols,
            Some(defaultTestTableComment), defaultTestTablePartitionColumns,
            defaultTestTableProperty
          )
        }
      }
    }
  }

  def defaultCreateTableBuilder(
      ifNotExists: Boolean,
      tableName: Option[String] = None,
      location: Option[String] = None): DeltaTableBuilder = {
    val tableBuilder = if (ifNotExists) {
      io.delta.tables.DeltaTable.createIfNotExists(spark)
    } else {
      io.delta.tables.DeltaTable.create(spark)
    }
    defaultTableBuilder(tableBuilder, tableName, location)
  }

  def defaultReplaceTableBuilder(
      orCreate: Boolean,
      tableName: Option[String] = None,
      location: Option[String] = None): DeltaTableBuilder = {
    val tableBuilder = if (orCreate) {
      io.delta.tables.DeltaTable.createOrReplace(spark)
    } else {
      io.delta.tables.DeltaTable.replace(spark)
    }
    defaultTableBuilder(tableBuilder, tableName, location)
  }

  private def defaultTableBuilder(
      builder: DeltaTableBuilder,
      tableName: Option[String],
      location: Option[String],
      clusterBy: Boolean = false) = {
    var tableBuilder = builder
    if (tableName.nonEmpty) {
      tableBuilder = tableBuilder.tableName(tableName.get)
    }
    if (location.nonEmpty) {
      tableBuilder = tableBuilder.location(location.get)
    }
    tableBuilder.addColumn(
      io.delta.tables.DeltaTable.columnBuilder(spark, "c1").dataType("int")
        .nullable(true).comment("foo").build()
    )
    tableBuilder.addColumn(
      io.delta.tables.DeltaTable.columnBuilder(spark, "c2").dataType("int")
        .nullable(false).generatedAlwaysAs("c1 + 10").build()
    )
    tableBuilder.addColumn(
      io.delta.tables.DeltaTable.columnBuilder(spark, "c3").dataType("string")
        .comment("bar").build()
    )
    if (clusterBy) {
      tableBuilder.clusterBy("c1", "c3")
    } else {
      tableBuilder.partitionedBy("c1")
    }
    tableBuilder.property("foo", "bar")
    tableBuilder.comment("tbl comment")
    tableBuilder
  }

  test("create table with existing schema and extra column") {
    withTable("table") {
      withTempPath { dir =>
        spark.range(10).toDF("key").write.format("parquet").saveAsTable("table")
        val existingSchema = spark.read.format("parquet").table("table").schema
        io.delta.tables.DeltaTable.create(spark)
          .location(dir.getAbsolutePath)
          .addColumns(existingSchema)
          .addColumn("value", "string", false)
          .execute()
        verifyTestTableMetadata(s"delta.`${dir.getAbsolutePath}`",
          "key bigint, value string", colNullables = Set("key"))
      }
    }
  }

  test("create table with variation of addColumns - with spark session") {
    withTable("test") {
      io.delta.tables.DeltaTable.create(spark)
        .tableName("test")
        .addColumn("c1", "int")
        .addColumn("c2", IntegerType)
        .addColumn("c3", "string", false)
        .addColumn("c4", StringType, true)
        .addColumn(
          io.delta.tables.DeltaTable.columnBuilder(spark, "c5")
            .dataType("bigint")
            .comment("foo")
            .nullable(false)
            .build
        )
        .addColumn(
          io.delta.tables.DeltaTable.columnBuilder(spark, "c6")
            .dataType(LongType)
            .generatedAlwaysAs("c5 + 10")
            .build
        ).execute()
      verifyTestTableMetadata(
        "test", "c1 int, c2 int, c3 string, c4 string, c5 bigint, c6 bigint",
        generatedColumns = Map("c6" -> "c5 + 10"),
        colComments = Map("c5" -> "foo"),
        colNullables = Set("c1", "c2", "c4", "c6")
      )
    }
  }

  test("test addColumn using columnBuilder, without dataType") {
    val e = intercept[AnalysisException] {
      DeltaTable.columnBuilder(spark, "value")
        .generatedAlwaysAs("true")
        .nullable(true)
        .build()
    }
    assert(e.getMessage == "The data type of the column value is not provided")
  }

  testCreateTable("create_table") { table =>
    defaultCreateTableBuilder(ifNotExists = false, Some(table)).execute()
  }

  testCreateTableWithNameAndLocation("create_table") { (name, path) =>
    defaultCreateTableBuilder(ifNotExists = false, Some(name), Some(path)).execute()
  }

  testCreateTableWithLocationOnly("create_table") { path =>
    defaultCreateTableBuilder(ifNotExists = false, location = Some(path)).execute()
  }

  test("create table - errors if already exists") {
    withTable("testTable") {
      spark.sql(s"CREATE TABLE testTable (c1 int) USING DELTA")
      intercept[AnalysisException] {
        defaultCreateTableBuilder(ifNotExists = false, Some("testTable")).execute()
      }
    }
  }

  test("create table - ignore if already exists") {
    withTable("testTable") {
      spark.sql(s"CREATE TABLE testTable (c1 int) USING DELTA")
      defaultCreateTableBuilder(ifNotExists = true, Some("testTable")).execute()
      verifyTestTableMetadata("testTable", "c1 int", colNullables = Set("c1"))
    }
  }

  testCreateTable("create_table_if_not_exists") { table =>
    defaultCreateTableBuilder(ifNotExists = true, Some(table)).execute()
  }

  testCreateTableWithNameAndLocation("create_table_if_not_exists") { (name, path) =>
    defaultCreateTableBuilder(ifNotExists = true, Some(name), Some(path)).execute()
  }

  testCreateTableWithLocationOnly("create_table_if_not_exists") { path =>
    defaultCreateTableBuilder(ifNotExists = true, location = Some(path)).execute()
  }

  test("replace table - errors if not exists") {
    intercept[AnalysisException] {
      defaultReplaceTableBuilder(orCreate = false, Some("testTable")).execute()
    }
  }

  testCreateTable("replace_table") { table =>
    spark.sql(s"CREATE TABLE replace_table(c1 int) USING DELTA")
    defaultReplaceTableBuilder(orCreate = false, Some(table)).execute()
  }

  testCreateTableWithNameAndLocation("replace_table") { (name, path) =>
    spark.sql(s"CREATE TABLE $name (c1 int) USING DELTA LOCATION '$path'")
    defaultReplaceTableBuilder(orCreate = false, Some(name), Some(path)).execute()
  }

  testCreateTableWithLocationOnly("replace_table") { path =>
    spark.sql(s"CREATE TABLE delta.`$path` (c1 int) USING DELTA")
    defaultReplaceTableBuilder(orCreate = false, location = Some(path)).execute()
  }

  testCreateTable("replace_or_create_table") { table =>
    defaultReplaceTableBuilder(orCreate = true, Some(table)).execute()
  }

  testCreateTableWithNameAndLocation("replace_or_create_table") { (name, path) =>
    defaultReplaceTableBuilder(orCreate = true, Some(name), Some(path)).execute()
  }

  testCreateTableWithLocationOnly("replace_or_create_table") { path =>
    defaultReplaceTableBuilder(orCreate = true, location = Some(path)).execute()
  }

  test("test no identifier and no location") {
    val e = intercept[AnalysisException] {
      io.delta.tables.DeltaTable.create(spark).addColumn("c1", "int").execute()
    }
    assert(e.getMessage.equals("Table name or location has to be specified"))
  }

  test("partitionedBy only should contain columns in the schema") {
    val e = intercept[AnalysisException] {
      io.delta.tables.DeltaTable.create(spark).tableName("testTable")
        .addColumn("c1", "int")
        .partitionedBy("c2")
        .execute()
    }
    assert(e.getMessage.contains("Couldn't find column c2"))
  }

  test("errors if table name and location are different paths") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.create(spark).tableName(s"delta.`$path`")
          .addColumn("c1", "int")
          .location("src/test/resources/delta/dbr_8_0_non_generated_columns")
          .execute()
      }
      assert(e.getMessage.contains(
        "Creating path-based Delta table with a different location isn't supported."))
    }
  }

  test("table name and location are the same") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      io.delta.tables.DeltaTable.create(spark).tableName(s"delta.`$path`")
        .addColumn("c1", "int")
        .location(path)
        .execute()
    }
  }

  test("errors if use parquet path as identifier") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      val e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.create(spark).tableName(s"parquet.`$path`")
          .addColumn("c1", "int")
          .location(path)
          .execute()
      }
      assert(e.getMessage == "Database 'main.parquet' not found" ||
        e.getMessage == "Database 'parquet' not found" ||
        e.getMessage.contains("is not a valid name") ||
        e.getMessage.contains("schema `parquet` cannot be found")
        || e.getMessage.contains("schema `main.parquet` cannot be found")
      )
    }
  }

  private def testCreateTableWithClusterBy(testName: String)(createFunc: String => Unit): Unit = {
    test(testName) {
      withTable(testName) {
        createFunc(testName)
        verifyTestTableMetadata(
          testName, defaultTestTableSchema, defaultTestTableGeneratedColumns,
          defaultTestTableColumnComments, defaultTestTableNullableCols,
          Some(defaultTestTableComment), expectedTableProperties = defaultTestTableProperty,
          expectedClusteringCols = defaultTestTableClusteringColumns
        )
      }
    }
  }

  testCreateTableWithClusterBy("create_table_with_clusterBy") { table =>
    val builder = io.delta.tables.DeltaTable.create()
    defaultTableBuilder(builder, Some(table), None, true).execute()
  }

  testCreateTableWithClusterBy("replace_table_with_clusterBy") { table =>
    spark.sql(s"CREATE TABLE $table(c1 int) USING DELTA")
    val builder = io.delta.tables.DeltaTable.replace()
    defaultTableBuilder(builder, Some(table), None, true).execute()
  }

  testCreateTableWithClusterBy("create_or_replace_table_with_clusterBy") { table =>
    spark.sql(s"CREATE TABLE $table(c1 int) USING DELTA")
    val builder = io.delta.tables.DeltaTable.createOrReplace()
    defaultTableBuilder(builder, Some(table), None, true).execute()
  }

  test("clusterBy only should contain columns in the schema") {
    val e = intercept[AnalysisException] {
      io.delta.tables.DeltaTable.create().tableName("testTable")
        .addColumn("c1", "int")
        .clusterBy("c2")
        .execute()
    }
    assert(e.getMessage.startsWith("Couldn't find column c2"))
  }

  test("partitionedBy and clusterBy cannot be used together") {
    val e = intercept[AnalysisException] {
      io.delta.tables.DeltaTable.create().tableName("testTable")
        .addColumn("c1", "int")
        .addColumn("c2", "int")
        .partitionedBy("c2")
        .clusterBy("c1")
        .execute()
    }
    assert(e.getMessage.contains("Clustering and partitioning cannot both be specified."))
  }
}
