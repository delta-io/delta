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
import org.apache.spark.sql.test.DeltaQueryTest
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructType}

class DeltaTableBuilderSuite extends DeltaQueryTest with RemoteSparkSession {

  // Define the information for a default test table used by many tests.
  private val defaultTestTableSchema = "c1 int, c2 int, c3 string"
  private val defaultTestTableGeneratedColumns = Map("c2" -> "c1 + 10")
  private val defaultTestTableIdentityColumns = Map.empty[String, String]
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
      identityColumns: Map[String, String] = Map.empty,
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

    val schemaFields = deltaTable.toDF.schema.fields

    // Verify generated columns
    generatedColumns.foreach { case (col, expr) =>
      val fieldOpt = schemaFields.find(_.name == col)
      assert(fieldOpt.isDefined, s"Generated column $col not found in table schema")
      val field = fieldOpt.get
      
      // Check if the metadata contains the generation expression key
      if (field.metadata.contains("delta.generationExpression")) {
        val generationExpr = field.metadata.getString("delta.generationExpression")
        assert(generationExpr == expr, 
          s"Generated column $col has expression '$generationExpr' but expected '$expr'")
      }
    }
    
    // Verify identity columns without detailed metadata validation
    identityColumns.foreach { case (col, _) =>
      val fieldOpt = schemaFields.find(_.name == col)
      assert(fieldOpt.isDefined, s"Identity column $col not found in table schema")
    }
  }

  protected def testCreateTable(testName: String)(createFunc: String => Unit): Unit = {
    test(testName) {
      withTable(testName) {
        createFunc(testName)
        verifyTestTableMetadata(
          testName, defaultTestTableSchema, defaultTestTableGeneratedColumns,
          defaultTestTableIdentityColumns, defaultTestTableColumnComments,
          defaultTestTableNullableCols,
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
            defaultTestTableIdentityColumns, defaultTestTableColumnComments,
            defaultTestTableNullableCols, Some(defaultTestTableComment),
            defaultTestTablePartitionColumns, defaultTestTableProperty
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
            defaultTestTableIdentityColumns, defaultTestTableColumnComments,
            defaultTestTableNullableCols, Some(defaultTestTableComment),
            defaultTestTablePartitionColumns, defaultTestTableProperty
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
      clusterBy: Boolean = false): DeltaTableBuilder = {
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
      // TODO: This should be an AnalysisException, but it ends up as a Spark Exception
      // that arises from the Connect Client failing to enrich the exception with the Delta
      // error class that we expect.
      val e = intercept[Exception] {
        io.delta.tables.DeltaTable.create(spark).tableName(s"delta.`$path`")
          .addColumn("c1", "int")
          .location("src/test/resources/delta/non_generated_columns")
          .execute()
      }
      val deltaErrorClass = "DELTA_CREATE_TABLE_IDENTIFIER_LOCATION_MISMATCH"
      assert(e.getMessage.contains(deltaErrorClass))
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
      )
    }
  }

  private def testCreateTableWithClusterBy(testName: String)(createFunc: String => Unit): Unit = {
    test(testName) {
      withTable(testName) {
        createFunc(testName)
        verifyTestTableMetadata(
          testName, defaultTestTableSchema, defaultTestTableGeneratedColumns,
          defaultTestTableIdentityColumns, defaultTestTableColumnComments,
          defaultTestTableNullableCols, Some(defaultTestTableComment),
          expectedTableProperties = defaultTestTableProperty,
          expectedClusteringCols = defaultTestTableClusteringColumns
        )
      }
    }
  }

  testCreateTableWithClusterBy("create_table_with_clusterBy") { table =>
    val builder = DeltaTable.create(spark)
    defaultTableBuilder(builder, Some(table), None, true).execute()
  }

  testCreateTableWithClusterBy("replace_table_with_clusterBy") { table =>
    spark.sql(s"CREATE TABLE $table(c1 int) USING DELTA")
    val builder = DeltaTable.replace(spark)
    defaultTableBuilder(builder, Some(table), None, true).execute()
  }

  testCreateTableWithClusterBy("create_or_replace_table_with_clusterBy") { table =>
    spark.sql(s"CREATE TABLE $table(c1 int) USING DELTA")
    val builder = DeltaTable.createOrReplace(spark)
    defaultTableBuilder(builder, Some(table), None, true).execute()
  }

  test("clusterBy only should contain columns in the schema") {
    val e = intercept[AnalysisException] {
      io.delta.tables.DeltaTable.create(spark).tableName("testTable")
        .addColumn("c1", "int")
        .clusterBy("c2")
        .execute()
    }
    assert(e.getMessage.contains("`c2` is missing"))
  }

  test("partitionedBy and clusterBy cannot be used together") {
    // TODO: This should be an AnalysisException, but it ends up as a Spark Exception
    // that arises from the Connect Client failing to enrich the exception with the Delta
    // error class that we expect.
    val e = intercept[Exception] {
      io.delta.tables.DeltaTable.create(spark).tableName("testTable")
        .addColumn("c1", "int")
        .addColumn("c2", "int")
        .partitionedBy("c2")
        .clusterBy("c1")
        .execute()
    }
    val deltaErrorClass = "DELTA_CLUSTER_BY_WITH_PARTITIONED_BY"
    assert(e.getMessage.contains(deltaErrorClass))
  }

  test("create table with identity columns") {
    withTempPath { dir =>
      val path = dir.getAbsolutePath
      DeltaTable.create(spark)
        .tableName("testTable")
        .addColumn(
          DeltaTable.columnBuilder(spark, "id1")
            .dataType("long")
            .nullable(false)
            .generatedAlwaysAsIdentity()
            .build()
        )
        .addColumn(
          DeltaTable.columnBuilder(spark, "id2")
            .dataType("long")
            .nullable(false)
            .generatedByDefaultAsIdentity()
            .build()
        )
        .addColumn(
          DeltaTable.columnBuilder(spark, "id3")
            .dataType("long")
            .nullable(false)
            .generatedAlwaysAsIdentity(start = 100, step = 10)
            .build()
        )
        .addColumn(
          DeltaTable.columnBuilder(spark, "id4")
            .dataType("long")
            .nullable(false)
            .generatedByDefaultAsIdentity(start = 100, step = 10)
            .build()
        )
        .location(path)
        .execute()

      // Verify the columns exist in the schema
      val table = DeltaTable.forPath(spark, path)
      val schema = table.toDF.schema
      
      // Verify basic structure
      assert(schema.fieldNames.toSeq === Seq("id1", "id2", "id3", "id4"))
      schema.fields.foreach { field =>
        assert(field.dataType.typeName === "long")
        assert(!field.nullable)
      }
      
      // Test identity column functionality: 
      // Insert data without providing values for GENERATED ALWAYS identity columns
      // and verify correct identity values are generated
      spark.sql(s"INSERT INTO delta.`$path` (id2, id4) VALUES (20, 200)")
      
      // First row should have id1=1, id3=100 (start values) generated automatically
      // id2=20, id4=200 should be the explicitly provided values
      val result = spark.sql(s"SELECT * FROM delta.`$path`").collect()
      assert(result.length === 1)
      assert(result(0).getLong(0) === 1) // id1 = 1 (default start)
      assert(result(0).getLong(1) === 20) // id2 = 20 (explicitly provided)
      assert(result(0).getLong(2) === 100) // id3 = 100 (custom start)
      assert(result(0).getLong(3) === 200) // id4 = 200 (explicitly provided)
      
      // Verify identity column insertion restrictions
      // Attempting to explicitly insert a value for a GENERATED ALWAYS identity column should fail
      val e = intercept[Exception] {
        spark.sql(s"INSERT INTO delta.`$path` (id1, id2, id3, id4) VALUES (10, 20, 30, 40)")
      }
      // TODO: This should be an AnalysisException, but it ends up as a Spark Exception
      // that arises from the Connect Client failing to enrich the exception with the Delta
      // error class that we expect.
      val deltaErrorClass = "DELTA_IDENTITY_COLUMNS_EXPLICIT_INSERT_NOT_SUPPORTED"
      assert(
      e.getMessage.contains(deltaErrorClass),
        "Explicit inserts are never possible for a GENERATED ALWAYS identity column.")
    }
  }
}

