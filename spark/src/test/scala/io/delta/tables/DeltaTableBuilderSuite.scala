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

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchDatabaseException, TableAlreadyExistsException}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StringType, StructType}

class DeltaTableBuilderSuite extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  // Define the information for a default test table used by many tests.
  protected val defaultTestTableSchema = "c1 int, c2 int, c3 string"
  protected val defaultTestTableGeneratedColumns = Map("c2" -> "c1 + 10")
  protected val defaultTestTablePartitionColumns = Seq("c1")
  protected val defaultTestTableColumnComments = Map("c1" -> "foo", "c3" -> "bar")
  protected val defaultTestTableComment = "tbl comment"
  protected val defaultTestTableNullableCols = Set("c1", "c3")
  protected val defaultTestTableProperty = ("foo", "bar")


  /**
   * Verify if the table metadata matches the test table. We use this to verify DDLs
   * write correct table metadata into the transaction logs.
   */
  protected def verifyTestTableMetadata(
      table: String,
      schemaString: String,
      generatedColumns: Map[String, String] = Map.empty,
      colComments: Map[String, String] = Map.empty,
      colNullables: Set[String] = Set.empty,
      tableComment: Option[String] = None,
      partitionCols: Seq[String] = Seq.empty,
      tableProperty: Option[(String, String)] = None): Unit = {
    val deltaLog = if (table.startsWith("delta.")) {
      DeltaLog.forTable(spark, table.stripPrefix("delta.`").stripSuffix("`"))
    } else {
      DeltaLog.forTable(spark, TableIdentifier(table))
    }
    val schema = StructType.fromDDL(schemaString)
    val expectedSchema = StructType(schema.map { field =>
      val newMetadata = new MetadataBuilder()
        .withMetadata(field.metadata)
      if (generatedColumns.contains(field.name)) {
        newMetadata.putString(GENERATION_EXPRESSION_METADATA_KEY, generatedColumns(field.name))
      }
      if (colComments.contains(field.name)) {
        newMetadata.putString("comment", colComments(field.name))
      }
      field.copy(
        nullable = colNullables.contains(field.name),
        metadata = newMetadata.build)
    })
    val metadata = deltaLog.snapshot.metadata
    assert(metadata.schema == expectedSchema)
    assert(metadata.partitionColumns == partitionCols)
    if (tableProperty.nonEmpty) {
      assert(metadata.configuration(tableProperty.get._1).contentEquals(tableProperty.get._2))
    }
    if (tableComment.nonEmpty) {
      assert(metadata.description.contentEquals(tableComment.get))
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
          Some(defaultTestTableProperty)
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
            Some(defaultTestTableProperty)
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
            Some(defaultTestTableProperty)
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
      io.delta.tables.DeltaTable.createIfNotExists()
    } else {
      io.delta.tables.DeltaTable.create()
    }
    defaultTableBuilder(tableBuilder, tableName, location)
  }

  def defaultReplaceTableBuilder(
      orCreate: Boolean,
      tableName: Option[String] = None,
      location: Option[String] = None): DeltaTableBuilder = {
    var tableBuilder = if (orCreate) {
      io.delta.tables.DeltaTable.createOrReplace()
    } else {
      io.delta.tables.DeltaTable.replace()
    }
    defaultTableBuilder(tableBuilder, tableName, location)
  }

  private def defaultTableBuilder(
      builder: DeltaTableBuilder, tableName: Option[String], location: Option[String]) = {
    var tableBuilder = builder
    if (tableName.nonEmpty) {
      tableBuilder = tableBuilder.tableName(tableName.get)
    }
    if (location.nonEmpty) {
      tableBuilder = tableBuilder.location(location.get)
    }
    tableBuilder.addColumn(
      io.delta.tables.DeltaTable.columnBuilder("c1").dataType("int").nullable(true).comment("foo")
        .build()
    )
    tableBuilder.addColumn(
      io.delta.tables.DeltaTable.columnBuilder("c2").dataType("int")
        .nullable(false).generatedAlwaysAs("c1 + 10").build()
    )
    tableBuilder.addColumn(
      io.delta.tables.DeltaTable.columnBuilder("c3").dataType("string").comment("bar").build()
    )
    tableBuilder.partitionedBy("c1")
    tableBuilder.property("foo", "bar")
    tableBuilder.comment("tbl comment")
    tableBuilder
  }

  test("create table with existing schema and extra column") {
    withTable("table") {
      withTempDir { dir =>
        spark.range(10).toDF("key").write.format("parquet").saveAsTable("table")
        val existingSchema = spark.read.format("parquet").table("table").schema
        io.delta.tables.DeltaTable.create()
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
      DeltaTable.columnBuilder("value")
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
      sql(s"CREATE TABLE testTable (c1 int) USING DELTA")
      intercept[TableAlreadyExistsException] {
        defaultCreateTableBuilder(ifNotExists = false, Some("testTable")).execute()
      }
    }
  }

  test("create table - ignore if already exists") {
    withTable("testTable") {
      sql(s"CREATE TABLE testTable (c1 int) USING DELTA")
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
    intercept[CannotReplaceMissingTableException] {
      defaultReplaceTableBuilder(orCreate = false, Some("testTable")).execute()
    }
  }

  testCreateTable("replace_table") { table =>
    sql(s"CREATE TABLE replace_table(c1 int) USING DELTA")
    defaultReplaceTableBuilder(orCreate = false, Some(table)).execute()
  }

  testCreateTableWithNameAndLocation("replace_table") { (name, path) =>
    sql(s"CREATE TABLE $name (c1 int) USING DELTA LOCATION '$path'")
    defaultReplaceTableBuilder(orCreate = false, Some(name), Some(path)).execute()
  }

  testCreateTableWithLocationOnly("replace_table") { path =>
    sql(s"CREATE TABLE delta.`$path` (c1 int) USING DELTA")
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
      io.delta.tables.DeltaTable.create().addColumn("c1", "int").execute()
    }
    assert(e.getMessage.equals("Table name or location has to be specified"))
  }

  test("partitionedBy only should contain columns in the schema") {
    val e = intercept[AnalysisException] {
      io.delta.tables.DeltaTable.create().tableName("testTable")
        .addColumn("c1", "int")
        .partitionedBy("c2")
        .execute()
    }
    assert(e.getMessage.startsWith("Couldn't find column c2"))
  }

  test("errors if table name and location are different paths") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.create().tableName(s"delta.`$path`")
          .addColumn("c1", "int")
          .location("src/test/resources/delta/dbr_8_0_non_generated_columns")
          .execute()
      }
      assert(e.getMessage.startsWith(
        "Creating path-based Delta table with a different location isn't supported."))
    }
  }

  test("table name and location are the same") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      io.delta.tables.DeltaTable.create().tableName(s"delta.`$path`")
        .addColumn("c1", "int")
        .location(path)
        .execute()
    }
  }

  test("errors if use parquet path as identifier") {
    withTempDir { dir =>
      val path = dir.getAbsolutePath
      val e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.create().tableName(s"parquet.`$path`")
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

  test("delta table property case") {
    val preservedCaseConfig = Map("delta.appendOnly" -> "true", "Foo" -> "Bar", "foo" -> "Bar")
    val lowerCaseEnforcedConfig = Map("delta.appendOnly" -> "true", "foo" -> "Bar")

    sealed trait DeltaTablePropertySetOperation {
      def setTableProperty(tablePath: String): Unit

      def expectedConfig: Map[String, String]

      def description: String
    }

    trait CasePreservingTablePropertySetOperation extends DeltaTablePropertySetOperation {

      val expectedConfig = preservedCaseConfig
    }

    case object SetPropertyThroughCreate extends CasePreservingTablePropertySetOperation {
      def setTableProperty(tablePath: String): Unit = sql(
        s"CREATE TABLE delta.`$tablePath`(id INT) " +
          s"USING delta TBLPROPERTIES('delta.appendOnly'='true', 'Foo'='Bar', 'foo'='Bar' ) "
      )

      val description = "Setting Table Property at Table Creation"
    }

    case object SetPropertyThroughAlter extends CasePreservingTablePropertySetOperation {
      def setTableProperty(tablePath: String): Unit = {
        spark.range(1, 10).write.format("delta").save(tablePath)
        sql(s"ALTER TABLE delta.`$tablePath` " +
          s"SET TBLPROPERTIES('delta.appendOnly'='true', 'Foo'='Bar', 'foo'='Bar')")
      }

      val description = "Setting Table Property via Table Alter"
    }

    case class SetPropertyThroughTableBuilder(backwardCompatible: Boolean) extends
      DeltaTablePropertySetOperation {

      def setTableProperty(tablePath: String): Unit = {
        withSQLConf(DeltaSQLConf.TABLE_BUILDER_FORCE_TABLEPROPERTY_LOWERCASE.key
          -> backwardCompatible.toString) {
          DeltaTable.create()
            .location(tablePath)
            .property("delta.appendOnly", "true")
            .property("Foo", "Bar")
            .property("foo", "Bar")
            .execute()
        }
      }

      override def expectedConfig : Map[String, String] = {
        if (backwardCompatible) {
          lowerCaseEnforcedConfig
        }
        else {
          preservedCaseConfig
        }
      }

      val description = s"Setting Table Property on DeltaTableBuilder." +
        s" Backward compatible enabled = ${backwardCompatible}"
    }

    val examples = Seq(
      SetPropertyThroughCreate,
      SetPropertyThroughAlter,
      SetPropertyThroughTableBuilder(backwardCompatible = true),
      SetPropertyThroughTableBuilder(backwardCompatible = false)
    )

    for (example <- examples) {
      withClue(example.description) {
        withTempDir { dir =>
          val path = dir.getCanonicalPath()
          example.setTableProperty(path)
          val config = DeltaLog.forTable(spark, path).snapshot.metadata.configuration
          assert(
            config == example.expectedConfig,
            s"$example's result is not correct: $config")
        }
      }
    }
  }

}
