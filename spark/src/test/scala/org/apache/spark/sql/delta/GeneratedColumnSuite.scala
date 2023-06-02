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
import java.io.PrintWriter

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.commands.cdc.CDCReader
import org.apache.spark.sql.delta.schema.{DeltaInvariantViolationException, InvariantViolationException, SchemaUtils}
import org.apache.spark.sql.delta.sources.DeltaSourceUtils.GENERATION_EXPRESSION_METADATA_KEY
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.tables.DeltaTableBuilder

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp, toJavaDate, toJavaTimestamp}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{StreamingQueryException, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, MetadataBuilder, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

trait GeneratedColumnTest extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {


  protected def sqlDate(date: String): java.sql.Date = {
    toJavaDate(stringToDate(UTF8String.fromString(date)).get)
  }

  protected def sqlTimestamp(timestamp: String): java.sql.Timestamp = {
    toJavaTimestamp(stringToTimestamp(
      UTF8String.fromString(timestamp),
      getZoneId(SQLConf.get.sessionLocalTimeZone)).get)
  }

  protected def withTableName[T](tableName: String)(func: String => T): Unit = {
    withTable(tableName) {
      func(tableName)
    }
  }

  /** Create a new field with the given generation expression. */
  def withGenerationExpression(field: StructField, expr: String): StructField = {
    val newMetadata = new MetadataBuilder()
      .withMetadata(field.metadata)
      .putString(GENERATION_EXPRESSION_METADATA_KEY, expr)
      .build()
    field.copy(metadata = newMetadata)
  }

  protected def buildTable(
      builder: DeltaTableBuilder,
      tableName: String,
      path: Option[String],
      schemaString: String,
      generatedColumns: Map[String, String],
      partitionColumns: Seq[String],
      notNullColumns: Set[String],
      comments: Map[String, String],
      properties: Map[String, String]): DeltaTableBuilder = {
    val schema = if (schemaString.nonEmpty) {
      StructType.fromDDL(schemaString)
    } else {
      new StructType()
    }
    val cols = schema.map(field => (field.name, field.dataType))
    if (tableName != null) {
      builder.tableName(tableName)
    }
    cols.foreach(col => {
      val (colName, dataType) = col
      val nullable = !notNullColumns.contains(colName)
      var columnBuilder = io.delta.tables.DeltaTable.columnBuilder(spark, colName)
      columnBuilder.dataType(dataType.sql)
      columnBuilder.nullable(nullable)
      if (generatedColumns.contains(colName)) {
        columnBuilder.generatedAlwaysAs(generatedColumns(colName))
      }
      if (comments.contains(colName)) {
        columnBuilder.comment(comments(colName))
      }
      builder.addColumn(columnBuilder.build())
    })
    if (partitionColumns.nonEmpty) {
      builder.partitionedBy(partitionColumns: _*)
    }
    if (path.nonEmpty) {
      builder.location(path.get)
    }
    properties.foreach { case (key, value) =>
      builder.property(key, value)
    }
    builder
  }

  protected def createTable(
      tableName: String,
      path: Option[String],
      schemaString: String,
      generatedColumns: Map[String, String],
      partitionColumns: Seq[String],
      notNullColumns: Set[String] = Set.empty,
      comments: Map[String, String] = Map.empty,
      properties: Map[String, String] = Map.empty): Unit = {
    var tableBuilder = io.delta.tables.DeltaTable.create(spark)
    buildTable(tableBuilder, tableName, path, schemaString,
      generatedColumns, partitionColumns, notNullColumns, comments, properties)
      .execute()
  }
}

trait GeneratedColumnSuiteBase extends GeneratedColumnTest {

  import GeneratedColumn._
  import testImplicits._

  protected def replaceTable(
      tableName: String,
      path: Option[String],
      schemaString: String,
      generatedColumns: Map[String, String],
      partitionColumns: Seq[String],
      notNullColumns: Set[String] = Set.empty,
      comments: Map[String, String] = Map.empty,
      properties: Map[String, String] = Map.empty,
      orCreate: Option[Boolean] = None): Unit = {
    var tableBuilder = if (orCreate.getOrElse(false)) {
      io.delta.tables.DeltaTable.createOrReplace(spark)
    } else {
      io.delta.tables.DeltaTable.replace(spark)
    }
    buildTable(tableBuilder, tableName, path, schemaString,
      generatedColumns, partitionColumns, notNullColumns, comments, properties).execute()
  }

  // Define the information for a default test table used by many tests.
  protected val defaultTestTableSchema =
    "c1 bigint, c2_g bigint, c3_p string, c4_g_p date, c5 timestamp, c6 int, c7_g_p int, c8 date"
  protected val defaultTestTableGeneratedColumns = Map(
    "c2_g" -> "c1 + 10",
    "c4_g_p" -> "cast(c5 as date)",
    "c7_g_p" -> "c6 * 10"
  )
  protected val defaultTestTablePartitionColumns = "c3_p, c4_g_p, c7_g_p".split(", ").toList

  protected def createDefaultTestTable(tableName: String, path: Option[String] = None): Unit = {
    createTable(
      tableName,
      path,
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns
    )
  }

  /**
   * @param updateFunc A function that's called with the table information (tableName, path). It
   *                   should execute update operations, and return the expected data after
   *                   updating.
   */
  protected def testTableUpdate(
      testName: String,
      isStreaming: Boolean = false)(updateFunc: (String, String) => Seq[Row]): Unit = {
    def testBody(): Unit = {
      val table = testName
      withTempDir { path =>
        withTable(table) {
          createDefaultTestTable(tableName = table, path = Some(path.getCanonicalPath))
          val expected = updateFunc(testName, path.getCanonicalPath)
          checkAnswer(sql(s"select * from $table"), expected)
        }
      }
    }

    if (isStreaming) {
      test(testName) {
        testBody()
      }
    } else {
      test(testName) {
        testBody()
      }
    }
  }

  private def errorContains(errMsg: String, str: String): Unit = {
     assert(errMsg.contains(str))
  }

  protected def testTableUpdateDPO(
    testName: String)(updateFunc: (String, String) => Seq[Row]): Unit = {
    withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->
      SQLConf.PartitionOverwriteMode.DYNAMIC.toString) {
      testTableUpdate("dpo_" + testName)(updateFunc)
    }
  }

  testTableUpdate("append_data") { (table, path) =>
    Seq(
      Tuple5(1L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12")
    ).toDF("c1", "c3_p", "c5", "c6", "c8")
      .withColumn("c5", $"c5".cast(TimestampType))
      .withColumn("c8", $"c8".cast(DateType))
      .write
      .format("delta")
      .mode("append")
      .save(path)
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("append_data_in_different_column_order") { (table, path) =>
    Seq(
      Tuple5("2020-10-11 12:30:30", 100, "2020-11-12", 1L, "foo")
    ).toDF("c5", "c6", "c8", "c1", "c3_p")
      .withColumn("c5", $"c5".cast(TimestampType))
      .withColumn("c8", $"c8".cast(DateType))
      .write
      .format("delta")
      .mode("append")
      .save(path)
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("append_data_v2") { (table, _) =>
    Seq(
      Tuple5(1L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12")
    ).toDF("c1", "c3_p", "c5", "c6", "c8")
      .withColumn("c5", $"c5".cast(TimestampType))
      .withColumn("c8", $"c8".cast(DateType))
      .writeTo(table)
      .append()
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("append_data_in_different_column_order_v2") { (table, _) =>
    Seq(
      Tuple5("2020-10-11 12:30:30", 100, "2020-11-12", 1L, "foo")
    ).toDF("c5", "c6", "c8", "c1", "c3_p")
      .withColumn("c5", $"c5".cast(TimestampType))
      .withColumn("c8", $"c8".cast(DateType))
      .writeTo(table)
      .append()
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }


  testTableUpdate("insert_into_values_provide_all_columns") { (table, path) =>
    sql(s"INSERT INTO $table VALUES" +
      s"(1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_into_by_name_provide_all_columns") { (table, _) =>
    sql(s"INSERT INTO $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p) VALUES" +
      s"('2020-10-11 12:30:30', 100, 1000, '2020-11-12', 1, 11, 'foo', '2020-10-11')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_into_by_name_not_provide_generated_columns") { (table, _) =>
    sql(s"INSERT INTO $table (c6, c8, c1, c3_p, c5) VALUES" +
      s"(100, '2020-11-12', 1L, 'foo', '2020-10-11 12:30:30')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_into_by_name_with_some_generated_columns") { (table, _) =>
    sql(s"INSERT INTO $table (c5, c6, c8, c1, c3_p, c4_g_p) VALUES" +
      s"('2020-10-11 12:30:30', 100, '2020-11-12', 1L, 'foo', '2020-10-11')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_into_select_provide_all_columns") { (table, path) =>
    sql(s"INSERT INTO $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_into_by_name_not_provide_normal_columns") { (table, _) =>
    val e = intercept[AnalysisException] {
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        sql(s"INSERT INTO $table (c6, c8, c1, c3_p) VALUES" +
          s"(100, '2020-11-12', 1L, 'foo')")
      }
    }
    errorContains(e.getMessage, "Column c5 is not specified in INSERT")
    Nil
  }

  testTableUpdate("insert_overwrite_values_provide_all_columns") { (table, path) =>
    sql(s"INSERT OVERWRITE TABLE $table VALUES" +
      s"(1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_overwrite_select_provide_all_columns") { (table, path) =>
    sql(s"INSERT OVERWRITE TABLE $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_overwrite_by_name_provide_all_columns") { (table, _) =>
    sql(s"INSERT OVERWRITE $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p) VALUES" +
      s"('2020-10-11 12:30:30', 100, 1000, '2020-11-12', 1, 11, 'foo', '2020-10-11')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_overwrite_by_name_not_provide_generated_columns") { (table, _) =>
    sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p, c5) VALUES" +
      s"(100, '2020-11-12', 1L, 'foo', '2020-10-11 12:30:30')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_overwrite_by_name_with_some_generated_columns") { (table, _) =>
    sql(s"INSERT OVERWRITE $table (c5, c6, c8, c1, c3_p, c4_g_p) VALUES" +
      s"('2020-10-11 12:30:30', 100, '2020-11-12', 1L, 'foo', '2020-10-11')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("insert_overwrite_by_name_not_provide_normal_columns") { (table, _) =>
    val e = intercept[AnalysisException] {
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p) VALUES" +
          s"(100, '2020-11-12', 1L, 'foo')")
      }
    }
    errorContains(e.getMessage, "Column c5 is not specified in INSERT")
    Nil
  }

  testTableUpdateDPO("insert_overwrite_values_provide_all_columns") { (table, path) =>
    sql(s"INSERT OVERWRITE TABLE $table VALUES" +
      s"(1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12')")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdateDPO("insert_overwrite_select_provide_all_columns") { (table, path) =>
    sql(s"INSERT OVERWRITE TABLE $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdateDPO("insert_overwrite_by_name_values_provide_all_columns") { (table, _) =>
    sql(s"INSERT OVERWRITE $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p) VALUES" +
      s"(CAST('2020-10-11 12:30:30' AS TIMESTAMP), 100, 1000, CAST('2020-11-12' AS DATE), " +
      s"1L, 11L, 'foo', CAST('2020-10-11' AS DATE))")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdateDPO(
    "insert_overwrite_by_name_not_provide_generated_columns") { (table, _) =>
    sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p, c5) VALUES" +
      s"(100, CAST('2020-11-12' AS DATE), 1L, 'foo', CAST('2020-10-11 12:30:30' AS TIMESTAMP))")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdateDPO("insert_overwrite_by_name_with_some_generated_columns") { (table, _) =>
    sql(s"INSERT OVERWRITE $table (c5, c6, c8, c1, c3_p, c4_g_p) VALUES" +
      s"(CAST('2020-10-11 12:30:30' AS TIMESTAMP), 100, CAST('2020-11-12' AS DATE), 1L, " +
      s"'foo', CAST('2020-10-11' AS DATE))")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdateDPO("insert_overwrite_by_name_not_provide_normal_columns") { (table, _) =>
    val e = intercept[AnalysisException] {
      sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p) VALUES" +
        s"(100, '2020-11-12', 1L, 'foo')")
    }
    assert(e.getMessage.contains("Column c5 is not specified in INSERT"))
    Nil
  }

  testTableUpdate("delete") { (table, path) =>
    Seq(
      Tuple5(1L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12"),
      Tuple5(2L, "foo", "2020-10-11 13:30:30", 100, "2020-12-12")
    ).toDF("c1", "c3_p", "c5", "c6", "c8")
      .withColumn("c5", $"c5".cast(TimestampType))
      .withColumn("c8", $"c8".cast(DateType))
      .coalesce(1)
      .write
      .format("delta")
      .mode("append")
      .save(path)
    // Make sure we create only one file so that we will trigger file rewriting.
    assert(DeltaLog.forTable(spark, path).snapshot.allFiles.count == 1)
    sql(s"DELETE FROM $table WHERE c1 = 2")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("update_generated_column_with_correct_value") { (table, path) =>
    sql(s"INSERT INTO $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    sql(s"UPDATE $table SET c2_g = 11 WHERE c1 = 1")
    Row(1, 11, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("update_generated_column_with_incorrect_value") { (table, path) =>
    sql(s"INSERT INTO $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    val e = intercept[InvariantViolationException] {
      quietly {
        sql(s"UPDATE $table SET c2_g = 12 WHERE c1 = 1")
      }
    }
    errorContains(e.getMessage,
      "CHECK constraint Generated Column (c2_g <=> (c1 + 10)) violated by row with values")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("update_source_column_used_by_generated_column") { (table, _) =>
    sql(s"INSERT INTO $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    sql(s"UPDATE $table SET c1 = 2 WHERE c1 = 1")
    Row(2, 12, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("update_source_and_generated_columns_with_correct_value") { (table, _) =>
    sql(s"INSERT INTO $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    sql(s"UPDATE $table SET c2_g = 12, c1 = 2 WHERE c1 = 1")
    Row(2, 12, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("update_source_and_generated_columns_with_incorrect_value") { (table, _) =>
    sql(s"INSERT INTO $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    val e = intercept[InvariantViolationException] {
      quietly {
        sql(s"UPDATE $table SET c2_g = 12, c1 = 3 WHERE c1 = 1")
      }
    }
    errorContains(e.getMessage,
      "CHECK constraint Generated Column (c2_g <=> (c1 + 10)) violated by row with values")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  test("various update commands") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath
      withTableName("update_commands") { table =>
        createTable(table, Some(path), "c INT, g INT", Map("g" -> "c + 10"), Nil)
        sql(s"INSERT INTO $table VALUES(10, 20)")
        sql(s"UPDATE $table SET c = 20")
        checkAnswer(spark.table(table), Row(20, 30) :: Nil)
        sql(s"UPDATE delta.`$path` SET c = 30")
        checkAnswer(spark.table(table), Row(30, 40) :: Nil)
        io.delta.tables.DeltaTable.forName(table).updateExpr(Map("c" -> "40"))
        checkAnswer(spark.table(table), Row(40, 50) :: Nil)
        io.delta.tables.DeltaTable.forPath(path).updateExpr(Map("c" -> "50"))
        checkAnswer(spark.table(table), Row(50, 60) :: Nil)
      }
    }
  }

  test("update with various column references") {
    withTableName("update_with_various_references") { table =>
      createTable(table, None, "c1 INT, c2 INT, g INT", Map("g" -> "c1 + 10"), Nil)
      sql(s"INSERT INTO $table VALUES(10, 50, 20)")
      sql(s"UPDATE $table SET c1 = 20")
      checkAnswer(spark.table(table), Row(20, 50, 30) :: Nil)
      sql(s"UPDATE $table SET c1 = c2 + 100, c2 = 1000")
      checkAnswer(spark.table(table), Row(150, 1000, 160) :: Nil)
      sql(s"UPDATE $table SET c1 = c2 + g")
      checkAnswer(spark.table(table), Row(1160, 1000, 1170) :: Nil)
      sql(s"UPDATE $table SET c1 = g")
      checkAnswer(spark.table(table), Row(1170, 1000, 1180) :: Nil)
    }
  }

  test("update a struct source column") {
    withTableName("update_struct_column") { table =>
      createTable(table,
        None,
        "s STRUCT<s1: INT, s2: STRING>, g INT",
        Map("g" -> "s.s1 + 10"),
        Nil)
      sql(s"INSERT INTO $table VALUES(struct(10, 'foo'), 20)")
      sql(s"UPDATE $table SET s.s1 = 20 WHERE s.s1 = 10")
      checkAnswer(spark.table(table), Row(Row(20, "foo"), 30) :: Nil)
    }
  }

  test("updating a temp view is not supported") {
    withTableName("update_temp_view") { table =>
      createTable(table, None, "c1 INT, c2 INT", Map("c2" -> "c1 + 10"), Nil)
      withTempView("test_view") {
        sql(s"CREATE TEMP VIEW test_view AS SELECT * FROM $table")
        val e = intercept[AnalysisException] {
          sql(s"UPDATE test_view SET c1 = 2 WHERE c1 = 1")
        }
        assert(e.getMessage.contains("a temp view"))
      }
    }
  }

  testTableUpdate("streaming_write", isStreaming = true) { (table, path) =>
    withTempDir { checkpointDir =>
      val stream = MemoryStream[Int]
      val q = stream.toDF
        .map(_ => Tuple5(1L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12"))
        .toDF("c1", "c3_p", "c5", "c6", "c8")
        .withColumn("c5", $"c5".cast(TimestampType))
        .withColumn("c8", $"c8".cast(DateType))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(path)
      stream.addData(1)
      q.processAllAvailable()
      q.stop()
    }
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("streaming_write_with_different_case", isStreaming = true) { (table, path) =>
    withTempDir { checkpointDir =>
      val stream = MemoryStream[Int]
      val q = stream.toDF
        .map(_ => Tuple5(1L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12"))
        .toDF("C1", "c3_p", "c5", "c6", "c8") // C1 is using upper case
        .withColumn("c5", $"c5".cast(TimestampType))
        .withColumn("c8", $"c8".cast(DateType))
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .start(path)
      stream.addData(1)
      q.processAllAvailable()
      q.stop()
    }
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
  }

  testTableUpdate("streaming_write_incorrect_value", isStreaming = true) { (table, path) =>
    withTempDir { checkpointDir =>
      quietly {
        val stream = MemoryStream[Int]
        val q = stream.toDF
          // 2L is an incorrect value. The correct value should be 11L
          .map(_ => Tuple6(1L, 2L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12"))
          .toDF("c1", "c2_g", "c3_p", "c5", "c6", "c8")
          .withColumn("c5", $"c5".cast(TimestampType))
          .withColumn("c8", $"c8".cast(DateType))
          .writeStream
          .format("delta")
          .outputMode("append")
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .start(path)
        stream.addData(1)
        val e = intercept[StreamingQueryException] {
          q.processAllAvailable()
        }
        errorContains(e.getMessage,
          "CHECK constraint Generated Column (c2_g <=> (c1 + 10)) violated by row with values")
        q.stop()
      }
    }
    Nil
  }

  testQuietly("write to a generated column with an incorrect value") {
    withTableName("write_incorrect_value") { table =>
      createTable(table, None, "id INT, id2 INT", Map("id2" -> "id + 10"), partitionColumns = Nil)
      val e = intercept[InvariantViolationException] {
        sql(s"INSERT INTO $table VALUES(1, 12)")
      }
      errorContains(e.getMessage,
        "CHECK constraint Generated Column (id2 <=> (id + 10)) violated by row with values")
    }
  }

  test("dot in the column name") {
    withTableName("dot_in_column_name") { table =>
      createTable(table, None, "`a.b` INT, `x.y` INT", Map("x.y" -> "`a.b` + 10"), Nil)
      sql(s"INSERT INTO $table VALUES(1, 11)")
      sql(s"INSERT INTO $table VALUES(2, 12)")
      checkAnswer(sql(s"SELECT * FROM $table"), Row(1, 11) :: Row(2, 12) :: Nil)
    }
  }

  test("validateGeneratedColumns: generated columns should not refer to non-existent columns") {
    val f1 = StructField("c1", IntegerType)
    val f2 = withGenerationExpression(StructField("c2", IntegerType), "c10 + 10")
    val schema = StructType(f1 :: f2 :: Nil)
    val e = intercept[DeltaAnalysisException](validateGeneratedColumns(spark, schema))
    errorContains(e.getMessage,
      "A generated column cannot use a non-existent column or another generated column")
  }

  test("validateGeneratedColumns: no generated columns") {
    val f1 = StructField("c1", IntegerType)
    val f2 = StructField("c2", IntegerType)
    val schema = StructType(f1 :: f2 :: Nil)
    validateGeneratedColumns(spark, schema)
  }

  test("validateGeneratedColumns: all generated columns") {
    val f1 = withGenerationExpression(StructField("c1", IntegerType), "1 + 2")
    val f2 = withGenerationExpression(StructField("c1", IntegerType), "3 + 4")
    val schema = StructType(f1 :: f2 :: Nil)
    validateGeneratedColumns(spark, schema)
  }

  test("validateGeneratedColumns: generated columns should not refer to other generated columns") {
    val f1 = StructField("c1", IntegerType)
    val f2 = withGenerationExpression(StructField("c2", IntegerType), "c1 + 10")
    val f3 = withGenerationExpression(StructField("c3", IntegerType), "c2 + 10")
    val schema = StructType(f1 :: f2 :: f3 :: Nil)
    val e = intercept[DeltaAnalysisException](validateGeneratedColumns(spark, schema))
    errorContains(e.getMessage,
      "A generated column cannot use a non-existent column or another generated column")
  }

  test("validateGeneratedColumns: supported expressions") {
    for (exprString <- Seq(
      // Generated column should support timestamp to date
      "to_date(foo, \"yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'\")")) {
      val f1 = StructField("foo", TimestampType)
      val f2 = withGenerationExpression(StructField("bar", DateType), exprString)
      val schema = StructType(Seq(f1, f2))
      validateGeneratedColumns(spark, schema)
    }
  }

  test("validateGeneratedColumns: unsupported expressions") {
    spark.udf.register("myudf", (s: Array[Int]) => s)
    for ((exprString, error) <- Seq(
      "myudf(foo)" -> "Found myudf(foo). A generated column cannot use a user-defined function",
      "rand()" ->
        "Found rand(). A generated column cannot use a non deterministic expression",
      "max(foo)" -> "Found max(foo). A generated column cannot use an aggregate expression",
      "explode(foo)" -> "explode(foo) cannot be used in a generated column",
      "current_timestamp" -> "current_timestamp() cannot be used in a generated column"
    )) {
      val f1 = StructField("foo", ArrayType(IntegerType, true))
      val f2 = withGenerationExpression(StructField("bar", IntegerType), exprString)
      val schema = StructType(f1 :: f2 :: Nil)
      val e = intercept[AnalysisException](validateGeneratedColumns(spark, schema))
      errorContains(e.getMessage, error)
    }
  }

  test("validateGeneratedColumns: column type doesn't match expression type") {
    val f1 = StructField("foo", IntegerType)
    val f2 = withGenerationExpression(StructField("bar", IntegerType), "CAST(foo AS string)")
    val schema = StructType(f1 :: f2 :: Nil)
    val e = intercept[AnalysisException](validateGeneratedColumns(spark, schema))
    errorContains(e.getMessage, "The expression type of the generated column bar is STRING, " +
      "but the column type is INT")
  }

  test("test partition transform expressions end to end") {
    withTableName("partition_transform_expressions") { table =>
      createTable(table, None,
        "time TIMESTAMP, year DATE, month DATE, day DATE, hour TIMESTAMP",
        Map(
          "year" -> "make_date(year(time), 1, 1)",
          "month" -> "make_date(year(time), month(time), 1)",
          "day" -> "make_date(year(time), month(time), day(time))",
          "hour" -> "make_timestamp(year(time), month(time), day(time), hour(time), 0, 0)"
        ),
        partitionColumns = Nil)
      Seq("2020-10-11 12:30:30")
        .toDF("time")
        .withColumn("time", $"time".cast(TimestampType))
        .write
        .format("delta")
        .mode("append").
        saveAsTable(table)
      checkAnswer(
        sql(s"SELECT * from $table"),
        Row(sqlTimestamp("2020-10-11 12:30:30"), sqlDate("2020-01-01"), sqlDate("2020-10-01"),
          sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:00:00"))
      )
    }
  }

  test("the generation expression constraint should support null values") {
    withTableName("null") { table =>
      createTable(table, None, "c1 STRING, c2 STRING", Map("c2" -> "CONCAT(c1, 'y')"), Nil)
      sql(s"INSERT INTO $table VALUES('x', 'xy')")
      sql(s"INSERT INTO $table VALUES(null, null)")
      checkAnswer(
        sql(s"SELECT * from $table"),
        Row("x", "xy") :: Row(null, null) :: Nil
      )
      quietly {
        val e =
          intercept[InvariantViolationException](sql(s"INSERT INTO $table VALUES('foo', null)"))
        errorContains(e.getMessage,
          "CHECK constraint Generated Column (c2 <=> CONCAT(c1, 'y')) " +
            "violated by row with values")
      }
      quietly {
        val e =
          intercept[InvariantViolationException](sql(s"INSERT INTO $table VALUES(null, 'foo')"))
        errorContains(e.getMessage,
          "CHECK constraint Generated Column (c2 <=> CONCAT(c1, 'y')) " +
            "violated by row with values")
      }
    }
  }

  test("complex type extractors") {
    withTableName("struct_field") { table =>
      createTable(
        table,
        None,
        "`a.b` STRING, a STRUCT<b: INT, c: STRING>, array ARRAY<INT>, " +
          "c1 STRING, c2 INT, c3 INT",
        Map("c1" -> "CONCAT(`a.b`, 'b')", "c2" -> "a.b + 100", "c3" -> "array[1]"),
        Nil)
      sql(s"INSERT INTO $table VALUES(" +
        s"'a', struct(100, 'foo'), array(1000, 1001), " +
        s"'ab', 200, 1001)")
      checkAnswer(
        spark.table(table),
        Row("a", Row(100, "foo"), Array(1000, 1001), "ab", 200, 1001) :: Nil)
    }
  }

  test("getGeneratedColumnsAndColumnsUsedByGeneratedColumns") {
    def testSchema(schema: Seq[StructField], expected: Set[String]): Unit = {
      assert(getGeneratedColumnsAndColumnsUsedByGeneratedColumns(StructType(schema)) == expected)
    }

    val f1 = StructField("c1", IntegerType)
    val f2 = withGenerationExpression(StructField("c2", IntegerType), "c1 + 10")
    val f3 = StructField("c3", IntegerType)
    val f4 = withGenerationExpression(StructField("c4", IntegerType), "hash(c3 + 10)")
    val f5 = withGenerationExpression(StructField("c5", IntegerType), "hash(C1 + 10)")
    val f6 = StructField("c6", StructType(StructField("x", IntegerType) :: Nil))
    val f6x = StructField("c6.x", IntegerType)
    val f7x = withGenerationExpression(StructField("c7.x", IntegerType), "`c6.x` + 10")
    val f8 = withGenerationExpression(StructField("c8", IntegerType), "c6.x + 10")
    testSchema(Seq(f1, f2), Set("c1", "c2"))
    testSchema(Seq(f1, f2, f3), Set("c1", "c2"))
    testSchema(Seq(f1, f2, f3, f4), Set("c1", "c2", "c3", "c4"))
    testSchema(Seq(f1, f2, f5), Set("c1", "c2", "c5"))
    testSchema(Seq(f6x, f7x), Set("c6.x", "c7.x"))
    testSchema(Seq(f6, f6x, f7x), Set("c6.x", "c7.x"))
    testSchema(Seq(f6, f6x, f8), Set("c6", "c8"))
  }

  test("disallow column type evolution") {
    withTableName("disallow_column_type_evolution") { table =>
      // "CAST(HASH(c1 + 32767s) AS SMALLINT)" is a special expression that returns different
      // results for SMALLINT and INT. For example, "CAST(hash(32767 + 32767s) AS SMALLINT)" returns
      // 9876, but "SELECT CAST(hash(32767s + 32767s) AS SMALLINT)" returns 31349. Hence we should
      // not allow updating column type from SMALLINT to INT.
      createTable(table, None, "c1 SMALLINT, c2 SMALLINT",
        Map("c2" -> "CAST(HASH(c1 + 32767s) AS SMALLINT)"), Nil)
      val tableSchema = spark.table(table).schema
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        Seq(32767.toShort).toDF("c1").write.format("delta").mode("append").saveAsTable(table)
      }
      assert(tableSchema == spark.table(table).schema)
      // Insert an INT to `c1` should fail rather than changing the `c1` type to INT
      val e = intercept[AnalysisException] {
        Seq(32767).toDF("c1").write.format("delta").mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(table)
      }.getMessage
      assert(e.contains("Column c1") &&
        e.contains("The data type is SMALLINT. It doesn't accept data type INT"))
      checkAnswer(spark.table(table), Row(32767, 31349) :: Nil)
    }
  }


  test("reading from a Delta table should not see generation expressions") {
    def verifyNoGenerationExpression(df: Dataset[_]): Unit = {
      assert(!hasGeneratedColumns(df.schema))
    }

    withTableName("test_source") { table =>
      createTable(table, None, "c1 INT, c2 INT", Map("c1" -> "c2 + 1"), Nil)
      sql(s"INSERT INTO $table VALUES(2, 1)")
      val path = DeltaLog.forTable(spark, TableIdentifier(table)).dataPath.toString

      verifyNoGenerationExpression(spark.table(table))
      verifyNoGenerationExpression(spark.sql(s"select * from $table"))
      verifyNoGenerationExpression(spark.sql(s"select * from delta.`$path`"))
      verifyNoGenerationExpression(spark.read.format("delta").load(path))
      verifyNoGenerationExpression(spark.read.format("delta").table(table))
      verifyNoGenerationExpression(spark.readStream.format("delta").load(path))
      verifyNoGenerationExpression(spark.readStream.format("delta").table(table))
      withTempDir { checkpointDir =>
        val q = spark.readStream.format("delta").table(table).writeStream
          .trigger(Trigger.Once)
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .foreachBatch { (ds: DataFrame, _: Long) =>
            verifyNoGenerationExpression(ds)
          }.start()
        try {
          q.processAllAvailable()
        } finally {
          q.stop()
        }
      }
      withTempDir { outputDir =>
        withTempDir { checkpointDir =>
          val q = spark.readStream.format("delta").table(table).writeStream
            .trigger(Trigger.Once)
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("delta")
            .start(outputDir.getCanonicalPath)
          try {
            q.processAllAvailable()
          } finally {
            q.stop()
          }
          val deltaLog = DeltaLog.forTable(spark, outputDir)
          assert(deltaLog.snapshot.version >= 0)
          assert(!hasGeneratedColumns(deltaLog.snapshot.schema))
        }
      }
    }
  }

  /**
   * Verify if the table metadata matches the default test table. We use this to verify DDLs
   * write correct table metadata into the transaction logs.
   */
  protected def verifyDefaultTestTableMetadata(table: String): Unit = {
    val deltaLog = if (table.startsWith("delta.")) {
      DeltaLog.forTable(spark, table.stripPrefix("delta.`").stripSuffix("`"))
    } else {
      DeltaLog.forTable(spark, TableIdentifier(table))
    }
    val schema = StructType.fromDDL(defaultTestTableSchema)
    val expectedSchema = StructType(schema.map { field =>
      defaultTestTableGeneratedColumns.get(field.name).map { expr =>
        withGenerationExpression(field, expr)
      }.getOrElse(field)
    })
    val partitionColumns = defaultTestTablePartitionColumns
    val metadata = deltaLog.snapshot.metadata
    assert(metadata.schema == expectedSchema)
    assert(metadata.partitionColumns == partitionColumns)
  }

  protected def testCreateTable(testName: String)(createFunc: String => Unit): Unit = {
    test(testName) {
      withTable(testName) {
        createFunc(testName)
        verifyDefaultTestTableMetadata(testName)
      }
    }
  }

  protected def testCreateTableWithLocation(
      testName: String)(createFunc: (String, String) => Unit): Unit = {
    test(testName + ": external") {
      withTempPath { path =>
        withTable(testName) {
          createFunc(testName, path.getCanonicalPath)
          verifyDefaultTestTableMetadata(testName)
          verifyDefaultTestTableMetadata(s"delta.`${path.getCanonicalPath}`")
        }
      }
    }
  }

  testCreateTable("create_table") { table =>
    createTable(
      table,
      None,
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns
    )
  }

  testCreateTable("replace_table") { table =>
    createTable(table, None, "id bigint", Map.empty, Seq.empty)
    replaceTable(
      table,
      None,
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns
    )
  }

  testCreateTable("create_or_replace_table_non_exist") { table =>
    replaceTable(
      table,
      None,
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns,
      orCreate = Some(true)
    )
  }

  testCreateTable("create_or_replace_table_exist") { table =>
    createTable(table, None, "id bigint", Map.empty, Seq.empty)
    replaceTable(
      table,
      None,
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns,
      orCreate = Some(true)
    )
  }

  testCreateTableWithLocation("create_table") { (table, path) =>
    createTable(
      table,
      Some(path),
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns
    )
  }

  testCreateTableWithLocation("replace_table") { (table, path) =>
    createTable(
      table,
      Some(path),
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns
    )
    replaceTable(
      table,
      Some(path),
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns
    )
  }

  testCreateTableWithLocation("create_or_replace_table_non_exist") { (table, path) =>
    replaceTable(
      table,
      Some(path),
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns,
      orCreate = Some(true)
    )
  }

  testCreateTableWithLocation("create_or_replace_table_exist") { (table, path) =>
    createTable(
      table,
      Some(path),
      "id bigint",
      Map.empty,
      Seq.empty
    )
    replaceTable(
      table,
      Some(path),
      defaultTestTableSchema,
      defaultTestTableGeneratedColumns,
      defaultTestTablePartitionColumns,
      orCreate = Some(true)
    )
  }

  test("using generated columns should upgrade the protocol") {
    withTableName("upgrade_protocol") { table =>
      def protocolVersions: (Int, Int) = {
        sql(s"DESC DETAIL $table")
          .select("minReaderVersion", "minWriterVersion")
          .as[(Int, Int)]
          .head()
      }

      // Use the default protocol versions when not using computed partitions
      createTable(table, None, "i INT", Map.empty, Seq.empty)
      assert(protocolVersions == (1, 2))
      assert(DeltaLog.forTable(spark, TableIdentifier(tableName = table)).snapshot.version == 0)

      // Protocol versions should be upgraded when using computed partitions
      replaceTable(
        table,
        None,
        defaultTestTableSchema,
        defaultTestTableGeneratedColumns,
        defaultTestTablePartitionColumns)
      assert(protocolVersions == (1, 4))
      // Make sure we did overwrite the table rather than deleting and re-creating.
      assert(DeltaLog.forTable(spark, TableIdentifier(tableName = table)).snapshot.version == 1)
    }
  }

  test("creating a table with a different schema should fail") {
    withTempPath { path =>
      // Currently SQL is the only way to define a table using generated columns. So we create a
      // temp table and drop it to get a path for such table.
      withTableName("temp_generated_column_table") { table =>
        createTable(
          null,
          Some(path.toString),
          defaultTestTableSchema,
          defaultTestTableGeneratedColumns,
          defaultTestTablePartitionColumns
        )
      }
      withTableName("table_with_no_schema") { table =>
        createTable(
          table,
          Some(path.toString),
          "",
          Map.empty,
          Seq.empty
        )
        verifyDefaultTestTableMetadata(table)
      }
      withTableName("table_with_different_expr") { table =>
        val e = intercept[AnalysisException](
          createTable(
            table,
            Some(path.toString),
            defaultTestTableSchema,
            Map(
              "c2_g" -> "c1 + 11", // use a different generated expr
              "c4_g_p" -> "CAST(c5 AS date)",
              "c7_g_p" -> "c6 * 10"
            ),
            defaultTestTablePartitionColumns
          )
        )
        assert(e.getMessage.contains(
          "Specified generation expression for field c2_g is different from existing schema"))
        assert(e.getMessage.contains("Specified: c1 + 11"))
        assert(e.getMessage.contains("Existing:  c1 + 10"))
      }
      withTableName("table_add_new_expr") { table =>
        val e = intercept[AnalysisException](
          createTable(
            table,
            Some(path.toString),
            defaultTestTableSchema,
            Map(
              "c2_g" -> "c1 + 10",
              "c3_p" -> "CAST(c1 AS string)", // add a generated expr
              "c4_g_p" -> "CAST(c5 AS date)",
              "c7_g_p" -> "c6 * 10"
            ),
            defaultTestTablePartitionColumns
          )
        )
        assert(e.getMessage.contains(
          "Specified generation expression for field c3_p is different from existing schema"))
        assert(e.getMessage.contains("CAST(c1 AS string)"))
        assert(e.getMessage.contains("Existing:  \n"))
      }
    }
  }

  test("use the generation expression, column comment and NOT NULL at the same time") {
    withTableName("generation_expression_comment") { table =>
      createTable(
        table,
        None,
        "c1 INT, c2 INT, c3 INT",
        Map("c2" -> "c1 + 10", "c3" -> "c1 + 10"),
        Seq.empty,
        Set("c3"),
        Map("c2" -> "foo", "c3" -> "foo")
      )
      // Verify schema
      val f1 = StructField("c1", IntegerType, nullable = true)
      val fieldMetadata = new MetadataBuilder()
        .putString(GENERATION_EXPRESSION_METADATA_KEY, "c1 + 10")
        .putString("comment", "foo")
        .build()
      val f2 = StructField("c2", IntegerType, nullable = true, metadata = fieldMetadata)
      val f3 = StructField("c3", IntegerType, nullable = false, metadata = fieldMetadata)
      val expectedSchema = StructType(f1 :: f2 :: f3 :: Nil)
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(table))
      assert(deltaLog.snapshot.metadata.schema == expectedSchema)
      // Verify column comment
      val comments = sql(s"DESC $table")
        .where("col_name = 'c2'")
        .select("comment")
        .as[String]
        .collect()
        .toSeq
      assert("foo" :: Nil == comments)
    }
  }

  test("MERGE UPDATE basic") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE SET ${tgt}.c2 = ${src}.c2
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 3, 4))
        )
      }
    }
  }

  test("MERGE UPDATE set both generated column and its input") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE SET ${tgt}.c2 = ${src}.c2, ${tgt}.c3 = ${src}.c3
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 3, 4))
        )
      }
    }
  }

  test("MERGE UPDATE set star") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 4, 5);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE SET *
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 4, 5))
        )
      }
    }
  }

  test("MERGE UPDATE set star add column") {
    withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {
      withTableName("source") { src =>
        withTableName("target") { tgt =>
          createTable(src, None, "c1 INT, c2 INT, c4 INT", Map.empty, Seq.empty)
          sql(s"INSERT INTO ${src} values (1, 20, 40);")
          createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
          sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
          sql(
            s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE SET *
               |""".stripMargin)
          checkAnswer(
            sql(s"SELECT * FROM ${tgt}"),
            Seq(Row(1, 20, 21, 40))
          )
        }
      }
    }
  }

  test("MERGE UPDATE using value from target") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE SET ${tgt}.c2 = ${tgt}.c3
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 3, 4))
        )
      }
    }
  }

  test("MERGE UPDATE using value from both target and source") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE SET ${tgt}.c2 = ${tgt}.c3 + ${src}.c3
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 7, 8))
        )
      }
    }
  }

  test("MERGE UPDATE set to null") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE SET ${tgt}.c2 = null
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, null, null))
        )
      }
    }
  }

  test("MERGE UPDATE multiple columns") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED THEN UPDATE
               |  SET ${tgt}.c2 = ${src}.c1 * 10, ${tgt}.c1 = ${tgt}.c1 * 100
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(100, 10, 11))
        )
      }
    }
  }

  test("MERGE UPDATE source is a query") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING (SELECT c1, max(c3) + min(c2) AS m FROM ${src} GROUP BY c1) source
               |on ${tgt}.c1 = source.c1
               |WHEN MATCHED THEN UPDATE SET ${tgt}.c2 = source.m
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 7, 8))
        )
      }
    }
  }

  test("MERGE UPDATE temp view is not supported") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        withTempView("test_temp_view") {
          createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
          sql(s"INSERT INTO ${src} values (1, 3, 4);")
          createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
          sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
          sql(s"CREATE TEMP VIEW test_temp_view AS SELECT c1 as c2, c2 as c1, c3 FROM ${tgt}")
          val e = intercept[AnalysisException] {
            sql(s"""
                   |MERGE INTO test_temp_view
                   |USING ${src}
                   |on test_temp_view.c2 = ${src}.c1
                   |WHEN MATCHED THEN UPDATE SET test_temp_view.c1 = ${src}.c2
                   |""".stripMargin)
          }
          assert(e.getMessage.contains("a temp view"))
        }
      }
    }
  }

  test("MERGE INSERT star satisfies constraint") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 2, 3), Row(2, 3, 4))
        )
      }
    }
  }

  test("MERGE INSERT star violates constraint") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 5);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        val e = intercept[InvariantViolationException](
          sql(s"""
                 |MERGE INTO ${tgt}
                 |USING ${src}
                 |on ${tgt}.c1 = ${src}.c1
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin)
        )
        assert(e.getMessage.contains("CHECK constraint Generated Column"))
      }
    }
  }

  test("MERGE INSERT star add column") {
    withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {
      withTableName("source") { src =>
        withTableName("target") { tgt =>
          createTable(src, None, "c1 INT, c2 INT, c4 INT", Map.empty, Seq.empty)
          sql(s"INSERT INTO ${src} values (2, 3, 5);")
          createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
          sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
          sql(s"""
                 |MERGE INTO ${tgt}
                 |USING ${src}
                 |on ${tgt}.c1 = ${src}.c1
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin)
          checkAnswer(
            sql(s"SELECT * FROM ${tgt}"),
            Seq(Row(1, 2, 3, null), Row(2, 3, 4, 5))
          )
        }
      }
    }
  }

  test("MERGE INSERT star add column violates constraint") {
    withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {
      withTableName("source") { src =>
        withTableName("target") { tgt =>
          createTable(src, None, "c1 INT, c3 INT, c4 INT", Map.empty, Seq.empty)
          sql(s"INSERT INTO ${src} values (2, 3, 5);")
          createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
          sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
          val e = intercept[InvariantViolationException](
            sql(s"""
                 |MERGE INTO ${tgt}
                 |USING ${src}
                 |on ${tgt}.c1 = ${src}.c1
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin)
          )
          assert(e.getMessage.contains("CHECK constraint Generated Column"))
        }
      }
    }
  }

  test("MERGE INSERT star add column unrelated to generated columns") {
    withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {
      withTableName("source") { src =>
        withTableName("target") { tgt =>
          createTable(src, None, "c1 INT, c4 INT, c5 INT", Map.empty, Seq.empty)
          sql(s"INSERT INTO ${src} values (2, 3, 5);")
          createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
          sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
          sql(s"""
                 |MERGE INTO ${tgt}
                 |USING ${src}
                 |on ${tgt}.c1 = ${src}.c1
                 |WHEN NOT MATCHED THEN INSERT *
                 |""".stripMargin)
          checkAnswer(
            sql(s"SELECT * FROM ${tgt}"),
            Seq(Row(1, 2, 3, null, null), Row(2, null, null, 3, 5))
          )
        }
      }
    }
  }

  test("MERGE INSERT unrelated columns") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN NOT MATCHED THEN INSERT (c1) VALUES (${src}.c1)
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 2, 3), Row(2, null, null))
        )
      }
    }
  }

  test("MERGE INSERT unrelated columns with const") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN NOT MATCHED THEN INSERT (c1) VALUES (3)
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 2, 3), Row(3, null, null))
        )
      }
    }
  }

  test("MERGE INSERT referenced column only") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN NOT MATCHED THEN INSERT (c2) VALUES (10)
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 2, 3), Row(null, 10, 11))
        )
      }
    }
  }

  test("MERGE INSERT referenced column with null") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN NOT MATCHED THEN INSERT (c2) VALUES (null)
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 2, 3), Row(null, null, null))
        )
      }
    }
  }

  test("MERGE INSERT not all referenced column inserted") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + c1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN NOT MATCHED THEN INSERT (c2) VALUES (5)
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 2, 3), Row(null, 5, null))
        )
      }
    }
  }

  test("MERGE INSERT generated column only") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        val e = intercept[InvariantViolationException](
          sql(s"""
                 |MERGE INTO ${tgt}
                 |USING ${src}
                 |on ${tgt}.c1 = ${src}.c1
                 |WHEN NOT MATCHED THEN INSERT (c3) VALUES (10)
                 |""".stripMargin)
        )
        assert(e.getMessage.contains("CHECK constraint Generated Column"))
      }
    }
  }

  test("MERGE INSERT referenced and generated columns satisfies constraint") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 4);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN NOT MATCHED THEN INSERT (c2, c3) VALUES (${src}.c2, ${src}.c3)
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 2, 3), Row(null, 3, 4))
        )
      }
    }
  }

  test("MERGE INSERT referenced and generated columns violates constraint") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (2, 3, 5);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3);")
        val e = intercept[InvariantViolationException](
          sql(s"""
                 |MERGE INTO ${tgt}
                 |USING ${src}
                 |on ${tgt}.c1 = ${src}.c1
                 |WHEN NOT MATCHED THEN INSERT (c2, c3) VALUES (${src}.c2, ${src}.c3)
                 |""".stripMargin)
        )
        assert(e.getMessage.contains("CHECK constraint Generated Column"))
      }
    }
  }

  test("MERGE INSERT and UPDATE") {
    withTableName("source") { src =>
      withTableName("target") { tgt =>
        createTable(src, None, "c1 INT, c2 INT, c3 INT", Map.empty, Seq.empty)
        sql(s"INSERT INTO ${src} values (1, 11, 12), (2, 3, 4), (3, 20, 21), (4, 5, 6), (5, 6, 7);")
        createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
        sql(s"INSERT INTO ${tgt} values (1, 2, 3), (2, 100, 101);")
        sql(s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED AND ${src}.c1 = 1 THEN UPDATE SET ${tgt}.c2 = 100
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED AND ${src}.c1 = 4 THEN INSERT (c1, c2) values (${src}.c1, 22)
               |WHEN NOT MATCHED AND ${src}.c1 = 5 THEN INSERT (c1, c2) values (5, ${src}.c3)
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
        checkAnswer(
          sql(s"SELECT * FROM ${tgt}"),
          Seq(Row(1, 100, 101), Row(2, 3, 4), Row(3, 20, 21), Row(4, 22, 23), Row(5, 7, 8))
        )
      }
    }
  }

  test("MERGE INSERT and UPDATE schema evolution") {
    withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {
      withTableName("source") { src =>
        withTableName("target") { tgt =>
          createTable(src, None, "c1 INT, c2 INT, c4 INT", Map.empty, Seq.empty)
          sql(s"INSERT INTO ${src} values (1, 11, 12), (2, 3, 4), (3, 20, 21), " +
            "(4, 5, 6), (5, 6, 7);")
          createTable(tgt, None, "c1 INT, c2 INT, c3 INT", Map("c3" -> "c2 + 1"), Seq.empty)
          sql(s"INSERT INTO ${tgt} values (1, 2, 3), (2, 100, 101);")
          sql(
            s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED AND ${src}.c1 = 1 THEN UPDATE SET ${tgt}.c2 = 100
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED AND ${src}.c1 = 4 THEN INSERT (c1, c2) values (${src}.c1, 22)
               |WHEN NOT MATCHED AND ${src}.c1 = 5 THEN INSERT (c1, c2) values (5, ${src}.c4)
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
          checkAnswer(
            sql(s"SELECT * FROM ${tgt}"),
            Seq(
              Row(1, 100, 101, null),
              Row(2, 3, 4, 4),
              Row(3, 20, 21, 21),
              Row(4, 22, 23, null),
              Row(5, 7, 8, null)
            )
          )
        }
      }
    }
  }

  test("MERGE INSERT and UPDATE schema evolution multiple referenced columns") {
    withSQLConf(("spark.databricks.delta.schema.autoMerge.enabled", "true")) {
      withTableName("source") { src =>
        withTableName("target") { tgt =>
          createTable(src, None, "c1 INT, c2 INT, c4 INT", Map.empty, Seq.empty)
          sql(s"INSERT INTO ${src} values (1, 11, 12), (2, null, 4), (3, 20, 21), " +
            "(4, 5, 6), (5, 6, 7);")
          createTable(tgt, None, "c1 INT, c2 INT, c3 INT",
            Map("c3" -> "c1 + CAST(ISNULL(c2) AS INT)"), Seq.empty)
          sql(s"INSERT INTO ${tgt} values (1, 2, 1), (2, 100, 2);")
          sql(
            s"""
               |MERGE INTO ${tgt}
               |USING ${src}
               |on ${tgt}.c1 = ${src}.c1
               |WHEN MATCHED AND ${src}.c1 = 1 THEN UPDATE SET ${tgt}.c2 = 100
               |WHEN MATCHED THEN UPDATE SET *
               |WHEN NOT MATCHED AND ${src}.c1 = 4 THEN INSERT (c1, c2) values (${src}.c1, 22)
               |WHEN NOT MATCHED AND ${src}.c1 = 5 THEN INSERT (c1) values (5)
               |WHEN NOT MATCHED THEN INSERT *
               |""".stripMargin)
          checkAnswer(
            sql(s"SELECT * FROM ${tgt}"),
            Seq(
              Row(1, 100, 1, null),
              Row(2, null, 3, 4),
              Row(3, 20, 3, 21),
              Row(4, 22, 4, null),
              Row(5, null, 6, null)
            )
          )
        }
      }
    }
  }

  test("generated columns with cdf") {
    val tableName1 = "gcEnabledCDCOn"
    val tableName2 = "gcEnabledCDCOff"
    withTable(tableName1, tableName2) {

      createTable(
        tableName1,
        None,
        schemaString = "id LONG, timeCol TIMESTAMP, dateCol DATE",
        generatedColumns = Map(
          "dateCol" -> "CAST(timeCol AS DATE)"
        ),
        partitionColumns = Seq("dateCol"),
        properties = Map(
          "delta.enableChangeDataFeed" -> "true"
        )
      )

      spark.range(100).repartition(10)
        .withColumn("timeCol", current_timestamp())
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(tableName1)

      spark.sql(s"DELETE FROM ${tableName1} WHERE id < 3")

      val changeData = spark.read.format("delta").option("readChangeData", "true")
        .option("startingVersion", "2")
        .table(tableName1)
        .select("id", CDCReader.CDC_TYPE_COLUMN_NAME, CDCReader.CDC_COMMIT_VERSION)

      val expected = spark.range(0, 3)
        .withColumn(CDCReader.CDC_TYPE_COLUMN_NAME, lit("delete"))
        .withColumn(CDCReader.CDC_COMMIT_VERSION, lit(2))
      checkAnswer(changeData, expected)

      // Now write out the data frame of cdc to another table that has generated columns but not
      // cdc enabled.
      createTable(
        tableName2,
        None,
        schemaString = "id LONG, _change_type STRING, timeCol TIMESTAMP, dateCol DATE",
        generatedColumns = Map(
          "dateCol" -> "CAST(timeCol AS DATE)"
        ),
        partitionColumns = Seq("dateCol"),
        properties = Map(
          "delta.enableChangeDataFeed" -> "false"
        )
      )

      val cdcRead = spark.read.format("delta").option("readChangeData", "true")
        .option("startingVersion", "2")
        .table(tableName1)
        .select("id", CDCReader.CDC_TYPE_COLUMN_NAME, "timeCol")

      cdcRead
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(tableName2)

      checkAnswer(
        cdcRead,
        spark.table(tableName2).drop("dateCol")
      )
    }
  }

  test("not null should be enforced with generated columns") {
    withTableName("tbl") { tbl =>
      createTable(tbl,
        None, "c1 INT, c2 STRING, c3 INT", Map("c3" -> "c1 + 1"), Seq.empty, Set("c1", "c2", "c3"))

      // try to write data without c2 in the DF
      val schemaWithoutColumnC2 = StructType(
        Seq(StructField("c1", IntegerType, true)))
      val data1 = List(Row(3))
      val df1 = spark.createDataFrame(data1.asJava, schemaWithoutColumnC2)

      val e1 = intercept[DeltaInvariantViolationException] {
        df1.write.format("delta").mode("append").saveAsTable("tbl")
      }
      assert(e1.getMessage.contains("Column c2, which has a NOT NULL constraint," +
        " is missing from the data being written into the table."))
    }
  }

  Seq(true, false).foreach { allowNullInsert =>
    test("nullable column should work with generated columns - " +
      "allowNullInsert enabled=" + allowNullInsert) {
      withTableName("tbl") { tbl =>
        withSQLConf(DeltaSQLConf.GENERATED_COLUMN_ALLOW_NULLABLE.key -> allowNullInsert.toString) {
          createTable(
            tbl, None, "c1 INT, c2 STRING, c3 INT", Map("c3" -> "c1 + 1"), Seq.empty)

          // create data frame that matches the table's schema
          val data1 = List(Row(1, "a1"), Row(2, "a2"))
          val schema = StructType(
            Seq(StructField("c1", IntegerType, true), StructField("c2", StringType, true)))
          val df1 = spark.createDataFrame(data1.asJava, schema)
          df1.write.format("delta").mode("append").saveAsTable("tbl")

          // create a data frame that does not have c2
          val schemaWithoutOptionalColumnC2 = StructType(
            Seq(StructField("c1", IntegerType, true)))

          val data2 = List(Row(3))
          val df2 = spark.createDataFrame(data2.asJava, schemaWithoutOptionalColumnC2)

          if (allowNullInsert) {
            df2.write.format("delta").mode("append").saveAsTable("tbl")
            // check correctness
            val expectedDF = df1
              .union(df2.withColumn("c2", lit(null).cast(StringType)))
              .withColumn("c3", 'c1 + 1)
            checkAnswer(spark.read.table(tbl), expectedDF)
          } else {
            // when allow null insert is not enabled.
            val e = intercept[AnalysisException] {
              df2.write.format("delta").mode("append").saveAsTable("tbl")
            }
            e.getMessage.contains(
              "A column or function parameter with name `c2` cannot be resolved")
          }
        }
      }
    }
  }
}

class GeneratedColumnSuite extends GeneratedColumnSuiteBase

