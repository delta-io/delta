/*
 * Copyright (2020) The Delta Lake Project Authors.
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
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeUtils.{getZoneId, stringToDate, stringToTimestamp, toJavaDate, toJavaTimestamp}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.StreamingQueryException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, DateType, IntegerType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

trait GeneratedColumnSuiteBase extends QueryTest with SharedSparkSession with DeltaSQLCommandTest {

  import GeneratedColumn._
  import testImplicits._

  protected def sqlDate(date: String): java.sql.Date = {
    toJavaDate(stringToDate(
      UTF8String.fromString(date),
      getZoneId(SQLConf.get.sessionLocalTimeZone)).get)
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

  /** Manually generate table metadata to create a table with generated columns */
  protected def createTable(
      tableName: String,
      path: Option[String],
      schemaString: String,
      generatedColumns: Map[String, String],
      partitionColumns: Seq[String]): Unit = {
    def updateTableMetadataWithGeneratedColumn(deltaLog: DeltaLog): Unit = {
      val txn = deltaLog.startTransaction()
      val schema = StructType.fromDDL(schemaString)
      val finalSchema = StructType(schema.map { field =>
        generatedColumns.get(field.name).map { expr =>
          GeneratedColumn.withGenerationExpression(field, expr)
        }.getOrElse(field)
      })
      val metadata = Metadata(schemaString = finalSchema.json, partitionColumns = partitionColumns)
      txn.updateMetadataForNewTable(metadata)
      txn.commit(Nil, ManualUpdate)
    }

    if (path.isEmpty) {
      sql(s"CREATE TABLE $tableName(foo INT) USING delta")
      val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))
      updateTableMetadataWithGeneratedColumn(deltaLog)
      spark.catalog.refreshTable(tableName)
    } else {
      sql(s"CREATE TABLE $tableName(foo INT) USING delta LOCATION '${path.get}'")
      val deltaLog = DeltaLog.forTable(spark, path.get)
      updateTableMetadataWithGeneratedColumn(deltaLog)
      spark.catalog.refreshTable(tableName)
    }
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
      defaultTestTablePartitionColumns)
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


  testTableUpdate("insert_into_select_provide_all_columns") { (table, path) =>
    sql(s"INSERT INTO $table SELECT " +
      s"1, 11, 'foo', '2020-10-11', '2020-10-11 12:30:30', 100, 1000, '2020-11-12'")
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
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
    assert(e.getMessage.contains(
      "CHECK constraint Generated Column (`c2_g` <=> (`c1` + 10)) violated by row with values"))
    Row(1L, 11L, "foo", sqlDate("2020-10-11"), sqlTimestamp("2020-10-11 12:30:30"),
      100, 1000, sqlDate("2020-11-12")) :: Nil
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
        assert(e.getMessage.contains(
          "CHECK constraint Generated Column (`c2_g` <=> (`c1` + 10)) violated by row with values"))
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
      assert(e.getMessage.contains(
        "CHECK constraint Generated Column (`id2` <=> (`id` + 10)) violated by row with values"))
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
    val e = intercept[AnalysisException](validateGeneratedColumns(spark, schema))
    assert(e.getMessage.contains(
      "A generated column cannot use a non-existent column or another generated column"))
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
    val e = intercept[AnalysisException](validateGeneratedColumns(spark, schema))
    assert(e.getMessage.contains(
      "A generated column cannot use a non-existent column or another generated column"))
  }

  test("validateGeneratedColumns: unsupported expressions") {
    spark.udf.register("myudf", (s: Array[Int]) => s)
    for ((exprString, error) <- Seq(
      "myudf(foo)" -> "Found myudf(foo). A generated column cannot use a user-defined function",
      "first(foo)" ->
        "Found first(`foo`). A generated column cannot use a non deterministic expression",
      "max(foo)" -> "Found max(`foo`). A generated column cannot use an aggregate expression",
      "explode(foo)" -> "explode(`foo`) cannot be used in a generated column",
      "current_timestamp" -> "current_timestamp() cannot be used in a generated column"
    )) {
      val f1 = StructField("foo", ArrayType(IntegerType, true))
      val f2 = withGenerationExpression(StructField("bar", IntegerType), exprString)
      val schema = StructType(f1 :: f2 :: Nil)
      val e = intercept[AnalysisException](validateGeneratedColumns(spark, schema))
      assert(e.getMessage.contains(error))
    }
  }

  test("validateGeneratedColumns: column type doesn't match expression type") {
    val f1 = StructField("foo", IntegerType)
    val f2 = withGenerationExpression(StructField("bar", IntegerType), "CAST(foo AS string)")
    val schema = StructType(f1 :: f2 :: Nil)
    val e = intercept[AnalysisException](validateGeneratedColumns(spark, schema))
    assert(e.getMessage.contains("The expression type of the generated column bar is STRING, " +
      "but the column type is INT"))
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
        assert(e.getMessage.contains(
          "CHECK constraint Generated Column (`c2` <=> CONCAT(`c1`, 'y')) " +
            "violated by row with values"))
      }
      quietly {
        val e =
          intercept[InvariantViolationException](sql(s"INSERT INTO $table VALUES(null, 'foo')"))
        assert(e.getMessage.contains(
          "CHECK constraint Generated Column (`c2` <=> CONCAT(`c1`, 'y')) " +
            "violated by row with values"))
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

}

class GeneratedColumnSuite extends GeneratedColumnSuiteBase
