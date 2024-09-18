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
import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaSQLTestUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchPartitionException
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}

class DeltaDDLSuite extends DeltaDDLTestBase with SharedSparkSession
  with DeltaSQLCommandTest {

  override protected def verifyNullabilityFailure(exception: AnalysisException): Unit = {
    exception.getMessage.contains("Cannot change nullable column to non-nullable")
  }

  test("protocol-related properties are not considered during duplicate table creation") {
    def createTable(tableName: String, location: String): Unit = {
      sql(s"""
             |CREATE TABLE $tableName (id INT, val STRING)
             |USING DELTA
             |LOCATION '$location'
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.minReaderVersion' = '2',
             |  'delta.minWriterVersion' = '5'
             |)""".stripMargin
      )
    }
    withTempDir { dir =>
      val table1 = "t1"
      val table2 = "t2"
      withTable(table1, table2) {
        withSQLConf(DeltaSQLConf.DELTA_UPDATE_CATALOG_ENABLED.key -> "true") {
          createTable(table1, dir.getCanonicalPath)
          createTable(table2, dir.getCanonicalPath)
          val catalogTable1 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table1))
          val catalogTable2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier(table2))
          assert(catalogTable1.properties("delta.columnMapping.mode") == "name")
          assert(catalogTable2.properties("delta.columnMapping.mode") == "name")
        }
      }
    }
  }

  test("table creation with ambiguous paths only allowed with legacy flag") {
    // ambiguous paths not allowed
    withTempDir { foo =>
      withTempDir { bar =>
          val fooPath = foo.getCanonicalPath()
          val barPath = bar.getCanonicalPath()
          val e = intercept[AnalysisException] {
            sql(s"CREATE TABLE delta.`$fooPath`(id LONG) USING delta LOCATION '$barPath'")
          }
          assert(e.message.contains("legacy.allowAmbiguousPathsInCreateTable"))
      }
    }

    // allowed with legacy flag
    withTempDir { foo =>
      withTempDir { bar =>
        val fooPath = foo.getCanonicalPath()
        val barPath = bar.getCanonicalPath()
        withSQLConf(DeltaSQLConf.DELTA_LEGACY_ALLOW_AMBIGUOUS_PATHS.key -> "true") {
          sql(s"CREATE TABLE delta.`$fooPath`(id LONG) USING delta LOCATION '$barPath'")
          assert(io.delta.tables.DeltaTable.isDeltaTable(fooPath))
          assert(!io.delta.tables.DeltaTable.isDeltaTable(barPath))
        }
      }
    }

    // allowed if paths are the same
    withTempDir { foo =>
      val fooPath = foo.getCanonicalPath()
      sql(s"CREATE TABLE delta.`$fooPath`(id LONG) USING delta LOCATION '$fooPath'")
      assert(io.delta.tables.DeltaTable.isDeltaTable(fooPath))
    }
  }

  test("append table when column name with special chars") {
    withTable("t") {
      val schema = new StructType().add("a`b", "int")
      val df = spark.createDataFrame(sparkContext.emptyRDD[Row], schema)
      df.write.format("delta").saveAsTable("t")
      df.write.format("delta").mode("append").saveAsTable("t")
      assert(spark.table("t").collect().isEmpty)
    }
  }
}


class DeltaDDLNameColumnMappingSuite extends DeltaDDLSuite
  with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests = Seq(
    "create table with NOT NULL - check violation through file writing",
    "ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed"
  )
}


abstract class DeltaDDLTestBase extends QueryTest with DeltaSQLTestUtils {
  import testImplicits._

  protected def verifyDescribeTable(tblName: String): Unit = {
    val res = sql(s"DESCRIBE TABLE $tblName").collect()
    assert(res.takeRight(2).map(_.getString(0)) === Seq("name", "dept"))
  }

  protected def verifyNullabilityFailure(exception: AnalysisException): Unit

  protected def getDeltaLog(tableLocation: String): DeltaLog = {
      DeltaLog.forTable(spark, tableLocation)
  }


  testQuietly("create table with NOT NULL - check violation through file writing") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String NOT NULL)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("delta_test").schema === expectedSchema)

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("delta_test"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        Seq((1L, "a")).toDF("a", "b")
          .write.format("delta").mode("append").save(table.location.toString)
        val read = spark.read.format("delta").load(table.location.toString)
        checkAnswer(read, Seq(Row(1L, "a")))

        intercept[InvariantViolationException] {
          Seq((2L, null)).toDF("a", "b")
            .write.format("delta").mode("append").save(table.location.getPath)
        }
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL - not supported") {
    withTempDir { dir =>
      val tableName = "delta_test_add_not_null"
      withTable(tableName) {
        sql(s"""
               |CREATE TABLE $tableName(a LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType().add("a", LongType, nullable = true)
        assert(spark.table(tableName).schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE $tableName
               |ADD COLUMNS (b String NOT NULL, c Int)""".stripMargin)
        }
        val msg = "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for Delta tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from nullable to NOT NULL - not supported") {
    withTempDir { dir =>
      val tableName = "delta_test_from_nullable_to_not_null"
      withTable(tableName) {
        sql(s"""
               |CREATE TABLE $tableName(a LONG, b String)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table(tableName).schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE $tableName
               |CHANGE COLUMN b b String NOT NULL""".stripMargin)
        }
        verifyNullabilityFailure(e)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from NOT NULL to nullable") {
    withTempDir { dir =>
      val tableName = "delta_test_not_null_to_nullable"
      withTable(tableName) {
        sql(
          s"""
             |CREATE TABLE $tableName(a LONG NOT NULL, b String)
             |USING delta
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

        val expectedSchema = new StructType()
          .add("a", LongType, nullable = false)
          .add("b", StringType, nullable = true)
        assert(spark.table(tableName).schema === expectedSchema)

        sql(s"INSERT INTO $tableName SELECT 1, 'a'")
        checkAnswer(
          sql(s"SELECT * FROM $tableName"),
          Seq(Row(1L, "a")))

        sql(
          s"""
             |ALTER TABLE $tableName
             |ALTER COLUMN a DROP NOT NULL""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = true)
        assert(spark.table(tableName).schema === expectedSchema2)

        sql(s"INSERT INTO $tableName SELECT NULL, 'b'")
        checkAnswer(
          sql(s"SELECT * FROM $tableName"),
          Seq(Row(1L, "a"), Row(null, "b")))
      }
    }
  }

  testQuietly("create table with NOT NULL - check violation through SQL") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String NOT NULL)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("a", LongType, nullable = true)
          .add("b", StringType, nullable = false)
        assert(spark.table("delta_test").schema === expectedSchema)

        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(1L, "a")))

        val e = intercept[InvariantViolationException] {
          sql("INSERT INTO delta_test VALUES (2, null)")
        }
        if (!e.getMessage.contains("nullable values to non-null column")) {
          verifyInvariantViolationException(e)
        }
      }
    }
  }

  testQuietly("create table with NOT NULL in struct type - check violation") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test
               |(x struct<a: LONG, b: String NOT NULL>, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType().
            add("a", LongType, nullable = true)
            .add("b", StringType, nullable = false))
          .add("y", LongType, nullable = true)
        assert(spark.table("delta_test").schema === expectedSchema)

        sql("INSERT INTO delta_test SELECT (1, 'a'), 1")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(Row(1L, "a"), 1)))

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("delta_test"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        val schema = new StructType()
          .add("x",
            new StructType()
              .add("a", "bigint")
              .add("b", "string"))
          .add("y", "bigint")
        val e = intercept[InvariantViolationException] {
          spark.createDataFrame(
            Seq(Row(Row(2L, null), 2L)).asJava,
            schema
          ).write.format("delta").mode("append").save(table.location.getPath)
        }
        verifyInvariantViolationException(e)
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL in struct type - not supported") {
    withTempDir { dir =>
      val tableName = "delta_test_not_null_struct"
      withTable(tableName) {
        sql(s"""
               |CREATE TABLE $tableName
               |(y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = true)
        assert(spark.table(tableName).schema === expectedSchema)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE $tableName
               |ADD COLUMNS (x struct<a: LONG, b: String NOT NULL>, z INT)""".stripMargin)
        }
        val msg = "Operation not allowed: " +
          "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for Delta tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS to table with existing NOT NULL fields") {
    withTempDir { dir =>
      val tableName = "delta_test_existing_not_null"
      withTable(tableName) {
        sql(
          s"""
             |CREATE TABLE $tableName
             |(y LONG NOT NULL)
             |USING delta
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("y", LongType, nullable = false)
        assert(spark.table(tableName).schema === expectedSchema)

        sql(
          s"""
             |ALTER TABLE $tableName
             |ADD COLUMNS (x struct<a: LONG, b: String>, z INT)""".stripMargin)
        val expectedSchema2 = new StructType()
          .add("y", LongType, nullable = false)
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("z", IntegerType)
        assert(spark.table(tableName).schema === expectedSchema2)
      }
    }
  }

  /**
   * Covers adding and changing a nested field using the ALTER TABLE command.
   * @param initialColumnType Type of the single column used to create the initial test table.
   * @param fieldToAdd        Tuple (name, type) of the nested field to add and change.
   * @param updatedColumnType Expected type of the single column after adding the nested field.
   */
  def testAlterTableNestedFields(testName: String)(
      initialColumnType: String,
      fieldToAdd: (String, String),
      updatedColumnType: String): Unit = {
    // Remove spaces in test name so we can re-use it as a unique table name.
    val tableName = testName.replaceAll(" ", "")
    test(s"ALTER TABLE ADD/CHANGE COLUMNS - nested $testName") {
      withTempDir { dir =>
        withTable(tableName) {
          sql(
            s"""
               |CREATE TABLE $tableName (data $initialColumnType)
               |USING delta
               |TBLPROPERTIES (${DeltaConfigs.COLUMN_MAPPING_MODE.key} = 'name')
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)

          val expectedInitialType = initialColumnType.filterNot(_.isWhitespace)
          val expectedUpdatedType = updatedColumnType.filterNot(_.isWhitespace)
          val fieldName = s"data.${fieldToAdd._1}"
          val fieldType = fieldToAdd._2

          def columnType: DataFrame =
            sql(s"DESCRIBE TABLE $tableName")
              .where("col_name = 'data'")
              .select("data_type")
          checkAnswer(columnType, Row(expectedInitialType))

          sql(s"ALTER TABLE $tableName ADD COLUMNS ($fieldName $fieldType)")
          checkAnswer(columnType, Row(expectedUpdatedType))

          sql(s"ALTER TABLE $tableName CHANGE COLUMN $fieldName TYPE $fieldType")
          checkAnswer(columnType, Row(expectedUpdatedType))
        }
      }
    }
  }

  testAlterTableNestedFields("struct in map key")(
    initialColumnType = "map<struct<a: int>, int>",
    fieldToAdd = "key.b" -> "string",
    updatedColumnType = "map<struct<a: int, b: string>, int>")

  testAlterTableNestedFields("struct in map value")(
    initialColumnType = "map<int, struct<a: int>>",
    fieldToAdd = "value.b" -> "string",
    updatedColumnType = "map<int, struct<a: int, b: string>>")

  testAlterTableNestedFields("struct in array")(
    initialColumnType = "array<struct<a: int>>",
    fieldToAdd = "element.b" -> "string",
    updatedColumnType = "array<struct<a: int, b: string>>")

  testAlterTableNestedFields("struct in nested map keys")(
    initialColumnType = "map<map<struct<a: int>, int>, int>",
    fieldToAdd = "key.key.b" -> "string",
    updatedColumnType = "map<map<struct<a: int, b: string>, int>, int>")

  testAlterTableNestedFields("struct in nested map values")(
    initialColumnType = "map<int, map<int, struct<a: int>>>",
    fieldToAdd = "value.value.b" -> "string",
    updatedColumnType = "map<int, map<int, struct<a: int, b: string>>>")

  testAlterTableNestedFields("struct in nested arrays")(
    initialColumnType = "array<array<struct<a: int>>>",
    fieldToAdd = "element.element.b" -> "string",
    updatedColumnType = "array<array<struct<a: int, b: string>>>")

  testAlterTableNestedFields("struct in nested array and map")(
    initialColumnType = "array<map<int, struct<a: int>>>",
    fieldToAdd = "element.value.b" -> "string",
    updatedColumnType = "array<map<int, struct<a: int, b: string>>>")

  testAlterTableNestedFields("struct in nested map key and array")(
    initialColumnType = "map<array<struct<a: int>>, int>",
    fieldToAdd = "key.element.b" -> "string",
    updatedColumnType = "map<array<struct<a: int, b: string>>, int>")

  testAlterTableNestedFields("struct in nested map value and array")(
    initialColumnType = "map<int, array<struct<a: int>>>",
    fieldToAdd = "value.element.b" -> "string",
    updatedColumnType = "map<int, array<struct<a: int, b: string>>>")

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - not supported") {
    withTempDir { dir =>
      val tableName = "not_supported_delta_test"
      withTable(tableName) {
        sql(s"""
               |CREATE TABLE $tableName
               |(x struct<a: LONG, b: String>, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        val expectedSchema = new StructType()
          .add("x", new StructType()
            .add("a", LongType)
            .add("b", StringType))
          .add("y", LongType, nullable = true)
        assert(spark.table(tableName).schema === expectedSchema)

        val e1 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE $tableName
               |CHANGE COLUMN x x struct<a: LONG, b: String NOT NULL>""".stripMargin)
        }
        assert(e1.getMessage.contains("Cannot update"))
        val e2 = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE $tableName
               |CHANGE COLUMN x.b b String NOT NULL""".stripMargin) // this syntax may change
        }
        verifyNullabilityFailure(e2)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { dir =>
        val tblName = "delta_test2"
        withTable(tblName) {
          sql(
            s"""
               |CREATE TABLE $tblName
               |(x struct<a: LONG, b: String NOT NULL> NOT NULL, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
          val expectedSchema = new StructType()
            .add("x", new StructType()
              .add("a", LongType)
              .add("b", StringType, nullable = false), nullable = false)
            .add("y", LongType)
          assert(spark.table(tblName).schema === expectedSchema)
          sql(s"INSERT INTO $tblName SELECT (1, 'a'), 1")
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            Seq(Row(Row(1L, "a"), 1)))

          sql(
            s"""
               |ALTER TABLE $tblName
               |ALTER COLUMN x.b DROP NOT NULL""".stripMargin) // relax nullability
          sql(s"INSERT INTO $tblName SELECT (2, null), null")
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null)))

          sql(
            s"""
               |ALTER TABLE $tblName
               |ALTER COLUMN x DROP NOT NULL""".stripMargin)
          sql(s"INSERT INTO $tblName SELECT null, 3")
          checkAnswer(
            sql(s"SELECT * FROM $tblName"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null),
              Row(null, 3)))
        }
      }
    }
  }

  private def verifyInvariantViolationException(e: InvariantViolationException): Unit = {
    if (e == null) {
      fail("Didn't receive a InvariantViolationException.")
    }
    assert(e.getMessage.contains("NOT NULL constraint violated for column"))
  }

  test("ALTER TABLE RENAME TO") {
    withTable("tbl", "newTbl") {
      sql(s"""
            |CREATE TABLE tbl
            |USING delta
            |AS SELECT 1 as a, 'a' as b
           """.stripMargin)
      sql(s"ALTER TABLE tbl RENAME TO newTbl")
      checkDatasetUnorderly(sql("SELECT * FROM newTbl").as[(Long, String)], 1L -> "a")
    }
  }


  /**
   * Although Spark 3.2 adds the support for SHOW CREATE TABLE for v2 tables, it doesn't work
   * properly for Delta. For example, table properties, constraints and generated columns are not
   * showed properly.
   *
   * TODO Implement Delta's own ShowCreateTableCommand to show the Delta table definition correctly
   */
  test("SHOW CREATE TABLE is not supported") {
    withTable("delta_test") {
      sql(
        s"""
           |CREATE TABLE delta_test(a LONG, b String)
           |USING delta
           """.stripMargin)

      val e = intercept[AnalysisException] {
        sql("SHOW CREATE TABLE delta_test").collect()(0).getString(0)
      }
      assert(e.message.contains("`SHOW CREATE TABLE` is not supported for Delta table"))
    }

    withTempDir { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath()
        sql(
          s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta
             |LOCATION '$path'
             """.stripMargin)

        val e = intercept[AnalysisException] {
          sql("SHOW CREATE TABLE delta_test").collect()(0).getString(0)
        }
        assert(e.message.contains("`SHOW CREATE TABLE` is not supported for Delta table"))
      }
    }
  }


  test("DESCRIBE TABLE for partitioned table") {
    withTempDir { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath()

        val df = Seq(
          (1, "IT", "Alice"),
          (2, "CS", "Bob"),
          (3, "IT", "Carol")).toDF("id", "dept", "name")
        df.write.format("delta").partitionBy("name", "dept").save(path)

        sql(s"CREATE TABLE delta_test USING delta LOCATION '$path'")

        verifyDescribeTable("delta_test")
        verifyDescribeTable(s"delta.`$path`")

        assert(sql("DESCRIBE EXTENDED delta_test").collect().length > 0)
      }
    }
  }

  test("snapshot returned after a dropped managed table should be empty") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test USING delta AS SELECT 'foo' as a")
      val tableLocation = sql("DESC DETAIL delta_test").select("location").as[String].head()
      val snapshotBefore = getDeltaLog(tableLocation).update()
      sql("DROP TABLE delta_test")
      val snapshotAfter = getDeltaLog(tableLocation).update()
      assert(snapshotBefore ne snapshotAfter)
      assert(snapshotAfter.version === -1)
    }
  }

  test("snapshot returned after renaming a managed table should be empty") {
    val oldTableName = "oldTableName"
    val newTableName = "newTableName"
    withTable(oldTableName, newTableName) {
      sql(s"CREATE TABLE $oldTableName USING delta AS SELECT 'foo' as a")
      val tableLocation = sql(s"DESC DETAIL $oldTableName").select("location").as[String].head()
      val snapshotBefore = getDeltaLog(tableLocation).update()
      sql(s"ALTER TABLE $oldTableName RENAME TO $newTableName")
      val snapshotAfter = getDeltaLog(tableLocation).update()
      assert(snapshotBefore ne snapshotAfter)
      assert(snapshotAfter.version === -1)
    }
  }

}
