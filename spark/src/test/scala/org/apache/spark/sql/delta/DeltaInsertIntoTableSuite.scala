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
import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkContext, SparkException, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.functions.{lit, struct}
import org.apache.spark.sql.internal.{LegacyBehaviorPolicy, SQLConf}
import org.apache.spark.sql.internal.SQLConf.{LEAF_NODE_DEFAULT_PARALLELISM, PARTITION_OVERWRITE_MODE, PartitionOverwriteMode}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class DeltaInsertIntoSQLSuite
  extends DeltaInsertIntoTestsWithTempViews(
    supportsDynamicOverwrite = true,
    includeSQLOnlyTests = true)
  with DeltaSQLCommandTest {

  import testImplicits._

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }

  test("insert overwrite should work with selecting constants") {
    withTable("t1") {
      sql("CREATE TABLE t1 (a int, b int, c int) USING delta PARTITIONED BY (b, c)")
      sql("INSERT OVERWRITE TABLE t1 PARTITION (c=3) SELECT 1, 2")
      checkAnswer(
        sql("SELECT * FROM t1"),
        Row(1, 2, 3) :: Nil
      )
      sql("INSERT OVERWRITE TABLE t1 PARTITION (b=2, c=3) SELECT 1")
      checkAnswer(
        sql("SELECT * FROM t1"),
        Row(1, 2, 3) :: Nil
      )
      sql("INSERT OVERWRITE TABLE t1 PARTITION (b=2, c) SELECT 1, 3")
      checkAnswer(
        sql("SELECT * FROM t1"),
        Row(1, 2, 3) :: Nil
      )
    }
  }

  test("insertInto: append by name") {
    import testImplicits._
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      sql(s"INSERT INTO $t1(id, data) VALUES(1L, 'a')")
      // Can be in a different order
      sql(s"INSERT INTO $t1(data, id) VALUES('b', 2L)")
      // Can be casted automatically
      sql(s"INSERT INTO $t1(data, id) VALUES('c', 3)")
      verifyTable(t1, df)
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        // Missing columns
        assert(intercept[AnalysisException] {
          sql(s"INSERT INTO $t1(data) VALUES(4)")
        }.getMessage.contains("Column id is not specified in INSERT"))
        // Missing columns with matching dataType
        assert(intercept[AnalysisException] {
          sql(s"INSERT INTO $t1(data) VALUES('b')")
        }.getMessage.contains("Column id is not specified in INSERT"))
      }
      // Duplicate columns
      assert(intercept[AnalysisException](
        sql(s"INSERT INTO $t1(data, data) VALUES(5)")).getMessage.nonEmpty)
    }
  }

  test("insertInto: overwrite by name") {
    import testImplicits._
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      sql(s"INSERT OVERWRITE $t1(id, data) VALUES(1L, 'a')")
      verifyTable(t1, Seq((1L, "a")).toDF("id", "data"))
      // Can be in a different order
      sql(s"INSERT OVERWRITE $t1(data, id) VALUES('b', 2L)")
      verifyTable(t1, Seq((2L, "b")).toDF("id", "data"))
      // Can be casted automatically
      sql(s"INSERT OVERWRITE $t1(data, id) VALUES('c', 3)")
      verifyTable(t1, Seq((3L, "c")).toDF("id", "data"))
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        // Missing columns
        assert(intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1(data) VALUES(4)")
        }.getMessage.contains("Column id is not specified in INSERT"))
        // Missing columns with matching datatype
        assert(intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1(data) VALUES(4L)")
        }.getMessage.contains("Column id is not specified in INSERT"))
      }
      // Duplicate columns
      assert(intercept[AnalysisException](
        sql(s"INSERT OVERWRITE $t1(data, data) VALUES(5)")).getMessage.nonEmpty)
    }
  }

  test("insertInto should throw an AnalysisError on name mismatch") {
    def testInsertByNameError(targetSchema: String, expectedErrorClass: String): Unit = {
      val sourceTableName = "source"
      val targetTableName = "target"
      val format = "delta"
      withTable(sourceTableName, targetTableName) {
        sql(s"CREATE TABLE $sourceTableName (a int, b int) USING $format")
        sql(s"CREATE TABLE $targetTableName $targetSchema USING $format")
        val e = intercept[AnalysisException] {
          sql(s"INSERT INTO $targetTableName BY NAME SELECT * FROM $sourceTableName")
        }
        assert(e.getErrorClass === expectedErrorClass)
      }
    }

    // NOTE: We use upper case in the target schema so that needsSchemaAdjustmentByName returns
    // true (due to case sensitivity) so that we call resolveQueryColumnsByName and hit the right
    // code path.

    // when the number of columns does not match, throw an arity mismatch error.
    testInsertByNameError(
      targetSchema = "(A int)",
      expectedErrorClass = "INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS")

    // when the number of columns matches, but the names do not, throw a missing column error.
    testInsertByNameError(
      targetSchema = "(A int, c int)", expectedErrorClass = "DELTA_MISSING_COLUMN")
  }

  dynamicOverwriteTest("insertInto: dynamic overwrite by name") {
    import testImplicits._
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string, data2 string) " +
        s"USING $v2Format PARTITIONED BY (id)")
      sql(s"INSERT OVERWRITE $t1(id, data, data2) VALUES(1L, 'a', 'b')")
      verifyTable(t1, Seq((1L, "a", "b")).toDF("id", "data", "data2"))
      // Can be in a different order
      sql(s"INSERT OVERWRITE $t1(data, data2, id) VALUES('b', 'd', 2L)")
      verifyTable(t1, Seq((1L, "a", "b"), (2L, "b", "d")).toDF("id", "data", "data2"))
      // Can be casted automatically
      sql(s"INSERT OVERWRITE $t1(data, data2, id) VALUES('c', 'e', 1)")
      verifyTable(t1, Seq((1L, "c", "e"), (2L, "b", "d")).toDF("id", "data", "data2"))
      withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "false") {
        // Missing columns
        assert(intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1(data, id) VALUES('c', 1)")
        }.getMessage.contains("Column data2 is not specified in INSERT"))
        // Missing columns with matching datatype
        assert(intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1(data, id) VALUES('c', 1L)")
        }.getMessage.contains("Column data2 is not specified in INSERT"))
      }
      // Duplicate columns
      assert(intercept[AnalysisException](
        sql(s"INSERT OVERWRITE $t1(data, data) VALUES(5)")).getMessage.nonEmpty)
    }
  }

  test("insertInto: static partition column name should not be used in the column list") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING $v2Format PARTITIONED BY (c)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("INSERT OVERWRITE t PARTITION (c='1') (c) VALUES ('2')")
        },
        errorClass = "STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST",
        parameters = Map("staticName" -> "c"))
    }
  }


  Seq(("ordinal", ""), ("name", "(id, col2, col)")).foreach { case (testName, values) =>
    test(s"INSERT OVERWRITE schema evolution works for array struct types - $testName") {
      val sourceSchema = "id INT, col2 STRING, col ARRAY<STRUCT<f1: STRING, f2: STRING, f3: DATE>>"
      val sourceRecord = "1, '2022-11-01', array(struct('s1', 's2', DATE'2022-11-01'))"
      val targetSchema = "id INT, col2 DATE, col ARRAY<STRUCT<f1: STRING, f2: STRING>>"
      val targetRecord = "1, DATE'2022-11-02', array(struct('t1', 't2'))"

      runInsertOverwrite(sourceSchema, sourceRecord, targetSchema, targetRecord) {
        (sourceTable, targetTable) =>
          sql(s"INSERT OVERWRITE $targetTable $values SELECT * FROM $sourceTable")

          // make sure table is still writeable
          sql(s"""INSERT INTO $targetTable VALUES (2, DATE'2022-11-02',
               | array(struct('s3', 's4', DATE'2022-11-02')))""".stripMargin)
          sql(s"""INSERT INTO $targetTable VALUES (3, DATE'2022-11-03',
               |array(struct('s5', 's6', NULL)))""".stripMargin)
          val df = spark.sql(
            """SELECT 1 as id, DATE'2022-11-01' as col2,
              | array(struct('s1', 's2', DATE'2022-11-01')) as col UNION
              | SELECT 2 as id, DATE'2022-11-02' as col2,
              | array(struct('s3', 's4', DATE'2022-11-02')) as col UNION
              | SELECT 3 as id, DATE'2022-11-03' as col2,
              | array(struct('s5', 's6', NULL)) as col""".stripMargin)
          verifyTable(targetTable, df)
      }
    }
  }

  Seq(("ordinal", ""), ("name", "(id, col2, col)")).foreach { case (testName, values) =>
    test(s"INSERT OVERWRITE schema evolution works for array nested types - $testName") {
      val sourceSchema = "id INT, col2 STRING, " +
        "col ARRAY<STRUCT<f1: INT, f2: STRUCT<f21: STRING, f22: DATE>, f3: STRUCT<f31: STRING>>>"
      val sourceRecord = "1, '2022-11-01', " +
        "array(struct(1, struct('s1', DATE'2022-11-01'), struct('s1')))"
      val targetSchema = "id INT, col2 DATE, col ARRAY<STRUCT<f1: INT, f2: STRUCT<f21: STRING>>>"
      val targetRecord = "2, DATE'2022-11-02', array(struct(2, struct('s2')))"

      runInsertOverwrite(sourceSchema, sourceRecord, targetSchema, targetRecord) {
        (sourceTable, targetTable) =>
          sql(s"INSERT OVERWRITE $targetTable $values SELECT * FROM $sourceTable")

          // make sure table is still writeable
          sql(s"""INSERT INTO $targetTable VALUES (2, DATE'2022-11-02',
               | array(struct(2, struct('s2', DATE'2022-11-02'), struct('s2'))))""".stripMargin)
          sql(s"""INSERT INTO $targetTable VALUES (3, DATE'2022-11-03',
               | array(struct(3, struct('s3', NULL), struct(NULL))))""".stripMargin)
          val df = spark.sql(
            """SELECT 1 as id, DATE'2022-11-01' as col2,
              | array(struct(1, struct('s1', DATE'2022-11-01'), struct('s1'))) as col UNION
              | SELECT 2 as id, DATE'2022-11-02' as col2,
              | array(struct(2, struct('s2', DATE'2022-11-02'), struct('s2'))) as col UNION
              | SELECT 3 as id, DATE'2022-11-03' as col2,
              | array(struct(3, struct('s3', NULL), struct(NULL))) as col
              |""".stripMargin)
          verifyTable(targetTable, df)
      }
    }
  }

  def runInsertOverwrite(
      sourceSchema: String,
      sourceRecord: String,
      targetSchema: String,
      targetRecord: String)(
      runAndVerify: (String, String) => Unit): Unit = {
    val sourceTable = "source"
    val targetTable = "target"
    withTable(sourceTable) {
      withTable(targetTable) {
        withSQLConf("spark.databricks.delta.schema.autoMerge.enabled" -> "true") {
          // prepare source table
          sql(s"""CREATE TABLE $sourceTable ($sourceSchema)
                 | USING DELTA""".stripMargin)
          sql(s"INSERT INTO $sourceTable VALUES ($sourceRecord)")
          // prepare target table
          sql(s"""CREATE TABLE $targetTable ($targetSchema)
                 | USING DELTA""".stripMargin)
          sql(s"INSERT INTO $targetTable VALUES ($targetRecord)")
          runAndVerify(sourceTable, targetTable)
        }
      }
    }
  }
}

class DeltaInsertIntoSQLByPathSuite
  extends DeltaInsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = true)
  with DeltaSQLCommandTest {
  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      val ident = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
      val catalogTable = spark.sessionState.catalog.getTableMetadata(ident)
      sql(s"INSERT $overwrite TABLE delta.`${catalogTable.location}` SELECT * FROM $tmpView")
    }
  }

  testQuietly("insertInto: cannot insert into a table that doesn't exist") {
    import testImplicits._
    Seq(SaveMode.Append, SaveMode.Overwrite).foreach { mode =>
      withTempDir { dir =>
        val t1 = s"delta.`${dir.getCanonicalPath}`"
        val tmpView = "tmp_view"
        withTempView(tmpView) {
          val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
          val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
          df.createOrReplaceTempView(tmpView)

          intercept[AnalysisException] {
            sql(s"INSERT $overwrite TABLE $t1 SELECT * FROM $tmpView")
          }

          assert(new File(dir, "_delta_log").mkdirs(), "Failed to create a _delta_log directory")
          intercept[AnalysisException] {
            sql(s"INSERT $overwrite TABLE $t1 SELECT * FROM $tmpView")
          }
        }
      }
    }
  }
}

class DeltaInsertIntoDataFrameSuite
  extends DeltaInsertIntoTestsWithTempViews(
    supportsDynamicOverwrite = true,
    includeSQLOnlyTests = false)
  with DeltaSQLCommandTest {
  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    dfw.insertInto(tableName)
  }
}

class DeltaInsertIntoDataFrameByPathSuite
  extends DeltaInsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = false)
  with DeltaSQLCommandTest {
  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    val ident = spark.sessionState.sqlParser.parseTableIdentifier(tableName)
    val catalogTable = spark.sessionState.catalog.getTableMetadata(ident)
    dfw.insertInto(s"delta.`${catalogTable.location}`")
  }

  testQuietly("insertInto: cannot insert into a table that doesn't exist") {
    import testImplicits._
    Seq(SaveMode.Append, SaveMode.Overwrite).foreach { mode =>
      withTempDir { dir =>
        val t1 = s"delta.`${dir.getCanonicalPath}`"
        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")

        intercept[AnalysisException] {
          df.write.mode(mode).insertInto(t1)
        }

        assert(new File(dir, "_delta_log").mkdirs(), "Failed to create a _delta_log directory")
        intercept[AnalysisException] {
          df.write.mode(mode).insertInto(t1)
        }

        // Test DataFrameWriterV2 as well
        val dfW2 = df.writeTo(t1)
        if (mode == SaveMode.Append) {
          intercept[AnalysisException] {
            dfW2.append()
          }
        } else {
          intercept[AnalysisException] {
            dfW2.overwrite(lit(true))
          }
        }
      }
    }
  }
}


trait DeltaInsertIntoColumnMappingSelectedTests extends DeltaColumnMappingSelectedTestMixin {
  override protected def runOnlyTests = Seq(
    "InsertInto: overwrite - mixed clause reordered - static mode",
    "InsertInto: overwrite - multiple static partitions - dynamic mode"
  )
}

class DeltaInsertIntoSQLNameColumnMappingSuite extends DeltaInsertIntoSQLSuite
  with DeltaColumnMappingEnableNameMode
  with DeltaInsertIntoColumnMappingSelectedTests {
  override protected def runOnlyTests: Seq[String] = super.runOnlyTests :+
    "insert overwrite should work with selecting constants"
}

class DeltaInsertIntoSQLByPathNameColumnMappingSuite extends DeltaInsertIntoSQLByPathSuite
  with DeltaColumnMappingEnableNameMode
  with DeltaInsertIntoColumnMappingSelectedTests

class DeltaInsertIntoDataFrameNameColumnMappingSuite extends DeltaInsertIntoDataFrameSuite
  with DeltaColumnMappingEnableNameMode
  with DeltaInsertIntoColumnMappingSelectedTests

class DeltaInsertIntoDataFrameByPathNameColumnMappingSuite
  extends DeltaInsertIntoDataFrameByPathSuite
    with DeltaColumnMappingEnableNameMode
    with DeltaInsertIntoColumnMappingSelectedTests

abstract class DeltaInsertIntoTestsWithTempViews(
    supportsDynamicOverwrite: Boolean,
    includeSQLOnlyTests: Boolean)
  extends DeltaInsertIntoTests(supportsDynamicOverwrite, includeSQLOnlyTests)
  with DeltaTestUtilsForTempViews {
  protected def testComplexTempViews(name: String)(text: String, expectedResult: Seq[Row]): Unit = {
    testWithTempView(s"insertInto a temp view created on top of a table - $name") { isSQLTempView =>
      import testImplicits._
      val t1 = "tbl"
      sql(s"CREATE TABLE $t1 (key int, value int) USING $v2Format")
      Seq(SaveMode.Append, SaveMode.Overwrite).foreach { mode =>
        createTempViewFromSelect(text, isSQLTempView)
        val df = Seq((0, 3), (1, 2)).toDF("key", "value")
        try {
          doInsert("v", df, mode)
          checkAnswer(spark.table("v"), expectedResult)
        } catch {
          case e: AnalysisException =>
            assert(e.getMessage.contains("Inserting into a view is not allowed") ||
              e.getMessage.contains("Inserting into an RDD-based table is not allowed") ||
              e.getMessage.contains("Table default.v not found") ||
              e.getMessage.contains("Table or view 'v' not found in database 'default'") ||
              e.getMessage.contains("The table or view `default`.`v` cannot be found") ||
              e.getMessage.contains("[UNSUPPORTED_INSERT.RDD_BASED] Can't insert into the target."))
        }
      }
    }
  }

  testComplexTempViews("basic") (
    "SELECT * FROM tbl",
    Seq(Row(0, 3), Row(1, 2))
  )

  testComplexTempViews("subset cols")(
    "SELECT key FROM tbl",
    Seq(Row(0), Row(1))
  )

  testComplexTempViews("superset cols")(
    "SELECT key, value, 1 FROM tbl",
    Seq(Row(0, 3, 1), Row(1, 2, 1))
  )

  testComplexTempViews("nontrivial projection")(
    "SELECT value as key, key as value FROM tbl",
    Seq(Row(3, 0), Row(2, 1))
  )

  testComplexTempViews("view with too many internal aliases")(
    "SELECT * FROM (SELECT * FROM tbl AS t1) AS t2",
    Seq(Row(0, 3), Row(1, 2))
  )

}

class DeltaColumnDefaultsInsertSuite extends InsertIntoSQLOnlyTests with DeltaSQLCommandTest {

  import testImplicits._

  override val supportsDynamicOverwrite = true
  override val includeSQLOnlyTests = true

  val tblPropertiesAllowDefaults =
    """tblproperties (
      |  'delta.feature.allowColumnDefaults' = 'enabled',
      |  'delta.columnMapping.mode' = 'name'
      |)""".stripMargin

  test("Column DEFAULT value support with Delta Lake, positive tests") {
    Seq(
      PartitionOverwriteMode.STATIC.toString,
      PartitionOverwriteMode.DYNAMIC.toString
    ).foreach { partitionOverwriteMode =>
      withSQLConf(
        SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true",
        SQLConf.PARTITION_OVERWRITE_MODE.key -> partitionOverwriteMode,
        // Set these configs to allow writing test values like timestamps of Jan. 1, year 1, etc.
        SQLConf.PARQUET_REBASE_MODE_IN_WRITE.key -> LegacyBehaviorPolicy.LEGACY.toString,
        SQLConf.PARQUET_INT96_REBASE_MODE_IN_WRITE.key -> LegacyBehaviorPolicy.LEGACY.toString) {
        withTable("t1", "t2", "t3", "t4") {
          // Positive tests:
          // Create some columns with default values and then insert into them.
          sql("create table t1(" +
            s"a int default 42, b boolean default true, c string default 'abc') using $v2Format " +
            s"partitioned by (a) $tblPropertiesAllowDefaults")
          sql("insert into t1 values (1, false, default)")
          sql("insert into t1 values (1, default, default)")
          sql("alter table t1 alter column c set default 'def'")
          sql("insert into t1 values (default, default, default)")
          sql("alter table t1 alter column c drop default")
          // Exercise INSERT INTO commands with VALUES lists mapping columns positionally.
          sql("insert into t1 values (default, default, default)")
          // Write the data in the table 't1' to new table 't4' and then perform an INSERT OVERWRITE
          // back to 't1' here, to exercise static and dynamic partition overwrites.
          sql(f"create table t4(a int, b boolean, c string) using $v2Format " +
            s"partitioned by (a) $tblPropertiesAllowDefaults")
          // Exercise INSERT INTO commands with SELECT queries mapping columns by name.
          sql("insert into t4(a, b, c) select a, b, c from t1")
          sql("insert overwrite table t1 select * from t4")
          checkAnswer(spark.table("t1"), Seq(
            Row(1, false, "abc"),
            Row(1, true, "abc"),
            Row(42, true, "def"),
            Row(42, true, null)
          ))
          // Insert default values with all supported types.
          sql("create table t2(" +
            "s boolean default true, " +
            "t byte default cast(null as byte), " +
            "u short default cast(42 as short), " +
            "v float default 0, " +
            "w double default 0, " +
            "x date default date'0000', " +
            "y timestamp default timestamp'0000', " +
            "z decimal(5, 2) default 123.45," +
            "a1 bigint default 43," +
            "a2 smallint default cast(5 as smallint)," +
            s"a3 tinyint default cast(6 as tinyint)) using $v2Format " +
            tblPropertiesAllowDefaults)
          sql("insert into t2 values (default, default, default, default, default, default, " +
            "default, default, default, default, default)")
          val result: Array[Row] = spark.table("t2").collect()
          assert(result.length == 1)
          val row: Row = result(0)
          assert(row.length == 11)
          assert(row(0) == true)
          assert(row(1) == null)
          assert(row(2) == 42)
          assert(row(3) == 0.0f)
          assert(row(4) == 0.0d)
          assert(row(5).toString == "0001-01-01")
          assert(row(6).toString == "0001-01-01 00:00:00.0")
          assert(row(7).toString == "123.45")
          assert(row(8) == 43L)
          assert(row(9) == 5)
          assert(row(10) == 6)
        }
        withTable("t3") {
          // Set a default value for a partitioning column.
          sql(s"create table t3(i boolean, s bigint, q int default 42) using $v2Format " +
            s"partitioned by (i) $tblPropertiesAllowDefaults")
          sql("alter table t3 alter column i set default true")
          sql("insert into t3(i, s, q) values (default, default, default)")
          checkAnswer(spark.table("t3"), Seq(
            Row(true, null, 42)))
          // Drop the column and add it again without the default. Querying the column now returns
          // NULL.
          sql("alter table t3 drop column q")
          sql("alter table t3 add column q int")
          checkAnswer(spark.table("t3"), Seq(
            Row(true, null, null)))
        }
      }
    }
  }

  test("Column DEFAULT value support with Delta Lake, negative tests") {
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "true") {
      // The table feature is not enabled via TBLPROPERTIES.
      withTable("createTableWithDefaultFeatureNotEnabled") {
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql(s"create table createTableWithDefaultFeatureNotEnabled(" +
              s"i boolean, s bigint, q int default 42) using $v2Format " +
              "partitioned by (i)")
          },
          errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED"
        )
      }
      withTable("alterTableSetDefaultFeatureNotEnabled") {
        sql(s"create table alterTableSetDefaultFeatureNotEnabled(a int) using $v2Format")
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("alter table alterTableSetDefaultFeatureNotEnabled alter column a set default 42")
          },
          errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_FEATURE_NOT_ENABLED"
        )
      }
      // Adding a new column with a default value to an existing table is not allowed.
      withTable("alterTableTest") {
        sql(s"create table alterTableTest(i boolean, s bigint, q int default 42) using $v2Format " +
          s"partitioned by (i) $tblPropertiesAllowDefaults")
        checkError(
          exception = intercept[DeltaAnalysisException] {
            sql("alter table alterTableTest add column z int default 42")
          },
          errorClass = "WRONG_COLUMN_DEFAULTS_FOR_DELTA_ALTER_TABLE_ADD_COLUMN_NOT_SUPPORTED")
      }
      // The default value fails to analyze.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"create table t4 (s int default badvalue) using $v2Format " +
            s"$tblPropertiesAllowDefaults")
        },
        errorClass = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "defaultValue" -> "badvalue"))

      // The default value analyzes to a table not in the catalog.
      // The error message reports that we failed to execute the command because subquery
      // expressions are not allowed in DEFAULT values.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"create table t4 (s int default (select min(x) from badtable)) using $v2Format " +
            tblPropertiesAllowDefaults)
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "defaultValue" -> "(select min(x) from badtable)"))
      // The default value has an explicit alias. It fails to evaluate when inlined into the
      // VALUES list at the INSERT INTO time.
      // The error message reports that we failed to execute the command because subquery
      // expressions are not allowed in DEFAULT values.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"create table t4 (s int default (select 42 as alias)) using $v2Format " +
            tblPropertiesAllowDefaults)
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "defaultValue" -> "(select 42 as alias)"))
      // The default value parses but the type is not coercible.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"create table t4 (s bigint default false) " +
            s"using $v2Format $tblPropertiesAllowDefaults")
        },
        errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`s`",
          "expectedType" -> "\"BIGINT\"",
          "actualType" -> "\"BOOLEAN\"",
          "defaultValue" -> "false"))
      // It is possible to create a table with NOT NULL constraint and a DEFAULT value of NULL.
      // However, future inserts into that table will fail.
      withTable("t4") {
        sql(s"create table t4(i boolean, s bigint, q int default null not null) using $v2Format " +
          s"partitioned by (i) $tblPropertiesAllowDefaults")
        // The InvariantViolationException is not a SparkThrowable, so just check we receive one.
        assert(intercept[InvariantViolationException] {
          sql("insert into t4 values (default, default, default)")
        }.getMessage.nonEmpty)
      }
      // It is possible to create a table with a check constraint and a DEFAULT value that does not
      // conform. However, future inserts into that table will fail.
      withTable("t4") {
        sql(s"create table t4(i boolean, s bigint, q int default 42) using $v2Format " +
          s"partitioned by (i) $tblPropertiesAllowDefaults")
        sql("alter table t4 add constraint smallq check (q < 10)")
        assert(intercept[InvariantViolationException] {
          sql("insert into t4 values (default, default, default)")
        }.getMessage.nonEmpty)
      }
    }
    // Column default values are disabled per configuration in general.
    withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
      checkError(
        exception = intercept[ParseException] {
          sql(s"create table t4 (s int default 41 + 1) using $v2Format " +
            tblPropertiesAllowDefaults)
        },
        errorClass = "UNSUPPORTED_DEFAULT_VALUE.WITH_SUGGESTION",
        parameters = Map.empty,
        context = ExpectedContext(fragment = "s int default 41 + 1", start = 17, stop = 36))
    }
  }

  test("Exercise column defaults with dataframe writes") {
    // There are three column types exercising various combinations of implicit and explicit
    // default column value references in the 'insert into' statements. Note these tests depend on
    // enabling the configuration to use NULLs for missing DEFAULT column values.
    withSQLConf(SQLConf.USE_NULLS_FOR_MISSING_DEFAULT_COLUMN_VALUES.key -> "true") {
      for (useDataFrames <- Seq(false, true)) {
        withTable("t1", "t2") {
          sql(s"create table t1(j int, s bigint default 42, x bigint default 43) using $v2Format " +
            tblPropertiesAllowDefaults)
          if (useDataFrames) {
            // Use 'saveAsTable' to exercise mapping columns by name. Note that we have to specify
            // values for all columns of the target table here whether we use 'saveAsTable' or
            // 'insertInto', since the DataFrame generates a LogicalPlan equivalent to a SQL INSERT
            // INTO command without any explicit user-specified column list. For example, if we
            // used Seq((1)).toDF("j", "s", "x").write.mode("append") here instead, it would
            // generate an unresolved LogicalPlan equivalent to the SQL query
            // "INSERT INTO t1 VALUES (1)". This would fail with an error reporting the VALUES
            // list is not long enough, since the analyzer would consider this equivalent to
            // "INSERT INTO t1 (j, s, x) VALUES (1)".
            Seq((1, 42L, 43L)).toDF("j", "s", "x").write.mode("append")
              .format("delta").saveAsTable("t1")
            Seq((2, 42L, 43L)).toDF("j", "s", "x").write.mode("append")
              .format("delta").saveAsTable("t1")
            Seq((3, 42L, 43L)).toDF("j", "s", "x").write.mode("append")
              .format("delta").saveAsTable("t1")
            Seq((4, 44L, 43L)).toDF("j", "s", "x").write.mode("append")
              .format("delta").saveAsTable("t1")
            Seq((5, 44L, 45L)).toDF("j", "s", "x")
              .write.mode("append").format("delta").saveAsTable("t1")
          } else {
            sql("insert into t1(j) values(1)")
            sql("insert into t1(j, s) values(2, default)")
            sql("insert into t1(j, s, x) values(3, default, default)")
            sql("insert into t1(j, s) values(4, 44)")
            sql("insert into t1(j, s, x) values(5, 44, 45)")
          }
          sql(s"create table t2(j int, s bigint default 42, x bigint default 43) using $v2Format " +
            tblPropertiesAllowDefaults)
          if (useDataFrames) {
            // Use 'insertInto' to exercise mapping columns positionally.
            spark.table("t1").where("j = 1").write.insertInto("t2")
            spark.table("t1").where("j = 2").write.insertInto("t2")
            spark.table("t1").where("j = 3").write.insertInto("t2")
            spark.table("t1").where("j = 4").write.insertInto("t2")
            spark.table("t1").where("j = 5").write.insertInto("t2")
          } else {
            sql("insert into t2(j) select j from t1 where j = 1")
            sql("insert into t2(j, s) select j, default from t1 where j = 2")
            sql("insert into t2(j, s, x) select j, default, default from t1 where j = 3")
            sql("insert into t2(j, s) select j, s from t1 where j = 4")
            sql("insert into t2(j, s, x) select j, s, 45L from t1 where j = 5")
          }
          checkAnswer(
            spark.table("t2"),
            Row(1, 42L, 43L) ::
              Row(2, 42L, 43L) ::
              Row(3, 42L, 43L) ::
              Row(4, 44L, 43L) ::
              Row(5, 44L, 45L) :: Nil)
          // Also exercise schema evolution with DataFrames.
          if (useDataFrames) {
            Seq((5, 44L, 45L, 46L)).toDF("j", "s", "x", "y")
              .write.mode("append").format("delta").option("mergeSchema", "true")
              .saveAsTable("t2")
            checkAnswer(
              spark.table("t2"),
              Row(1, 42L, 43L, null) ::
                Row(2, 42L, 43L, null) ::
                Row(3, 42L, 43L, null) ::
                Row(4, 44L, 43L, null) ::
                Row(5, 44L, 45L, null) ::
                Row(5, 44L, 45L, 46L) :: Nil)
          }
        }
      }
    }
  }

  test("ReplaceWhere with column defaults with dataframe writes") {
    withTable("t1", "t2", "t3") {
      sql(s"create table t1(j int, s bigint default 42, x bigint default 43) using $v2Format " +
        tblPropertiesAllowDefaults)
      Seq((1, 42L, 43L)).toDF.write.insertInto("t1")
      Seq((2, 42L, 43L)).toDF.write.insertInto("t1")
      Seq((3, 42L, 43L)).toDF.write.insertInto("t1")
      Seq((4, 44L, 43L)).toDF.write.insertInto("t1")
      Seq((5, 44L, 45L)).toDF.write.insertInto("t1")
      spark.table("t1")
        .write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", "j = default and s = default and x = default")
        .saveAsTable("t2")
      Seq("t1", "t2").foreach { t =>
        checkAnswer(
          spark.table(t),
          Row(1, 42L, 43L) ::
            Row(2, 42L, 43L) ::
            Row(3, 42L, 43L) ::
            Row(4, 44L, 43L) ::
            Row(5, 44L, 45L) :: Nil)
      }
    }
  }

  test("DESCRIBE and SHOW CREATE TABLE with column defaults") {
    withTable("t") {
      spark.sql(s"CREATE TABLE t (id bigint default 42) " +
        s"using $v2Format $tblPropertiesAllowDefaults")
      val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED t")
      assert(descriptionDf.schema.map { field =>
        (field.name, field.dataType)
      } === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
      QueryTest.checkAnswer(
        descriptionDf.filter(
          "!(col_name in ('Catalog', 'Created Time', 'Created By', 'Database', " +
            "'index', 'Is_managed_location', 'Location', 'Name', 'Owner', 'Partition Provider'," +
            "'Provider', 'Table', 'Table Properties',  'Type', '_partition', 'Last Access', " +
            "'Statistics', ''))"),
        Seq(
          Row("# Column Default Values", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("id", "bigint", "42"),
          Row("id", "bigint", null)
        ))
    }
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t (
           |  a bigint NOT NULL,
           |  b bigint DEFAULT 42,
           |  c string DEFAULT 'abc, "def"' COMMENT 'comment'
           |)
           |USING parquet
           |COMMENT 'This is a comment'
           |$tblPropertiesAllowDefaults
        """.stripMargin)
      val currentCatalog = spark.sessionState.catalogManager.currentCatalog.name()
      QueryTest.checkAnswer(sql("SHOW CREATE TABLE T"),
        Seq(
          Row(
            s"""CREATE TABLE ${currentCatalog}.default.T (
               |  a BIGINT,
               |  b BIGINT DEFAULT 42,
               |  c STRING DEFAULT 'abc, "def"' COMMENT 'comment')
               |USING parquet
               |COMMENT 'This is a comment'
               |TBLPROPERTIES (
               |  'delta.columnMapping.mode' = 'name',
               |  'delta.feature.allowColumnDefaults' = 'enabled')
               |""".stripMargin)))
    }
  }
}

/** These tests come from Apache Spark with some modifications to match Delta behavior. */
abstract class DeltaInsertIntoTests(
    override protected val supportsDynamicOverwrite: Boolean,
    override protected val includeSQLOnlyTests: Boolean)
  extends InsertIntoSQLOnlyTests {

  import testImplicits._

  override def afterEach(): Unit = {
    spark.catalog.listTables().collect().foreach(t =>
      sql(s"drop table ${t.name}"))
    super.afterEach()
  }

  // START Apache Spark tests

  /**
   * Insert data into a table using the insertInto statement. Implementations can be in SQL
   * ("INSERT") or using the DataFrameWriter (`df.write.insertInto`). Insertions will be
   * by column ordinal and not by column name.
   */
  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode = null): Unit

  test("insertInto: append") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    doInsert(t1, df)
    verifyTable(t1, df)
  }

  test("insertInto: append by position") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")

    doInsert(t1, dfr)
    verifyTable(t1, df)
  }

  test("insertInto: append cast automatically") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
    doInsert(t1, df)
    verifyTable(t1, df)
  }


  test("insertInto: append partitioned table") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df)
      verifyTable(t1, df)
    }
  }

  test("insertInto: overwrite non-partitioned table") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val df2 = Seq((4L, "d"), (5L, "e"), (6L, "f")).toDF("id", "data")
    doInsert(t1, df)
    doInsert(t1, df2, SaveMode.Overwrite)
    verifyTable(t1, df2)
  }

  test("insertInto: overwrite partitioned table in static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "tbl"
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df, SaveMode.Overwrite)
      verifyTable(t1, df)
    }
  }


  test("insertInto: overwrite by position") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
        doInsert(t1, init)

        val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
        doInsert(t1, dfr, SaveMode.Overwrite)

        val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
        verifyTable(t1, df)
      }
    }
  }

  test("insertInto: overwrite cast automatically") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    val df2 = Seq((4L, "d"), (5L, "e"), (6L, "f")).toDF("id", "data")
    val df2c = Seq((4, "d"), (5, "e"), (6, "f")).toDF("id", "data")
    doInsert(t1, df)
    doInsert(t1, df2c, SaveMode.Overwrite)
    verifyTable(t1, df2)
  }

  test("insertInto: fails when missing a column") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string, missing string) USING $v2Format")
    val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    // mismatched datatype
    val df2 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
    for (df <- Seq(df1, df2)) {
      val exc = intercept[AnalysisException] {
        doInsert(t1, df)
      }
      verifyTable(t1, Seq.empty[(Long, String, String)].toDF("id", "data", "missing"))
      assert(exc.getMessage.contains("not enough data columns"))
    }
  }

  test("insertInto: overwrite fails when missing a column") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string, missing string) USING $v2Format")
    val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    // mismatched datatype
    val df2 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
    for (df <- Seq(df1, df2)) {
      val exc = intercept[AnalysisException] {
        doInsert(t1, df, SaveMode.Overwrite)
      }
      verifyTable(t1, Seq.empty[(Long, String, String)].toDF("id", "data", "missing"))
      assert(exc.getMessage.contains("not enough data columns"))
    }
  }

  // This behavior is specific to Delta
  test("insertInto: fails when an extra column is present but can evolve schema") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a", "mango")).toDF("id", "data", "fruit")
      val exc = intercept[AnalysisException] {
        doInsert(t1, df)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))
      assert(exc.getMessage.contains(s"mergeSchema"))

      withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
        doInsert(t1, df)
      }
      verifyTable(t1, Seq((1L, "a", "mango")).toDF("id", "data", "fruit"))
    }
  }

  // This behavior is specific to Delta
  testQuietly("insertInto: schema enforcement") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    val df = Seq(("a", 1L)).toDF("id", "data") // reverse order

    def getDF(rows: Row*): DataFrame = {
      spark.createDataFrame(spark.sparkContext.parallelize(rows), spark.table(t1).schema)
    }

    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "strict") {
      intercept[AnalysisException] {
        doInsert(t1, df, SaveMode.Overwrite)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))

      intercept[AnalysisException] {
        doInsert(t1, df)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))
    }

    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "ansi") {
      intercept[SparkException] {
        doInsert(t1, df, SaveMode.Overwrite)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))

      intercept[SparkException] {
        doInsert(t1, df)
      }

      verifyTable(t1, Seq.empty[(Long, String)].toDF("id", "data"))
    }

    withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "legacy") {
      doInsert(t1, df, SaveMode.Overwrite)
      verifyTable(
        t1,
        getDF(Row(null, "1")))

      doInsert(t1, df)

      verifyTable(
        t1,
        getDF(Row(null, "1"), Row(null, "1")))
    }
  }

  testQuietly("insertInto: struct types and schema enforcement") {
    val t1 = "tbl"
    withTable(t1) {
      sql(
        s"""CREATE TABLE $t1 (
           |  id bigint,
           |  point struct<x: double, y: double>
           |)
           |USING delta""".stripMargin)
      val init = Seq((1L, (0.0, 1.0))).toDF("id", "point")
      doInsert(t1, init)

      doInsert(t1, Seq((2L, (1.0, 0.0))).toDF("col1", "col2")) // naming doesn't matter

      // can handle null types
      doInsert(t1, Seq((3L, (1.0, null))).toDF("col1", "col2"))
      doInsert(t1, Seq((4L, (null, 1.0))).toDF("col1", "col2"))

      val expected = Seq(
        Row(1L, Row(0.0, 1.0)),
        Row(2L, Row(1.0, 0.0)),
        Row(3L, Row(1.0, null)),
        Row(4L, Row(null, 1.0)))
      verifyTable(
        t1,
        spark.createDataFrame(expected.asJava, spark.table(t1).schema))

      // schema enforcement
      val complexSchema = Seq((5L, (0.5, 0.5), (2.5, 2.5, 1.0), "a", (0.5, "b")))
        .toDF("long", "struct", "newstruct", "string", "badstruct")
        .select(
          $"long",
          $"struct",
          struct(
            $"newstruct._1".as("x"),
            $"newstruct._2".as("y"),
            $"newstruct._3".as("z")) as "newstruct",
          $"string",
          $"badstruct")

      // new column in root
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select("long", "struct", "string"))
      }

      // new column in struct not accepted
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select("long", "newstruct"))
      }

      withSQLConf(SQLConf.STORE_ASSIGNMENT_POLICY.key -> "strict") {
        // bad data type not accepted
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("string", "struct"))
        }

        // nested bad data type in struct not accepted
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "badstruct"))
        }
      }

      // missing column in struct
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select($"long", struct(lit(0.1))))
      }

      // wrong ordering
      intercept[AnalysisException] {
        doInsert(t1, complexSchema.select("struct", "long"))
      }

      // schema evolution
      withSQLConf(
          DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true",
          SQLConf.STORE_ASSIGNMENT_POLICY.key -> "strict") {
        // ordering should still match
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("struct", "long"))
        }

        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("struct", "long", "string"))
        }

        // new column to the end works
        doInsert(t1, complexSchema.select($"long", $"struct", $"string".as("letter")))

        // still cannot insert missing column
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "struct"))
        }

        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select($"long", struct(lit(0.1)), $"string"))
        }

        // still perform nested data type checks
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "badstruct", "string"))
        }

        // bad column within struct
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select(
            $"long", struct(lit(0.1), lit("a"), lit(0.2)), $"string"))
        }

        // Add column to nested field
        doInsert(t1, complexSchema.select($"long", $"newstruct", lit(null)))

        // cannot insert missing field into struct now
        intercept[AnalysisException] {
          doInsert(t1, complexSchema.select("long", "struct", "string"))
        }
      }

      val expected2 = Seq(
        Row(1L, Row(0.0, 1.0, null), null),
        Row(2L, Row(1.0, 0.0, null), null),
        Row(3L, Row(1.0, null, null), null),
        Row(4L, Row(null, 1.0, null), null),
        Row(5L, Row(0.5, 0.5, null), "a"),
        Row(5L, Row(2.5, 2.5, 1.0), null))
      verifyTable(
        t1,
        spark.createDataFrame(expected2.asJava, spark.table(t1).schema))

      val expectedSchema = new StructType()
        .add("id", LongType)
        .add("point", new StructType()
          .add("x", DoubleType)
          .add("y", DoubleType)
          .add("z", DoubleType))
        .add("letter", StringType)
      val diff = SchemaUtils.reportDifferences(spark.table(t1).schema, expectedSchema)
      if (diff.nonEmpty) {
        fail(diff.mkString("\n"))
      }
    }
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      doInsert(t1, df, SaveMode.Overwrite)

      verifyTable(t1, df.union(sql("SELECT 4L, 'keep'")))
    }
  }

  dynamicOverwriteTest("insertInto: overwrite partitioned table in dynamic mode by position") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val dfr = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("data", "id")
      doInsert(t1, dfr, SaveMode.Overwrite)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c"), (4L, "keep")).toDF("id", "data")
      verifyTable(t1, df)
    }
  }

  dynamicOverwriteTest(
    "insertInto: overwrite partitioned table in dynamic mode automatic casting") {
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
      val init = Seq((2L, "dummy"), (4L, "keep")).toDF("id", "data")
      doInsert(t1, init)

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      val dfc = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
      doInsert(t1, df, SaveMode.Overwrite)

      verifyTable(t1, df.union(sql("SELECT 4L, 'keep'")))
    }
  }

  dynamicOverwriteTest("insertInto: overwrite fails when missing a column in dynamic mode") {
    val t1 = "tbl"
    sql(s"CREATE TABLE $t1 (id bigint, data string, missing string) USING $v2Format")
    val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    // mismatched datatype
    val df2 = Seq((1, "a"), (2, "b"), (3, "c")).toDF("id", "data")
    for (df <- Seq(df1, df2)) {
      val exc = intercept[AnalysisException] {
        doInsert(t1, df, SaveMode.Overwrite)
      }
      verifyTable(t1, Seq.empty[(Long, String, String)].toDF("id", "data", "missing"))
      assert(exc.getMessage.contains("not enough data columns"))
    }
  }

  test("insert nested struct from view into delta") {
    withTable("testNestedStruct") {
      sql(s"CREATE TABLE testNestedStruct " +
        s" (num INT, text STRING, s STRUCT<a:STRING, s2: STRUCT<c:STRING,d:STRING>, b:STRING>)" +
        s" USING DELTA")
      val data = sql(s"SELECT 1, 'a', struct('a', struct('c', 'd'), 'b')")
      doInsert("testNestedStruct", data)
      verifyTable("testNestedStruct",
        sql(s"SELECT 1 AS num, 'a' AS text, struct('a', struct('c', 'd') AS s2, 'b') AS s"))
    }
  }
}

trait InsertIntoSQLOnlyTests
    extends QueryTest
    with SharedSparkSession
    with BeforeAndAfter {

  import testImplicits._

  /** Check that the results in `tableName` match the `expected` DataFrame. */
  protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  protected val v2Format: String = "delta"

  /**
   * Whether dynamic partition overwrites are supported by the `Table` definitions used in the
   * test suites. Tables that leverage the V1 Write interface do not support dynamic partition
   * overwrites.
   */
  protected val supportsDynamicOverwrite: Boolean

  /** Whether to include the SQL specific tests in this trait within the extending test suite. */
  protected val includeSQLOnlyTests: Boolean

  private def withTableAndData(tableName: String)(testFn: String => Unit): Unit = {
    withTable(tableName) {
      val viewName = "tmp_view"
      val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
      df.createOrReplaceTempView(viewName)
      withTempView(viewName) {
        testFn(viewName)
      }
    }
  }

  protected def dynamicOverwriteTest(testName: String)(f: => Unit): Unit = {
    test(testName) {
      try {
        withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
          f
        }
        if (!supportsDynamicOverwrite) {
          fail("Expected failure from test, because the table doesn't support dynamic overwrites")
        }
      } catch {
        case a: AnalysisException if !supportsDynamicOverwrite =>
          assert(a.getMessage.contains("does not support dynamic overwrite"))
      }
    }
  }

  if (includeSQLOnlyTests) {
    test("InsertInto: when the table doesn't exist") {
      val t1 = "tbl"
      val t2 = "tbl2"
      withTableAndData(t1) { _ =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        val e = intercept[AnalysisException] {
          sql(s"INSERT INTO $t2 VALUES (2L, 'dummy')")
        }
        assert(e.getMessage.contains(t2))
        assert(e.getMessage.contains("Table not found") ||
          e.getMessage.contains(s"table or view `$t2` cannot be found")
        )
      }
    }

    test("InsertInto: append to partitioned table - static clause") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 PARTITION (id = 23) SELECT data FROM $view")
        verifyTable(t1, sql(s"SELECT 23, data FROM $view"))
      }
    }

    test("InsertInto: static PARTITION clause fails with non-partition column") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (data)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (id=1) SELECT data FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame)
        assert(
          exc.getMessage.contains("PARTITION clause cannot contain a non-partition column") ||
          exc.getMessage.contains("PARTITION clause cannot contain the non-partition column") ||
          exc.getMessage.contains(
            "[NON_PARTITION_COLUMN] PARTITION clause cannot contain the non-partition column"))
        assert(exc.getMessage.contains("id"))
      }
    }

    test("InsertInto: dynamic PARTITION clause fails with non-partition column") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")

        val exc = intercept[AnalysisException] {
          sql(s"INSERT INTO TABLE $t1 PARTITION (data) SELECT * FROM $view")
        }

        verifyTable(t1, spark.emptyDataFrame)
        assert(
          exc.getMessage.contains("PARTITION clause cannot contain a non-partition column") ||
          exc.getMessage.contains("PARTITION clause cannot contain the non-partition column") ||
          exc.getMessage.contains(
            "[NON_PARTITION_COLUMN] PARTITION clause cannot contain the non-partition column"))
        assert(exc.getMessage.contains("data"))
      }
    }

    test("InsertInto: overwrite - dynamic clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF())
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - dynamic clause - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"))
      }
    }

    test("InsertInto: overwrite - missing clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
          sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a"),
            (2, "b"),
            (3, "c")).toDF("id", "data"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - missing clause - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a"),
          (2, "b"),
          (3, "c"),
          (4, "keep")).toDF("id", "data"))
      }
    }

    test("InsertInto: overwrite - static clause") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p1 int) " +
            s"USING $v2Format PARTITIONED BY (p1)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 23), (4L, 'keep', 2)")
        verifyTable(t1, Seq(
          (2L, "dummy", 23),
          (4L, "keep", 2)).toDF("id", "data", "p1"))
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p1 = 23) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 23),
          (2, "b", 23),
          (3, "c", 23),
          (4, "keep", 2)).toDF("id", "data", "p1"))
      }
    }

    test("InsertInto: overwrite - mixed clause - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
              s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - mixed clause reordered - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
              s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    test("InsertInto: overwrite - implicit dynamic partition - static mode") {
      withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
        val t1 = "tbl"
        withTableAndData(t1) { view =>
          sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
              s"USING $v2Format PARTITIONED BY (id, p)")
          sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
          sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM $view")
          verifyTable(t1, Seq(
            (1, "a", 2),
            (2, "b", 2),
            (3, "c", 2)).toDF("id", "data", "p"))
        }
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - mixed clause reordered - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - implicit dynamic partition - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM $view")
        verifyTable(t1, Seq(
          (1, "a", 2),
          (2, "b", 2),
          (3, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    test("insert nested struct literal into delta") {
      withTable("insertNestedTest") {
        sql(s"CREATE TABLE insertNestedTest " +
          s" (num INT, text STRING, s STRUCT<a:STRING, s2: STRUCT<c:STRING,d:STRING>, b:STRING>)" +
          s" USING DELTA")
        sql(s"INSERT INTO insertNestedTest VALUES (1, 'a', struct('a', struct('c', 'd'), 'b'))")
      }
    }

    dynamicOverwriteTest("InsertInto: overwrite - multiple static partitions - dynamic mode") {
      val t1 = "tbl"
      withTableAndData(t1) { view =>
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) " +
            s"USING $v2Format PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id = 2, p = 2) SELECT data FROM $view")
        verifyTable(t1, Seq(
          (2, "a", 2),
          (2, "b", 2),
          (2, "c", 2),
          (4, "keep", 2)).toDF("id", "data", "p"))
      }
    }

    test("InsertInto: overwrite - dot in column names - static mode") {
      import testImplicits._
      val t1 = "tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (`a.b` string, `c.d` string) USING $v2Format PARTITIONED BY (`a.b`)")
        sql(s"INSERT OVERWRITE $t1 PARTITION (`a.b` = 'a') VALUES('b')")
        verifyTable(t1, Seq("a" -> "b").toDF("id", "data"))
      }
    }
  }

  // END Apache Spark tests
}
