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

import java.io.File
import java.util.Locale

// scalastyle:off import.ordering.noEmptyLine
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions

import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.{DeltaColumnMappingSelectedTestMixin, DeltaSQLCommandTest}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, ExternalCatalogUtils, SessionCatalog}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, Table, TableCatalog}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{MetadataBuilder, StructType}
import org.apache.spark.util.Utils

trait DeltaTableCreationTests
  extends QueryTest
  with SharedSparkSession
  with DeltaColumnMappingTestUtils {

  import testImplicits._

  val format = "delta"

  override protected def sparkConf: SparkConf = {
    super.sparkConf
      // to make compatible with existing empty schema fail tests
      .set(DeltaSQLConf.DELTA_ALLOW_CREATE_EMPTY_SCHEMA_TABLE.key, "false")
  }

  private def createDeltaTableByPath(
      path: File,
      df: DataFrame,
      tableName: String,
      partitionedBy: Seq[String] = Nil): Unit = {
    df.write
      .partitionBy(partitionedBy: _*)
      .mode(SaveMode.Append)
      .format(format)
      .save(path.getCanonicalPath)

    sql(s"""
           |CREATE TABLE delta_test
           |USING delta
           |LOCATION '${path.getCanonicalPath}'
         """.stripMargin)
  }

  private implicit def toTableIdentifier(tableName: String): TableIdentifier = {
    spark.sessionState.sqlParser.parseTableIdentifier(tableName)
  }

  protected def getTablePath(tableName: String): String = {
    new Path(spark.sessionState.catalog.getTableMetadata(tableName).location).toString
  }

  protected def getDefaultTablePath(tableName: String): String = {
    new Path(spark.sessionState.catalog.defaultTablePath(tableName)).toString
  }

  protected def getPartitioningColumns(tableName: String): Seq[String] = {
    spark.sessionState.catalog.getTableMetadata(tableName).partitionColumnNames
  }

  protected def getSchema(tableName: String): StructType = {
    spark.sessionState.catalog.getTableMetadata(tableName).schema
  }

  protected def getTableProperties(tableName: String): Map[String, String] = {
    spark.sessionState.catalog.getTableMetadata(tableName).properties
  }

  private def getDeltaLog(table: CatalogTable): DeltaLog = {
    getDeltaLog(new Path(table.storage.locationUri.get))
  }

  private def getDeltaLog(tableName: String): DeltaLog = {
    getDeltaLog(spark.sessionState.catalog.getTableMetadata(tableName))
  }

  protected def getDeltaLog(path: Path): DeltaLog = {
    DeltaLog.forTable(spark, path)
  }

  protected def verifyTableInCatalog(catalog: SessionCatalog, table: String): Unit = {
    val externalTable =
        catalog.externalCatalog.getTable("default", table)
    assertEqual(externalTable.schema, new StructType())
    assert(externalTable.partitionColumnNames.isEmpty)
  }

  protected def checkResult(
    result: DataFrame,
    expected: Seq[Any],
    columns: Seq[String]): Unit = {
    checkAnswer(
      result.select(columns.head, columns.tail: _*),
      Seq(Row(expected: _*))
    )
  }

  Seq("partitioned" -> Seq("v2"), "non-partitioned" -> Nil).foreach { case (isPartitioned, cols) =>
    SaveMode.values().foreach { saveMode =>
      test(s"saveAsTable to a new table (managed) - $isPartitioned, saveMode: $saveMode") {
        val tbl = "delta_test"
        withTable(tbl) {
          Seq(1L -> "a").toDF("v1", "v2")
            .write
            .partitionBy(cols: _*)
            .mode(saveMode)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
            assert(getTablePath(tbl) === getDefaultTablePath(tbl), "Table path is wrong")
          assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
        }
      }

      test(s"saveAsTable to a new table (managed) - $isPartitioned," +
        s" saveMode: $saveMode (empty df)") {
        val tbl = "delta_test"
        withTable(tbl) {
          Seq(1L -> "a").toDF("v1", "v2").where("false")
            .write
            .partitionBy(cols: _*)
            .mode(saveMode)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)])
            assert(getTablePath(tbl) === getDefaultTablePath(tbl), "Table path is wrong")
          assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
        }
      }
    }

    SaveMode.values().foreach { saveMode =>
      test(s"saveAsTable to a new table (external) - $isPartitioned, saveMode: $saveMode") {
        withTempDir { dir =>
          val tbl = "delta_test"
          withTable(tbl) {
            Seq(1L -> "a").toDF("v1", "v2")
              .write
              .partitionBy(cols: _*)
              .mode(saveMode)
              .format(format)
              .option("path", dir.getCanonicalPath)
              .saveAsTable(tbl)

            checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
            assert(getTablePath(tbl) === new Path(dir.toURI).toString.stripSuffix("/"),
              "Table path is wrong")
            assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
          }
        }
      }

      test(s"saveAsTable to a new table (external) - $isPartitioned," +
        s" saveMode: $saveMode (empty df)") {
        withTempDir { dir =>
          val tbl = "delta_test"
          withTable(tbl) {
            Seq(1L -> "a").toDF("v1", "v2").where("false")
              .write
              .partitionBy(cols: _*)
              .mode(saveMode)
              .format(format)
              .option("path", dir.getCanonicalPath)
              .saveAsTable(tbl)

            checkDatasetUnorderly(spark.table(tbl).as[(Long, String)])
            assert(getTablePath(tbl) === new Path(dir.toURI).toString.stripSuffix("/"),
              "Table path is wrong")
            assert(getPartitioningColumns(tbl) === cols, "Partitioning columns don't match")
          }
        }
      }
    }

    test(s"saveAsTable (append) to an existing table - $isPartitioned") {
      withTempDir { dir =>
        val tbl = "delta_test"
        withTable(tbl) {
          createDeltaTableByPath(dir, Seq(1L -> "a").toDF("v1", "v2"), tbl, cols)

          Seq(2L -> "b").toDF("v1", "v2")
            .write
            .partitionBy(cols: _*)
            .mode(SaveMode.Append)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a", 2L -> "b")
        }
      }
    }

    test(s"saveAsTable (overwrite) to an existing table - $isPartitioned") {
      withTempDir { dir =>
        val tbl = "delta_test"
        withTable(tbl) {
          createDeltaTableByPath(dir, Seq(1L -> "a").toDF("v1", "v2"), tbl, cols)

          Seq(2L -> "b").toDF("v1", "v2")
            .write
            .partitionBy(cols: _*)
            .mode(SaveMode.Overwrite)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 2L -> "b")
        }
      }
    }

    test(s"saveAsTable (ignore) to an existing table - $isPartitioned") {
      withTempDir { dir =>
        val tbl = "delta_test"
        withTable(tbl) {
          createDeltaTableByPath(dir, Seq(1L -> "a").toDF("v1", "v2"), tbl, cols)

          Seq(2L -> "b").toDF("v1", "v2")
            .write
            .partitionBy(cols: _*)
            .mode(SaveMode.Ignore)
            .format(format)
            .saveAsTable(tbl)

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
        }
      }
    }

    test(s"saveAsTable (error if exists) to an existing table - $isPartitioned") {
      withTempDir { dir =>
        val tbl = "delta_test"
        withTable(tbl) {
          createDeltaTableByPath(dir, Seq(1L -> "a").toDF("v1", "v2"), tbl, cols)

          val e = intercept[AnalysisException] {
            Seq(2L -> "b").toDF("v1", "v2")
              .write
              .partitionBy(cols: _*)
              .mode(SaveMode.ErrorIfExists)
              .format(format)
              .saveAsTable(tbl)
          }
          assert(e.getMessage.contains(tbl))
          assert(e.getMessage.contains("already exists"))

          checkDatasetUnorderly(spark.table(tbl).as[(Long, String)], 1L -> "a")
        }
      }
    }
  }

  test("saveAsTable (append) + insert to a table created without a schema") {
    withTempDir { dir =>
      withTable("delta_test") {
        Seq(1L -> "a").toDF("v1", "v2")
          .write
          .mode(SaveMode.Append)
          .partitionBy("v2")
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("delta_test")

        // Out of order
        Seq("b" -> 2L).toDF("v2", "v1")
          .write
          .partitionBy("v2")
          .mode(SaveMode.Append)
          .format(format)
          .saveAsTable("delta_test")

        Seq(3L -> "c").toDF("v1", "v2")
          .write
          .format(format)
          .insertInto("delta_test")

        checkDatasetUnorderly(
          spark.table("delta_test").as[(Long, String)], 1L -> "a", 2L -> "b", 3L -> "c")
      }
    }
  }

  test("saveAsTable to a table created with an invalid partitioning column") {
    withTempDir { dir =>
      withTable("delta_test") {
        Seq(1L -> "a").toDF("v1", "v2")
          .write
          .mode(SaveMode.Append)
          .partitionBy("v2")
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("delta_test")
        checkDatasetUnorderly(spark.table("delta_test").as[(Long, String)], 1L -> "a")

        var ex = intercept[Exception] {
          Seq("b" -> 2L).toDF("v2", "v1")
            .write
            .partitionBy("v1")
            .mode(SaveMode.Append)
            .format(format)
            .saveAsTable("delta_test")
        }.getMessage
        assert(ex.contains("not match"))
        assert(ex.contains("partition"))
        checkDatasetUnorderly(spark.table("delta_test").as[(Long, String)], 1L -> "a")

        ex = intercept[Exception] {
          Seq("b" -> 2L).toDF("v3", "v1")
            .write
            .partitionBy("v1")
            .mode(SaveMode.Append)
            .format(format)
            .saveAsTable("delta_test")
        }.getMessage
        assert(ex.contains("not match"))
        assert(ex.contains("partition"))
        checkDatasetUnorderly(spark.table("delta_test").as[(Long, String)], 1L -> "a")

        Seq("b" -> 2L).toDF("v1", "v3")
          .write
          .partitionBy("v1")
          .mode(SaveMode.Ignore)
          .format(format)
          .saveAsTable("delta_test")
        checkDatasetUnorderly(spark.table("delta_test").as[(Long, String)], 1L -> "a")

        ex = intercept[AnalysisException] {
          Seq("b" -> 2L).toDF("v1", "v3")
            .write
            .partitionBy("v1")
            .mode(SaveMode.ErrorIfExists)
            .format(format)
            .saveAsTable("delta_test")
        }.getMessage
        assert(ex.contains("delta_test"))
        assert(ex.contains("already exists"))
        checkDatasetUnorderly(spark.table("delta_test").as[(Long, String)], 1L -> "a")
      }
    }
  }

  testQuietly("create delta table with spaces in column names") {
    val tableName = "delta_test"

    val tableLoc =
      new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier(tableName)))
    Utils.deleteRecursively(tableLoc)

    def createTableUsingDF: Unit = {
      Seq(1, 2, 3).toDF("a column name with spaces")
        .write
        .format(format)
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }

    def createTableUsingSQL: DataFrame = {
      sql(s"CREATE TABLE $tableName(`a column name with spaces` LONG, b String) USING delta")
    }

    withTable(tableName) {
      if (!columnMappingEnabled) {
        val ex = intercept[AnalysisException] {
          createTableUsingDF
        }
        assert(
          ex.getMessage.contains("[INVALID_COLUMN_NAME_AS_PATH]") ||
            ex.getMessage.contains("invalid character(s)")
        )
        assert(!tableLoc.exists())
      } else {
        // column mapping modes support creating table with arbitrary col names
        createTableUsingDF
          assert(tableLoc.exists())
      }
    }

    withTable(tableName) {
      if (!columnMappingEnabled) {
        val ex2 = intercept[AnalysisException] {
          createTableUsingSQL
        }
        assert(
          ex2.getMessage.contains("[INVALID_COLUMN_NAME_AS_PATH]") ||
            ex2.getMessage.contains("invalid character(s)")
        )
        assert(!tableLoc.exists())
      } else {
        // column mapping modes support creating table with arbitrary col names
        createTableUsingSQL
          assert(tableLoc.exists())
      }
    }
  }

  testQuietly("cannot create delta table when using buckets") {
    withTable("bucketed_table") {
      val e = intercept[AnalysisException] {
        Seq(1L -> "a").toDF("i", "j").write
          .format(format)
          .partitionBy("i")
          .bucketBy(numBuckets = 8, "j")
          .saveAsTable("bucketed_table")
      }
      assert(e.getMessage.toLowerCase(Locale.ROOT).contains(
        "is not supported for delta tables"))
    }
  }

  test("save without a path") {
    val e = intercept[IllegalArgumentException] {
      Seq(1L -> "a").toDF("i", "j").write
        .format(format)
        .partitionBy("i")
        .save()
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("'path' is not specified"))
  }

  test("save with an unknown partition column") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val e = intercept[AnalysisException] {
        Seq(1L -> "a").toDF("i", "j").write
          .format(format)
          .partitionBy("unknownColumn")
          .save(path)
      }
      assert(e.getMessage.contains("unknownColumn"))
    }
  }

  test("create a table with special column names") {
    withTable("t") {
      Seq(1 -> "a").toDF("x.x", "y.y").write.format(format).saveAsTable("t")
      Seq(2 -> "b").toDF("x.x", "y.y").write.format(format).mode("append").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Nil)
    }
  }

  testQuietly("saveAsTable (overwrite) to a non-partitioned table created with different paths") {
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTable("delta_test") {
          Seq(1L -> "a").toDF("v1", "v2")
            .write
            .mode(SaveMode.Append)
            .format(format)
            .option("path", dir1.getCanonicalPath)
            .saveAsTable("delta_test")

          val ex = intercept[AnalysisException] {
            Seq((3L, "c")).toDF("v1", "v2")
              .write
              .mode(SaveMode.Overwrite)
              .format(format)
              .option("path", dir2.getCanonicalPath)
              .saveAsTable("delta_test")
          }.getMessage
          assert(ex.contains("The location of the existing table"))
          assert(ex.contains("`default`.`delta_test`"))
          checkAnswer(
            spark.table("delta_test"), Row(1L, "a") :: Nil)
        }
      }
    }
  }

  test("saveAsTable (append) to a non-partitioned table created without path") {
    withTempDir { dir =>
      withTable("delta_test") {
        Seq(1L -> "a").toDF("v1", "v2")
          .write
          .mode(SaveMode.Overwrite)
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("delta_test")

        Seq((3L, "c")).toDF("v1", "v2")
          .write
          .mode(SaveMode.Append)
          .format(format)
          .saveAsTable("delta_test")

        checkAnswer(
          spark.table("delta_test"), Row(1L, "a") :: Row(3L, "c") :: Nil)
      }
    }
  }

  test("saveAsTable (append) to a non-partitioned table created with identical paths") {
    withTempDir { dir =>
      withTable("delta_test") {
        Seq(1L -> "a").toDF("v1", "v2")
          .write
          .mode(SaveMode.Overwrite)
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("delta_test")

        Seq((3L, "c")).toDF("v1", "v2")
          .write
          .mode(SaveMode.Append)
          .format(format)
          .option("path", dir.getCanonicalPath)
          .saveAsTable("delta_test")

        checkAnswer(
          spark.table("delta_test"), Row(1L, "a") :: Row(3L, "c") :: Nil)
      }
    }
  }

  test("overwrite mode saveAsTable without path shouldn't create managed table") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(
          s"""CREATE TABLE delta_test
             |USING delta
             |LOCATION '${dir.getAbsolutePath}'
             |AS SELECT 1 as a
          """.stripMargin)
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.version === 0, "CTAS should be a single commit")

        checkAnswer(spark.table("delta_test"), Row(1) :: Nil)

        Seq((2, "key")).toDF("a", "b")
          .write
          .mode(SaveMode.Overwrite)
          .option(DeltaOptions.OVERWRITE_SCHEMA_OPTION, "true")
          .format(format)
          .saveAsTable("delta_test")

        assert(deltaLog.snapshot.version === 1, "Overwrite mode shouldn't create new managed table")

        checkAnswer(spark.table("delta_test"), Row(2, "key") :: Nil)

      }
    }
  }

  testQuietly("reject table creation with column names that only differ by case") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDir { dir =>
        withTable("delta_test") {
          intercept[AnalysisException] {
            sql(
              s"""CREATE TABLE delta_test
                 |USING delta
                 |LOCATION '${dir.getAbsolutePath}'
                 |AS SELECT 1 as a, 2 as A
              """.stripMargin)
          }

          intercept[AnalysisException] {
            sql(
              s"""CREATE TABLE delta_test(
                 |  a string,
                 |  A string
                 |)
                 |USING delta
                 |LOCATION '${dir.getAbsolutePath}'
              """.stripMargin)
          }

          intercept[ParseException] {
            sql(
              s"""CREATE TABLE delta_test(
                 |  a string,
                 |  b string
                 |)
                 |partitioned by (a, a)
                 |USING delta
                 |LOCATION '${dir.getAbsolutePath}'
              """.stripMargin)
          }
        }
      }
    }
  }

  testQuietly("saveAsTable into a view throws exception around view definition") {
    withTempDir { dir =>
      val viewName = "delta_test"
      withView(viewName) {
        Seq((1, "key")).toDF("a", "b").write.format(format).save(dir.getCanonicalPath)
        sql(s"create view $viewName as select * from delta.`${dir.getCanonicalPath}`")
        val e = intercept[AnalysisException] {
          Seq((2, "key")).toDF("a", "b").write.format(format).mode("append").saveAsTable(viewName)
        }
        assert(e.getMessage.contains("a view"))
      }
    }
  }

  testQuietly("saveAsTable into a parquet table throws exception around format") {
    withTempPath { dir =>
      val tabName = "delta_test"
      withTable(tabName) {
        Seq((1, "key")).toDF("a", "b").write.format("parquet")
          .option("path", dir.getCanonicalPath).saveAsTable(tabName)
        intercept[AnalysisException] {
          Seq((2, "key")).toDF("a", "b").write.format("delta").mode("append").saveAsTable(tabName)
        }
      }
    }
  }

  test("create table with schema and path") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(
          s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkDatasetUnorderly(
          sql("SELECT * FROM delta_test").as[(Long, String)],
          1L -> "a")

      }
    }
  }

  protected def createTableWithEmptySchemaQuery(
      tableName: String,
      provider: String = "delta",
      location: Option[String] = None): String = {
    var query = s"CREATE TABLE $tableName USING $provider"
    if (location.nonEmpty) {
      query = s"$query LOCATION '${location.get}'"
    }
    query
  }

  testQuietly("failed to create a table and then able to recreate it") {
    withTable("delta_test") {
      val createEmptySchemaQuery = createTableWithEmptySchemaQuery("delta_test")
      val e = intercept[AnalysisException] {
        sql(createEmptySchemaQuery)
      }.getMessage
      assert(e.contains("but the schema is not specified"))

      sql("CREATE TABLE delta_test(a LONG, b String) USING delta")

      sql("INSERT INTO delta_test SELECT 1, 'a'")

      checkDatasetUnorderly(
        sql("SELECT * FROM delta_test").as[(Long, String)],
        1L -> "a")
    }
  }

  test("create external table without schema") {
    withTempDir { dir =>
      withTable("delta_test", "delta_test1") {
        Seq(1L -> "a").toDF()
          .selectExpr("_1 as v1", "_2 as v2")
          .write
          .mode("append")
          .partitionBy("v2")
          .format("delta")
          .save(dir.getCanonicalPath)

        sql(s"""
               |CREATE TABLE delta_test
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')
            """.stripMargin)

        spark.catalog.createTable("delta_test1", dir.getCanonicalPath, "delta")

        checkDatasetUnorderly(
          sql("SELECT * FROM delta_test").as[(Long, String)],
          1L -> "a")

        checkDatasetUnorderly(
          sql("SELECT * FROM delta_test1").as[(Long, String)],
          1L -> "a")
      }
    }
  }

  testQuietly("create managed table without schema") {
    withTable("delta_test") {
      val createEmptySchemaQuery = createTableWithEmptySchemaQuery("delta_test")
      val e = intercept[AnalysisException] {
        sql(createEmptySchemaQuery)
      }.getMessage
      assert(e.contains("but the schema is not specified"))
    }
  }

  testQuietly("reject creating a delta table pointing to non-delta files") {
    withTempPath { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath
        Seq(1L -> "a").toDF("col1", "col2").write.parquet(path)
        val e = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE TABLE delta_test (col1 int, col2 string)
               |USING delta
               |LOCATION '$path'
             """.stripMargin)
        }.getMessage
        var catalogPrefix = ""
        assert(e.contains(
          s"Cannot create table ('$catalogPrefix`default`.`delta_test`'). The associated location"))
      }
    }
  }

  testQuietly("create external table without schema but using non-delta files") {
    withTempDir { dir =>
      withTable("delta_test") {
        Seq(1L -> "a").toDF().selectExpr("_1 as v1", "_2 as v2").write
          .mode("append").partitionBy("v2").format("parquet").save(dir.getCanonicalPath)

        val createEmptySchemaQuery = createTableWithEmptySchemaQuery(
          "delta_test", location = Some(dir.getCanonicalPath))
        val e = intercept[AnalysisException] {
          sql(createEmptySchemaQuery)
        }.getMessage
        assert(e.contains("but there is no transaction log"))
      }
    }
  }

  testQuietly("create external table without schema and input files") {
    withTempDir { dir =>
      withTable("delta_test") {
        val createEmptySchemaQuery = createTableWithEmptySchemaQuery(
          "delta_test", location = Some(dir.getCanonicalPath))
        val e = intercept[AnalysisException] {
          sql(createEmptySchemaQuery)
        }.getMessage
        assert(e.contains("but the schema is not specified") && e.contains("input path is empty"))
      }
    }
  }

  test("create and drop delta table - external") {
    val catalog = spark.sessionState.catalog
    withTempDir { tempDir =>
      withTable("delta_test") {
        sql("CREATE TABLE delta_test(a LONG, b String) USING delta " +
          s"OPTIONS (path='${tempDir.getCanonicalPath}')")
        val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
        assert(table.tableType == CatalogTableType.EXTERNAL)
        assert(table.provider.contains("delta"))

        // Query the data and the metadata directly via the DeltaLog
        val deltaLog = getDeltaLog(table)

        assertEqual(
          deltaLog.snapshot.schema, new StructType().add("a", "long").add("b", "string"))
        assertEqual(
          deltaLog.snapshot.metadata.partitionSchema, new StructType())

        assertEqual(deltaLog.snapshot.schema, getSchema("delta_test"))
        assert(getPartitioningColumns("delta_test").isEmpty)

        // External catalog does not contain the schema and partition column names.
        verifyTableInCatalog(catalog, "delta_test")

        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkDatasetUnorderly(
          sql("SELECT * FROM delta_test").as[(Long, String)],
          1L -> "a")

        sql("DROP TABLE delta_test")
        intercept[NoSuchTableException](catalog.getTableMetadata(TableIdentifier("delta_test")))
        // Verify that the underlying location is not deleted for an external table
        checkAnswer(spark.read.format("delta")
          .load(new Path(tempDir.getCanonicalPath).toString), Seq(Row(1L, "a")))
      }
    }
  }

  test("create and drop delta table - managed") {
    val catalog = spark.sessionState.catalog
    withTable("delta_test") {
      sql("CREATE TABLE delta_test(a LONG, b String) USING delta")
      val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
      assert(table.tableType == CatalogTableType.MANAGED)
      assert(table.provider.contains("delta"))

      // Query the data and the metadata directly via the DeltaLog
      val deltaLog = getDeltaLog(table)

      assertEqual(
        deltaLog.snapshot.schema, new StructType().add("a", "long").add("b", "string"))
      assertEqual(
        deltaLog.snapshot.metadata.partitionSchema, new StructType())

      assertEqual(deltaLog.snapshot.schema, getSchema("delta_test"))
      assert(getPartitioningColumns("delta_test").isEmpty)
      assertEqual(getSchema("delta_test"), new StructType().add("a", "long").add("b", "string"))

      // External catalog does not contain the schema and partition column names.
      verifyTableInCatalog(catalog, "delta_test")

      sql("INSERT INTO delta_test SELECT 1, 'a'")
      checkDatasetUnorderly(
        sql("SELECT * FROM delta_test").as[(Long, String)],
        1L -> "a")

      sql("DROP TABLE delta_test")
      intercept[NoSuchTableException](catalog.getTableMetadata(TableIdentifier("delta_test")))
      // Verify that the underlying location is deleted for a managed table
        assert(!new File(table.location).exists())
    }
  }

  test("create table using - with partitioned by") {
    val catalog = spark.sessionState.catalog
    withTable("delta_test") {
      sql("CREATE TABLE delta_test(a LONG, b String) USING delta PARTITIONED BY (a)")
      val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
      assert(table.tableType == CatalogTableType.MANAGED)
      assert(table.provider.contains("delta"))


      // Query the data and the metadata directly via the DeltaLog
      val deltaLog = getDeltaLog(table)

      assertEqual(
        deltaLog.snapshot.schema, new StructType().add("a", "long").add("b", "string"))
      assertEqual(
        deltaLog.snapshot.metadata.partitionSchema, new StructType().add("a", "long"))

      assertEqual(deltaLog.snapshot.schema, getSchema("delta_test"))
      assert(getPartitioningColumns("delta_test") == Seq("a"))
      assertEqual(getSchema("delta_test"), new StructType().add("a", "long").add("b", "string"))

      // External catalog does not contain the schema and partition column names.
      verifyTableInCatalog(catalog, "delta_test")

      sql("INSERT INTO delta_test SELECT 1, 'a'")

      assertPartitionWithValueExists("a", "1", deltaLog)

      checkDatasetUnorderly(
        sql("SELECT * FROM delta_test").as[(Long, String)],
        1L -> "a")
    }
  }

  test("CTAS a managed table with the existing empty directory") {
    val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
    try {
      tableLoc.mkdir()
      withTable("tab1") {
        sql("CREATE TABLE tab1 USING delta AS SELECT 2, 'b'")
        checkAnswer(spark.table("tab1"), Row(2, "b"))
      }
    } finally {
      waitForTasksToFinish()
      Utils.deleteRecursively(tableLoc)
    }
  }

  test("create a managed table with the existing empty directory") {
    val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
    try {
      tableLoc.mkdir()
      withTable("tab1") {
        sql("CREATE TABLE tab1 (col1 int, col2 string) USING delta")
        sql("INSERT INTO tab1 VALUES (2, 'B')")
        checkAnswer(spark.table("tab1"), Row(2, "B"))
      }
    } finally {
      waitForTasksToFinish()
      Utils.deleteRecursively(tableLoc)
    }
  }

  testQuietly(
      "create a managed table with the existing non-empty directory") {
    withTable("tab1") {
      val tableLoc = new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier("tab1")))
      try {
        // create an empty hidden file
        tableLoc.mkdir()
        val hiddenGarbageFile = new File(tableLoc.getCanonicalPath, ".garbage")
        hiddenGarbageFile.createNewFile()
        var ex = intercept[AnalysisException] {
          sql("CREATE TABLE tab1 USING delta AS SELECT 2, 'b'")
        }.getMessage
        assert(ex.contains("Cannot create table"))

        ex = intercept[AnalysisException] {
          sql("CREATE TABLE tab1 (col1 int, col2 string) USING delta")
        }.getMessage
        assert(ex.contains("Cannot create table"))
      } finally {
        waitForTasksToFinish()
        Utils.deleteRecursively(tableLoc)
      }
    }
  }

  test("create table with table properties") {
    withTable("delta_test") {
      sql(s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta
             |TBLPROPERTIES(
             |  'delta.logRetentionDuration' = '2 weeks',
             |  'delta.checkpointInterval' = '20',
             |  'key' = 'value'
             |)
          """.stripMargin)

      val deltaLog = getDeltaLog("delta_test")

      val snapshot = deltaLog.update()
      assertEqual(snapshot.metadata.configuration, Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value"))
      assert(deltaLog.deltaRetentionMillis(snapshot.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot.metadata) == 20)
    }
  }

  test("create table with table properties - case insensitivity") {
    withTable("delta_test") {
      sql(s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta
             |TBLPROPERTIES(
             |  'dEltA.lOgrEteNtiOndURaTion' = '2 weeks',
             |  'DelTa.ChEckPoiNtinTervAl' = '20'
             |)
          """.stripMargin)

      val deltaLog = getDeltaLog("delta_test")

      val snapshot = deltaLog.update()
      assertEqual(snapshot.metadata.configuration,
        Map("delta.logRetentionDuration" -> "2 weeks", "delta.checkpointInterval" -> "20"))
      assert(deltaLog.deltaRetentionMillis(snapshot.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot.metadata) == 20)
    }
  }

  test(
      "create table with table properties - case insensitivity with existing configuration") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val path = tempDir.getCanonicalPath

        val deltaLog = getDeltaLog(new Path(path))

        val txn = deltaLog.startTransaction()
        txn.commit(Seq(Metadata(
          schemaString = new StructType().add("a", "long").add("b", "string").json,
          configuration = Map(
            "delta.logRetentionDuration" -> "2 weeks",
            "delta.checkpointInterval" -> "20",
            "key" -> "value"))),
          ManualUpdate)

        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String)
               |USING delta LOCATION '$path'
               |TBLPROPERTIES(
               |  'dEltA.lOgrEteNtiOndURaTion' = '2 weeks',
               |  'DelTa.ChEckPoiNtinTervAl' = '20',
               |  'key' = "value"
               |)
            """.stripMargin)

        val snapshot = deltaLog.update()
        assertEqual(snapshot.metadata.configuration, Map(
          "delta.logRetentionDuration" -> "2 weeks",
          "delta.checkpointInterval" -> "20",
          "key" -> "value"))
        assert(deltaLog.deltaRetentionMillis(snapshot.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
        assert(deltaLog.checkpointInterval(snapshot.metadata) == 20)
      }
    }
  }


  test("schema mismatch between DDL and table location should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = getDeltaLog(new Path(tempDir.getCanonicalPath))

        val txn = deltaLog.startTransaction()
        txn.commit(
          Seq(Metadata(schemaString = new StructType().add("a", "long").add("b", "long").json)),
          DeltaOperations.ManualUpdate)

        val ex = intercept[AnalysisException] {
          sql("CREATE TABLE delta_test(a LONG, b String)" +
            s" USING delta OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex.getMessage.contains("Specified type for b is different"))

        val ex1 = intercept[AnalysisException] {
          sql("CREATE TABLE delta_test(a LONG)" +
            s" USING delta OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex1.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex1.getMessage.contains("Specified schema is missing field"))

        val ex2 = intercept[AnalysisException] {
          sql("CREATE TABLE delta_test(a LONG, b String, c INT, d LONG)" +
            s" USING delta OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex2.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex2.getMessage.contains("Specified schema has additional field"))
        assert(ex2.getMessage.contains("Specified type for b is different"))
      }
    }
  }

  test(
      "schema metadata mismatch between DDL and table location should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = getDeltaLog(new Path(tempDir.getCanonicalPath))

        val txn = deltaLog.startTransaction()
        txn.commit(
          Seq(Metadata(schemaString = new StructType().add("a", "long")
            .add("b", "string", nullable = true,
              new MetadataBuilder().putBoolean("pii", value = true).build()).json)),
          DeltaOperations.ManualUpdate)
        val ex = intercept[AnalysisException] {
          sql("CREATE TABLE delta_test(a LONG, b String)" +
            s" USING delta OPTIONS (path '${tempDir.getCanonicalPath}')")
        }
        assert(ex.getMessage.contains("The specified schema does not match the existing schema"))
        assert(ex.getMessage.contains("metadata for field b is different"))
      }
    }
  }

  test(
      "partition schema mismatch between DDL and table location should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = getDeltaLog(new Path(tempDir.getCanonicalPath))

        val txn = deltaLog.startTransaction()
        txn.commit(
          Seq(Metadata(
            schemaString = new StructType().add("a", "long").add("b", "string").json,
            partitionColumns = Seq("a"))),
          DeltaOperations.ManualUpdate)
        val ex = intercept[AnalysisException](sql("CREATE TABLE delta_test(a LONG, b String)" +
          s" USING delta PARTITIONED BY(b) LOCATION '${tempDir.getCanonicalPath}'"))
        assert(ex.getMessage.contains(
          "The specified partitioning does not match the existing partitioning"))
      }
    }
  }

  testQuietly("create table with unknown table properties should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val ex = intercept[AnalysisException](sql(
          s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta LOCATION '${tempDir.getCanonicalPath}'
             |TBLPROPERTIES('delta.key' = 'value')
          """.stripMargin))
        assert(ex.getMessage.contains(
          "Unknown configuration was specified: delta.key"))
      }
    }
  }

  testQuietly("create table with invalid table properties should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val ex1 = intercept[IllegalArgumentException](sql(
          s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta LOCATION '${tempDir.getCanonicalPath}'
             |TBLPROPERTIES('delta.randomPrefixLength' = '-1')
          """.stripMargin))
        assert(ex1.getMessage.contains(
          "randomPrefixLength needs to be greater than 0."))

        val ex2 = intercept[IllegalArgumentException](sql(
          s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta LOCATION '${tempDir.getCanonicalPath}'
             |TBLPROPERTIES('delta.randomPrefixLength' = 'value')
          """.stripMargin))
        assert(ex2.getMessage.contains(
          "randomPrefixLength needs to be greater than 0."))
      }
    }
  }

  test(
      "table properties mismatch between DDL and table location should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = getDeltaLog(new Path(tempDir.getCanonicalPath))

        val txn = deltaLog.startTransaction()
        txn.commit(
          Seq(Metadata(
            schemaString = new StructType().add("a", "long").add("b", "string").json)),
          DeltaOperations.ManualUpdate)
        val ex = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE TABLE delta_test(a LONG, b String)
               |USING delta LOCATION '${tempDir.getCanonicalPath}'
               |TBLPROPERTIES('delta.randomizeFilePrefixes' = 'true')
            """.stripMargin)
        }

        assert(ex.getMessage.contains(
          "The specified properties do not match the existing properties"))
      }
    }
  }

  test("create table on an existing table location") {
    val catalog = spark.sessionState.catalog
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = getDeltaLog(new Path(tempDir.getCanonicalPath))

        val txn = deltaLog.startTransaction()
        txn.commit(
          Seq(Metadata(
            schemaString = new StructType().add("a", "long").add("b", "string").json,
            partitionColumns = Seq("b"))),
          DeltaOperations.ManualUpdate)
        sql("CREATE TABLE delta_test(a LONG, b String) USING delta " +
          s"OPTIONS (path '${tempDir.getCanonicalPath}') PARTITIONED BY(b)")
        val table = catalog.getTableMetadata(TableIdentifier("delta_test"))
        assert(table.tableType == CatalogTableType.EXTERNAL)
        assert(table.provider.contains("delta"))

        // Query the data and the metadata directly via the DeltaLog
        val deltaLog2 = getDeltaLog(table)

        // Since we manually committed Metadata without schema, we won't have column metadata in
        // the latest deltaLog snapshot
        assert(
          deltaLog2.snapshot.schema == new StructType().add("a", "long").add("b", "string"))
        assert(
          deltaLog2.snapshot.metadata.partitionSchema == new StructType().add("b", "string"))

        assert(getSchema("delta_test") === deltaLog2.snapshot.schema)
        assert(getPartitioningColumns("delta_test") === Seq("b"))

        // External catalog does not contain the schema and partition column names.
        verifyTableInCatalog(catalog, "delta_test")
      }
    }
  }

  test("create datasource table with a non-existing location") {
    withTempPath { dir =>
      withTable("t") {
        spark.sql(s"CREATE TABLE t(a int, b int) USING delta LOCATION '${dir.toURI}'")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        spark.sql("INSERT INTO TABLE t SELECT 1, 2")
        assert(dir.exists())

        checkDatasetUnorderly(
          sql("SELECT * FROM t").as[(Int, Int)],
          1 -> 2)
      }
    }

    // partition table
    withTempPath { dir =>
      withTable("t1") {
        spark.sql(
          s"CREATE TABLE t1(a int, b int) USING delta PARTITIONED BY(a) LOCATION '${dir.toURI}'")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        Seq((1, 2)).toDF("a", "b")
          .write.format("delta").mode("append").save(table.location.toString)
        val read = spark.read.format("delta").load(table.location.toString)
        checkAnswer(read, Seq(Row(1, 2)))

        val deltaLog = loadDeltaLog(table.location.toString)
        assertPartitionWithValueExists("a", "1", deltaLog)
      }
    }
  }

  Seq(true, false).foreach { shouldDelete =>
    val tcName = if (shouldDelete) "non-existing" else "existing"
    test(s"CTAS for external data source table with $tcName location") {
      val catalog = spark.sessionState.catalog
      withTable("t", "t1") {
        withTempDir { dir =>
          if (shouldDelete) dir.delete()
          spark.sql(
            s"""
               |CREATE TABLE t
               |USING delta
               |LOCATION '${dir.toURI}'
               |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
             """.stripMargin)
          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
          assert(table.tableType == CatalogTableType.EXTERNAL)
          assert(table.provider.contains("delta"))
          assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

          // Query the data and the metadata directly via the DeltaLog
          val deltaLog = getDeltaLog(table)

          assertEqual(deltaLog.snapshot.schema, new StructType()
            .add("a", "integer").add("b", "integer")
            .add("c", "integer").add("d", "integer"))
          assertEqual(
            deltaLog.snapshot.metadata.partitionSchema, new StructType())

          assertEqual(getSchema("t"), deltaLog.snapshot.schema)
          assert(getPartitioningColumns("t").isEmpty)

          // External catalog does not contain the schema and partition column names.
          verifyTableInCatalog(catalog, "t")

          // Query the table
          checkAnswer(spark.table("t"), Row(3, 4, 1, 2))

          // Directly query the reservoir
          checkAnswer(spark.read.format("delta")
            .load(new Path(table.storage.locationUri.get).toString), Seq(Row(3, 4, 1, 2)))
        }
        // partition table
        withTempDir { dir =>
          if (shouldDelete) dir.delete()
          spark.sql(
            s"""
               |CREATE TABLE t1
               |USING delta
               |PARTITIONED BY(a, b)
               |LOCATION '${dir.toURI}'
               |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
             """.stripMargin)
          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
          assert(table.tableType == CatalogTableType.EXTERNAL)
          assert(table.provider.contains("delta"))
          assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

          // Query the data and the metadata directly via the DeltaLog
          val deltaLog = getDeltaLog(table)

          assertEqual(deltaLog.snapshot.schema, new StructType()
            .add("a", "integer").add("b", "integer")
            .add("c", "integer").add("d", "integer"))
          assertEqual(
            deltaLog.snapshot.metadata.partitionSchema, new StructType()
            .add("a", "integer").add("b", "integer"))

          assertEqual(getSchema("t1"), deltaLog.snapshot.schema)
          assert(getPartitioningColumns("t1") == Seq("a", "b"))

          // External catalog does not contain the schema and partition column names.
          verifyTableInCatalog(catalog, "t1")

          // Query the table
          checkAnswer(spark.table("t1"), Row(3, 4, 1, 2))

          // Directly query the reservoir
          checkAnswer(spark.read.format("delta")
            .load(new Path(table.storage.locationUri.get).toString), Seq(Row(3, 4, 1, 2)))
        }
      }
    }
  }

  test("CTAS with table properties") {
    withTable("delta_test") {
      sql(
        s"""
           |CREATE TABLE delta_test
           |USING delta
           |TBLPROPERTIES(
           |  'delta.logRetentionDuration' = '2 weeks',
           |  'delta.checkpointInterval' = '20',
           |  'key' = 'value'
           |)
           |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
        """.stripMargin)

      val deltaLog = getDeltaLog("delta_test")

      val snapshot = deltaLog.update()
      assertEqual(snapshot.metadata.configuration, Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value"))
      assert(deltaLog.deltaRetentionMillis(snapshot.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot.metadata) == 20)
    }
  }

  test("CTAS with table properties - case insensitivity") {
    withTable("delta_test") {
      sql(
        s"""
           |CREATE TABLE delta_test
           |USING delta
           |TBLPROPERTIES(
           |  'dEltA.lOgrEteNtiOndURaTion' = '2 weeks',
           |  'DelTa.ChEckPoiNtinTervAl' = '20'
           |)
           |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
        """.stripMargin)

      val deltaLog = getDeltaLog("delta_test")

      val snapshot = deltaLog.update()
      assertEqual(snapshot.metadata.configuration,
        Map("delta.logRetentionDuration" -> "2 weeks", "delta.checkpointInterval" -> "20"))
      assert(deltaLog.deltaRetentionMillis(snapshot.metadata) == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval(snapshot.metadata) == 20)
    }
  }

  testQuietly("CTAS external table with existing data should fail") {
    withTable("t") {
      withTempDir { dir =>
        dir.delete()
        Seq((3, 4)).toDF("a", "b")
          .write.format("delta")
          .save(dir.toString)
        val ex = intercept[AnalysisException](spark.sql(
          s"""
             |CREATE TABLE t
             |USING delta
             |LOCATION '${dir.toURI}'
             |AS SELECT 1 as a, 2 as b
             """.stripMargin))
        assert(ex.getMessage.contains("Cannot create table"))
      }
    }

    withTable("t") {
      withTempDir { dir =>
        dir.delete()
        Seq((3, 4)).toDF("a", "b")
          .write.format("parquet")
          .save(dir.toString)
        val ex = intercept[AnalysisException](spark.sql(
          s"""
             |CREATE TABLE t
             |USING delta
             |LOCATION '${dir.toURI}'
             |AS SELECT 1 as a, 2 as b
             """.stripMargin))
        assert(ex.getMessage.contains("Cannot create table"))
      }
    }
  }

  testQuietly("CTAS with unknown table properties should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val ex = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE TABLE delta_test
               |USING delta
               |LOCATION '${tempDir.getCanonicalPath}'
               |TBLPROPERTIES('delta.key' = 'value')
               |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
            """.stripMargin)
        }
        assert(ex.getMessage.contains(
          "Unknown configuration was specified: delta.key"))
      }
    }
  }

  testQuietly("CTAS with invalid table properties should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val ex1 = intercept[IllegalArgumentException] {
          sql(
            s"""
               |CREATE TABLE delta_test
               |USING delta
               |LOCATION '${tempDir.getCanonicalPath}'
               |TBLPROPERTIES('delta.randomPrefixLength' = '-1')
               |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
            """.stripMargin)
        }
        assert(ex1.getMessage.contains(
          "randomPrefixLength needs to be greater than 0."))

        val ex2 = intercept[IllegalArgumentException] {
          sql(
            s"""
               |CREATE TABLE delta_test
               |USING delta
               |LOCATION '${tempDir.getCanonicalPath}'
               |TBLPROPERTIES('delta.randomPrefixLength' = 'value')
               |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
            """.stripMargin)
        }
        assert(ex2.getMessage.contains(
          "randomPrefixLength needs to be greater than 0."))
      }
    }
  }

  Seq("a:b", "a%b").foreach { specialChars =>
    test(s"data source table:partition column name containing $specialChars") {
      // On Windows, it looks colon in the file name is illegal by default. See
      // https://support.microsoft.com/en-us/help/289627
      assume(!Utils.isWindows || specialChars != "a:b")

      withTable("t") {
        withTempDir { dir =>
          spark.sql(
            s"""
               |CREATE TABLE t(a string, `$specialChars` string)
               |USING delta
               |PARTITIONED BY(`$specialChars`)
               |LOCATION '${dir.toURI}'
             """.stripMargin)

          assert(dir.listFiles().forall(_.toString.contains("_delta_log")))
          spark.sql(s"INSERT INTO TABLE t SELECT 1, 2")

          val deltaLog = loadDeltaLog(dir.toString)
          assertPartitionWithValueExists(specialChars, "2", deltaLog)

          checkAnswer(spark.table("t"), Row("1", "2") :: Nil)
        }
      }
    }
  }

  Seq("a b", "a:b", "a%b").foreach { specialChars =>
    test(s"location uri contains $specialChars for datasource table") {
      // On Windows, it looks colon in the file name is illegal by default. See
      // https://support.microsoft.com/en-us/help/289627
      assume(!Utils.isWindows || specialChars != "a:b")

      withTable("t", "t1") {
        withTempDir { dir =>
          val loc = new File(dir, specialChars)
          loc.mkdir()
          // The parser does not recognize the backslashes on Windows as they are.
          // These currently should be escaped.
          val escapedLoc = loc.getAbsolutePath.replace("\\", "\\\\")
          spark.sql(
            s"""
               |CREATE TABLE t(a string)
               |USING delta
               |LOCATION '$escapedLoc'
             """.stripMargin)

          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
          assert(table.location == makeQualifiedPath(loc.getAbsolutePath))
          assert(new Path(table.location).toString.contains(specialChars))

          assert(loc.listFiles().forall(_.toString.contains("_delta_log")))
          spark.sql("INSERT INTO TABLE t SELECT 1")
          assert(!loc.listFiles().forall(_.toString.contains("_delta_log")))
          checkAnswer(spark.table("t"), Row("1") :: Nil)
        }

        withTempDir { dir =>
          val loc = new File(dir, specialChars)
          loc.mkdir()
          // The parser does not recognize the backslashes on Windows as they are.
          // These currently should be escaped.
          val escapedLoc = loc.getAbsolutePath.replace("\\", "\\\\")
          spark.sql(
            s"""
               |CREATE TABLE t1(a string, b string)
               |USING delta
               |PARTITIONED BY(b)
               |LOCATION '$escapedLoc'
             """.stripMargin)

          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
          assert(table.location == makeQualifiedPath(loc.getAbsolutePath))
          assert(new Path(table.location).toString.contains(specialChars))

          assert(loc.listFiles().forall(_.toString.contains("_delta_log")))
          spark.sql("INSERT INTO TABLE t1 SELECT 1, 2")

          checkAnswer(spark.table("t1"), Row("1", "2") :: Nil)

          if (columnMappingEnabled) {
           // column mapping always use random file prefixes so we can't compare path
            val deltaLog = loadDeltaLog(loc.getCanonicalPath)
            val partPaths = getPartitionFilePathsWithValue("b", "2", deltaLog)
            assert(partPaths.nonEmpty)
            assert(partPaths.forall { p =>
              val parentPath = new File(p).getParentFile
              !parentPath.listFiles().forall(_.toString.contains("_delta_log"))
            })

            // In column mapping mode, as we are using random file prefixes,
            // this partition value is valid
            spark.sql("INSERT INTO TABLE t1 SELECT 1, '2017-03-03 12:13%3A14'")
            assertPartitionWithValueExists("b", "2017-03-03 12:13%3A14", deltaLog)
            checkAnswer(
                spark.table("t1"), Row("1", "2") :: Row("1", "2017-03-03 12:13%3A14") :: Nil)
          } else {
            val partFile = new File(loc, "b=2")
            assert(!partFile.listFiles().forall(_.toString.contains("_delta_log")))
            spark.sql("INSERT INTO TABLE t1 SELECT 1, '2017-03-03 12:13%3A14'")
            val partFile1 = new File(loc, "b=2017-03-03 12:13%3A14")
            assert(!partFile1.exists())

            if (!Utils.isWindows) {
              // Actual path becomes "b=2017-03-03%2012%3A13%253A14" on Windows.
              val partFile2 = new File(loc, "b=2017-03-03 12%3A13%253A14")
              assert(!partFile2.listFiles().forall(_.toString.contains("_delta_log")))
              checkAnswer(
                spark.table("t1"), Row("1", "2") :: Row("1", "2017-03-03 12:13%3A14") :: Nil)
            }
          }
        }
      }
    }
  }

  test("the qualified path of a delta table is stored in the catalog") {
    withTempDir { dir =>
      withTable("t", "t1") {
        assert(!dir.getAbsolutePath.startsWith("file:/"))
        // The parser does not recognize the backslashes on Windows as they are.
        // These currently should be escaped.
        val escapedDir = dir.getAbsolutePath.replace("\\", "\\\\")
        spark.sql(
          s"""
             |CREATE TABLE t(a string)
             |USING delta
             |LOCATION '$escapedDir'
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location.toString.startsWith("file:/"))
      }
    }

    withTempDir { dir =>
      withTable("t", "t1") {
        assert(!dir.getAbsolutePath.startsWith("file:/"))
        // The parser does not recognize the backslashes on Windows as they are.
        // These currently should be escaped.
        val escapedDir = dir.getAbsolutePath.replace("\\", "\\\\")
        spark.sql(
          s"""
             |CREATE TABLE t1(a string, b string)
             |USING delta
             |PARTITIONED BY(b)
             |LOCATION '$escapedDir'
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        assert(table.location.toString.startsWith("file:/"))
      }
    }
  }

  testQuietly("CREATE TABLE with existing data path") {
    // Re-use `filterV2TableProperties()` from `SQLTestUtils` as soon as it will be released.
    def isReservedProperty(propName: String): Boolean = {
      CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(propName) ||
        propName.startsWith(TableCatalog.OPTION_PREFIX) ||
        propName == TableCatalog.PROP_EXTERNAL
    }
    def filterV2TableProperties(properties: Map[String, String]): Map[String, String] = {
      properties.filterNot(kv => isReservedProperty(kv._1))
    }

    withTempPath { path =>
      withTable("src", "t1", "t2", "t3", "t4", "t5", "t6") {
        sql("CREATE TABLE src(i int, p string) USING delta PARTITIONED BY (p) " +
          "TBLPROPERTIES('delta.randomizeFilePrefixes' = 'true') " +
          s"LOCATION '${path.getAbsolutePath}'")
        sql("INSERT INTO src SELECT 1, 'a'")

        // CREATE TABLE without specifying anything works
        sql(s"CREATE TABLE t1 USING delta LOCATION '${path.getAbsolutePath}'")
        checkAnswer(spark.table("t1"), Row(1, "a"))

        // CREATE TABLE with the same schema and partitioning but no properties works
        sql(s"CREATE TABLE t2(i int, p string) USING delta PARTITIONED BY (p) " +
          s"LOCATION '${path.getAbsolutePath}'")
        checkAnswer(spark.table("t2"), Row(1, "a"))
        // Table properties should not be changed to empty.
        assert(filterV2TableProperties(getTableProperties("t2")) ==
          Map("delta.randomizeFilePrefixes" -> "true"))

        // CREATE TABLE with the same schema but no partitioning fails.
        val e0 = intercept[AnalysisException] {
          sql(s"CREATE TABLE t3(i int, p string) USING delta LOCATION '${path.getAbsolutePath}'")
        }
        assert(e0.message.contains("The specified partitioning does not match the existing"))

        // CREATE TABLE with different schema fails
        val e1 = intercept[AnalysisException] {
          sql(s"CREATE TABLE t4(j int, p string) USING delta LOCATION '${path.getAbsolutePath}'")
        }
        assert(e1.message.contains("The specified schema does not match the existing"))

        // CREATE TABLE with different partitioning fails
        val e2 = intercept[AnalysisException] {
          sql(s"CREATE TABLE t5(i int, p string) USING delta PARTITIONED BY (i) " +
            s"LOCATION '${path.getAbsolutePath}'")
        }
        assert(e2.message.contains("The specified partitioning does not match the existing"))

        // CREATE TABLE with different table properties fails
        val e3 = intercept[AnalysisException] {
          sql(s"CREATE TABLE t6 USING delta " +
            "TBLPROPERTIES ('delta.randomizeFilePrefixes' = 'false') " +
            s"LOCATION '${path.getAbsolutePath}'")
        }
        assert(e3.message.contains("The specified properties do not match the existing"))
      }
    }
  }

  test("CREATE TABLE on existing data should not commit metadata") {
    withTempDir { tempDir =>
      val path = tempDir.getCanonicalPath()
      val df = Seq(1, 2, 3, 4, 5).toDF()
      df.write.format("delta").save(path)
      val deltaLog = getDeltaLog(new Path(path))

      val oldVersion = deltaLog.snapshot.version
      sql(s"CREATE TABLE table USING delta LOCATION '$path'")
      assert(oldVersion == deltaLog.snapshot.version)
    }
  }
}

class DeltaTableCreationSuite
  extends DeltaTableCreationTests
  with DeltaSQLCommandTest {

  private def loadTable(tableName: String): Table = {
    val ti = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val namespace = if (ti.length == 1) Array("default") else ti.init.toArray
    spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
      .loadTable(Identifier.of(namespace, ti.last))
  }

  override protected def getPartitioningColumns(tableName: String): Seq[String] = {
    loadTable(tableName).partitioning()
      .map(_.references().head.fieldNames().mkString("."))
  }

  override def getSchema(tableName: String): StructType = {
    loadTable(tableName).schema()
  }

  override protected def getTableProperties(tableName: String): Map[String, String] = {
    loadTable(tableName).properties().asScala.toMap
      .filterKeys(!CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(_))
      .filterKeys(!TableFeatureProtocolUtils.isTableProtocolProperty(_))
      .toMap
  }

  testQuietly("REPLACE TABLE") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(
          s"""CREATE TABLE delta_test
             |USING delta
             |LOCATION '${dir.getAbsolutePath}'
             |AS SELECT 1 as a
          """.stripMargin)
        val deltaLog = DeltaLog.forTable(spark, dir)
        assert(deltaLog.snapshot.version === 0, "CTAS should be a single commit")

        sql(
          s"""REPLACE TABLE delta_test (col string)
             |USING delta
             |LOCATION '${dir.getAbsolutePath}'
          """.stripMargin)
        assert(deltaLog.snapshot.version === 1)
        assertEqual(
          deltaLog.snapshot.schema, new StructType().add("col", "string"))


        val e2 = intercept[AnalysisException] {
          sql(
            s"""REPLACE TABLE delta_test
               |USING delta
               |LOCATION '${dir.getAbsolutePath}'
          """.stripMargin)
        }
        assert(e2.getMessage.contains("schema is not provided"))
      }
    }
  }

  testQuietly("CREATE OR REPLACE TABLE on table without schema") {
    withTempDir { dir =>
      withTable("delta_test") {
        spark.range(10).write.format("delta").option("path", dir.getCanonicalPath)
          .saveAsTable("delta_test")
        // We need the schema
        val e = intercept[AnalysisException] {
          sql(s"""CREATE OR REPLACE TABLE delta_test
                 |USING delta
                 |LOCATION '${dir.getAbsolutePath}'
               """.stripMargin)
        }
        assert(e.getMessage.contains("schema is not provided"))
      }
    }
  }

  testQuietly("CREATE OR REPLACE TABLE on non-empty directory") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getCanonicalPath)
      withTable("delta_test") {
        // We need the schema
        val e = intercept[AnalysisException] {
          sql(s"""CREATE OR REPLACE TABLE delta_test
                 |USING delta
                 |LOCATION '${dir.getAbsolutePath}'
               """.stripMargin)
        }
        assert(e.getMessage.contains("schema is not provided"))
      }
    }
  }

  testQuietly(
      "REPLACE TABLE on non-empty directory") {
    withTempDir { dir =>
      spark.range(10).write.format("delta").save(dir.getCanonicalPath)
      withTable("delta_test") {
        val e = intercept[AnalysisException] {
          sql(
            s"""REPLACE TABLE delta_test
               |USING delta
               |LOCATION '${dir.getAbsolutePath}'
           """.stripMargin)
        }
        assert(e.getMessage.contains("cannot be replaced as it did not exist") ||
          e.getMessage.contains(s"table or view `default`.`delta_test` cannot be found"))
      }
    }
  }

  test("Create a table without comment") {
    withTempDir { dir =>
      val table = "delta_without_comment"
      withTable(table) {
        sql(s"CREATE TABLE $table (col string) USING delta LOCATION '${dir.getAbsolutePath}'")
        checkResult(
          sql(s"DESCRIBE DETAIL $table"),
          Seq("delta", null),
          Seq("format", "description"))
      }
    }
  }

  protected def withEmptySchemaTable(emptyTableName: String)(f: => Unit): Unit = {
    def getDeltaLog: DeltaLog =
      DeltaLog.forTable(spark, TableIdentifier(emptyTableName))

    // create using SQL API
    withTable(emptyTableName) {
      sql(s"CREATE TABLE $emptyTableName USING delta")
      assert(getDeltaLog.snapshot.schema.isEmpty)
      f

      // just make sure this statement runs
      sql(s"CREATE TABLE IF NOT EXISTS $emptyTableName USING delta")
    }

    // create using Delta table API (creates v1 table)
    withTable(emptyTableName) {
      io.delta.tables.DeltaTable
        .create(spark)
        .tableName(emptyTableName)
        .execute()
      assert(getDeltaLog.snapshot.schema.isEmpty)
      f
      io.delta.tables.DeltaTable
        .createIfNotExists(spark)
        .tableName(emptyTableName)
        .execute()
    }

  }

  test("Create an empty table without schema - unsupported cases") {
    import testImplicits._

    withSQLConf(DeltaSQLConf.DELTA_ALLOW_CREATE_EMPTY_SCHEMA_TABLE.key -> "true") {
      val emptySchemaTableName = "t1"

      // TODO: support CREATE OR REPLACE code path if needed in the future
      intercept[AnalysisException] {
        sql(s"CREATE OR REPLACE TABLE $emptySchemaTableName USING delta")
      }

      // similarly blocked using Delta Table API
      withTable(emptySchemaTableName) {
        intercept[AnalysisException] {
          io.delta.tables.DeltaTable
            .createOrReplace(spark)
            .tableName(emptySchemaTableName)
            .execute()
        }
      }

      withTable(emptySchemaTableName) {
        io.delta.tables.DeltaTable
          .create(spark)
          .tableName(emptySchemaTableName)
          .execute()

        intercept[AnalysisException] {
          io.delta.tables.DeltaTable
            .replace(spark)
            .tableName(emptySchemaTableName)
            .execute()
        }
      }

      // external table with an invalid location it shouldn't work (e.g. no transaction log present)
      withTable(emptySchemaTableName) {
        withTempDir { dir =>
          Seq(1, 2, 3).toDF().write.format("delta").save(dir.getAbsolutePath)
          Utils.deleteRecursively(new File(dir, "_delta_log"))
          val e = intercept[AnalysisException] {
            sql(s"CREATE TABLE $emptySchemaTableName USING delta LOCATION '${dir.getAbsolutePath}'")
          }
          assert(e.getErrorClass == "DELTA_CREATE_EXTERNAL_TABLE_WITHOUT_TXN_LOG")
        }
      }

      // CTAS from an empty schema dataframe should be blocked
      intercept[AnalysisException] {
        withTable(emptySchemaTableName) {
          val df = spark.emptyDataFrame
          df.createOrReplaceTempView("empty_df")
          sql(s"CREATE TABLE $emptySchemaTableName USING delta AS SELECT * FROM empty_df")
        }
      }

      // create empty schema table using dataframe api should be blocked
      intercept[AnalysisException] {
        withTable(emptySchemaTableName) {
          spark.emptyDataFrame
            .write.format("delta")
            .saveAsTable(emptySchemaTableName)
        }
      }

      intercept[AnalysisException] {
        withTable(emptySchemaTableName) {
          spark.emptyDataFrame
            .writeTo(emptySchemaTableName)
            .using("delta")
            .create()
        }
      }

      def assertFailToRead(f: => Any): Unit = {
        try f catch {
          case e: AnalysisException =>
            assert(e.getMessage.contains("that does not have any columns."))
        }
      }

      def assertSchemaEvolutionRequired(f: => Any): Unit = {
        val e = intercept[AnalysisException] {
          f
        }
        assert(e.getMessage.contains("A schema mismatch detected when writing to the Delta"))
      }

      // data reading or writing without mergeSchema should fail
      withEmptySchemaTable(emptySchemaTableName) {
        assertFailToRead {
          spark.read.table(emptySchemaTableName).collect()
        }

        assertFailToRead {
          sql(s"SELECT * FROM $emptySchemaTableName").collect()
        }

        assertSchemaEvolutionRequired {
          sql(s"INSERT INTO $emptySchemaTableName VALUES (1,2,3)")
        }

        // but enabling auto merge should make insert work
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
          sql(s"INSERT INTO $emptySchemaTableName VALUES (1,2,3)")
          checkAnswer(spark.read.table(emptySchemaTableName), Seq(Row(1, 2, 3)))
        }
      }

      // allows drop and recreate the same table with empty schema
      withTempDir { dir =>
        withTable(emptySchemaTableName) {
          sql(s"CREATE TABLE $emptySchemaTableName USING delta LOCATION '${dir.getCanonicalPath}'")
          val snapshot = DeltaLog.forTable(spark, TableIdentifier(emptySchemaTableName)).update()
          assert(snapshot.schema.isEmpty && snapshot.version == 0)
          assertFailToRead {
            sql(s"SELECT * FROM $emptySchemaTableName")
          }
          // drop the table
          sql(s"DROP TABLE $emptySchemaTableName")
          // recreate the table again should work
          sql(s"CREATE TABLE $emptySchemaTableName USING delta LOCATION '${dir.getCanonicalPath}'")
          assertFailToRead {
            sql(s"SELECT * FROM $emptySchemaTableName")
          }
          // write some data to it
          withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
            sql(s"INSERT INTO $emptySchemaTableName VALUES (1,2,3)")
            checkAnswer(spark.read.table(emptySchemaTableName), Seq(Row(1, 2, 3)))
          }
          // drop again
          sql(s"DROP TABLE $emptySchemaTableName")
          // recreate the table again should work
          sql(s"CREATE TABLE $emptySchemaTableName USING delta LOCATION '${dir.getCanonicalPath}'")
          checkAnswer(spark.read.table(emptySchemaTableName), Seq(Row(1, 2, 3)))
        }
      }
    }
  }

  test("Create an empty table without schema - supported cases") {
    import testImplicits._

    withSQLConf(DeltaSQLConf.DELTA_ALLOW_CREATE_EMPTY_SCHEMA_TABLE.key -> "true") {
      val emptyTableName = "t1"

      def getDeltaLog: DeltaLog = DeltaLog.forTable(spark, TableIdentifier(emptyTableName))

      // yet CTAS should be allowed
      withTable(emptyTableName) {
        sql(s"CREATE TABLE $emptyTableName USING delta AS SELECT 1")
        assert(getDeltaLog.snapshot.schema.size == 1)
      }

      // and create Delta table using existing valid location should work without ()
      withTable(emptyTableName) {
        withTempDir { dir =>
          Seq(1, 2, 3).toDF().write.format("delta").save(dir.getAbsolutePath)
          sql(s"CREATE TABLE $emptyTableName USING delta LOCATION '${dir.getAbsolutePath}'")
          assert(getDeltaLog.snapshot.schema.size == 1)
        }
      }

      // checkpointing should work
      withEmptySchemaTable(emptyTableName) {
        getDeltaLog.checkpoint()
        assert(getDeltaLog.readLastCheckpointFile().exists(_.version == 0))
        // run some operations
        withSQLConf(DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key -> "true") {
          sql(s"INSERT INTO $emptyTableName VALUES (1,2,3)")
          checkAnswer(spark.read.table(emptyTableName), Seq(Row(1, 2, 3)))
        }
        getDeltaLog.checkpoint()
        assert(getDeltaLog.readLastCheckpointFile().exists(_.version == 1))
      }

      withEmptySchemaTable(emptyTableName) {
        // TODO: possibly support MERGE into the future
        try {
          val source = "t2"
          withTable(source) {
            sql(s"CREATE TABLE $source USING delta AS SELECT 1")
            sql(
              s"""
                 |MERGE INTO $emptyTableName
                 |USING $source
                 |ON FALSE
                 |WHEN NOT MATCHED
                 |  THEN INSERT *
                 |""".stripMargin)
          }
        } catch {
            case _: AssertionError | _: SparkException =>
        }
      }

      // Delta specific DMLs should work, though they should basically be noops
      withEmptySchemaTable(emptyTableName) {
        sql(s"OPTIMIZE $emptyTableName")
        sql(s"VACUUM $emptyTableName")

        assert(getDeltaLog.snapshot.schema.isEmpty)
      }

      // metadata DDL should work
      withEmptySchemaTable(emptyTableName) {
        sql(s"ALTER TABLE $emptyTableName SET TBLPROPERTIES ('a' = 'b')")
        assert(DeltaLog.forTable(spark,
          TableIdentifier(emptyTableName)).snapshot.metadata.configuration.contains("a"))

        checkAnswer(
          sql(s"COMMENT ON TABLE $emptyTableName IS 'My Empty Cool Table'"), Nil)
        assert(sql(s"DESCRIBE TABLE $emptyTableName").collect().length == 0)

        // create table, alter tbl property, tbl comment
        assert(sql(s"DESCRIBE HISTORY $emptyTableName").collect().length == 3)

        checkAnswer(sql(s"SHOW COLUMNS IN $emptyTableName"), Nil)
      }

      // schema evolution ddl should work
      withEmptySchemaTable(emptyTableName) {
        sql(s"ALTER TABLE $emptyTableName ADD COLUMN (id long COMMENT 'haha')")
        assert(getDeltaLog.snapshot.schema.size == 1)
      }

      withEmptySchemaTable(emptyTableName) {
        sql(s"ALTER TABLE $emptyTableName ADD COLUMNS (id long, id2 long)")
        assert(getDeltaLog.snapshot.schema.size == 2)
      }

      // schema evolution through df should work
      // - v1 api
      withEmptySchemaTable(emptyTableName) {
        Seq(1, 2, 3).toDF()
          .write.format("delta")
          .mode("append")
          .option("mergeSchema", "true")
          .saveAsTable(emptyTableName)

        assert(getDeltaLog.snapshot.schema.size == 1)
      }

      withEmptySchemaTable(emptyTableName) {
        Seq(1, 2, 3).toDF()
          .write.format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(emptyTableName)

        assert(getDeltaLog.snapshot.schema.size == 1)
      }

      // - v2 api
      withEmptySchemaTable(emptyTableName) {
        Seq(1, 2, 3).toDF()
            .writeTo(emptyTableName)
            .option("mergeSchema", "true")
            .append()

        assert(getDeltaLog.snapshot.schema.size == 1)
      }

      withEmptySchemaTable(emptyTableName) {
        Seq(1, 2, 3).toDF()
            .writeTo(emptyTableName)
            .using("delta")
            .replace()

        assert(getDeltaLog.snapshot.schema.size == 1)
      }


    }
  }

  test("Create a table with comment") {
    val table = "delta_with_comment"
    withTempDir { dir =>
      withTable(table) {
        sql(
          s"""
             |CREATE TABLE $table (col string)
             |USING delta
             |COMMENT 'This is my table'
             |LOCATION '${dir.getAbsolutePath}'
            """.stripMargin)
        checkResult(
          sql(s"DESCRIBE DETAIL $table"),
          Seq("delta", "This is my table"),
          Seq("format", "description"))
      }
    }
  }

  test("Replace a table without comment") {
    withTempDir { dir =>
      val table = "replace_table_without_comment"
      val location = dir.getAbsolutePath
      withTable(table) {
        sql(s"CREATE TABLE $table (col string) USING delta COMMENT 'Table' LOCATION '$location'")
        sql(s"REPLACE TABLE $table (col string) USING delta LOCATION '$location'")
        checkResult(
          sql(s"DESCRIBE DETAIL $table"),
          Seq("delta", null),
          Seq("format", "description"))
      }
    }
  }

  test("Replace a table with comment") {
    withTempDir { dir =>
      val table = "replace_table_with_comment"
      val location = dir.getAbsolutePath
      withTable(table) {
        sql(s"CREATE TABLE $table (col string) USING delta LOCATION '$location'")
        sql(
          s"""
             |REPLACE TABLE $table (col string)
             |USING delta
             |COMMENT 'This is my table'
             |LOCATION '$location'
            """.stripMargin)
        checkResult(
          sql(s"DESCRIBE DETAIL $table"),
          Seq("delta", "This is my table"),
          Seq("format", "description"))
      }
    }
  }

  test("CTAS a table without comment") {
    val table = "ctas_without_comment"
    withTable(table) {
      sql(s"CREATE TABLE $table USING delta AS SELECT * FROM range(10)")
      checkResult(
        sql(s"DESCRIBE DETAIL $table"),
        Seq("delta", null),
        Seq("format", "description"))
    }
  }

  test("CTAS a table with comment") {
    val table = "ctas_with_comment"
    withTable(table) {
      sql(
        s"""CREATE TABLE $table
           |USING delta
           |COMMENT 'This table is created with existing data'
           |AS SELECT * FROM range(10)
          """.stripMargin)
      checkResult(
        sql(s"DESCRIBE DETAIL $table"),
        Seq("delta", "This table is created with existing data"),
        Seq("format", "description"))
    }
  }

  test("Replace CTAS a table without comment") {
    val table = "replace_ctas_without_comment"
    withTable(table) {
      sql(
        s"""CREATE TABLE $table
           |USING delta
           |COMMENT 'This table is created with existing data'
           |AS SELECT * FROM range(10)
          """.stripMargin)
      sql(s"REPLACE TABLE $table USING delta AS SELECT * FROM range(10)")
      checkResult(
        sql(s"DESCRIBE DETAIL $table"),
        Seq("delta", null),
        Seq("format", "description"))
    }
  }

  test("Replace CTAS a table with comment") {
    val table = "replace_ctas_with_comment"
    withTable(table) {
      sql(s"CREATE TABLE $table USING delta COMMENT 'a' AS SELECT * FROM range(10)")
      sql(
        s"""REPLACE TABLE $table
           |USING delta
           |COMMENT 'This table is created with existing data'
           |AS SELECT * FROM range(10)
          """.stripMargin)
      checkResult(
        sql(s"DESCRIBE DETAIL $table"),
        Seq("delta", "This table is created with existing data"),
        Seq("format", "description"))
    }
  }

  /**
   * Verifies that the correct table properties are stored in the transaction log as well as the
   * catalog.
   */
  private def verifyTableProperties(
      tableName: String,
      deltaLogPropertiesContains: Seq[String],
      deltaLogPropertiesMissing: Seq[String],
      catalogStorageProps: Seq[String] = Nil): Unit = {
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))

    if (catalogStorageProps.isEmpty) {
      assert(table.storage.properties.isEmpty)
    } else {
      assert(catalogStorageProps.forall(table.storage.properties.contains),
        s"Catalog didn't contain properties: ${catalogStorageProps}.\n" +
          s"Catalog: ${table.storage.properties}")
    }
    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tableName))

    deltaLogPropertiesContains.foreach { prop =>
      assert(deltaLog.snapshot.getProperties.contains(prop))
    }

    deltaLogPropertiesMissing.foreach { prop =>
      assert(!deltaLog.snapshot.getProperties.contains(prop))
    }
  }

  test("do not store write options in the catalog - DataFrameWriter") {
    withTempDir { dir =>
      withTable("t") {
        spark.range(10).write.format("delta")
          .option("path", dir.getCanonicalPath)
          .option("mergeSchema", "true")
          .option("delta.appendOnly", "true")
          .saveAsTable("t")

        verifyTableProperties(
          "t",
          // Still allow delta prefixed confs
          Seq("delta.appendOnly"),
          Seq("mergeSchema")
        )
        // Sanity check that table is readable
        checkAnswer(spark.table("t"), spark.range(10).toDF())
      }
    }
  }


  test("do not store write options in the catalog - DataFrameWriterV2") {
    withTempDir { dir =>
      withTable("t") {
        spark.range(10).writeTo("t").using("delta")
          .option("path", dir.getCanonicalPath)
          .option("mergeSchema", "true")
          .option("delta.appendOnly", "true")
          .tableProperty("key", "value")
          .create()

        verifyTableProperties(
          "t",
          Seq(
            "delta.appendOnly",   // Still allow delta prefixed confs
            "key"                 // Explicit properties should work
          ),
          Seq("mergeSchema")
        )
        // Sanity check that table is readable
        checkAnswer(spark.table("t"), spark.range(10).toDF())
      }
    }
  }

  test(
      "do not store write options in the catalog - legacy flag") {
    withTempDir { dir =>
      withTable("t") {
        withSQLConf(DeltaSQLConf.DELTA_LEGACY_STORE_WRITER_OPTIONS_AS_PROPS.key -> "true") {
          spark.range(10).write.format("delta")
            .option("path", dir.getCanonicalPath)
            .option("mergeSchema", "true")
            .option("delta.appendOnly", "true")
            .saveAsTable("t")

          verifyTableProperties(
            "t",
            // Everything gets stored in the transaction log
            Seq("delta.appendOnly", "mergeSchema"),
            Nil,
            // Things get stored in the catalog props as well
            Seq("delta.appendOnly", "mergeSchema")
          )

          checkAnswer(spark.table("t"), spark.range(10).toDF())
        }
      }
    }
  }

  test("create table using varchar at the same location should succeed") {
    withTempDir { location =>
      withTable("t1", "t2") {
        sql(s"""
               |create table t1
               |(colourID string, colourName varchar(128), colourGroupID string)
               |USING delta LOCATION '$location'""".stripMargin)
        sql(
          s"""
             |insert into t1 (colourID, colourName, colourGroupID)
             |values ('1', 'RED', 'a'), ('2', 'BLUE', 'b')
             |""".stripMargin)
        sql(s"""
               |create table t2
               |(colourID string, colourName varchar(128), colourGroupID string)
               |USING delta LOCATION '$location'""".stripMargin)
        // Verify that select from the second table should be the same as inserted
        val readout = sql(
          s"""
             |select * from t2 order by colourID
             |""".stripMargin).collect()
        assert(readout.length == 2)
        assert(readout(0).get(0) == "1")
        assert(readout(0).get(1) == "RED")
        assert(readout(1).get(0) == "2")
        assert(readout(1).get(1) == "BLUE")
      }
    }
  }

  test("CREATE OR REPLACE TABLE on a catalog table where the backing " +
    "directory has been deleted") {
    val tbl = "delta_tbl"
    withTempDir { dir =>
      withTable(tbl) {
        val subdir = new File(dir, "subdir")
        sql(s"CREATE OR REPLACE table $tbl (id String) USING delta " +
          s"LOCATION '${subdir.getCanonicalPath}'")
        val tableIdentifier =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl)).identifier
        val tableName = tableIdentifier.copy(catalog = None).toString
        val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tbl))
        sql(s"INSERT INTO $tbl VALUES ('1')")
        FileUtils.deleteDirectory(subdir)
        val e = intercept[DeltaIllegalStateException] {
          sql(
            s"CREATE OR REPLACE table $tbl (id String) USING delta" +
              s" LOCATION '${subdir.getCanonicalPath}'")
        }
        checkError(
          e,
          "DELTA_METADATA_ABSENT_EXISTING_CATALOG_TABLE",
          parameters = Map(
            "tableName" -> tableName,
            "tablePath" -> deltaLog.logPath.toString,
            "tableNameForDropCmd" -> tableName
          ))

        // Table creation should work after running DROP TABLE.
        sql(s"DROP table ${e.getMessageParameters().get("tableNameForDropCmd")}")
        sql(s"CREATE OR REPLACE table $tbl (id String) USING delta " +
          s"LOCATION '${subdir.getCanonicalPath}'")
        sql(s"INSERT INTO $tbl VALUES ('21')")
        val data = sql(s"SELECT * FROM $tbl").collect()
        assert(data.length == 1)
      }
    }
  }
}

trait DeltaTableCreationColumnMappingSuiteBase extends DeltaColumnMappingSelectedTestMixin {
  override protected def runOnlyTests: Seq[String] = Seq(
    "create table with schema and path",
    "create external table without schema",
    "REPLACE TABLE",
    "CREATE OR REPLACE TABLE on non-empty directory"
  ) ++ Seq("partitioned" -> Seq("v2"), "non-partitioned" -> Nil)
    .flatMap { case (isPartitioned, cols) =>
      SaveMode.values().flatMap { saveMode =>
        Seq(
          s"saveAsTable to a new table (managed) - $isPartitioned, saveMode: $saveMode",
          s"saveAsTable to a new table (external) - $isPartitioned, saveMode: $saveMode")
      }
    } ++ Seq("a b", "a:b", "a%b").map { specialChars =>
      s"location uri contains $specialChars for datasource table"
    }
}

class DeltaTableCreationIdColumnMappingSuite extends DeltaTableCreationSuite
  with DeltaColumnMappingEnableIdMode {

  override val defaultTempDirPrefix = "spark"

  override protected def getTableProperties(tableName: String): Map[String, String] = {
    // ignore comparing column mapping properties
    dropColumnMappingConfigurations(super.getTableProperties(tableName))
  }
}

class DeltaTableCreationNameColumnMappingSuite extends DeltaTableCreationSuite
  with DeltaColumnMappingEnableNameMode {
  override val defaultTempDirPrefix = "spark"

  override protected def getTableProperties(tableName: String): Map[String, String] = {
    // ignore comparing column mapping properties
    dropColumnMappingConfigurations(super.getTableProperties(tableName))
  }
}
