/*
 * DATABRICKS CONFIDENTIAL & PROPRIETARY
 * __________________
 *
 * Copyright 2019 Databricks, Inc.
 * All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains the property of Databricks, Inc.
 * and its suppliers, if any.  The intellectual and technical concepts contained herein are
 * proprietary to Databricks, Inc. and its suppliers and may be covered by U.S. and foreign Patents,
 * patents in process, and are protected by trade secret and/or copyright law. Dissemination, use,
 * or reproduction of this information is strictly forbidden unless prior written permission is
 * obtained from Databricks, Inc.
 *
 * If you view or obtain a copy of this information and believe Databricks, Inc. may not have
 * intended it to be made available, please promptly report it to Databricks Legal Department
 * @ legal@databricks.com.
 */

package org.apache.spark.sql.delta

import java.io.File

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkEnv, SparkException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTableType, CatalogUtils, ExternalCatalogUtils}
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.DeltaOperations.ManualUpdate
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.schema.InvariantViolationException
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

class DeltaDDLSuite extends DeltaDDLTestBase

abstract class DeltaDDLTestBase extends QueryTest with SharedSparkSession {
  import testImplicits._

  protected def withCloudProvider(cloudProvider: String)(f: => Unit): Unit = {
    f
  }

  protected def disableSparkService[T](f: => T): T = {
    f
  }

  override def sparkConf: SparkConf = super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getCanonicalName)

  test("create table with schema and path") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkDatasetUnorderly(
          sql("SELECT * FROM delta_test").as[(Long, String)],
          1L -> "a")
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("a", null, "bigint", true, false, false) ::
            Row("b", null, "string", true, false, false) :: Nil)
        checkAnswer(
          spark.catalog.listTables().toDF(),
          Row("delta_test", "default", null, "EXTERNAL", false)
        )
      }
    }
  }

  test("failed to create a table and then able to recreate it") {
    withTable("delta_test") {
      val e = intercept[AnalysisException] {
        sql("CREATE TABLE delta_test USING delta")
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

  test("create managed table without schema") {
    withTable("delta_test") {
      val e = intercept[AnalysisException] {
        sql("CREATE TABLE delta_test USING delta")
      }.getMessage
      assert(e.contains("but the schema is not specified"))
    }
  }

  test("reject creating a delta table pointing to non-delta files") {
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
        assert(e.contains(
          "Cannot create table ('`default`.`delta_test`'). The associated location"))
      }
    }
  }

  test("create external table without schema but using non-delta files") {
    withTempDir { dir =>
      withTable("delta_test") {
        Seq(1L -> "a").toDF().selectExpr("_1 as v1", "_2 as v2").write
          .mode("append").partitionBy("v2").format("parquet").save(dir.getCanonicalPath)
        val e = intercept[AnalysisException] {
          spark.catalog.createTable("delta_test", dir.getCanonicalPath, "delta")
        }.getMessage
        assert(e.contains("but there is no transaction log"))
      }
    }
  }

  test("create external table without schema and input files") {
    withTempDir { dir =>
      withTable("delta_test") {
        val e = intercept[AnalysisException] {
          spark.catalog.createTable("delta_test", dir.getCanonicalPath, "delta")
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
        val deltaLog = disableSparkService {
          DeltaLog.forTable(spark, new Path(table.storage.locationUri.get))
        }

        assert(deltaLog.snapshot.schema == new StructType().add("a", "long").add("b", "string"))
        assert(deltaLog.snapshot.metadata.partitionSchema == new StructType())

        assert(deltaLog.snapshot.schema == table.schema)
        assert(table.partitionColumnNames.isEmpty)

        // External catalog does not contain the schema and partition column names.
        val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
        assert(externalTable.schema == new StructType())
        assert(externalTable.partitionColumnNames.isEmpty)

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
      val deltaLog = disableSparkService {
        DeltaLog.forTable(spark, new Path(table.storage.locationUri.get))
      }
      assert(deltaLog.snapshot.schema == new StructType().add("a", "long").add("b", "string"))
      assert(deltaLog.snapshot.metadata.partitionSchema == new StructType())

      assert(deltaLog.snapshot.schema == table.schema)
      assert(table.partitionColumnNames.isEmpty)
      assert(table.partitionSchema.isEmpty)
      assert(table.dataSchema == new StructType().add("a", "long").add("b", "string"))

      // External catalog does not contain the schema and partition column names.
      val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
      assert(externalTable.schema == new StructType())
      assert(externalTable.partitionColumnNames.isEmpty)

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
      checkAnswer(
        spark.catalog.listColumns("delta_test").toDF(),
        Row("a", null, "bigint", true, true, false) ::
          Row("b", null, "string", true, false, false) :: Nil)
      checkAnswer(
        spark.catalog.listTables().toDF(),
        Row("delta_test", "default", null, "MANAGED", false)
      )

      // Query the data and the metadata directly via the DeltaLog
      val deltaLog = disableSparkService {
        DeltaLog.forTable(spark, new Path(table.storage.locationUri.get))
      }
      assert(deltaLog.snapshot.schema == new StructType().add("a", "long").add("b", "string"))
      assert(deltaLog.snapshot.metadata.partitionSchema == new StructType().add("a", "long"))

      assert(deltaLog.snapshot.schema == table.schema)
      assert(table.partitionColumnNames == Seq("a"))
      assert(table.partitionSchema == new StructType().add("a", "long"))
      assert(table.dataSchema == new StructType().add("b", "string"))

      // External catalog does not contain the schema and partition column names.
      val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
      assert(externalTable.schema == new StructType())
      assert(externalTable.partitionColumnNames.isEmpty)

      sql("INSERT INTO delta_test SELECT 1, 'a'")

      val path = new File(new File(table.storage.locationUri.get), "a=1")
      assert(path.listFiles().nonEmpty)

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

  test("create a managed table with the existing non-empty directory") {
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

      val deltaLog = disableSparkService {
        DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      }
      val snapshot = deltaLog.update()
      assert(snapshot.metadata.configuration == Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval == 20)
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

      val deltaLog = disableSparkService {
        DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      }
      val snapshot = deltaLog.update()
      assert(snapshot.metadata.configuration ==
        Map("delta.logRetentionDuration" -> "2 weeks", "delta.checkpointInterval" -> "20"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval == 20)
    }
  }

  test("create table with table properties - case insensitivity with existing configuration") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val path = tempDir.getCanonicalPath

        val deltaLog = disableSparkService {
          DeltaLog.forTable(spark, path)
        }
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
        assert(snapshot.metadata.configuration == Map(
          "delta.logRetentionDuration" -> "2 weeks",
          "delta.checkpointInterval" -> "20",
          "key" -> "value"))
        assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
        assert(deltaLog.checkpointInterval == 20)
      }
    }
  }

  // TODO re-enable this test after SC-11193
  ignore("create table with NOT NULL - check violation through SQL") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String NOT NULL)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("a", null, "bigint", true, false, false) ::
            Row("b", null, "string", false, false, false) :: Nil)

        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(1L, "a")))

        val e = intercept[SparkException] {
          sql("INSERT INTO delta_test VALUES (2, null)")
        }
        verifyInvariantViolationException(e)
      }
    }
  }

  test("create table with NOT NULL - check violation through file writing") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String NOT NULL)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("a", null, "bigint", true, false, false) ::
            Row("b", null, "string", false, false, false) :: Nil)

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("delta_test"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        Seq((1L, "a")).toDF("a", "b")
          .write.format("delta").mode("append").save(table.location.toString)
        val read = spark.read.format("delta").load(table.location.toString)
        checkAnswer(read, Seq(Row(1L, "a")))

        val e = intercept[SparkException] {
          Seq((2L, null)).toDF("a", "b")
            .write.format("delta").mode("append").save(table.location.toString)
        }
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("a", null, "bigint", true, false, false) :: Nil)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
               |ADD COLUMNS (b String NOT NULL, c Int)""".stripMargin)
        }
        val msg = "`NOT NULL in ALTER TABLE ADD COLUMNS` is not supported for Delta tables"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from nullable to NOT NULL - not supported") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("a", null, "bigint", true, false, false) ::
            Row("b", null, "string", true, false, false) :: Nil)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
               |CHANGE COLUMN b b String NOT NULL""".stripMargin)
        }
        val msg = "ALTER TABLE CHANGE COLUMN is not supported for changing column " +
          "'b' with type 'StringType (nullable = true)' to " +
          "'b' with type 'StringType (nullable = false)'"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN from NOT NULL to nullable") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(
          s"""
             |CREATE TABLE delta_test(a LONG NOT NULL, b String)
             |USING delta
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("a", null, "bigint", false, false, false) ::
            Row("b", null, "string", true, false, false) :: Nil)

        sql("INSERT INTO delta_test SELECT 1, 'a'")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row(1L, "a")))

        sql(
          s"""
             |ALTER TABLE delta_test
             |CHANGE COLUMN a a LONG AFTER b""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("b", null, "string", true, false, false) ::
            Row("a", null, "bigint", true, false, false) :: Nil)

        sql("INSERT INTO delta_test SELECT 'b', NULL")
        checkAnswer(
          sql("SELECT * FROM delta_test"),
          Seq(Row("a", 1L), Row("b", null)))
      }
    }
  }

  test("create table with NOT NULL in struct type - check violation") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test
               |(x struct<a: LONG, b: String NOT NULL>, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("x", null, "struct<a:bigint,b:string>", true, false, false) ::
            Row("y", null, "bigint", true, false, false) :: Nil)
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
        val e = intercept[SparkException] {
          spark.createDataFrame(
            Seq(Row(Row(2L, null), 2L)).asJava,
            schema
          ).write.format("delta").mode("append").save(table.location.toString)
        }
        verifyInvariantViolationException(e)
      }
    }
  }

  test("ALTER TABLE ADD COLUMNS with NOT NULL in struct type - not supported") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test
               |(y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("y", null, "bigint", true, false, false) :: Nil)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
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
      withTable("delta_test") {
        sql(
          s"""
             |CREATE TABLE delta_test
             |(y LONG NOT NULL)
             |USING delta
             |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("y", null, "bigint", false, false, false) :: Nil)

        sql(
          s"""
             |ALTER TABLE delta_test
             |ADD COLUMNS (x struct<a: LONG, b: String>, z INT)""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("y", null, "bigint", false, false, false) ::
            Row("x", null, "struct<a:bigint,b:string>", true, false, false) ::
            Row("z", null, "int", true, false, false) :: Nil)
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - not supported") {
    withTempDir { dir =>
      withTable("delta_test") {
        sql(s"""
               |CREATE TABLE delta_test
               |(x struct<a: LONG, b: String>, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
        checkAnswer(
          spark.catalog.listColumns("delta_test").toDF(),
          Row("x", null, "struct<a:bigint,b:string>", true, false, false) ::
            Row("y", null, "bigint", true, false, false) :: Nil)

        val e = intercept[AnalysisException] {
          sql(
            s"""
               |ALTER TABLE delta_test
               |CHANGE COLUMN x x struct<a: LONG, b: String NOT NULL>""".stripMargin)
        }
        val msg = "ALTER TABLE CHANGE COLUMN is not supported for changing column " +
          "'x' with type " +
          "'StructType(StructField(a,LongType,true), StructField(b,StringType,true)) " +
          "(nullable = true)' to " +
          "'x' with type " +
          "'StructType(StructField(a,LongType,true), StructField(b,StringType,false)) " +
          "(nullable = true)'"
        assert(e.getMessage.contains(msg))
      }
    }
  }

  test("ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTempDir { dir =>
        withTable("delta_test") {
          sql(
            s"""
               |CREATE TABLE delta_test
               |(x struct<a: LONG, b: String NOT NULL> NOT NULL, y LONG)
               |USING delta
               |OPTIONS('path'='${dir.getCanonicalPath}')""".stripMargin)
          checkAnswer(
            spark.catalog.listColumns("delta_test").toDF(),
            Row("x", null, "struct<a:bigint,b:string>", false, false, false) ::
              Row("y", null, "bigint", true, false, false) :: Nil)
          sql("INSERT INTO delta_test SELECT (1, 'a'), 1")
          checkAnswer(
            sql("SELECT * FROM delta_test"),
            Seq(Row(Row(1L, "a"), 1)))

          sql(
            s"""
               |ALTER TABLE delta_test
               |CHANGE COLUMN x x struct<A: LONG, B: String> NOT NULL""".stripMargin)
          sql("INSERT INTO delta_test SELECT (2, null), null")
          checkAnswer(
            sql("SELECT * FROM delta_test"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null)))

          sql(
            s"""
               |ALTER TABLE delta_test
               |CHANGE COLUMN x x struct<a: LONG, b: String>""".stripMargin)
          sql("INSERT INTO delta_test SELECT null, 3")
          checkAnswer(
            sql("SELECT * FROM delta_test"),
            Seq(
              Row(Row(1L, "a"), 1),
              Row(Row(2L, null), null),
              Row(null, 3)))
        }
      }
    }
  }

  private def verifyInvariantViolationException(e: Exception): Unit = {
    var violationException = e.getCause
    while (violationException != null &&
      !violationException.isInstanceOf[InvariantViolationException]) {
      violationException = violationException.getCause
    }
    if (violationException == null) {
      fail("Didn't receive a InvariantViolationException.")
    }
    assert(violationException.getMessage.contains("Invariant NOT NULL violated for column"))
  }

  test("schema mismatch between DDL and reservoir location should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = disableSparkService {
          DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        }
        val txn = deltaLog.startTransaction()
        txn.commit(
          Seq(Metadata(schemaString = new StructType().add("a", "long").add("b", "long").json)),
          DeltaOperations.ManualUpdate)
        val ex = intercept[AnalysisException](sql("CREATE TABLE delta_test(a LONG, b String)" +
          s" USING delta OPTIONS (path '${tempDir.getCanonicalPath}')"))
        assert(ex.getMessage.contains("The specified schema does not match the existing schema"))
      }
    }
  }

  test("partition schema mismatch between DDL and reservoir location should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = disableSparkService {
          DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        }
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

  test("create table with unknown table properties should throw an error") {
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

  test("create table with invalid table properties should throw an error") {
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

  test("table properties mismatch between DDL and reservoir location should throw an error") {
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = disableSparkService {
          DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        }
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

  test("create table on an existing reservoir location") {
    val catalog = spark.sessionState.catalog
    withTempDir { tempDir =>
      withTable("delta_test") {
        val deltaLog = disableSparkService {
          DeltaLog.forTable(spark, new Path(tempDir.getCanonicalPath))
        }
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
        val deltaLog2 = disableSparkService {
          DeltaLog.forTable(spark, new Path(table.storage.locationUri.get))
        }
        assert(deltaLog2.snapshot.schema == new StructType().add("a", "long").add("b", "string"))
        assert(deltaLog2.snapshot.metadata.partitionSchema == new StructType().add("b", "string"))

        assert(table.schema == deltaLog2.snapshot.schema)
        assert(table.partitionColumnNames == Seq("b"))

        // External catalog does not contain the schema and partition column names.
        val externalTable = catalog.externalCatalog.getTable("default", "delta_test")
        assert(externalTable.schema == new StructType())
        assert(externalTable.partitionColumnNames.isEmpty)
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

        val partDir = new File(dir, "a=1")
        assert(partDir.exists())
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
          val deltaLog = disableSparkService {
            DeltaLog.forTable(spark, new Path(table.storage.locationUri.get))
          }
          assert(deltaLog.snapshot.schema == new StructType()
            .add("a", "integer").add("b", "integer")
            .add("c", "integer").add("d", "integer"))
          assert(deltaLog.snapshot.metadata.partitionSchema == new StructType())

          assert(table.schema == deltaLog.snapshot.schema)
          assert(table.partitionColumnNames.isEmpty)

          // External catalog does not contain the schema and partition column names.
          val externalTable = catalog.externalCatalog.getTable("default", "t")
          assert(externalTable.schema == new StructType())
          assert(externalTable.partitionColumnNames.isEmpty)

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
          val deltaLog = disableSparkService {
            DeltaLog.forTable(spark, new Path(table.storage.locationUri.get))
          }
          assert(deltaLog.snapshot.schema == new StructType()
            .add("a", "integer").add("b", "integer")
            .add("c", "integer").add("d", "integer"))
          assert(deltaLog.snapshot.metadata.partitionSchema == new StructType()
            .add("a", "integer").add("b", "integer"))

          assert(table.schema == deltaLog.snapshot.schema)
          assert(table.partitionColumnNames == Seq("a", "b"))

          // External catalog does not contain the schema and partition column names.
          val externalTable = catalog.externalCatalog.getTable("default", "t1")
          assert(externalTable.schema == new StructType())
          assert(externalTable.partitionColumnNames.isEmpty)

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

      val deltaLog = disableSparkService {
        DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      }
      val snapshot = deltaLog.update()
      print(s"AAAAAAA ${deltaLog.logPath}\n")
      assert(snapshot.metadata.configuration == Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval == 20)
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

      val deltaLog = disableSparkService {
        DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      }
      val snapshot = deltaLog.update()
      assert(snapshot.metadata.configuration ==
        Map("delta.logRetentionDuration" -> "2 weeks", "delta.checkpointInterval" -> "20"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval == 20)
    }
  }

  test("CTAS external table with existing data should fail") {
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

  test("CTAS with unknown table properties should throw an error") {
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

  test("CTAS with invalid table properties should throw an error") {
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
          val partEscaped = s"${ExternalCatalogUtils.escapePathName(specialChars)}=2"
          val partFile = new File(dir, partEscaped)
          assert(partFile.listFiles().nonEmpty)
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
          val partFile = new File(loc, "b=2")
          assert(!partFile.listFiles().forall(_.toString.contains("_delta_log")))
          checkAnswer(spark.table("t1"), Row("1", "2") :: Nil)

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

  test("ALTER TABLE RENAME TO") {
    withTable("tbl", "newTbl") {
      sql(s"""
            |CREATE TABLE tbl
            |USING delta
            |AS SELECT 1 as a, 'a' as b
           """.stripMargin)

      withCloudProvider("AWS") {
        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE tbl RENAME TO newTbl")
        }
        assert(e.getMessage.contains("ALTER TABLE RENAME TO is not allowed"))
      }

      sql(s"ALTER TABLE tbl RENAME TO newTbl")
      checkDatasetUnorderly(
        sql("SELECT * FROM newTbl").as[(Long, String)],
        1L -> "a")
    }
  }

  test("CREATE TABLE with existing data path") {
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
        val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t2"))
        assert(tableMetadata.properties == Map("delta.randomizeFilePrefixes" -> "true"))

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
      val deltaLog = disableSparkService {
        DeltaLog.forTable(spark, path)
      }
      val oldVersion = deltaLog.snapshot.version
      sql(s"CREATE TABLE table USING delta LOCATION '$path'")
      assert(oldVersion == deltaLog.snapshot.version)
    }
  }

  test("SHOW CREATE TABLE should not include OPTIONS except for path") {
    withTable("delta_test") {
      sql(s"""
             |CREATE TABLE delta_test(a LONG, b String)
             |USING delta
           """.stripMargin)

      val statement = sql("SHOW CREATE TABLE delta_test").collect()(0).getString(0)
      assert(!statement.contains("OPTION"))
    }

    withTempDir { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath()
        sql(s"""
               |CREATE TABLE delta_test(a LONG, b String)
               |USING delta
               |LOCATION '$path'
             """.stripMargin)

        val statement = sql("SHOW CREATE TABLE delta_test").collect()(0).getString(0)
        assert(statement.contains(
          s"""
             |OPTIONS (
             |  path '${CatalogUtils.URIToString(makeQualifiedPath(path))}'
             |)""".stripMargin))
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

        def checkDescribe(describe: String): Unit = {
          assert(sql(describe).collect().takeRight(2).map(_.getString(0)) === Seq("name", "dept"))
        }

        checkDescribe("DESCRIBE TABLE delta_test")
        checkDescribe(s"DESCRIBE TABLE delta.`$path`")
      }
    }
  }

  test("drop managed Delta table should invalid DeltaLog cache") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test USING delta AS SELECT 'foo' as a")
      val tableLocation = sql("DESC DETAIL delta_test").select("location").as[String].head()
      val deltaLog = DeltaLog.forTable(spark, tableLocation)
      sql("DROP TABLE delta_test")
      assert(deltaLog ne disableSparkService(DeltaLog.forTable(spark, tableLocation)))
    }
  }

  test("rename managed Delta table should invalid DeltaLog cache") {
    withTable("delta_test", "delta_test2") {
      sql("CREATE TABLE delta_test USING delta AS SELECT 'foo' as a")
      val tableLocation = sql("DESC DETAIL delta_test").select("location").as[String].head()
      val deltaLog = DeltaLog.forTable(spark, tableLocation)
      sql("ALTER TABLE delta_test RENAME TO delta_test2")
      assert(deltaLog ne disableSparkService(DeltaLog.forTable(spark, tableLocation)))
    }
  }
}
