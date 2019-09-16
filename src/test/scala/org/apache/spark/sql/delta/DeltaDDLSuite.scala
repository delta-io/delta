/*
 * Copyright 2019 Databricks, Inc.
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

import io.delta.DeltaExtensions

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

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.extensions", classOf[DeltaExtensions].getName)
  }

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
        assert(spark.catalog.listColumns("delta_test").isEmpty)
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
          sql(s"CREATE TABLE delta_test USING delta LOCATION '${dir.getCanonicalPath}'")
        }.getMessage
        assert(e.contains("but there is no transaction log"))
      }
    }
  }

  test("create external table without schema and input files") {
    withTempDir { dir =>
      withTable("delta_test") {
        val e = intercept[AnalysisException] {
          sql(s"CREATE TABLE delta_test USING delta LOCATION '${dir.getCanonicalPath}'")
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

        // Catalog does not contain the schema and partition column names.
        assert(table.schema == new StructType())
        assert(table.partitionColumnNames.isEmpty)

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

      // External catalog does not contain the schema and partition column names.
      assert(table.schema == new StructType())
      assert(table.partitionColumnNames.isEmpty)

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
      assert(spark.catalog.listColumns("delta_test").isEmpty)
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

      // External catalog does not contain the schema and partition column names.
      assert(table.schema == new StructType())
      assert(table.partitionColumnNames == Seq("a"))

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

        // External catalog does not contain the schema.
        assert(table.schema == new StructType())
        assert(table.partitionColumnNames == Seq("b"))
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

          // External catalog does not contain the schema.
          assert(table.schema == new StructType())
          assert(table.partitionColumnNames.isEmpty)

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

          // External catalog does not contain the schema.
          assert(table.schema == new StructType())
          assert(table.partitionColumnNames == Seq("a", "b"))

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
        // Table properties should not be changed to empty, but are not stored in the catalog.
        assert(
          spark.sessionState.catalog.getTableMetadata(TableIdentifier("t2")).properties.isEmpty)
        val metadata = DeltaLog.forTable(spark, TableIdentifier("t2")).snapshot.metadata
        assert(metadata.configuration == Map("delta.randomizeFilePrefixes" -> "true"))

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
          s"""LOCATION '${CatalogUtils.URIToString(makeQualifiedPath(path))}'""".stripMargin))
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
          // TODO: The order of these columns matches their definition in the DF and not the order
          // specified in partitionBy, whereas DBR matches the partition by. Is that okay?
          assert(sql(describe).collect().takeRight(2).map(_.getString(0)) === Seq("dept", "name"))
        }

        checkDescribe("DESCRIBE TABLE delta_test")
        // checkDescribe(s"DESCRIBE TABLE delta.`$path`")
      }
    }
  }
}
