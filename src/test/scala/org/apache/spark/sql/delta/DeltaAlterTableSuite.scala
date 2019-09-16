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

import io.delta.DeltaExtensions
import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaConfigs.CHECKPOINT_INTERVAL
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, MapType, StructType}
import org.apache.spark.util.Utils

class DeltaAlterTableSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  override def sparkConf: SparkConf = {
    super.sparkConf
      .set("spark.sql.catalog.session", classOf[DeltaCatalog].getName)
      .set("spark.databricks.delta.snapshotPartitions", "1")
      .set("spark.sql.extensions", classOf[DeltaExtensions].getName)
  }

  private def assertNotSupported(command: String, messages: String*): Unit = {
    val ex = intercept[AnalysisException] {
      sql(command)
    }.getMessage
    assert(ex.contains("not supported") || ex.contains("Unsupported"))
    messages.foreach(msg => assert(ex.contains(msg)))
  }

  test("SET/UNSET TBLPROPERTIES - simple") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test (v1 int, v2 string) USING delta")

      sql("""
        |ALTER TABLE delta_test
        |SET TBLPROPERTIES (
        |  'delta.logRetentionDuration' = '2 weeks',
        |  'delta.checkpointInterval' = '20',
        |  'key' = 'value'
        |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      val snapshot1 = deltaLog.update()
      assert(snapshot1.metadata.configuration == Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval == 20)

      sql("ALTER TABLE delta_test UNSET TBLPROPERTIES ('delta.checkpointInterval', 'key')")

      val snapshot2 = deltaLog.update()
      assert(snapshot2.metadata.configuration == Map("delta.logRetentionDuration" -> "2 weeks"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval ==
        CHECKPOINT_INTERVAL.fromString(CHECKPOINT_INTERVAL.defaultValue))
    }
  }

  test("SET/UNSET TBLPROPERTIES - case insensitivity") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test (v1 int, v2 string) USING delta")

      sql("""
        |ALTER TABLE delta_test
        |SET TBLPROPERTIES (
        |  'dEltA.lOgrEteNtiOndURaTion' = '1 weeks',
        |  'DelTa.ChEckPoiNtinTervAl' = '5',
        |  'key' = 'value1'
        |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      val snapshot1 = deltaLog.update()
      assert(snapshot1.metadata.configuration == Map(
        "delta.logRetentionDuration" -> "1 weeks",
        "delta.checkpointInterval" -> "5",
        "key" -> "value1"))
      assert(deltaLog.deltaRetentionMillis == 1 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval == 5)

      sql("""
        |ALTER TABLE delta_test
        |SET TBLPROPERTIES (
        |  'dEltA.lOgrEteNtiOndURaTion' = '2 weeks',
        |  'DelTa.ChEckPoiNtinTervAl' = '20',
        |  'kEy' = 'value2'
        |)""".stripMargin)

      val snapshot2 = deltaLog.update()
      assert(snapshot2.metadata.configuration == Map(
        "delta.logRetentionDuration" -> "2 weeks",
        "delta.checkpointInterval" -> "20",
        "key" -> "value1",
        "kEy" -> "value2"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval == 20)

      sql("ALTER TABLE delta_test UNSET TBLPROPERTIES ('DelTa.ChEckPoiNtinTervAl', 'kEy')")

      val snapshot3 = deltaLog.update()
      assert(snapshot3.metadata.configuration ==
        Map("delta.logRetentionDuration" -> "2 weeks", "key" -> "value1"))
      assert(deltaLog.deltaRetentionMillis == 2 * 7 * 24 * 60 * 60 * 1000)
      assert(deltaLog.checkpointInterval ==
        CHECKPOINT_INTERVAL.fromString(CHECKPOINT_INTERVAL.defaultValue))
    }
  }

  test("SET/UNSET TBLPROPERTIES - set unknown config") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test (v1 int, v2 string) USING delta")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test SET TBLPROPERTIES ('delta.key' = 'value')")
      }
      assert(ex.getMessage.contains("Unknown configuration was specified: delta.key"))
    }
  }

  test("SET/UNSET TBLPROPERTIES - set invalid value") {
    withTable("delta_test") {
      sql("CREATE TABLE delta_test (v1 int, v2 string) USING delta")

      val ex1 = intercept[SparkException] {
        sql("ALTER TABLE delta_test SET TBLPROPERTIES ('delta.randomPrefixLength' = '-1')")
      }
      assert(ex1.getCause.getMessage.contains("randomPrefixLength needs to be greater than 0."))

      val ex2 = intercept[SparkException] {
        sql("ALTER TABLE delta_test SET TBLPROPERTIES ('delta.randomPrefixLength' = 'value')")
      }
      assert(ex2.getCause.getMessage.contains("randomPrefixLength needs to be greater than 0."))
    }
  }

  // TODO: this is not an error in dsv2
  /* test("SET/UNSET TBLPROPERTIES - unset non-existent config") {
    withTable("delta_test") {
      sql("""
        |CREATE TABLE delta_test (v1 int, v2 string)
        |USING delta
        |TBLPROPERTIES (
        |  'delta.randomizeFilePrefixes' = 'true',
        |  'delta.randomPrefixLength' = '5',
        |  'key' = 'value'
        |)""".stripMargin)

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test UNSET TBLPROPERTIES ('delta.randomizeFilePrefixes', 'kEy')")
      }
      assert(ex.getMessage.contains("Attempted to unset non-existent property 'kEy'"))

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      val snapshot1 = deltaLog.update()
      assert(snapshot1.metadata.configuration == Map(
        "delta.randomizeFilePrefixes" -> "true",
        "delta.randomPrefixLength" -> "5",
        "key" -> "value"))

      sql("ALTER TABLE delta_test UNSET TBLPROPERTIES IF EXISTS " +
        "('delta.randomizeFilePrefixes', 'kEy')")

      val snapshot2 = deltaLog.update()
      assert(snapshot2.metadata.configuration ==
        Map("delta.randomPrefixLength" -> "5", "key" -> "value"))
    }
  } */

  test("ADD COLUMNS - simple") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String)],
        (1, "a"), (2, "b"))

      sql("ALTER TABLE delta_test ADD COLUMNS (v3 long, v4 double)")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  // TODO: path with saveAsTable isn't working for some reason.
  /*
  test("ADD COLUMNS - external table") {
    withTempDir { dir =>
      withTable("delta_test") {
        val path = dir.getCanonicalPath
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .write
          .format("delta")
          .option("path", path)
          .saveAsTable("delta_test")

        checkDatasetUnorderly(
          spark.table("delta_test").as[(Int, String)],
          (1, "a"), (2, "b"))

        sql("ALTER TABLE delta_test ADD COLUMNS (v3 long, v4 double)")

        val deltaLog = DeltaLog.forTable(spark, path)
        assert(deltaLog.snapshot.schema == new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("v3", "long").add("v4", "double"))

        checkDatasetUnorderly(
          spark.table("delta_test").as[(Int, String, Option[Long], Option[Double])],
          (1, "a", None, None), (2, "b", None, None))
        checkDatasetUnorderly(
          spark.read.format("delta").load(path).as[(Int, String, Option[Long], Option[Double])],
          (1, "a", None, None), (2, "b", None, None))
      }
    }
  } */

  test("ADD COLUMNS - a partitioned table") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .partitionBy("v2")
        .saveAsTable("delta_test")

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String)],
        (1, "a"), (2, "b"))

      sql("ALTER TABLE delta_test ADD COLUMNS (v3 long, v4 double)")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long").add("v4", "double"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, Option[Long], Option[Double])],
        (1, "a", None, None), (2, "b", None, None))
    }
  }

  test("ADD COLUMNS - with a comment") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String)],
        (1, "a"), (2, "b"))

      sql("ALTER TABLE delta_test ADD COLUMNS (v3 long COMMENT 'new column')")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("v3", "long", true, "new column"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, Option[Long])],
        (1, "a", None), (2, "b", None))
    }
  }

  /* test("ADD COLUMNS - with positions") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String)],
        (1, "a"), (2, "b"))

      sql("ALTER TABLE delta_test ADD COLUMNS (v3 long FIRST, v4 long AFTER v1, v5 long)")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v3", "long").add("v1", "integer")
        .add("v4", "long").add("v2", "string").add("v5", "long"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Option[Long], Int, Option[Long], String, Option[Long])],
        (None, 1, None, "a", None), (None, 2, None, "b", None))
    }
  }

  test("ADD COLUMNS - with positions using an added column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String)],
        (1, "a"), (2, "b"))

      sql("ALTER TABLE delta_test ADD COLUMNS (v3 long FIRST, v4 long AFTER v3, v5 long AFTER v4)")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v3", "long").add("v4", "long").add("v5", "long")
        .add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Option[Long], Option[Long], Option[Long], Int, String)],
        (None, None, None, 1, "a"), (None, None, None, 2, "b"))
    }
  }

  test("ADD COLUMNS - nested columns") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))

      sql("ALTER TABLE delta_test ADD COLUMNS " +
        "(struct.v3 long FIRST, struct.v4 long AFTER v1, struct.v5 long)")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v3", "long").add("v1", "integer")
          .add("v4", "long").add("v2", "string").add("v5", "long")))

      checkDatasetUnorderly(
        spark.table("delta_test")
          .as[(Int, String, (Option[Long], Int, Option[Long], String, Option[Long]))],
        (1, "a", (None, 1, None, "a", None)), (2, "b", (None, 2, None, "b", None)))
    }
  } */

  test("ADD COLUMNS - special column names") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("z.z", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))

      // TODO: This was originally `x.x` AFTER `z.z`.
      sql("ALTER TABLE delta_test ADD COLUMNS (`x.x` long, `z.z`.`y.y` double)")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("z.z", new StructType()
          .add("v1", "integer").add("v2", "string").add("y.y", "double"))
        .add("x.x", "long"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (Int, String, Option[Double]), Option[Long])],
        (1, "a", (1, "a", None), None), (2, "b", (2, "b", None), None))
    }
  }

  /* test("ADD COLUMNS - adding after an unknown column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test ADD COLUMNS (v3 long AFTER unknown)")
      }
      assert(ex.getMessage.contains("Couldn't find column unknown in"))
    }
  } */

  test("ADD COLUMNS - adding to a non-struct column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test ADD COLUMNS (v2.x long)")
      }
      assert(ex.getMessage.contains("Can only add nested columns to StructType."))
    }
  }

  test("ADD COLUMNS - a duplicate name") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test ADD COLUMNS (v2 long)")
      }
      assert(ex.getMessage.contains("Found duplicate column(s) in adding columns"))
    }
  }

  test("ADD COLUMNS - a duplicate name (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test ADD COLUMNS (struct.v2 long)")
      }
      assert(ex.getMessage.contains("Found duplicate column(s) in adding columns"))
    }
  }

  test("ADD COLUMNS - an invalid column name") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test ADD COLUMNS (`a column name with spaces` long)")
      }
      assert(ex.getMessage.contains("contains invalid character(s)"))
    }
  }

  test("ADD COLUMNS - an invalid column name (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test ADD COLUMNS (struct.`a column name with spaces` long)")
      }
      assert(ex.getMessage.contains("contains invalid character(s)"))
    }
  }

  // TODO: I think these two tests can't be expressed without column positions.
  // Is there some other aspect of ADD COLUMNS case sensitivity we can test?
  /* test("ADD COLUMNS - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTable("delta_test") {
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .write
          .format("delta")
          .saveAsTable("delta_test")

        sql("ALTER TABLE delta_test ADD COLUMNS (v3 long AFTER V1)")

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
        assert(deltaLog.snapshot.schema == new StructType()
          .add("v1", "integer").add("v3", "long").add("v2", "string"))
      }
    }
  }

  test("ADD COLUMNS - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable("delta_test") {
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .write
          .format("delta")
          .saveAsTable("delta_test")

        val ex = intercept[AnalysisException] {
          sql("ALTER TABLE delta_test ADD COLUMNS (v3 long AFTER V1)")
        }
        assert(ex.getMessage.contains("Couldn't find column V1"))
      }
    }
  } */

  test("CHANGE COLUMN - add a comment") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN v1 COMMENT 'a comment'")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer", true, "a comment").add("v2", "string"))
    }
  }

  test("CHANGE COLUMN - add a comment (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN struct.v1 COMMENT 'a comment'")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.update().schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v1", "integer", true, "a comment").add("v2", "string")))

      // TODO: this syntax doesn't seem to work in OSS, and I can't find an equivalent.
      /*
      sql("ALTER TABLE delta_test CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string COMMENT 'a comment for v2'>")

      assert(deltaLog.update().schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v1", "integer", true, "a comment")
          .add("v2", "string", true, "a comment for v2"))) */
    }
  }

  test("CHANGE COLUMN - add a comment to a partitioned table") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .partitionBy("v2")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN v2 COMMENT 'a comment'")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string", true, "a comment"))
    }
  }

  /* test("CHANGE COLUMN - move to first") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN v2 v2 string FIRST")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v2", "string").add("v1", "integer"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - move to first (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN struct.v2 v2 string FIRST")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.update().schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v2", "string").add("v1", "integer")))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))

      sql("ALTER TABLE delta_test CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string> FIRST")

      assert(deltaLog.update().schema == new StructType()
        .add("struct", new StructType().add("v1", "integer").add("v2", "string"))
        .add("v1", "integer").add("v2", "string"))
    }
  }

  test("CHANGE COLUMN - move a partitioned column to first") {
    withTable("delta_test") {
      Seq((1, "a", true), (2, "b", false)).toDF("v1", "v2", "v3")
        .write
        .format("delta")
        .partitionBy("v2", "v3")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN v3 v3 boolean FIRST")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v3", "boolean").add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Boolean, Int, String)],
        (true, 1, "a"), (false, 2, "b"))
    }
  }

  test("CHANGE COLUMN - move to after some column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN v1 v1 integer AFTER v2")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v2", "string").add("v1", "integer"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - move to after some column (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN struct.v1 v1 integer AFTER v2")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.update().schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v2", "string").add("v1", "integer")))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))

      sql("ALTER TABLE delta_test CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v2:string> AFTER v1")

      assert(deltaLog.update().schema == new StructType()
        .add("v1", "integer")
        .add("struct", new StructType().add("v1", "integer").add("v2", "string"))
        .add("v2", "string"))
    }
  }

  test("CHANGE COLUMN - move to after the same column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN v1 v1 integer AFTER v1")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String)],
        (1, "a"), (2, "b"))
    }
  }

  test("CHANGE COLUMN - move to after the same column (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN struct.v1 v1 integer AFTER v1")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v2", "string")
        .add("struct", new StructType()
          .add("v1", "integer").add("v2", "string")))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (Int, String))],
        (1, "a", (1, "a")), (2, "b", (2, "b")))
    }
  }

  test("CHANGE COLUMN - move a partitioned column to after some column") {
    withTable("delta_test") {
      Seq((1, "a", true), (2, "b", false)).toDF("v1", "v2", "v3")
        .write
        .format("delta")
        .partitionBy("v2", "v3")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN v3 v3 boolean AFTER v1")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer").add("v3", "boolean").add("v2", "string"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, Boolean, String)],
        (1, true, "a"), (2, false, "b"))
    }
  }

  test("CHANGE COLUMN - special column names") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN `x.x` `x.x` integer AFTER `y.y`")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("y.y", "string").add("x.x", "integer"))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(String, Int)],
        ("a", 1), ("b", 2))
    }
  }

  test("CHANGE COLUMN - special column names (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
        .withColumn("z.z", struct("`x.x`", "`y.y`"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN `z.z`.`x.x` `x.x` integer AFTER `y.y`")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("x.x", "integer").add("y.y", "string")
        .add("z.z", new StructType()
          .add("y.y", "string").add("x.x", "integer")))

      checkDatasetUnorderly(
        spark.table("delta_test").as[(Int, String, (String, Int))],
        (1, "a", ("a", 1)), (2, "b", ("b", 2)))
    }
  }

  test("CHANGE COLUMN - move unknown column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test CHANGE COLUMN unknown unknown string FIRST")
      }
      assert(ex.getMessage.contains("Couldn't find column unknown in"))
    }
  }

  test("CHANGE COLUMN - move unknown column (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test CHANGE COLUMN struct.unknown unknown string FIRST")
      }
      assert(ex.getMessage.contains("Couldn't find column struct.unknown in"))
    }
  }

  test("CHANGE COLUMN - move to after an unknown column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test CHANGE COLUMN v1 v1 integer AFTER unknown")
      }
      assert(ex.getMessage.contains("Couldn't find column unknown in"))
    }
  }

  test("CHANGE COLUMN - move to after an unknown column (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      val ex = intercept[AnalysisException] {
        sql("ALTER TABLE delta_test CHANGE COLUMN struct.v1 v1 integer AFTER unknown")
      }
      assert(ex.getMessage.contains("Couldn't find column struct.unknown in"))
    }
  }

  test("CHANGE COLUMN - change name") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN v2 v3 string",
        "'v2' with type 'StringType (nullable = true)'",
        "'v3' with type 'StringType (nullable = true)'")
    }
  }

  test("CHANGE COLUMN - change name (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN struct.v2 v3 string")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN struct struct " +
        "STRUCT<v1:integer, v3:string>")
    }
  } */

  test("CHANGE COLUMN - incompatible") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .write
        .format("delta")
        .saveAsTable("delta_test")

      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN v1 TYPE long",
        "'v1' with type 'IntegerType (nullable = true)'",
        "'v1' with type 'LongType (nullable = true)'")
    }
  }

  test("CHANGE COLUMN - incompatible (nested)") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("struct", struct("v1", "v2"))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN struct.v1 TYPE long",
        "'struct.v1' with type 'IntegerType (nullable = true)'",
        "'v1' with type 'LongType (nullable = true)'")
    }
  }

  // TODO: OSS Spark CheckAnalysis makes this fail because it is always case sensitive regardless.
  /*
  test("CHANGE COLUMN - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTable("delta_test") {
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .withColumn("s", struct("v1", "v2"))
          .write
          .format("delta")
          .saveAsTable("delta_test")

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))

        sql("ALTER TABLE delta_test CHANGE COLUMN V1 TYPE integer")

        assert(deltaLog.update().schema == new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        sql("ALTER TABLE delta_test CHANGE COLUMN v1 TYPE integer")

        assert(deltaLog.update().schema == new StructType()
          .add("v1", "integer").add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        // This part of the test can't be expressed without column position support
        sql("ALTER TABLE delta_test CHANGE COLUMN v1 v1 integer AFTER V2")

        assert(deltaLog.update().schema == new StructType()
          .add("v2", "string").add("v1", "integer")
          .add("s", new StructType().add("v1", "integer").add("v2", "string")))

        sql("ALTER TABLE delta_test CHANGE COLUMN s s struct<V1:integer,v2:string> AFTER v2")

        assert(deltaLog.update().schema == new StructType()
          .add("v2", "string")
          .add("s", new StructType().add("v1", "integer").add("v2", "string"))
          .add("v1", "integer"))
      }
    }
  }

  test("CHANGE COLUMN - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable("delta_test") {
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .withColumn("s", struct("v1", "v2"))
          .write
          .format("delta")
          .saveAsTable("delta_test")

        val ex1 = intercept[AnalysisException] {
          sql("ALTER TABLE delta_test CHANGE COLUMN V1 TYPE integer")
        }
        assert(ex1.getMessage.contains("Couldn't find column V1"))

        val ex2 = intercept[AnalysisException] {
          sql("ALTER TABLE delta_test CHANGE COLUMN v1 TYPE integer")
        }
        assert(ex2.getMessage.contains("not supported for changing column 'v1'"))


        // This part of the test can't be expressed without column position support
        /* val ex3 = intercept[AnalysisException] {
          sql("ALTER TABLE delta_test CHANGE COLUMN v1 v1 integer AFTER V2")
        }
        assert(ex3.getMessage.contains("Couldn't find column V2"))

        val ex4 = intercept[AnalysisException] {
          sql("ALTER TABLE delta_test CHANGE COLUMN s s struct<V1:integer,v2:string> AFTER v2")
        }
        assert(ex4.getMessage.contains("not supported for changing column 's'")) */
      }
    }
  } */

  // TODO: OSS Spark does not support changing entire complex types.
  /* test("CHANGE COLUMN - complex types") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("ALTER TABLE delta_test CHANGE COLUMN s TYPE STRUCT<v1:int, v2:string, sv3:long>")
      sql("ALTER TABLE delta_test CHANGE COLUMN a TYPE ARRAY<STRUCT<v1:int, v2:string, av3:long>>")
      sql("ALTER TABLE delta_test CHANGE COLUMN m TYPE " +
        "MAP<STRUCT<v1:int, v2:string, mkv3:long>, STRUCT<v1:int, v2:string, mvv3:long>>")

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer")
        .add("v2", "string")
        .add("s", new StructType().add("v1", "integer").add("v2", "string").add("sv3", "long"))
        .add("a", ArrayType(
          new StructType().add("v1", "integer").add("v2", "string").add("av3", "long")))
        .add("m", MapType(
          new StructType().add("v1", "integer").add("v2", "string").add("mkv3", "long"),
          new StructType().add("v1", "integer").add("v2", "string").add("mvv3", "long"))))

      implicit val ordering = Ordering.by[
        (Int, String,
          (Int, String, Option[Long]),
          Seq[(Int, String, Option[Long])],
          Map[(Int, String, Option[Long]), (Int, String, Option[Long])]), Int] {
        case (v1, _, _, _, _) => v1
      }
      checkDatasetUnorderly(
        spark.table("delta_test").as[
          (Int, String,
            (Int, String, Option[Long]),
            Seq[(Int, String, Option[Long])],
            Map[(Int, String, Option[Long]), (Int, String, Option[Long])])],
        (1, "a", (1, "a", None),
          Seq((1, "a", None)), Map((1, "a", Option.empty[Long]) -> ((1, "a", None)))),
        (2, "b", (2, "b", None),
          Seq((2, "b", None)), Map((2, "b", Option.empty[Long]) -> ((2, "b", None)))))

      // not supported to tighten nullabilities.
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN s TYPE STRUCT<v1:int, v2:string, sv3:long NOT NULL>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN a TYPE " +
        "ARRAY<STRUCT<v1:int, v2:string, av3:long NOT NULL>>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN m TYPE " +
        "MAP<STRUCT<v1:int, v2:string, mkv3:long NOT NULL>, STRUCT<v1:int, v2:string, mvv3:long>>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN m TYPE " +
        "MAP<STRUCT<v1:int, v2:string, mkv3:long>, STRUCT<v1:int, v2:string, mvv3:long NOT NULL>>")

      // not supported to remove columns.
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN s TYPE STRUCT<v1:int, v2:string>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN a TYPE ARRAY<STRUCT<v1:int, v2:string>>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN m TYPE " +
        "MAP<STRUCT<v1:int, v2:string, mkv3:long>, STRUCT<v1:int, v2:string>>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN m TYPE " +
        "MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string, mvv3:long>>")

      // not supported to add not-null columns.
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN s TYPE " +
        "STRUCT<v1:int, v2:string, sv3:long, sv4:long NOT NULL>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN a TYPE " +
        "ARRAY<STRUCT<v1:int, v2:string, av3:long, av4:long NOT NULL>>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN m TYPE " +
        "MAP<STRUCT<v1:int, v2:string, mkv3:long, mkv4:long NOT NULL>, " +
        "STRUCT<v1:int, v2:string, mvv3:long>>")
      assertNotSupported(
        "ALTER TABLE delta_test CHANGE COLUMN m TYPE " +
        "MAP<STRUCT<v1:int, v2:string, mkv3:long>, " +
        "STRUCT<v1:int, v2:string, mvv3:long, mvv4:long NOT NULL>>")
    }
  } */

  // TODO: REPLACE not supported
  /*
  test("REPLACE COLUMNS - add a comment") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int COMMENT 'a comment for v1',
        |  v2 string COMMENT 'a comment for v2',
        |  s STRUCT<
        |    v1:int COMMENT 'a comment for s.v1',
        |    v2:string COMMENT 'a comment for s.v2'> COMMENT 'a comment for s',
        |  a ARRAY<STRUCT<
        |    v1:int COMMENT 'a comment for a.v1',
        |    v2:string COMMENT 'a comment for a.v2'>> COMMENT 'a comment for a',
        |  m MAP<STRUCT<
        |      v1:int COMMENT 'a comment for m.key.v1',
        |      v2:string COMMENT 'a comment for m.key.v2'>,
        |    STRUCT<
        |      v1:int COMMENT 'a comment for m.value.v1',
        |      v2:string COMMENT 'a comment for m.value.v2'>> COMMENT 'a comment for m'
        |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      val expectedSchema = new StructType()
        .add("v1", "integer", true, "a comment for v1")
        .add("v2", "string", true, "a comment for v2")
        .add("s", new StructType()
          .add("v1", "integer", true, "a comment for s.v1")
          .add("v2", "string", true, "a comment for s.v2"), true, "a comment for s")
        .add("a", ArrayType(new StructType()
          .add("v1", "integer", true, "a comment for a.v1")
          .add("v2", "string", true, "a comment for a.v2")), true, "a comment for a")
        .add("m", MapType(
          new StructType()
            .add("v1", "integer", true, "a comment for m.key.v1")
            .add("v2", "string", true, "a comment for m.key.v2"),
          new StructType()
            .add("v1", "integer", true, "a comment for m.value.v1")
            .add("v2", "string", true, "a comment for m.value.v2")), true, "a comment for m")
      assert(deltaLog.snapshot.schema == expectedSchema)

      implicit val ordering = Ordering.by[
        (Int, String, (Int, String), Seq[(Int, String)], Map[(Int, String), (Int, String)]), Int] {
        case (v1, _, _, _, _) => v1
      }
      checkDatasetUnorderly(
        spark.table("delta_test")
          .as[(Int, String, (Int, String), Seq[(Int, String)], Map[(Int, String), (Int, String)])],
        (1, "a", (1, "a"), Seq((1, "a")), Map((1, "a") -> ((1, "a")))),
        (2, "b", (2, "b"), Seq((2, "b")), Map((2, "b") -> ((2, "b")))))

      // REPLACE COLUMNS doesn't remove metadata.
      sql("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin)
      assert(deltaLog.snapshot.schema == expectedSchema)
    }
  }

  test("REPLACE COLUMNS - reorder") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  m MAP<STRUCT<v2:string, v1:int>, STRUCT<v2:string, v1:int>>,
        |  v2 string,
        |  a ARRAY<STRUCT<v2:string, v1:int>>,
        |  v1 int,
        |  s STRUCT<v2:string, v1:int>
        |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("m", MapType(
          new StructType().add("v2", "string").add("v1", "integer"),
          new StructType().add("v2", "string").add("v1", "integer")))
        .add("v2", "string")
        .add("a", ArrayType(new StructType().add("v2", "string").add("v1", "integer")))
        .add("v1", "integer")
        .add("s", new StructType().add("v2", "string").add("v1", "integer")))

      implicit val ordering = Ordering.by[
        (Map[(String, Int), (String, Int)], String, Seq[(String, Int)], Int, (String, Int)), Int] {
        case (_, _, _, v1, _) => v1
      }
      checkDatasetUnorderly(
        spark.table("delta_test")
          .as[(Map[(String, Int), (String, Int)], String, Seq[(String, Int)], Int, (String, Int))],
        (Map(("a", 1) -> (("a", 1))), "a", Seq(("a", 1)), 1, ("a", 1)),
        (Map(("b", 2) -> (("b", 2))), "b", Seq(("b", 2)), 2, ("b", 2)))
    }
  }

  test("REPLACE COLUMNS - add columns") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  v3 long,
        |  s STRUCT<v1:int, v2:string, v3:long>,
        |  a ARRAY<STRUCT<v1:int, v2:string, v3:long>>,
        |  m MAP<STRUCT<v1:int, v2:string, v3:long>, STRUCT<v1:int, v2:string, v3:long>>
        |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer")
        .add("v2", "string")
        .add("v3", "long")
        .add("s", new StructType()
          .add("v1", "integer").add("v2", "string").add("v3", "long"))
        .add("a", ArrayType(new StructType()
          .add("v1", "integer").add("v2", "string").add("v3", "long")))
        .add("m", MapType(
          new StructType().add("v1", "integer").add("v2", "string").add("v3", "long"),
          new StructType().add("v1", "integer").add("v2", "string").add("v3", "long"))))

      implicit val ordering = Ordering.by[
        (Int, String, Option[Long],
          (Int, String, Option[Long]),
          Seq[(Int, String, Option[Long])],
          Map[(Int, String, Option[Long]), (Int, String, Option[Long])]), Int] {
        case (v1, _, _, _, _, _) => v1
      }
      checkDatasetUnorderly(
        spark.table("delta_test").as[
          (Int, String, Option[Long],
            (Int, String, Option[Long]),
            Seq[(Int, String, Option[Long])],
            Map[(Int, String, Option[Long]), (Int, String, Option[Long])])],
        (1, "a", None, (1, "a", None),
          Seq((1, "a", None)), Map((1, "a", Option.empty[Long]) -> ((1, "a", None)))),
        (2, "b", None, (2, "b", None),
          Seq((2, "b", None)), Map((2, "b", Option.empty[Long]) -> ((2, "b", None)))))
    }
  }

  test("REPLACE COLUMNS - loosen nullability") {
    withTable("delta_test") {
      sql("""
        |CREATE TABLE delta_test (
        |  v1 int NOT NULL,
        |  v2 string,
        |  s STRUCT<v1:int NOT NULL, v2:string>,
        |  a ARRAY<STRUCT<v1:int NOT NULL, v2:string>>,
        |  m MAP<STRUCT<v1:int NOT NULL, v2:string>, STRUCT<v1:int NOT NULL, v2:string>>
        |)
        |USING delta""".stripMargin)

      sql("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("v1", "integer")
        .add("v2", "string")
        .add("s", new StructType()
          .add("v1", "integer").add("v2", "string"))
        .add("a", ArrayType(new StructType()
          .add("v1", "integer").add("v2", "string")))
        .add("m", MapType(
          new StructType().add("v1", "integer").add("v2", "string"),
          new StructType().add("v1", "integer").add("v2", "string"))))
    }
  }

  test("REPLACE COLUMNS - special column names") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("x.x", "y.y")
        .withColumn("s.s", struct("`x.x`", "`y.y`"))
        .withColumn("a.a", array("`s.s`"))
        .withColumn("m.m", map(col("`s.s`"), col("`s.s`")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      sql("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  `m.m` MAP<STRUCT<`y.y`:string, `x.x`:int>, STRUCT<`y.y`:string, `x.x`:int>>,
        |  `y.y` string,
        |  `a.a` ARRAY<STRUCT<`y.y`:string, `x.x`:int>>,
        |  `x.x` int,
        |  `s.s` STRUCT<`y.y`:string, `x.x`:int>
        |)""".stripMargin)

      val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
      assert(deltaLog.snapshot.schema == new StructType()
        .add("m.m", MapType(
          new StructType().add("y.y", "string").add("x.x", "integer"),
          new StructType().add("y.y", "string").add("x.x", "integer")))
        .add("y.y", "string")
        .add("a.a", ArrayType(new StructType().add("y.y", "string").add("x.x", "integer")))
        .add("x.x", "integer")
        .add("s.s", new StructType().add("y.y", "string").add("x.x", "integer")))
    }
  }

  test("REPLACE COLUMNS - add not-null column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      // trying to add not-null column, but it should fail because adding not-null column is
      // not supported.
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  v3 long NOT NULL,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "adding non-nullable column", "v3")
      // s.v3
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string, v3:long NOT NULL>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "adding non-nullable column", "s.v3")
      // a.v3
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string, v3:long NOT NULL>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "adding non-nullable column", "a.v3")
      // m.key.v3
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string, v3:long NOT NULL>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "adding non-nullable column", "m.key.v3")
      // m.value.v3
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string, v3:long NOT NULL>>
        |)""".stripMargin,
        "adding non-nullable column", "m.value.v3")
    }
  }

  test("REPLACE COLUMNS - drop column") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      // trying to drop v1 of each struct, but it should fail because dropping column is
      // not supported.
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "dropping column(s)", "v1")
      // s.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "dropping column(s)", "v1", "from s")
      // a.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "dropping column(s)", "v1", "from a")
      // m.key.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "dropping column(s)", "v1", "from m.key")
      // m.value.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v2:string>>
        |)""".stripMargin,
        "dropping column(s)", "v1", "from m.value")
    }
  }

  test("REPLACE COLUMNS - incompatible data type") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      // trying to change the data type of v1 of each struct to long, but it should fail because
      // changing data type is not supported.
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 long,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "changing data type", "v1", "from IntegerType to LongType")
      // s.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:long, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "changing data type", "s.v1", "from IntegerType to LongType")
      // a.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:long, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "changing data type", "a.v1", "from IntegerType to LongType")
      // m.key.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:long, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "changing data type", "m.key.v1", "from IntegerType to LongType")
      // m.value.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:long, v2:string>>
        |)""".stripMargin,
        "changing data type", "m.value.v1", "from IntegerType to LongType")
    }
  }

  test("REPLACE COLUMNS - incompatible nullability") {
    withTable("delta_test") {
      Seq((1, "a"), (2, "b")).toDF("v1", "v2")
        .withColumn("s", struct("v1", "v2"))
        .withColumn("a", array("s"))
        .withColumn("m", map(col("s"), col("s")))
        .write
        .format("delta")
        .saveAsTable("delta_test")

      // trying to change the data type of v1 of each struct to not null, but it should fail because
      // tightening nullability is not supported.
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int NOT NULL,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "tightening nullability", "v1")
      // s.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int NOT NULL, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "tightening nullability", "s.v1")
      // a.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int NOT NULL, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "tightening nullability", "a.v1")
      // m.key.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int NOT NULL, v2:string>, STRUCT<v1:int, v2:string>>
        |)""".stripMargin,
        "tightening nullability", "m.key.v1")
      // m.value.v1
      assertNotSupported("""
        |ALTER TABLE delta_test REPLACE COLUMNS (
        |  v1 int,
        |  v2 string,
        |  s STRUCT<v1:int, v2:string>,
        |  a ARRAY<STRUCT<v1:int, v2:string>>,
        |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int NOT NULL, v2:string>>
        |)""".stripMargin,
        "tightening nullability", "m.value.v1")
    }
  }

  test("REPLACE COLUMNS - case insensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTable("delta_test") {
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .withColumn("s", struct("v1", "v2"))
          .withColumn("a", array("s"))
          .withColumn("m", map(col("s"), col("s")))
          .write
          .format("delta")
          .saveAsTable("delta_test")

        val deltaLog = DeltaLog.forTable(spark, TableIdentifier("delta_test"))
        def checkSchema(command: String): Unit = {
          sql(command)

          assert(deltaLog.update().schema == new StructType()
            .add("v1", "integer")
            .add("v2", "string")
            .add("s", new StructType().add("v1", "integer").add("v2", "string"))
            .add("a", ArrayType(new StructType().add("v1", "integer").add("v2", "string")))
            .add("m", MapType(
              new StructType().add("v1", "integer").add("v2", "string"),
              new StructType().add("v1", "integer").add("v2", "string"))))
        }

        // trying to use V1 instead of v1 of each struct.
        checkSchema("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  V1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // s.V1
        checkSchema("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<V1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // a.V1
        checkSchema("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<V1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // m.key.V1
        checkSchema("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<V1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // m.value.V1
        checkSchema("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<V1:int, v2:string>>
          |)""".stripMargin)
      }
    }
  }

  test("REPLACE COLUMNS - case sensitive") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable("delta_test") {
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .withColumn("s", struct("v1", "v2"))
          .withColumn("a", array("s"))
          .withColumn("m", map(col("s"), col("s")))
          .write
          .format("delta")
          .saveAsTable("delta_test")

        // trying to use V1 instead of v1 of each struct, but it should fail because case sensitive.
        assertNotSupported("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  V1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin,
          "dropping column(s)", "v1")
        // s.V1
        assertNotSupported("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<V1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin,
          "dropping column(s)", "v1", "from s")
        // a.V1
        assertNotSupported("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          | v1 int,
          | v2 string,
          | s STRUCT<v1:int, v2:string>,
          | a ARRAY<STRUCT<V1:int, v2:string>>,
          | m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin,
          "dropping column(s)", "v1", "from a")
        // m.key.V1
        assertNotSupported("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<V1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin,
          "dropping column(s)", "v1", "from m.key")
        // m.value.V1
        assertNotSupported("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<V1:int, v2:string>>
          |)""".stripMargin,
          "dropping column(s)", "v1", "from m.value")
      }
    }
  }

  test("REPLACE COLUMNS - duplicate") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable("delta_test") {
        Seq((1, "a"), (2, "b")).toDF("v1", "v2")
          .withColumn("s", struct("v1", "v2"))
          .withColumn("a", array("s"))
          .withColumn("m", map(col("s"), col("s")))
          .write
          .format("delta")
          .saveAsTable("delta_test")

        def assertDuplicate(command: String): Unit = {
          val ex = intercept[AnalysisException] {
            sql(command)
          }
          assert(ex.getMessage.contains("duplicate column(s)"))
        }

        // trying to add a V1 column, but it should fail because Delta doesn't allow columns
        // at the same level of nesting that differ only by case.
        assertDuplicate("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  V1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // s.V1
        assertDuplicate("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, V1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // a.V1
        assertDuplicate("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, V1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // m.key.V1
        assertDuplicate("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |   v1 int,
          |   v2 string,
          |   s STRUCT<v1:int, v2:string>,
          |   a ARRAY<STRUCT<v1:int, v2:string>>,
          |   m MAP<STRUCT<v1:int, V1:int, v2:string>, STRUCT<v1:int, v2:string>>
          |)""".stripMargin)
        // m.value.V1
        assertDuplicate("""
          |ALTER TABLE delta_test REPLACE COLUMNS (
          |  v1 int,
          |  v2 string,
          |  s STRUCT<v1:int, v2:string>,
          |  a ARRAY<STRUCT<v1:int, v2:string>>,
          |  m MAP<STRUCT<v1:int, v2:string>, STRUCT<v1:int, V1:int, v2:string>>
          |)""".stripMargin)
      }
    }
  } */

  test("SET LOCATION") {
    withTable("delta_table") {
      spark.range(1).write.format("delta").saveAsTable("delta_table")
      val catalog = spark.sessionState.catalog
      val table = catalog.getTableMetadata(TableIdentifier(tableName = "delta_table"))
      val oldLocation = table.location.toString
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        spark.range(1, 2).write.format("delta").save(path)
        checkAnswer(spark.table("delta_table"), Seq(Row(0)))
        sql(s"alter table delta_table set location '$path'")
        checkAnswer(spark.table("delta_table"), Seq(Row(1)))
      }
      Utils.deleteRecursively(new File(oldLocation.stripPrefix("file:")))
    }
  }

  test("SET LOCATION - negative cases") {
    withTable("delta_table") {
      spark.range(1).write.format("delta").saveAsTable("delta_table")
      withTempDir { dir =>
        val path = dir.getCanonicalPath
        val catalog = spark.sessionState.catalog
        val table = catalog.getTableMetadata(TableIdentifier(tableName = "delta_table"))
        val oldLocation = table.location.toString

        // new location is not a delta table
        var e = intercept[AnalysisException] {
          sql(s"alter table delta_table set location '$path'")
        }
        assert(e.getMessage.contains("not a Delta table"))

        Seq("1").toDF("id").write.format("delta").save(path)

        // set location on specific partitions
        // TODO: We had to change the error a bit here. There's no Delta specific hook to check -
        // Spark on its own rejects this operation because we don't store partition data.
        e = intercept[AnalysisException] {
          sql(s"alter table delta_table partition (id = 1) set location '$path'")
        }
        assert(e.getMessage.contains("its partition metadata is not stored in the Hive metastore"))

        // schema mismatch
        e = intercept[AnalysisException] {
          sql(s"alter table delta_table set location '$path'")
        }
        assert(e.getMessage.contains("different than the current table schema"))

        // delta source
        e = intercept[AnalysisException] {
          sql(s"alter table delta.`$oldLocation` set location '$path'")
        }
        assert(e.getMessage.contains("Database 'delta' not found"))

        withSQLConf(DeltaSQLConf.DELTA_ALTER_LOCATION_BYPASS_SCHEMA_CHECK.key -> "true") {
          checkAnswer(spark.table("delta_table"), Seq(Row(0)))
          // now we can bypass the schema mismatch check
          sql(s"alter table delta_table set location '$path'")
          checkAnswer(spark.table("delta_table"), Seq(Row("1")))
        }
        Utils.deleteRecursively(new File(oldLocation.stripPrefix("file:")))
      }
    }
  }
}
