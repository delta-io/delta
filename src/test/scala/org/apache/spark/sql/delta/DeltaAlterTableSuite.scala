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
