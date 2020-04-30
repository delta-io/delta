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

import com.databricks.service.IgnoreIfSparkService
import org.apache.hadoop.fs.Path
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

trait DeltaDDLUsingPathTests extends QueryTest
  with SharedSparkSession {

  import testImplicits._

  protected def testUsingPath(command: String, tags: Tag*)(f: (String, String) => Unit): Unit = {
    test(s"$command - using path", tags: _*) {
      withTempDir { tempDir =>
        withTable("delta_test") {
          val path = tempDir.getCanonicalPath
          Seq((1, "a"), (2, "b")).toDF("v1", "v2")
            .withColumn("struct",
              struct((col("v1") * 10).as("x"), concat(col("v2"), col("v2")).as("y")))
            .write
            .format("delta")
            .partitionBy("v1")
            .option("path", path)
            .saveAsTable("delta_test")
          f("`delta_test`", path)
        }
      }
    }
    test(s"$command - using path in 'delta' database", tags: _*) {
      withTempDir { tempDir =>
        val path = tempDir.getCanonicalPath

        withDatabase("delta") {
          sql("CREATE DATABASE delta")

          withTable("delta.delta_test") {
            Seq((1, "a"), (2, "b")).toDF("v1", "v2")
              .withColumn("struct",
                struct((col("v1") * 10).as("x"), concat(col("v2"), col("v2")).as("y")))
              .write
              .format("delta")
              .partitionBy("v1")
              .option("path", path)
              .saveAsTable("delta.delta_test")
            f("`delta`.`delta_test`", path)
          }
        }
      }
    }
  }

  protected def toQualifiedPath(path: String): String = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    fs.makeQualified(hadoopPath).toString
  }

  protected def checkDescribe(describe: String, keyvalues: (String, String)*): Unit = {
    val result = sql(describe).collect()
    keyvalues.foreach { case (key, value) =>
      val row = result.find(_.getString(0) == key)
      assert(row.isDefined)
      if (key == "Location") {
        assert(toQualifiedPath(row.get.getString(1)) === toQualifiedPath(value))
      } else {
        assert(row.get.getString(1) === value)
      }
    }
  }

  testUsingPath("SELECT") { (table, path) =>
    Seq(table, s"delta.`$path`").foreach { tableOrPath =>
      checkDatasetUnorderly(
        sql(s"SELECT * FROM $tableOrPath").as[(Int, String, (Int, String))],
        (1, "a", (10, "aa")), (2, "b", (20, "bb")))
      checkDatasetUnorderly(
        spark.table(tableOrPath).as[(Int, String, (Int, String))],
        (1, "a", (10, "aa")), (2, "b", (20, "bb")))
    }

    val ex = intercept[AnalysisException] {
      spark.table(s"delta.`/path/to/delta`")
    }
    assert(ex.getMessage.contains("/path/to/delta doesn't exist"))

    withSQLConf(SQLConf.RUN_SQL_ON_FILES.key -> "false") {
      val ex = intercept[AnalysisException] {
        spark.table(s"delta.`/path/to/delta`")
      }
      assert(ex.getMessage.contains("Table or view not found: delta./path/to/delta"))
    }
  }

  testUsingPath("DESCRIBE TABLE") { (table, path) =>
    val qualifiedPath = toQualifiedPath(path)

    Seq(table, s"delta.`$path`").foreach { tableOrPath =>
      checkDescribe(s"DESCRIBE $tableOrPath",
        "v1" -> "int",
        "v2" -> "string",
        "struct" -> "struct<x:int,y:string>")

      checkDescribe(s"DESCRIBE EXTENDED $tableOrPath",
        "v1" -> "int",
        "v2" -> "string",
        "struct" -> "struct<x:int,y:string>",
        "Provider" -> "delta",
        "Location" -> qualifiedPath)
    }
  }

  testUsingPath("SHOW TBLPROPERTIES") { (table, path) =>
    sql(s"ALTER TABLE $table SET TBLPROPERTIES " +
      "('delta.logRetentionDuration' = '2 weeks', 'key' = 'value')")

    Seq(table, s"delta.`$path`").foreach { tableOrPath =>
      checkDatasetUnorderly(
        sql(s"SHOW TBLPROPERTIES $tableOrPath").as[(String, String)],
        "delta.logRetentionDuration" -> "2 weeks",
        "key" -> "value")
      checkDatasetUnorderly(
        sql(s"SHOW TBLPROPERTIES $tableOrPath('delta.logRetentionDuration')").as[String],
        "2 weeks")
      checkDatasetUnorderly(
        sql(s"SHOW TBLPROPERTIES $tableOrPath('dEltA.lOgrEteNtiOndURaTion')").as[String],
        "2 weeks")
      checkDatasetUnorderly(
        sql(s"SHOW TBLPROPERTIES $tableOrPath('key')").as[String],
        "value")
    }

    if (table == "`delta_test`") {
      checkDatasetUnorderly(
        sql(s"SHOW TBLPROPERTIES $table('kEy')").as[String],
        s"Table default.delta_test does not have property: kEy")
    } else {
      checkDatasetUnorderly(
        sql(s"SHOW TBLPROPERTIES $table('kEy')").as[String],
        s"Table delta.delta_test does not have property: kEy")
    }
    checkDatasetUnorderly(
      sql(s"SHOW TBLPROPERTIES delta.`$path`('kEy')").as[String],
      s"Table delta.$path does not have property: kEy")

    checkDatasetUnorderly(
      sql("SHOW TBLPROPERTIES delta.`/path/to/delta`").as[(String, String)])
  }

  testUsingPath("SHOW COLUMNS") { (table, path) =>
    Seq(table, s"delta.`$path`").foreach { tableOrPath =>
      checkDatasetUnorderly(
        sql(s"SHOW COLUMNS IN $tableOrPath").as[String],
        "v1", "v2", "struct")
    }

    if (table == "`delta_test`") {
      checkDatasetUnorderly(
        sql(s"SHOW COLUMNS IN $table").as[String],
        "v1", "v2", "struct")
    } else {
      checkDatasetUnorderly(
        sql(s"SHOW COLUMNS IN $table IN delta").as[String],
        "v1", "v2", "struct")
    }

    checkDatasetUnorderly(
      sql(s"SHOW COLUMNS IN `$path` IN delta").as[String],
      "v1", "v2", "struct")
    checkDatasetUnorderly(
      sql(s"SHOW COLUMNS IN delta.`$path` IN delta").as[String],
      "v1", "v2", "struct")

    checkDatasetUnorderly(
      sql("SHOW COLUMNS IN delta.`/path/to/delta`").as[String])
  }

  testUsingPath("SHOW PARTITIONS",
      IgnoreIfSparkService("show partitions return string type")) { (table, path) =>
    Seq(table, s"delta.`$path`").foreach { tableOrPath =>
      checkDatasetUnorderly(
        sql(s"SHOW PARTITIONS $tableOrPath").as[Int],
        1, 2)
      checkDatasetUnorderly(
        sql(s"SHOW PARTITIONS $tableOrPath PARTITION(v1=1)").as[Int],
        1)
    }

    val ex = intercept[AnalysisException] {
      sql("SHOW PARTITIONS delta.`/path/to/delta`")
    }
    assert(ex.getMessage.contains("`/path/to/delta` is not a Delta table."))
  }

  testUsingPath("delta table created time should be set") { (table, path) =>
    val deltaLog = DeltaLog.forTable(spark, path)
    val expectedCreatedTime = deltaLog.snapshot.metadata.createdTime.get
    assert(expectedCreatedTime ===
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(path, Some("delta"))).createTime)
    assert(expectedCreatedTime ===
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(path, Some("delta"))).createTime)
    val tableIdentifier = spark.sessionState.sqlParser.parseTableIdentifier(table)
    assert(expectedCreatedTime ===
      spark.sessionState.catalog.getTableMetadata(tableIdentifier).createTime)
  }
}

class DeltaDDLUsingPathSuite extends DeltaDDLUsingPathTests with DataSourceV2Test {
}

