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
import org.apache.spark.sql.delta.actions.Protocol
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.hadoop.fs.Path
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogManager, CatalogV2Util, TableCatalog}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

trait DeltaDDLUsingPathTests extends QueryTest
    with SharedSparkSession with DeltaColumnMappingTestUtils {

  import testImplicits._

  protected def catalogName: String = {
    CatalogManager.SESSION_CATALOG_NAME
  }

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
    // scalastyle:off deltahadoopconfiguration
    val fs = hadoopPath.getFileSystem(spark.sessionState.newHadoopConf())
    // scalastyle:on deltahadoopconfiguration
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

  private def errorContains(errMsg: String, str: String): Unit = {
    assert(errMsg.contains(str))
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
    assert(ex.getMessage.matches(
      ".*Path does not exist: (file:)?/path/to/delta.?.*"),
      "Found: " + ex.getMessage)

    withSQLConf(SQLConf.RUN_SQL_ON_FILES.key -> "false") {
      val ex = intercept[AnalysisException] {
        spark.table(s"delta.`/path/to/delta`")
      }
      assert(ex.getMessage.contains("Table or view not found: delta.`/path/to/delta`") ||
        ex.getMessage.contains("table or view `delta`.`/path/to/delta` cannot be found"))
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

    val metadata = loadDeltaLog(path).snapshot.metadata

    Seq(table, s"delta.`$path`").foreach { tableOrPath =>
      checkDatasetUnorderly(
        dropColumnMappingConfigurations(
          sql(s"SHOW TBLPROPERTIES $tableOrPath('delta.logRetentionDuration')")
            .as[(String, String)]),
        "delta.logRetentionDuration" -> "2 weeks")
      checkDatasetUnorderly(
        dropColumnMappingConfigurations(
          sql(s"SHOW TBLPROPERTIES $tableOrPath('key')").as[(String, String)]),
        "key" -> "value")
    }

    val protocol = Protocol.forNewTable(spark, Some(metadata))
    val supportedFeatures = protocol
      .readerAndWriterFeatureNames
      .map(name => s"delta.feature.$name" -> "supported")
    val expectedProperties = Seq(
      "delta.logRetentionDuration" -> "2 weeks",
      "delta.minReaderVersion" -> protocol.minReaderVersion.toString,
      "delta.minWriterVersion" -> protocol.minWriterVersion.toString,
      "key" -> "value") ++ supportedFeatures

    checkDatasetUnorderly(
      dropColumnMappingConfigurations(
        sql(s"SHOW TBLPROPERTIES $table").as[(String, String)]),
      expectedProperties: _*)

    checkDatasetUnorderly(
      dropColumnMappingConfigurations(
        sql(s"SHOW TBLPROPERTIES delta.`$path`").as[(String, String)]),
      expectedProperties: _*)

    if (table == "`delta_test`") {
      val tableName = s"$catalogName.default.delta_test"
      checkDatasetUnorderly(
        dropColumnMappingConfigurations(
          sql(s"SHOW TBLPROPERTIES $table('dEltA.lOgrEteNtiOndURaTion')").as[(String, String)]),
        "dEltA.lOgrEteNtiOndURaTion" ->
          s"Table $tableName does not have property: dEltA.lOgrEteNtiOndURaTion")
      checkDatasetUnorderly(
        dropColumnMappingConfigurations(
          sql(s"SHOW TBLPROPERTIES $table('kEy')").as[(String, String)]),
        "kEy" -> s"Table $tableName does not have property: kEy")
    } else {
      checkDatasetUnorderly(
        dropColumnMappingConfigurations(
          sql(s"SHOW TBLPROPERTIES $table('kEy')").as[(String, String)]),
        "kEy" -> s"Table $catalogName.delta.delta_test does not have property: kEy")
    }
    checkDatasetUnorderly(
      dropColumnMappingConfigurations(
        sql(s"SHOW TBLPROPERTIES delta.`$path`('dEltA.lOgrEteNtiOndURaTion')")
          .as[(String, String)]),
      "dEltA.lOgrEteNtiOndURaTion" ->
        s"Table $catalogName.delta.`$path` does not have property: dEltA.lOgrEteNtiOndURaTion")
    checkDatasetUnorderly(
      dropColumnMappingConfigurations(
        sql(s"SHOW TBLPROPERTIES delta.`$path`('kEy')").as[(String, String)]),
      "kEy" ->
        s"Table $catalogName.delta.`$path` does not have property: kEy")

    val e = intercept[AnalysisException] {
      sql(s"SHOW TBLPROPERTIES delta.`/path/to/delta`").as[(String, String)]
    }
    assert(e.getMessage.contains(s"not a Delta table"))
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
    val e = intercept[AnalysisException] {
      sql("SHOW COLUMNS IN delta.`/path/to/delta`")
    }
    assert(e.getMessage.contains(s"not a Delta table"))
  }

  testUsingPath("DESCRIBE COLUMN") { (table, path) =>
    Seq(table, s"delta.`$path`").foreach { tableOrPath =>
      checkDatasetUnorderly(
        sql(s"DESCRIBE $tableOrPath v1").as[(String, String)],
        "col_name" -> "v1",
        "data_type" -> "int",
        "comment" -> "NULL")
      checkDatasetUnorderly(
        sql(s"DESCRIBE $tableOrPath struct").as[(String, String)],
        "col_name" -> "struct",
        "data_type" -> "struct<x:int,y:string>",
        "comment" -> "NULL")
      checkDatasetUnorderly(
        sql(s"DESCRIBE EXTENDED $tableOrPath v1").as[(String, String)],
        "col_name" -> "v1",
        "data_type" -> "int",
        "comment" -> "NULL"
      )
      val ex1 = intercept[AnalysisException] {
        sql(s"DESCRIBE $tableOrPath unknown")
      }
      assert(ex1.getErrorClass() === "UNRESOLVED_COLUMN.WITH_SUGGESTION")
      val ex2 = intercept[AnalysisException] {
        sql(s"DESCRIBE $tableOrPath struct.x")
      }
      assert(ex2.getMessage.contains("DESC TABLE COLUMN does not support nested column: struct.x"))
    }
    val ex = intercept[AnalysisException] {
      sql("DESCRIBE delta.`/path/to/delta` v1")
    }
    assert(ex.getMessage.contains("not a Delta table"), s"Original message: ${ex.getMessage()}")
  }
}

class DeltaDDLUsingPathSuite extends DeltaDDLUsingPathTests with DeltaSQLCommandTest {
}


class DeltaDDLUsingPathNameColumnMappingSuite extends DeltaDDLUsingPathSuite
  with DeltaColumnMappingEnableNameMode {

  override protected def runOnlyTests = Seq(
    "create table with NOT NULL - check violation through file writing",
    "ALTER TABLE CHANGE COLUMN with nullability change in struct type - relaxed"
  )
}

