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
import java.io.FileNotFoundException

// scalastyle:off import.ordering.noEmptyLine
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils.{TABLE_FEATURES_MIN_READER_VERSION, TABLE_FEATURES_MIN_WRITER_VERSION}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

trait DescribeDeltaDetailSuiteBase extends QueryTest
  with SharedSparkSession  with DeltaTestUtilsForTempViews {

  import testImplicits._

  val catalogAndSchema = {
    var res = "default."
    res
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

  def describeDeltaDetailTest(f: File => String): Unit = {
    val tempDir = Utils.createTempDir()
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format("delta")
      .partitionBy("column1")
      .save(tempDir.toString())

    // Check SQL details
    checkResult(
      sql(s"DESCRIBE DETAIL ${f(tempDir)}"),
      Seq("delta", Array("column1"), 1),
      Seq("format", "partitionColumns", "numFiles"))

    // Check Scala details
    val deltaTable = io.delta.tables.DeltaTable.forPath(spark, tempDir.toString)
    checkResult(
      deltaTable.detail(),
      Seq("delta", Array("column1"), 1),
      Seq("format", "partitionColumns", "numFiles"))
  }

  test("delta table: Scala details using table name") {
    withTable("delta_test") {
      Seq(1, 2, 3).toDF().write.format("delta").saveAsTable("delta_test")

      val deltaTable = io.delta.tables.DeltaTable.forName(spark, "delta_test")
      checkAnswer(
        deltaTable.detail().select("format"),
        Seq(Row("delta"))
      )
    }
  }

  test("delta table: path") {
    describeDeltaDetailTest(f => s"'${f.toString()}'")
  }

  test("delta table: delta table identifier") {
    describeDeltaDetailTest(f => s"delta.`${f.toString()}`")
  }

  test("non-delta table: SQL details using table name") {
    withTable("describe_detail") {
      sql(
        """
          |CREATE TABLE describe_detail(column1 INT, column2 INT)
          |USING parquet
          |PARTITIONED BY (column1)
          |COMMENT "this is a table comment"
        """.stripMargin)
      sql(
        """
          |INSERT INTO describe_detail VALUES(1, 1)
        """.stripMargin
      )
      checkResult(
        sql("DESCRIBE DETAIL describe_detail"),
        Seq("parquet", Array("column1")),
        Seq("format", "partitionColumns"))
    }
  }

  test("non-delta table: SQL details using table path") {
    val tempDir = Utils.createTempDir().toString
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format("parquet")
      .partitionBy("column1")
      .mode("overwrite")
      .save(tempDir)
    checkResult(
      sql(s"DESCRIBE DETAIL '$tempDir'"),
      Seq(tempDir),
      Seq("location"))
  }

  test("non-delta table: SQL details when table path doesn't exist") {
    val tempDir = Utils.createTempDir()
    tempDir.delete()
    val e = intercept[FileNotFoundException] {
      sql(s"DESCRIBE DETAIL '$tempDir'")
    }
    assert(e.getMessage.contains(tempDir.toString))
  }

  test("delta table: SQL details using table name") {
    withTable("describe_detail") {
      sql(
        """
          |CREATE TABLE describe_detail(column1 INT, column2 INT)
          |USING delta
          |PARTITIONED BY (column1)
          |COMMENT "describe a non delta table"
        """.stripMargin)
      sql(
        """
          |INSERT INTO describe_detail VALUES(1, 1)
        """.stripMargin
      )
      checkResult(
        sql("DESCRIBE DETAIL describe_detail"),
        Seq("delta", Array("column1"), 1),
        Seq("format", "partitionColumns", "numFiles"))
    }
  }

  test("delta table: create table on an existing delta log") {
    val tempDir = Utils.createTempDir().toString
    Seq(1 -> 1).toDF("column1", "column2")
      .write
      .format("delta")
      .partitionBy("column1")
      .mode("overwrite")
      .save(tempDir)
    val tblName1 = "tbl_name1"
    val tblName2 = "tbl_name2"
    withTable(tblName1, tblName2) {
      sql(s"CREATE TABLE $tblName1 USING DELTA LOCATION '$tempDir'")
      sql(s"CREATE TABLE $tblName2 USING DELTA LOCATION '$tempDir'")
      checkResult(
        sql(s"DESCRIBE DETAIL $tblName1"),
        Seq(s"$catalogAndSchema$tblName1"),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL $tblName2"),
        Seq(s"$catalogAndSchema$tblName2"),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL delta.`$tempDir`"),
        Seq(null),
        Seq("name"))
      checkResult(
        sql(s"DESCRIBE DETAIL '$tempDir'"),
        Seq(null),
        Seq("name"))
    }
  }

  testWithTempView(s"SC-37296: describe detail on temp view") { isSQLTempView =>
    withTable("t1") {
      Seq(1, 2, 3).toDF().write.format("delta").saveAsTable("t1")
      val viewName = "v"
      createTempViewFromTable("t1", isSQLTempView)
      val e = intercept[AnalysisException] {
        sql(s"DESCRIBE DETAIL $viewName")
      }
      assert(e.getMessage.contains(
        s"`$viewName` is a view. DESCRIBE DETAIL is only supported for tables."))
    }
  }

  test("SC-37296: describe detail on permanent view") {
    val view = "detailTestView"
    withView(view) {
      sql(s"CREATE VIEW $view AS SELECT 1")
      val e = intercept[AnalysisException] { sql(s"DESCRIBE DETAIL $view") }
      assert(e.getMessage.contains(
        "`detailTestView` is a view. DESCRIBE DETAIL is only supported for tables."))
    }
  }

  test("delta table: describe detail always run on the latest snapshot") {
    val tableName = "tbl_name_on_latest_snapshot"
    withTable(tableName) {
        val tempDir = Utils.createTempDir().toString
        sql(s"CREATE TABLE $tableName USING DELTA LOCATION '$tempDir'")

        val deltaLog = DeltaLog.forTable(spark, tempDir)
        DeltaLog.clearCache()

        // Cache a new DeltaLog
        sql(s"DESCRIBE DETAIL $tableName")

        val txn = deltaLog.startTransaction()
        val metadata = txn.snapshot.metadata
        val newMetadata = metadata.copy(configuration =
          metadata.configuration ++ Map("foo" -> "bar")
        )
        txn.commit(newMetadata :: Nil, DeltaOperations.ManualUpdate)
        checkResult(sql(s"DESCRIBE DETAIL $tableName"),
          Seq(Map("foo" -> "bar")),
          Seq("properties")
        )
      }
  }

  test("delta table: describe detail shows table features") {
    withTable("t1") {
      withSQLConf(
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_READER_VERSION.key -> "1",
        DeltaSQLConf.DELTA_PROTOCOL_DEFAULT_WRITER_VERSION.key -> "2"
      ) {
        Seq(1, 2, 3).toDF().write.format("delta").saveAsTable("t1")
      }
      val p = DeltaLog.forTable(spark, TableIdentifier("t1")).snapshot.protocol

      checkResult(
        sql(s"DESCRIBE DETAIL t1"),
        Seq(
          p.minReaderVersion,
          p.minWriterVersion,
          p.implicitlySupportedFeatures.map(_.name).toArray.sorted),
        Seq("minReaderVersion", "minWriterVersion", "tableFeatures"))

      val features = p.readerAndWriterFeatureNames ++ p.implicitlySupportedFeatures.map(_.name)
      sql(s"""ALTER TABLE t1 SET TBLPROPERTIES (
             |  delta.minReaderVersion = $TABLE_FEATURES_MIN_READER_VERSION,
             |  delta.minWriterVersion = $TABLE_FEATURES_MIN_WRITER_VERSION,
             |  delta.feature.${TestReaderWriterFeature.name} = 'enabled'
             |)""".stripMargin)

      checkResult(
        sql(s"DESCRIBE DETAIL t1"),
        Seq(
          TABLE_FEATURES_MIN_READER_VERSION,
          TABLE_FEATURES_MIN_WRITER_VERSION,
          (features + TestReaderWriterFeature.name).toArray.sorted),
        Seq("minReaderVersion", "minWriterVersion", "tableFeatures"))
    }
  }

  test("describe detail contains table name") {
    val tblName = "test_table"
    withTable(tblName) {
      spark.sql(s"CREATE TABLE $tblName(id INT) USING delta")
      val deltaTable = io.delta.tables.DeltaTable.forName(tblName)
      checkResult(
        deltaTable.detail(),
        Seq(s"$catalogAndSchema$tblName"),
        Seq("name")
      )
    }
  }

  // TODO: run it with OSS Delta after it's supported
}

class DescribeDeltaDetailSuite
  extends DescribeDeltaDetailSuiteBase with DeltaSQLCommandTest
