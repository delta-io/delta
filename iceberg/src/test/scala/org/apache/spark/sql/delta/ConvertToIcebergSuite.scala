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

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.util.Utils
import org.apache.spark.SparkContext

class ConvertToIcebergSuite extends QueryTest with Eventually {

  private var _sparkSession: SparkSession = null
  private var _sparkSessionWithDelta: SparkSession = null
  private var _sparkSessionWithIceberg: SparkSession = null

  private var warehousePath: File = null
  private var testTablePath: String = null
  private val testTableName: String = "deltaTable"

  override def spark: SparkSession = _sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    warehousePath = Utils.createTempDir()
    _sparkSessionWithDelta = createSparkSessionWithDelta()
    _sparkSessionWithIceberg = createSparkSessionWithIceberg()
    require(!_sparkSessionWithDelta.eq(_sparkSessionWithIceberg), "separate sessions expected")
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    testTablePath = Utils.createTempDir().getAbsolutePath
  }

  override def afterEach(): Unit = {
    super.afterEach()
    Utils.deleteRecursively(new File(testTablePath))
    _sparkSessionWithDelta.sql(s"DROP TABLE IF EXISTS $testTableName")
    _sparkSessionWithIceberg.sql(s"DROP TABLE IF EXISTS $testTableName")
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (warehousePath != null) Utils.deleteRecursively(warehousePath)
    SparkContext.getActive.foreach(_.stop())
  }

  test("basic test - path based table created with SQL") {
    runDeltaSql(s"""CREATE TABLE delta.`$testTablePath` (col1 INT) USING DELTA
                   |TBLPROPERTIES (
                   |  'delta.columnMapping.mode' = 'id',
                   |  'delta.universalFormat.enabledFormats' = 'iceberg'
                   |)""".stripMargin)
    verifyReadWithIceberg(testTablePath, Seq())
    runDeltaSql(s"INSERT INTO delta.`$testTablePath` VALUES (123)")
    verifyReadWithIceberg(testTablePath, Seq(Row(123)))
  }

  test("basic test - catalog table created with SQL") {
    runDeltaSql(s"""CREATE TABLE $testTableName(col1 INT) USING DELTA
                   |LOCATION '$testTablePath'
                   |TBLPROPERTIES (
                   |  'delta.columnMapping.mode' = 'id',
                   |  'delta.universalFormat.enabledFormats' = 'iceberg'
                   |)""".stripMargin)
    verifyReadWithIceberg(testTablePath, Seq())
    runDeltaSql(s"INSERT INTO $testTableName VALUES (123)")
    verifyReadWithIceberg(testTablePath, Seq(Row(123)))
  }

  test("basic test - path based table created with DataFrame") {
    withDeltaSparkSession { deltaSpark =>
      withDefaultTablePropsInSQLConf {
        deltaSpark.range(10).write.format("delta").save(testTablePath)
      }
    }
    verifyReadWithIceberg(testTablePath, 0 to 9 map (Row(_)))
    withDeltaSparkSession { deltaSpark =>
      deltaSpark.range(10, 20, 1)
        .write.format("delta").mode("append").save(testTablePath)
    }
    verifyReadWithIceberg(testTablePath, 0 to 19 map (Row(_)))
  }

  test("basic test - catalog table created with DataFrame") {
    withDeltaSparkSession { deltaSpark =>
      withDefaultTablePropsInSQLConf {
        deltaSpark.range(10).write.format("delta")
          .option("path", testTablePath)
          .saveAsTable(testTableName)
      }
    }
    verifyReadWithIceberg(testTablePath, 0 to 9 map (Row(_)))
    withDeltaSparkSession { deltaSpark =>
      deltaSpark.range(10, 20, 1)
        .write.format("delta").mode("append")
        .option("path", testTablePath)
        .saveAsTable(testTableName)
    }
    verifyReadWithIceberg(testTablePath, 0 to 19 map (Row(_)))
  }

  def runDeltaSql(sqlStr: String): Unit = {
    withDeltaSparkSession { deltaSpark =>
      deltaSpark.sql(sqlStr)
    }
  }

  def verifyReadWithIceberg(tablePath: String, expectedAnswer: Seq[Row]): Unit = {
    withIcebergSparkSession { icebergSparkSession =>
      eventually(timeout(10.seconds)) {
        val icebergDf = icebergSparkSession.read.format("iceberg").load(tablePath)
        checkAnswer(icebergDf, expectedAnswer)
      }
    }
  }

  def tablePropsForCreate: String = {
    s"""  '${DeltaConfigs.COLUMN_MAPPING_MODE.key}' = 'id',
       |  '${DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.key}' = 'iceberg'""".stripMargin
  }

  def withDefaultTablePropsInSQLConf(f: => Unit): Unit = {
    withSQLConf(
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "id",
      DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.defaultTablePropertyKey -> "iceberg"
    ) { f }
  }

  def withDeltaSparkSession(f: SparkSession => Unit): Unit = {
    withSparkSession(_sparkSessionWithDelta, f)
  }

  def withIcebergSparkSession(f: SparkSession => Unit): Unit = {
    withSparkSession(_sparkSessionWithIceberg, f)
  }

  def withSparkSession(sessionToUse: SparkSession, f: SparkSession => Unit): Unit = {
    try {
      SparkSession.setDefaultSession(sessionToUse)
      SparkSession.setActiveSession(sessionToUse)
      _sparkSession = sessionToUse
      f(sessionToUse)
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      _sparkSession = null
    }
  }

  protected def createSparkSessionWithDelta(): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DeltaSession")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession
  }

  protected def createSparkSessionWithIceberg(): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("IcebergSession")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type", "hive")
      .getOrCreate()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession
  }

}
