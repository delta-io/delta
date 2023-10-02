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
import org.apache.spark.sql.{DataFrame, QueryTest, Row, SparkSession}
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
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    super.afterEach()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  def runDeltaSql(sqlStr: String): Unit = {
    withDeltaSparkSession { deltaSpark =>
      deltaSpark.sql(sqlStr)
    }
  }

  def runIcebergSql(sqlStr: String): DataFrame = {
    withIcebergSparkSession { icebergSpark =>
      icebergSpark.sql(sqlStr)
    }
  }

  def verifyReadWithIceberg(tableName: String, expectedAnswer: Seq[Row]): Unit = {
    withIcebergSparkSession { icebergSparkSession =>
      eventually(timeout(10.seconds)) {
        val icebergDf = icebergSparkSession.sql(s"SELECT * FROM ${tableName}")
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

  def withDeltaSparkSession[T](f: SparkSession => T): T = {
    withSparkSession(_sparkSessionWithDelta, f)
  }

  def withIcebergSparkSession[T](f: SparkSession => T): T = {
    withSparkSession(_sparkSessionWithIceberg, f)
  }

  def withSparkSession[T](sessionToUse: SparkSession, f: SparkSession => T): T = {
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
      .config("spark.sql.catalog.spark_catalog.type", "hive")
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
