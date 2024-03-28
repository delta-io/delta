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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.him.HIMTestSupport
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}



class ConvertToIcebergSuite extends QueryTest with Eventually with HIMTestSupport {

  private val testTableName: String = "deltatable"

  test("enforceSupportInCatalog") {
    var testTable = new CatalogTable(
      TableIdentifier("table"),
      CatalogTableType.EXTERNAL,
      CatalogStorageFormat(None, None, None, None, compressed = false, Map.empty),
      new StructType(Array(StructField("col1", IntegerType), StructField("col2", StringType))))
    var testMetadata = Metadata()

    assert(UniversalFormat.enforceSupportInCatalog(testTable, testMetadata).isEmpty)

    testTable = testTable.copy(properties = Map("table_type" -> "iceberg"))
    var resultTable = UniversalFormat.enforceSupportInCatalog(testTable, testMetadata)
    assert(resultTable.nonEmpty)
    assert(!resultTable.get.properties.contains("table_type"))

    testMetadata = testMetadata.copy(
      configuration = Map("delta.universalFormat.enabledFormats" -> "iceberg"))
    assert(UniversalFormat.enforceSupportInCatalog(testTable, testMetadata).isEmpty)

    testTable = testTable.copy(properties = Map.empty)
    resultTable = UniversalFormat.enforceSupportInCatalog(testTable, testMetadata)
    assert(resultTable.nonEmpty)
    assert(resultTable.get.properties("table_type") == "iceberg")
  }

  test("basic test - managed table created with SQL") {
    withMetaStore { conf =>
      withDeltaSparkSession(conf) { deltaSpark =>
        deltaSpark.sql(
          s"""CREATE TABLE `${testTableName}` (col1 INT) USING DELTA
             |TBLPROPERTIES (
             |  'delta.columnMapping.mode' = 'name',
             |  'delta.universalFormat.enabledFormats' = 'iceberg'
             |)""".stripMargin)
        deltaSpark.sql(s"INSERT INTO `$testTableName` VALUES (123)")
      }
      verifyReadWithIceberg(conf, testTableName, Seq(Row(123)))
    }
  }

  test("basic test - catalog table created with DataFrame") {
    withMetaStore { conf =>
      val whfolder = conf.get(METASTOREWAREHOUSE.varname)
      val tablePath = s"$whfolder/$testTableName"
      withDeltaSparkSession(conf) { deltaSpark =>
        withDefaultTablePropsInSQLConf {
          deltaSpark.range(10).write.format("delta")
            .option("path", tablePath)
            .saveAsTable(testTableName)
        }
        deltaSpark.range(10, 20, 1)
          .write.format("delta").mode("append")
          .option("path", tablePath)
          .saveAsTable(testTableName)
      }
      verifyReadWithIceberg(conf, testTableName, 0 to 19 map (Row(_)))
    }
  }

  def verifyReadWithIceberg(
      conf: Configuration, tableName: String, expectedAnswer: Seq[Row]): Unit = {
    withIcebergSparkSession(conf) { icebergSpark =>
      eventually(timeout(10.seconds)) {
        icebergSpark.sql(s"REFRESH TABLE ${tableName}")
        val icebergDf = icebergSpark.read.format("iceberg").load(tableName)
        checkAnswer(icebergDf, expectedAnswer)
      }
    }
  }


  def withDefaultTablePropsInSQLConf(f: => Unit): Unit = {
    withSQLConf(
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.defaultTablePropertyKey -> "iceberg"
    ) { f }
  }

  def withDeltaSparkSession[T](conf: Configuration) (f: SparkSession => T): T = {
    val deltaSession = createSparkSessionWithDelta(conf)
    try {
      withSparkSession(deltaSession, f)
    } finally {
      deltaSession.stop()
      deltaSession.close()
    }
  }

  def withIcebergSparkSession[T](conf: Configuration) (f: SparkSession => T): T = {
    val icebergSession = createSparkSessionWithIceberg(conf)
    try {
      withSparkSession(icebergSession, f)
    } finally {
      icebergSession.stop()
      icebergSession.close()
    }
  }

  def withSparkSession[T](sessionToUse: SparkSession, f: SparkSession => T): T = {
    try {
      SparkSession.setDefaultSession(sessionToUse)
      SparkSession.setActiveSession(sessionToUse)
      f(sessionToUse)
    } finally {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  protected def createSparkSessionWithDelta(conf: Configuration): SparkSession = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DeltaSession")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.warehouse.dir", conf.get(METASTOREWAREHOUSE.varname))
      .config("hive.metastore.uris", conf.get(METASTOREURIS.varname))
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()
    sparkSession
  }

  protected def createSparkSessionWithIceberg(conf: Configuration): SparkSession = {
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("IcebergSession")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.warehouse.dir", conf.get(METASTOREWAREHOUSE.varname))
      .config("hive.metastore.uris", conf.get(METASTOREURIS.varname))
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()
    sparkSession
  }

  override protected def spark: SparkSession = null
}
