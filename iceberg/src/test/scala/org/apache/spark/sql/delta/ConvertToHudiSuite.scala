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

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{QueryTest, Row, SparkSession}
import org.apache.spark.util.Utils
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import java.io.{File, IOException}
import java.net.ServerSocket

class ConvertToHudiSuite extends QueryTest with Eventually {

  private var _sparkSession: SparkSession = null
  private val TMP_DIR = Utils.createTempDir().getCanonicalPath
  private val testTableName: String = "deltatable"
  private val testTablePath: String = s"$TMP_DIR/$testTableName"

  override def spark: SparkSession = _sparkSession

  override def beforeAll(): Unit = {
    super.beforeAll()
    _sparkSession = createSparkSession()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    _sparkSession.sql(s"DROP TABLE IF EXISTS $testTableName")
    Utils.deleteRecursively(new File(testTablePath))
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SparkContext.getActive.foreach(_.stop())
  }

  test("basic test - managed table created with SQL") {
    _sparkSession.sql(
      s"""CREATE TABLE `${testTableName}` (col1 INT) USING DELTA
         |TBLPROPERTIES (
         |  'delta.columnMapping.mode' = 'name',
         |  'delta.universalFormat.enabledFormats' = 'hudi'
         |)""".stripMargin)
    _sparkSession.sql(s"INSERT INTO `$testTableName` VALUES (123)")
    verifyReadWithHudi(testTableName, Seq(Row(123)))
  }

  test("basic test - catalog table created with DataFrame") {
    withDefaultTablePropsInSQLConf {
      _sparkSession.range(10).write.format("delta")
        .option("path", testTablePath)
        .saveAsTable(testTableName)
    }
    _sparkSession.range(10, 20, 1)
      .write.format("delta").mode("append")
      .option("path", testTablePath)
      .saveAsTable(testTableName)
    verifyReadWithHudi(testTableName, 0 to 19 map (Row(_)))

  }

  def verifyReadWithHudi(tableName: String, expectedAnswer: Seq[Row]): Unit = {
    eventually(timeout(10.seconds)) {
      val hudiDf = _sparkSession.read.format("hudi")
        .option("hoodie.metadata.enable", "true").load(testTablePath)
      checkAnswer(hudiDf, expectedAnswer)
    }
  }


  def withDefaultTablePropsInSQLConf(f: => Unit): Unit = {
    withSQLConf(
      DeltaConfigs.COLUMN_MAPPING_MODE.defaultTablePropertyKey -> "name",
      DeltaConfigs.UNIVERSAL_FORMAT_ENABLED_FORMATS.defaultTablePropertyKey -> "hudi"
    ) { f }
  }

  def createSparkSession(): SparkSession = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("UniformSession")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession
  }
}
