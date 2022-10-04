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

package io.delta.sql

import java.nio.file.Files

import org.apache.spark.sql.delta.{DeltaAnalysisException, DeltaLog}
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import io.delta.tables.DeltaTable
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

class DeltaExtensionAndCatalogSuite extends SparkFunSuite {

  private def createTempDir(): String = {
    val dir = Files.createTempDirectory("DeltaSparkSessionExtensionSuite").toFile
    FileUtils.forceDeleteOnExit(dir)
    dir.getCanonicalPath
  }

  private def verifyDeltaSQLParserIsActivated(spark: SparkSession): Unit = {
    val input = Files.createTempDirectory("DeltaSparkSessionExtensionSuite").toFile
    try {
      spark.range(1, 10).write.format("delta").save(input.getCanonicalPath)
      spark.sql(s"vacuum delta.`${input.getCanonicalPath}`")
    } finally {
      JavaUtils.deleteRecursively(input)
    }
  }

  test("activate Delta SQL parser using SQL conf") {
    val spark = SparkSession.builder()
      .appName("DeltaSparkSessionExtensionSuiteUsingSQLConf")
      .master("local[2]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    try {
      verifyDeltaSQLParserIsActivated(spark)
    } finally {
      spark.close()
    }
  }

  test("activate Delta SQL parser using withExtensions") {
    val spark = SparkSession.builder()
      .appName("DeltaSparkSessionExtensionSuiteUsingWithExtensions")
      .master("local[2]")
      .withExtensions(new io.delta.sql.DeltaSparkSessionExtension)
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    try {
      verifyDeltaSQLParserIsActivated(spark)
    } finally {
      spark.close()
    }
  }

  test("DeltaCatalog class should be initialized correctly") {
    withSparkSession(
      SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key ->
        classOf[org.apache.spark.sql.delta.catalog.DeltaCatalog].getName
    ) { spark =>
      val v2Catalog = spark.sessionState.analyzer.catalogManager.catalog("spark_catalog")
      assert(v2Catalog.isInstanceOf[org.apache.spark.sql.delta.catalog.DeltaCatalog])
    }
  }

  test("DeltaLog should not throw exception if spark.sql.catalog.spark_catalog is set") {
    withTempDir { dir =>
      withSparkSession(
        SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key ->
          classOf[org.apache.spark.sql.delta.catalog.DeltaCatalog].getName
      ) { spark =>
        val path = new Path(dir.getCanonicalPath)
        assert(DeltaLog.forTable(spark, path).tableExists == false)
      }
    }
  }

  test("DeltaLog should throw exception if spark.sql.catalog.spark_catalog " +
    "config is not found") {
    withTempDir { dir =>
      withSparkSession("" -> "") { spark =>
        val path = new Path(dir.getCanonicalPath)
        val e = intercept[java.util.concurrent.ExecutionException] {
          DeltaLog.forTable(spark, path)
        }
        assert(e.getCause.isInstanceOf[DeltaAnalysisException])
        assert(e.getCause.asInstanceOf[DeltaAnalysisException].getErrorClass() ==
          "DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG")
      }
    }
  }

  test("DeltaLog should not throw exception if spark.sql.catalog.spark_catalog " +
    "config is not found and the check is disabled") {
    withTempDir { dir =>
      withSparkSession(DeltaSQLConf.DELTA_REQUIRED_SPARK_CONFS_CHECK.key -> "false") { spark =>
        val path = new Path(dir.getCanonicalPath)
          DeltaLog.forTable(spark, path)
        assert(DeltaLog.forTable(spark, path).tableExists == false)
      }
    }
  }

  private def withSparkSession(configs: (String, String)*)(f: SparkSession => Unit): Unit = {
    var builder = SparkSession.builder()
      .appName("DeltaSparkSessionExtensionSuite")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", createTempDir())

    configs.foreach { c => builder = builder.config(c._1, c._2) }
    val spark = builder.getOrCreate()
    try {
      f(spark)
    } finally {
      spark.close()
    }
  }

  private def checkErrorMessage(f: => Unit): Unit = {
    val e = intercept[AnalysisException](f)
    val expectedStrs = Seq(
      "Delta operation requires the SparkSession to be configured",
      "spark.sql.extensions",
      s"${classOf[DeltaSparkSessionExtension].getName}",
      SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key,
      s"${classOf[DeltaCatalog].getName}"
    )
    expectedStrs.foreach { m => assert(e.getMessage().contains(m), "full exception: " + e) }
  }
}
