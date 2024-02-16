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

package org.apache.spark.sql.delta.uniform.hms

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import org.scalatest.{BeforeAndAfterAll, Suite}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Provide support to testcases that need to use HIM
 */
trait HMSTest extends Suite with BeforeAndAfterAll {

  def withMetaStore(thunk: (Configuration) => Unit): Unit = {
    val conf = sharedHMS.conf()
    thunk(conf)
  }

  private var sharedHMS: EmbeddedHMS = _

  protected override def beforeAll(): Unit = {
    startHMS()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    stopHMS()
  }

  protected def startHMS(): Unit = {
    sharedHMS = new EmbeddedHMS()
    sharedHMS.start()
  }

  protected def stopHMS(): Unit = sharedHMS.stop()

  protected def setupSparkConfWithHMS(in: SparkConf): SparkConf = {
    val conf = sharedHMS.conf()
    in.set("spark.sql.warehouse.dir", conf.get(METASTOREWAREHOUSE.varname))
      .set("hive.metastore.uris", conf.get(METASTOREURIS.varname))
      .set("spark.sql.catalogImplementation", "hive")
  }

  protected def createDeltaSparkSession: SparkSession = {
    val conf = sharedHMS.conf()
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

  protected def createIcebergSparkSession: SparkSession = {
    val conf = sharedHMS.conf()
    val sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("IcebergSession")
      .config("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.cache-enabled", "false")
      .config("spark.sql.warehouse.dir", conf.get(METASTOREWAREHOUSE.varname))
      .config("hive.metastore.uris", conf.get(METASTOREURIS.varname))
      .config("spark.sql.catalogImplementation", "hive")
      .getOrCreate()
    sparkSession
  }
}
