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

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf.ConfVars._
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object HMSPool {
  private val pool = new ConcurrentLinkedQueue[EmbeddedHMS]()
  private val maxInstances = 5

  def acquire(): EmbeddedHMS = synchronized {
    while (pool.isEmpty && pool.size >= maxInstances) {
      wait()
    }
    if (pool.isEmpty) {
      new EmbeddedHMS()
    } else {
      pool.poll()
    }
  }

  def release(hms: EmbeddedHMS): Unit = synchronized {
    pool.offer(hms)
    notify()
  }
}

/**
 * Provide support to testcases that need to use HMS.
 */
trait HMSTest extends Suite with BeforeAndAfterAll {
  private var sharedHMS: EmbeddedHMS = _

  def withMetaStore(thunk: (Configuration) => Unit): Unit = {
    val conf = sharedHMS.conf()
    thunk(conf)
  }

  protected override def beforeAll(): Unit = {
    sharedHMS = HMSPool.acquire()
    sharedHMS.start()
    super.beforeAll()
  }

  protected override def afterAll(): Unit = {
    super.afterAll()
    releaseHMS()
  }

  protected def releaseHMS(): Unit = {
    if (sharedHMS != null) {
      HMSPool.release(sharedHMS)
      sharedHMS = null
    }
  }

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
