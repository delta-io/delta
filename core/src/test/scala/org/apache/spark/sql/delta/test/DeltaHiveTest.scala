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

package org.apache.spark.sql.delta.test

import org.apache.spark.sql.delta.catalog.DeltaCatalog
import io.delta.sql.DeltaSparkSessionExtension
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.test.{TestHive, TestHiveContext}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Test utility for initializing a SparkSession with a Hive Client and a Hive Catalog for testing
 * DDL operations. Typical tests leverage an in-memory catalog with a mock catalog client. Here we
 * use real Hive classes.
 */
trait DeltaHiveTest extends SparkFunSuite with BeforeAndAfterAll { self: SQLTestUtils =>

  private var _session: SparkSession = _
  private var _hiveContext: TestHiveContext = _
  private var _sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = TestHive.sparkSession.sparkContext.getConf.clone()
    TestHive.sparkSession.stop()
    conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
    conf.set(StaticSQLConf.SPARK_SESSION_EXTENSIONS.key,
      classOf[DeltaSparkSessionExtension].getName)
    _sc = new SparkContext("local", this.getClass.getName, conf)
    _hiveContext = new TestHiveContext(_sc)
    _session = _hiveContext.sparkSession
    SparkSession.setActiveSession(_session)
    super.beforeAll()
  }

  override protected def spark: SparkSession = _session

  override def afterAll(): Unit = {
    try {
      _hiveContext.reset()
    } finally {
      _sc.stop()
    }
  }
}
