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

import io.delta.storage.{AzureLogStore, S3SingleDriverLogStore}
import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaTestImplicits._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{LocalSparkSession, SparkSession}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.delta.catalog.DeltaCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

class DeltaCommitLockSuite extends SparkFunSuite with LocalSparkSession with SQLHelper {

  private def verifyIsCommitLockEnabled(path: File, expected: Boolean): Unit = {
    val deltaLog = DeltaLog.forTable(spark, path)
    val txn = deltaLog.startTransaction()
    assert(txn.isCommitLockEnabled == expected)
  }

  test("commit lock flag on Azure") {
    spark = SparkSession.builder()
      .config("spark.delta.logStore.class", classOf[AzureLogStore].getName)
      .master("local[2]")
      .config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .getOrCreate()
    val path = Utils.createTempDir()
    try {
      // Should lock by default on Azure
      verifyIsCommitLockEnabled(path, expected = true)
      // Should respect user config
      for (enabled <- true :: false :: Nil) {
        withSQLConf(DeltaSQLConf.DELTA_COMMIT_LOCK_ENABLED.key -> enabled.toString) {
          verifyIsCommitLockEnabled(path, expected = enabled)
        }
      }
    } finally {
      Utils.deleteRecursively(path)
    }
  }

  test("commit lock flag on S3") {
    spark = SparkSession.builder()
      .config("spark.delta.logStore.class", classOf[S3SingleDriverLogStore].getName)
      .master("local[2]")
      .config(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[DeltaCatalog].getName)
      .getOrCreate()
    val path = Utils.createTempDir()
    try {
      // Should not lock by default on S3
      verifyIsCommitLockEnabled(path, expected = false)
      // Should respect user config
      for (enabled <- true :: false :: Nil) {
        withSQLConf(DeltaSQLConf.DELTA_COMMIT_LOCK_ENABLED.key -> enabled.toString) {
          verifyIsCommitLockEnabled(path, expected = enabled)
        }
      }
    } finally {
      Utils.deleteRecursively(path)
    }
  }
}
