/*
 * Copyright (2025) The Delta Lake Project Authors.
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
package io.delta.spark.internal.v2.snapshot

import java.util.{HashMap => JHashMap}

import io.delta.kernel.defaults.engine.DefaultEngine
import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.spark.internal.v2.snapshot.unitycatalog.UCManagedTableSnapshotManager
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.test.SharedSparkSession

/** Unit tests for [[SnapshotManagerFactory]]. */
class SnapshotManagerFactorySuite extends SparkFunSuite with SharedSparkSession {

  private val FEATURE_CATALOG_MANAGED =
    TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX +
      TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName()
  private val FEATURE_SUPPORTED = TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE
  private val UC_TABLE_ID_KEY = UCCommitCoordinatorClient.UC_TABLE_ID_KEY
  private val UC_CATALOG_CONNECTOR = "io.unitycatalog.spark.UCSingleCatalog"

  private def kernelEngine = DefaultEngine.create(spark.sessionState.newHadoopConf())

  // ==================== forCreateTable ====================

  test("forCreateTable: non-UC properties returns PathBasedSnapshotManager") {
    val props = new JHashMap[String, String]()
    val manager =
      SnapshotManagerFactory.forCreateTable("/some/path", kernelEngine, props, "any_catalog", spark)
    assert(manager.isInstanceOf[PathBasedSnapshotManager])
  }

  test("forCreateTable: UC properties returns UCManagedTableSnapshotManager") {
    val catalogName = "uc_catalog_factory_test"
    val ucUri = "https://uc-factory-test.example.com/api/2.1/unity-catalog"
    val ucToken = "dapi_factory_test_token"

    val props = new JHashMap[String, String]()
    props.put(FEATURE_CATALOG_MANAGED, FEATURE_SUPPORTED)
    props.put(UC_TABLE_ID_KEY, "factory_test_table_id")

    val configs = Seq(
      s"spark.sql.catalog.$catalogName" -> UC_CATALOG_CONNECTOR,
      s"spark.sql.catalog.$catalogName.uri" -> ucUri,
      s"spark.sql.catalog.$catalogName.token" -> ucToken)
    val originals = configs.map { case (k, _) => k -> spark.conf.getOption(k) }.toMap

    try {
      configs.foreach { case (k, v) => spark.conf.set(k, v) }
      val manager = SnapshotManagerFactory.forCreateTable(
        "/some/path", kernelEngine, props, catalogName, spark)
      assert(manager.isInstanceOf[UCManagedTableSnapshotManager])
    } finally {
      configs.foreach { case (k, _) =>
        originals.get(k).flatten match {
          case Some(v) => spark.conf.set(k, v)
          case None => spark.conf.unset(k)
        }
      }
    }
  }
}
