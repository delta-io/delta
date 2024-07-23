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

package org.apache.spark.sql.delta.uniform

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.uniform.{UniFormE2EIcebergSuiteBase, UniFormE2ETest}
import org.apache.spark.SparkConf

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import org.apache.spark.sql.delta.uniform.hms.HMSTest

/**
 * This trait allows the tests to write with Delta
 * using a in-memory HiveMetaStore as catalog,
 * and read from the same HiveMetaStore with Iceberg.
 */
trait WriteDeltaHMSReadIceberg extends UniFormE2ETest with DeltaSQLCommandTest with HMSTest {

  override protected def sparkConf: SparkConf =
    setupSparkConfWithHMS(super.sparkConf)
      .set(DeltaSQLConf.DELTA_UNIFORM_ICEBERG_SYNC_CONVERT_ENABLED.key, "true")

  override protected def createReaderSparkSession: SparkSession = createIcebergSparkSession
}

class UniFormE2EIcebergSuite extends UniFormE2EIcebergSuiteBase with WriteDeltaHMSReadIceberg {
  private def runReorgUpgradeUniform(icebergCompatVersion: Int): Unit = {
    write(
      s"""
         | REORG TABLE $testTableName APPLY
         | (UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = $icebergCompatVersion))
         |""".stripMargin
    )
  }

  private def assertTagsExistForLatestSnapshot(tagsShouldExist: Boolean): Unit = {
    val snapshot = DeltaLog.forTable(spark, new TableIdentifier(testTableName)).update()
    snapshot.allFiles.collect().forall { addFile =>
      if (!tagsShouldExist) {
        addFile.tags == null
      } else {
        addFile.tags.getOrElse(AddFile.Tags.ICEBERG_COMPAT_VERSION.name, "0") == "2"
      }
    }
  }

  test("Reorg Upgrade Uniform Basic Test") {
    withTable(testTableName) {
      write(s"CREATE TABLE $testTableName (id INT, name STRING) USING DELTA")
      write(s"INSERT INTO $testTableName VALUES (1, 'Alex'), (2, 'Michael')")
      assertTagsExistForLatestSnapshot(tagsShouldExist = false)

      runReorgUpgradeUniform(icebergCompatVersion = 2)
      assertTagsExistForLatestSnapshot(tagsShouldExist = true)
    }
  }

  test("Reorg Upgrade Uniform V1 to V2") {
    withTable(testTableName) {
      write(
        s"""
           | CREATE TABLE $testTableName (id INT, name STRING)
           | USING DELTA
           | TBLPROPERTIES (
           |   'delta.columnMapping.mode' = 'name',
           |   'delta.enableIcebergCompatV1' = 'true',
           |   'delta.universalFormat.enabledFormats' = 'iceberg'
           | )
           |""".stripMargin
      )
      write(s"INSERT INTO $testTableName VALUES (1, 'Alex'), (2, 'Michael')")
      assertTagsExistForLatestSnapshot(tagsShouldExist = false)

      runReorgUpgradeUniform(icebergCompatVersion = 2)
      assertTagsExistForLatestSnapshot(tagsShouldExist = true)
    }
  }
}
