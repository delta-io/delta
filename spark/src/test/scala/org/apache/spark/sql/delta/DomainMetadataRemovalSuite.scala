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

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.test.SharedSparkSession

class DomainMetadataRemovalSuite
    extends QueryTest
    with SharedSparkSession
    with DeltaSQLCommandTest {

  val testTableName = "test_domain_metadata_removal_table"

  def addData(deltaLog: DeltaLog, start: Long, end: Long): Unit = {
    spark.range(start, end, step = 1, numPartitions = 2)
      .write
      .format("delta")
      .mode("append")
      .save(deltaLog.dataPath.toString)
  }

  def createTableWithDomainMetadata(): DeltaLog = {
    sql(s"DROP TABLE IF EXISTS $testTableName")
    sql(
      s"""CREATE TABLE $testTableName (id LONG)
         |USING DELTA
         |TBLPROPERTIES(
         |'delta.feature.${DomainMetadataTableFeature.name}' = 'supported'
         |)""".stripMargin)

    val deltaLog = DeltaLog.forTable(spark, TableIdentifier(testTableName))
    assert(deltaLog.update().protocol.isFeatureSupported(DomainMetadataTableFeature))
    addData(deltaLog, 0, 100)
    deltaLog
  }

  def dropFeature(
      feature: TableFeature,
      truncateHistory: Boolean = false): Unit = {
    val sqlText =
      s"""
         |ALTER TABLE $testTableName
         |DROP FEATURE ${feature.name}
         |${if (truncateHistory) "TRUNCATE HISTORY" else ""}
         |""".stripMargin
    sql(sqlText)
  }

  def validateDomainMetadataRemoval(deltaLog: DeltaLog): Unit = {
    val snapshot = deltaLog.update()
    assert(!snapshot.protocol.isFeatureSupported(DomainMetadataTableFeature))
    assert(snapshot.domainMetadata.isEmpty)
  }

  test("DomainMetadata can be dropped") {
    val deltaLog = createTableWithDomainMetadata()
    dropFeature(DomainMetadataTableFeature)
    validateDomainMetadataRemoval(deltaLog)
  }

  test("Drop DomainMetadata feature when leaked domain metadata exist in the table") {
    case class TestMetadataDomain() extends JsonMetadataDomain[TestMetadataDomain] {
      override val domainName: String = "delta.test"
    }

    val deltaLog = createTableWithDomainMetadata()

    // Add a domainMetadata action.
    deltaLog
      .startTransaction(catalogTableOpt = None)
      .commit(Seq(TestMetadataDomain().toDomainMetadata), DeltaOperations.ManualUpdate)

    dropFeature(DomainMetadataTableFeature)
    validateDomainMetadataRemoval(deltaLog)
  }

  test("Cannot drop DomainMetadata if there is a dependent feature on the table") {
    createTableWithDomainMetadata()

    // Enable row tracking on the table.
    sql(
      s"""ALTER TABLE $testTableName
         |SET TBLPROPERTIES(
         |${DeltaConfigs.ROW_TRACKING_ENABLED.key} = 'true'
         |)""".stripMargin)

    val e = intercept[DeltaTableFeatureException] {
      dropFeature(DomainMetadataTableFeature)
    }
    checkError(
      e,
      "DELTA_FEATURE_DROP_DEPENDENT_FEATURE",
      parameters = Map(
        "feature" -> "domainMetadata",
        "dependentFeatures" -> "rowTracking"))
  }

  test("Drop domainMetadata after dropping a dependent feature") {
    val deltaLog = createTableWithDomainMetadata()
    addData(deltaLog, 0, 100)

    // Enable row tracking on the table. This also enables the domainMetadata feature.
    sql(
      s"""ALTER TABLE $testTableName
         |SET TBLPROPERTIES(
         |${DeltaConfigs.ROW_TRACKING_ENABLED.key} = 'true'
         |)""".stripMargin)

    assert(deltaLog.update().protocol.isFeatureSupported(DomainMetadataTableFeature))

    dropFeature(RowTrackingFeature)
    dropFeature(DomainMetadataTableFeature)
    validateDomainMetadataRemoval(deltaLog)
  }
}
