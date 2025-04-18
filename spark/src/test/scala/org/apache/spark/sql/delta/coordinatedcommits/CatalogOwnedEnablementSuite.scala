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

package org.apache.spark.sql.delta.coordinatedcommits

import java.util.UUID

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.test.{DeltaExceptionTestUtils, DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.commons.lang3.NotImplementedException

import org.apache.spark.sql.catalyst.TableIdentifier

class CatalogOwnedEnablementSuite
  extends DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with DeltaExceptionTestUtils {

  override def beforeEach(): Unit = {
    super.beforeEach()
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      "spark_catalog", TrackingInMemoryCommitCoordinatorBuilder(batchSize = 3))
  }

  private val ICT_ENABLED_KEY = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key

  /**
   * Validate that the snapshot has the expected enablement of Catalog-Owned table feature.
   *
   * @param snapshot The snapshot to validate.
   * @param expectEnabled Whether the Catalog-Owned table features should be enabled or not.
   */
  private def validateCatalogOwnedCompleteEnablement(
      snapshot: Snapshot, expectEnabled: Boolean): Unit = {
    assert(snapshot.isCatalogOwned == expectEnabled)
    Seq(
      CatalogOwnedTableFeature,
      VacuumProtocolCheckTableFeature,
      InCommitTimestampTableFeature
    ).foreach { feature =>
      assert(snapshot.protocol.writerFeatures.exists(_.contains(feature.name)) == expectEnabled)
    }
    Seq(
      CatalogOwnedTableFeature,
      VacuumProtocolCheckTableFeature
    ).foreach { feature =>
      assert(snapshot.protocol.readerFeatures.exists(_.contains(feature.name)) == expectEnabled)
    }
  }

  /**
   * Test setup for ALTER TABLE commands on Catalog-Owned enabled tables.
   *
   * @param id The id of the table to be created for the test.
   * @param createCatalogOwnedTableAtInit Whether to enable Catalog-Owned table feature
   *                                      at the time of table creation.
   */
  private def testCatalogOwnedAlterTableSetup(
      id: String,
      createCatalogOwnedTableAtInit: Boolean): Unit = {
    if (createCatalogOwnedTableAtInit) {
      spark.sql(s"CREATE TABLE $id (id INT) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
    } else {
      spark.sql(s"CREATE TABLE $id (id INT) USING delta")
    }
    // Insert initial data to the table
    spark.sql(s"INSERT INTO $id VALUES 1") // commit 1
    spark.sql(s"INSERT INTO $id VALUES 2") // commit 2
    val log = DeltaLog.forTable(spark, new TableIdentifier(id))
    validateCatalogOwnedCompleteEnablement(
      snapshot = log.unsafeVolatileSnapshot,
      expectEnabled = createCatalogOwnedTableAtInit)
  }

  /**
   * Helper function to create a table and run the test.
   *
   * @param f The test function to run with the generated table id.
   */
  private def withTableIdAndAlterTableTestSetup(
      createCatalogOwnedTableAtInit: Boolean)(f: String => Unit): Unit = {
    val tableId = UUID.randomUUID().toString.replace("-", "")
    withTable(tableId) {
      testCatalogOwnedAlterTableSetup(id = tableId, createCatalogOwnedTableAtInit)
      f(tableId)
    }
  }

  test("Catalog-Owned: ALTER TABLE should be blocked if attempts to disable ICT") {
    withTableIdAndAlterTableTestSetup(createCatalogOwnedTableAtInit = true) { tableId =>
      val error = interceptWithUnwrapping[DeltaIllegalArgumentException] {
        spark.sql(s"ALTER TABLE $tableId SET TBLPROPERTIES ('$ICT_ENABLED_KEY' = 'false')")
      }
      checkError(
        error,
        "DELTA_CANNOT_MODIFY_CATALOG_OWNED_DEPENDENCIES",
        sqlState = "42616",
        parameters = Map[String, String]())
    }
  }

  test("Catalog-Owned: ALTER TABLE should be blocked if attempts to unset ICT") {
    withTableIdAndAlterTableTestSetup(createCatalogOwnedTableAtInit = true) { tableId =>
      val error = interceptWithUnwrapping[DeltaIllegalArgumentException] {
        spark.sql(s"ALTER TABLE $tableId UNSET TBLPROPERTIES ('$ICT_ENABLED_KEY')")
      }
      checkError(
        error,
        "DELTA_CANNOT_MODIFY_CATALOG_OWNED_DEPENDENCIES",
        sqlState = "42616",
        parameters = Map[String, String]())
    }
  }

  test("Catalog-Owned: ALTER TABLE should be blocked if attempts to" +
    " downgrade Catalog-Owned") {
    withTableIdAndAlterTableTestSetup(createCatalogOwnedTableAtInit = true)  { tableId =>
      val error = intercept[DeltaTableFeatureException] {
        spark.sql(s"ALTER TABLE $tableId DROP FEATURE '${CatalogOwnedTableFeature.name}'")
      }
      checkError(
        error,
        "DELTA_FEATURE_DROP_UNSUPPORTED_CLIENT_FEATURE",
        parameters = Map("feature" -> CatalogOwnedTableFeature.name))
    }
  }

  test("Catalog-Owned: Upgrade should be blocked since it is not supported yet") {
    withTableIdAndAlterTableTestSetup(
      // Do not enable Catalog-Owned at the beginning when creating table
      createCatalogOwnedTableAtInit = false
    ) { tableId =>
      val error = intercept[NotImplementedException] {
        spark.sql(s"ALTER TABLE $tableId SET TBLPROPERTIES " +
          s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      }
      assert(error.getMessage.contains("Upgrading to CatalogOwned table is not yet supported."))
    }
  }
}
