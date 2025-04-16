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

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.commons.lang3.NotImplementedException

import org.apache.spark.sql.catalyst.TableIdentifier

class CatalogOwnedEnablementSuite
  extends DeltaSQLTestUtils
  with DeltaSQLCommandTest {

  override def beforeEach(): Unit = {
    super.beforeEach()
    CatalogOwnedCommitCoordinatorProvider.clearBuilders()
    CatalogOwnedCommitCoordinatorProvider.registerBuilder(
      "spark_catalog", TrackingInMemoryCommitCoordinatorBuilder(batchSize = 3))
  }

  /**
   * Add `ucTableId` to the table [[Metadata.configuration]] to simulate the behavior
   * when we populate table UUID for a Catalog-Owned enabled table.
   */
  private def addCatalogOwnedUCTableId(
      deltaLog: DeltaLog,
      tableUUID: String): Unit = {
    val oldMetadata = deltaLog.update(
      catalogTableOpt = deltaLog.initialCatalogTable).metadata
    val newMetadata =
      oldMetadata.copy(configuration = oldMetadata.configuration ++ Map(
        CATALOG_OWNED_UC_TABLE_ID -> tableUUID))
    deltaLog.startTransaction(catalogTableOpt = deltaLog.initialCatalogTable)
      .commitManually(newMetadata)
  }

  private val CATALOG_OWNED_UC_TABLE_ID = UCCommitCoordinatorClient.UC_TABLE_ID_KEY

  private val ICT_ENABLED_KEY = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key

  private val allowedOperationsForTest = Set("SET", "UNSET", "UPGRADE", "DOWNGRADE")

  /**
   * Validate that the snapshot has the expected enablement of Catalog-Owned table feature.
   *
   * @param snapshot The snapshot to validate.
   * @param expectEnabled Whether the Catalog-Owned table features should be enabled or not.
   */
  private def validateCatalogOwnedCompleteEnablement(
      snapshot: Snapshot, expectEnabled: Boolean): Unit = {
    assert(snapshot.isCatalogOwned == expectEnabled)
    assert(snapshot.metadata.configuration.contains(CATALOG_OWNED_UC_TABLE_ID) == expectEnabled)
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
   * Test the blocking logic for ALTER TABLE command on Catalog-Owned enabled table.
   *
   * Specifically, we test the following two scenarios:
   * 1. Any SET/UNSET operation should be blocked for both [[CATALOG_OWNED_UC_TABLE_ID]]
    *   and [[ICT_ENABLED_KEY]].
   * 2. Both Upgrade/Downgrade should be blocked.
   *
   * @param property The optional property to be validated through blocking logic.
   * @param operation Whether the operation is SET/UNSET/UPGRADE/DOWNGRADE.
   * @param expectedErrorClass The expected error class/condition.
   * @param expectedMessageParameters The expected message parameters.
   * @param tableId The tableId to be used for the test.
   * @param setValue The value to be set for the property, this is only useful
   *                 for ALTER TABLE SET cases.
   */
  private def testCatalogOwnedBlockingAlterTable(
      property: String,
      operation: String,
      expectedErrorClass: String = "",
      expectedMessageParameters: Array[String] = Array.empty,
      tableId: String,
      setValue: Option[String],
      createCatalogOwnedTableAtInit: Boolean = true): Unit = {
    require(allowedOperationsForTest.contains(operation), s"Unsupported operation: $operation")
    withTable(tableId) {
      if (createCatalogOwnedTableAtInit) {
        spark.sql(s"CREATE TABLE $tableId (id INT) USING delta TBLPROPERTIES " +
          s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
        // Add the mock UC_TABLE_ID to [[Metadata.configuration]],
        // this simulates the behavior when we populate table UUID
        // after catalog discovery phase for a Catalog-Owned enabled table.
        val log = DeltaLog.forTable(spark, new TableIdentifier(tableId))
        addCatalogOwnedUCTableId(log, tableUUID = UUID.randomUUID().toString)
      } else {
        spark.sql(s"CREATE TABLE $tableId (id INT) USING delta")
      }
      spark.sql(s"INSERT INTO $tableId VALUES 1") // commit 1
      spark.sql(s"INSERT INTO $tableId VALUES 2") // commit 2
      val log = DeltaLog.forTable(spark, new TableIdentifier(tableId))
      validateCatalogOwnedCompleteEnablement(
        log.update(catalogTableOpt = log.initialCatalogTable),
        expectEnabled = createCatalogOwnedTableAtInit)
      // Blocking logic validation
      val sqlStr = operation match {
        case "SET" =>
          val value = setValue.getOrElse {
            throw new IllegalArgumentException(
              s"SET operation requires a value to be set for property: $property")
          }
          s"ALTER TABLE $tableId SET TBLPROPERTIES ('$property' = '$value')"
        case "UNSET" =>
          s"ALTER TABLE $tableId UNSET TBLPROPERTIES ('$property')"
        case "UPGRADE" =>
          s"ALTER TABLE $tableId SET TBLPROPERTIES " +
            s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')"
        case "DOWNGRADE" =>
          s"ALTER TABLE $tableId DROP FEATURE '${CatalogOwnedTableFeature.name}'"
      }
      if (operation == "UPGRADE") {
        val error = intercept[NotImplementedException] { sql(sqlStr) }
        assert(error.getMessage.contains("Upgrading to CatalogOwned table is not yet supported."))
      } else {
        val (error, expectedError) = operation match {
          case "SET" | "UNSET" =>
            if (property == CATALOG_OWNED_UC_TABLE_ID) {
              // [[DeltaUnsupportedOperationException]] will only be thrown when trying to SET/UNSET
              // [[CATALOG_OWNED_UC_TABLE_ID]] via ALTER TABLE.
              (intercept[DeltaUnsupportedOperationException] { sql(sqlStr) },
                new DeltaUnsupportedOperationException(
                  expectedErrorClass, expectedMessageParameters))
            } else {
              // [[DeltaIllegalArgumentException]] will be thrown when trying to SET/UNSET
              // [[ICT_ENABLED_KEY]] via ALTER TABLE.
              (intercept[DeltaIllegalArgumentException] { sql(sqlStr) },
                new DeltaIllegalArgumentException(
                  expectedErrorClass, expectedMessageParameters))
            }
          case "DOWNGRADE" =>
            (intercept[DeltaTableFeatureException] { sql(sqlStr) },
              new DeltaTableFeatureException(expectedErrorClass, expectedMessageParameters))
          case _ =>
            throw new IllegalArgumentException(
              s"Unsupported operation: $operation for property: $property")
        }
        checkError(
          error,
          expectedError.getErrorClass,
          if (operation != "DOWNGRADE") Some(expectedError.getSqlState) else None,
          expectedError.getMessageParameters.asScala.toMap)
      }
    }
  }

  test("Catalog-Owned: ALTER TABLE should be blocked if attempts to modify ICT") {
    // Should block the ALTER TABLE command that tries to disable [[ICT_ENABLED_KEY]]
    // for Catalog-Owned enabled table.
    testCatalogOwnedBlockingAlterTable(
      property = ICT_ENABLED_KEY,
      operation = "SET",
      expectedErrorClass = "DELTA_CANNOT_MODIFY_CATALOG_OWNED_DEPENDENCIES",
      expectedMessageParameters = Array("ALTER"),
      tableId = "t1",
      setValue = Some("false"))

    // Should block the ALTER TABLE command that tries to UNSET [[ICT_ENABLED_KEY]]
    // for Catalog-Owned enabled table.
    testCatalogOwnedBlockingAlterTable(
      property = ICT_ENABLED_KEY,
      operation = "UNSET",
      expectedErrorClass = "DELTA_CANNOT_MODIFY_CATALOG_OWNED_DEPENDENCIES",
      expectedMessageParameters = Array("ALTER"),
      tableId = "t2",
      setValue = None)
  }

  test("Catalog-Owned: ALTER TABLE should be blocked if attempts to modify UC_TABLE_ID") {
    // Should block the ALTER TABLE command that tries to modify `ucTableId`
    // for Catalog-Owned enabled table.
    testCatalogOwnedBlockingAlterTable(
      property = CATALOG_OWNED_UC_TABLE_ID,
      operation = "SET",
      expectedErrorClass = "DELTA_CANNOT_MODIFY_TABLE_PROPERTY",
      expectedMessageParameters = Array(CATALOG_OWNED_UC_TABLE_ID),
      tableId = "t1",
      setValue = Some(UUID.randomUUID().toString))

    // Should block the ALTER TABLE command that tries to UNSET `ucTableId`
    // for Catalog-Owned enabled table.
    testCatalogOwnedBlockingAlterTable(
      property = CATALOG_OWNED_UC_TABLE_ID,
      operation = "UNSET",
      expectedErrorClass = "DELTA_CANNOT_MODIFY_TABLE_PROPERTY",
      expectedMessageParameters = Array(CATALOG_OWNED_UC_TABLE_ID),
      tableId = "t2",
      setValue = None)
  }

  test("Catalog-Owned: ALTER TABLE should be blocked if attempts to" +
    " downgrade Catalog-Owned") {
    testCatalogOwnedBlockingAlterTable(
      property = "",
      operation = "DOWNGRADE",
      expectedErrorClass = "DELTA_FEATURE_DROP_UNSUPPORTED_CLIENT_FEATURE",
      expectedMessageParameters = Array(CatalogOwnedTableFeature.name),
      tableId = "t1",
      setValue = None)
  }

  test("Catalog-Owned: Upgrade should be blocked since it is not supported yet") {
    testCatalogOwnedBlockingAlterTable(
      property = "",
      operation = "UPGRADE",
      tableId = "t1",
      setValue = None,
      createCatalogOwnedTableAtInit = false)
  }

}
