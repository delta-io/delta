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

// scalastyle:off import.ordering.noEmptyLine
import java.util.UUID

import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.actions.TableFeatureProtocolUtils
import org.apache.spark.sql.delta.test.{DeltaSQLCommandTest, DeltaSQLTestUtils}
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
// scalastyle:on import.ordering.noEmptyLine

class CatalogOwnedPropertySuite extends QueryTest
  with DeltaSQLCommandTest
  with DeltaSQLTestUtils
  with CatalogOwnedTestBaseSuite {

  // Register the mock commit coordinator builder for testing, but don't enable
  // CatalogOwned by default (tests explicitly enable it when needed).
  override def catalogOwnedCoordinatorBackfillBatchSize: Option[Int] = Some(3)
  override def catalogOwnedDefaultCreationEnabledInTests: Boolean = false

  /**
   * Validate if the table is catalog owned or not by checking the following:
   * 1) [[Snapshot.isCatalogOwned]] === expected
   * 2) [[Metadata.configuration]] contains [[UCCommitCoordinatorClient.UC_TABLE_ID_KEY]]
   * 3) [[Protocol.readerAndWriterFeatureNames]] contains [[CatalogOwnedTableFeature.name]]
   *    and its dependent features.
   *
   * @param tableName The name of the table to validate.
   * @param expected The expected value of whether the table is catalog owned or not.
   */
  private def validateCatalogOwnedAndUCTableId(tableName: String, expected: Boolean): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))
    assert(snapshot.isCatalogOwned === expected)
    assert(snapshot.metadata.configuration.contains(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)
      === expected)
    // CatalogOwned enabled table should have the protocol version of (3, 7),
    // since the table can't have a [[ReaderWriterTableFeature]] w/o this version.
    if (expected) {
      // Only verify protocol version for expected CatalogOwned table.
      validateProtocolMinReaderWriterVersion(
        tableName,
        expectedMinReaderVersion = 3,
        expectedMinWriterVersion = 7)
      // Check dependent features as well.
      CatalogOwnedTableFeature.requiredFeatures.foreach { feature =>
        assert(snapshot.protocol.readerAndWriterFeatureNames.contains(feature.name),
          s"Table $tableName should have ${feature.name} in protocol.")
      }
    }
  }

  private def createTableAndValidateCatalogOwned(
      tableName: String,
      withCatalogOwned: Boolean): Unit = {
    val createTableSQLStr = if (withCatalogOwned) {
      s"CREATE TABLE $tableName (id LONG) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')"
    } else {
      s"CREATE TABLE $tableName (id LONG) USING delta"
    }
    sql(createTableSQLStr)
    // Manually insert UC_TABLE_ID to mock UC integration behavior.
    if (withCatalogOwned) {
      val catalogTable = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
      val deltaLog = DeltaLog.forTable(spark, catalogTable)
      val snapshot = deltaLog.update(catalogTableOpt = Some(catalogTable))
      val newMetadata = snapshot.metadata.copy(
        configuration = snapshot.metadata.configuration +
          (UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> java.util.UUID.randomUUID().toString)
      )
      deltaLog.startTransaction(Some(catalogTable))
        .commit(Seq(newMetadata), DeltaOperations.ManualUpdate)
    }
    validateCatalogOwnedAndUCTableId(tableName, expected = withCatalogOwned)
  }

  private def getUCTableIdFromTable(tableName: String): String = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))
    val ucTableId = snapshot.metadata.configuration.get(UCCommitCoordinatorClient.UC_TABLE_ID_KEY)
    assert(ucTableId.isDefined,
      s"Table $tableName should have `ucTableId` in metadata.")
    ucTableId.get
  }

  private def validateInCommitTimestampTableFeature(tableName: String, expected: Boolean): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))
    val writerFeatureNames = snapshot.protocol.writerFeatureNames
    assert(writerFeatureNames.contains(InCommitTimestampTableFeature.name) === expected)
  }

  private def validateInCommitTimestampTableProperties(
      tableName: String,
      expectedEnabled: Boolean,
      expectedEnablementInfo: Boolean): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))
    val conf = snapshot.metadata.configuration
    assert(conf.contains(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key) === expectedEnabled)
    // In certain cases, we can't verify all three ICT table properties here.
    // This is because we only need to persist the ICT enablement info
    // if there are non-ICT commits in the Delta log.
    // I.e., [[DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_VERSION]] and
    //       [[DeltaConfigs.IN_COMMIT_TIMESTAMP_ENABLEMENT_TIMESTAMP]].
    // E.g., If the 0th commit enables ICT, then we will not need the above
    //       two properties for the table in subsequent commits.
    // See [[InCommitTimestampUtils.getUpdatedMetadataWithICTEnablementInfo]] for details.
    CatalogOwnedTableUtils.ICT_TABLE_PROPERTY_KEYS.filter { k =>
      k != DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key
    }.foreach { key =>
      assert(conf.contains(key) === expectedEnablementInfo)
    }
  }

  private def validateInCommitTimestampPresent(
      tableName: String,
      expected: Boolean,
      expectedEnablementInfo: Boolean): Unit = {
    // Validate ICT table feature is present in [[Protocol]] first.
    validateInCommitTimestampTableFeature(tableName, expected)
    // Then validate ICT table properties are present in [[Metadata.configuration]].
    validateInCommitTimestampTableProperties(tableName, expected, expectedEnablementInfo)
  }

  private def validateProtocolMinReaderWriterVersion(
      tableName: String,
      expectedMinReaderVersion: Int,
      expectedMinWriterVersion: Int): Unit = {
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, new TableIdentifier(tableName))
    assert(snapshot.protocol.minReaderVersion === expectedMinReaderVersion)
    assert(snapshot.protocol.minWriterVersion === expectedMinWriterVersion)
  }

  test("[REPLACE] table UUID & table feature should retain for target " +
      "catalog-owned table during REPLACE TABLE") {
    withTable("t1") {
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = true)
      val ucTableId1 = getUCTableIdFromTable(tableName = "t1")

      // Target table should remain a catalog-owned table with same `ucTableId`
      sql("REPLACE TABLE t1 (id LONG) USING delta")
      val ucTableId2 = getUCTableIdFromTable(tableName = "t1")
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)

      // [[UCCommitCoordinatorClient.UC_TABLE_ID_KEY]] should be the same before and after REPLACE
      assert(ucTableId1 === ucTableId2)
    }
  }

  test("[REPLACE] Specifying table UUID for target table should be blocked " +
      "during REPLACE TABLE") {
    withTable("t1") {
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = true)

      val error = intercept[DeltaUnsupportedOperationException] {
        sql("REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
          s"('${UCCommitCoordinatorClient.UC_TABLE_ID_KEY}' = '${UUID.randomUUID().toString}')")
      }
      checkError(error, "DELTA_CANNOT_MODIFY_TABLE_PROPERTY", "42939",
        Map("prop" -> "io.unitycatalog.tableId"))
    }
  }

  test("[RTAS] Specifying table UUID when creating catalog-owned " +
      " table via RTAS should be blocked") {
    withTable("t1", "t2") {
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = false)
      sql("INSERT INTO t1 VALUES (1)")
      sql("INSERT INTO t1 VALUES (2)")

      createTableAndValidateCatalogOwned(tableName = "t2", withCatalogOwned = true)
      val ucTableIdOriginal = getUCTableIdFromTable(tableName = "t2")

      val error = intercept[DeltaUnsupportedOperationException] {
        sql(s"REPLACE TABLE t2 USING delta TBLPROPERTIES " +
          s"('${UCCommitCoordinatorClient.UC_TABLE_ID_KEY}' = '${UUID.randomUUID().toString}') " +
          s"AS SELECT * FROM t1")
      }
      checkError(error, "DELTA_CANNOT_MODIFY_TABLE_PROPERTY", "42939",
        Map("prop" -> "io.unitycatalog.tableId"))

      // Normal RTAS should not be blocked
      sql(s"REPLACE TABLE t2 USING delta AS SELECT * FROM t1")
      validateCatalogOwnedAndUCTableId(tableName = "t2", expected = true)
      val ucTableIdAfterRTAS = getUCTableIdFromTable(tableName = "t2")
      assert(ucTableIdOriginal === ucTableIdAfterRTAS)
      checkAnswer(sql("SELECT * FROM t2"), Seq(Row(1), Row(2)))
    }
  }

  test("[REPLACE] Specifying CatalogManaged for non-CatalogManaged table should " +
      "be blocked during REPLACE TABLE") {
    withTable("t1") {
      // Normal delta table.
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = false)

      val error = intercept[IllegalStateException] {
        sql("REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
          s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      }
      assert(error.getMessage.contains(
        "Specifying CatalogManaged in REPLACE TABLE command is not supported"))
    }
  }

  test("[REPLACE] Specifying CatalogManaged for existing CatalogManaged table should " +
      "succeed as a no-op during REPLACE TABLE") {
    withTable("t1") {
      // CatalogManaged enabled table.
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = true)
      val ucTableIdBefore = getUCTableIdFromTable(tableName = "t1")

      // Specifying CatalogManaged on an already CatalogManaged table should succeed.
      // The CatalogManaged properties are treated as a no-op.
      sql("REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")

      // Validate the table is still CatalogManaged with the same ucTableId.
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      val ucTableIdAfter = getUCTableIdFromTable(tableName = "t1")
      assert(ucTableIdBefore === ucTableIdAfter)
    }
  }

  test("[RTAS] Specifying CatalogManaged for non-CatalogManaged table should " +
      "be blocked during RTAS") {
    withTable("t1", "t2") {
      // Normal delta table.
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = false)

      // Source RTAS table.
      createTableAndValidateCatalogOwned(tableName = "t2", withCatalogOwned = false)
      sql("INSERT INTO t2 VALUES (1), (2)")

      val error = intercept[IllegalStateException] {
        sql("REPLACE TABLE t1 USING delta TBLPROPERTIES " +
          s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported') " +
          s"AS SELECT * FROM t2")
      }
      assert(error.getMessage.contains(
        "Specifying CatalogManaged in REPLACE TABLE command is not supported"))
    }
  }

  test("[RTAS] Specifying CatalogManaged for existing CatalogManaged table should " +
      "succeed as a no-op during RTAS") {
    withTable("t1", "t2") {
      // CatalogManaged enabled table.
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = true)
      val ucTableIdBefore = getUCTableIdFromTable(tableName = "t1")

      // Source RTAS table.
      createTableAndValidateCatalogOwned(tableName = "t2", withCatalogOwned = false)
      sql("INSERT INTO t2 VALUES (1), (2)")

      // Specifying CatalogManaged on an already CatalogManaged table should succeed.
      sql("REPLACE TABLE t1 USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported') " +
        s"AS SELECT * FROM t2")

      // Validate the table is still CatalogManaged with the same ucTableId.
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      val ucTableIdAfter = getUCTableIdFromTable(tableName = "t1")
      assert(ucTableIdBefore === ucTableIdAfter)
      checkAnswer(sql("SELECT * FROM t1"), Seq(Row(1), Row(2)))
    }
  }

  test("[CREATE OR REPLACE] with CatalogManaged on non-existing table should succeed") {
    withTable("t1") {
      // CREATE OR REPLACE on non-existing table with CatalogManaged should create a CC table.
      sql("CREATE OR REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")

      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      sql("INSERT INTO t1 VALUES (1), (2)")
      checkAnswer(sql("SELECT * FROM t1"), Seq(Row(1), Row(2)))
    }
  }

  test("[CREATE OR REPLACE] with CatalogManaged on existing CatalogManaged table " +
      "should succeed as a no-op") {
    withTable("t1") {
      // Create a CatalogManaged table first.
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = true)
      val ucTableIdBefore = getUCTableIdFromTable(tableName = "t1")
      sql("INSERT INTO t1 VALUES (1)")

      // CREATE OR REPLACE with CatalogManaged on existing CatalogManaged table should succeed.
      sql("CREATE OR REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")

      // Validate the table is still CatalogManaged with the same ucTableId.
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      val ucTableIdAfter = getUCTableIdFromTable(tableName = "t1")
      assert(ucTableIdBefore === ucTableIdAfter)
    }
  }

  test("[CREATE OR REPLACE] with CatalogManaged on existing non-CatalogManaged table " +
      "should be blocked") {
    withTable("t1") {
      // Create a non-CatalogManaged table first.
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = false)
      sql("INSERT INTO t1 VALUES (1)")

      // CREATE OR REPLACE with CatalogManaged on existing non-CatalogManaged table should fail.
      val error = intercept[IllegalStateException] {
        sql("CREATE OR REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
          s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      }
      assert(error.getMessage.contains(
        "Specifying CatalogManaged in REPLACE TABLE command is not supported"))

      // Original table should remain unchanged.
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = false)
      checkAnswer(sql("SELECT * FROM t1"), Seq(Row(1)))
    }
  }

  test("[CREATE OR REPLACE] continuous CREATE OR REPLACE with CatalogManaged " +
      "should succeed repeatedly") {
    withTable("t1") {
      // First CREATE OR REPLACE creates the table.
      sql("CREATE OR REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      val ucTableIdFirst = getUCTableIdFromTable(tableName = "t1")
      sql("INSERT INTO t1 VALUES (1)")
      checkAnswer(sql("SELECT * FROM t1"), Seq(Row(1)))

      // Second CREATE OR REPLACE should succeed as a no-op for CC properties.
      sql("CREATE OR REPLACE TABLE t1 (id LONG) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      val ucTableIdSecond = getUCTableIdFromTable(tableName = "t1")
      assert(ucTableIdFirst === ucTableIdSecond)

      // Third CREATE OR REPLACE should also succeed.
      sql("CREATE OR REPLACE TABLE t1 (id LONG, name STRING) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogOwnedTableFeature.name}' = 'supported')")
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      val ucTableIdThird = getUCTableIdFromTable(tableName = "t1")
      assert(ucTableIdFirst === ucTableIdThird)

      // Verify table is functional.
      sql("INSERT INTO t1 VALUES (1, 'a'), (2, 'b')")
      checkAnswer(sql("SELECT * FROM t1"), Seq(Row(1, "a"), Row(2, "b")))
    }
  }
  test("[CREATE LIKE] Specifying table UUID should be blocked") {
    withTable("t1", "t2", "t3") {
      // Source catalog-owned table
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = true)

      val error1 = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE t2 LIKE t1 TBLPROPERTIES " +
          s"('${UCCommitCoordinatorClient.UC_TABLE_ID_KEY}' = '${UUID.randomUUID().toString}')")
      }
      val error2 = intercept[DeltaUnsupportedOperationException] {
        sql(s"CREATE TABLE t3 LIKE t1 TBLPROPERTIES " +
          s"('${UCCommitCoordinatorClient.UC_TABLE_ID_KEY}' = '${UUID.randomUUID().toString}')")
      }
      Seq(error1, error2).foreach { error =>
        checkError(error, "DELTA_CANNOT_MODIFY_TABLE_PROPERTY", "42939",
          Map("prop" -> "io.unitycatalog.tableId"))
      }
    }
  }

  test("[REPLACE] Protocol should not be downgraded when adding new table feature " +
      "to existing CatalogOwned enabled table") {
    withTable("t1", "t2") {
      createTableAndValidateCatalogOwned(tableName = "t1", withCatalogOwned = true)
      val ucTableIdBeforeReplace = getUCTableIdFromTable(tableName = "t1")
      sql("CREATE TABLE t2 (col1 LONG) USING delta")
      sql("INSERT INTO t2 VALUES (1)")
      sql("INSERT INTO t2 VALUES (2)")
      // Adding [[ClusteringTableFeature]] when replacing `t1`
      sql("REPLACE TABLE t1 USING delta CLUSTER BY (col1) " +
        "TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = '1') " +
        "AS SELECT * FROM t2")
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = true)
      val ucTableIdAfterReplace = getUCTableIdFromTable(tableName = "t1")
      assert(ucTableIdBeforeReplace === ucTableIdAfterReplace)
      checkAnswer(sql("SELECT * FROM t1"), Seq(Row(1), Row(2)))
      val log = DeltaLog.forTable(spark, new TableIdentifier("t1"))
      val readerWriterFeatureNames =
        log.unsafeVolatileSnapshot.protocol.readerAndWriterFeatureNames
      assert(readerWriterFeatureNames.contains(ClusteringTableFeature.name) &&
        readerWriterFeatureNames.contains(CatalogOwnedTableFeature.name))
    }
  }

  test("[REPLACE] ICT should not be present after replacing existing normal delta " +
      "table w/o ICT w/ default spark configuration") {
    withSQLConf(
        TableFeatureProtocolUtils.defaultPropertyKey(CatalogOwnedTableFeature) -> "supported") {
      withTable("t1", "t2") {
        withoutDefaultCCTableFeature {
          sql("CREATE TABLE t1 (id LONG) USING delta")
          sql("INSERT INTO t1 VALUES (1), (2)")
          sql("CREATE TABLE t2 (id LONG) USING delta")
          validateInCommitTimestampPresent(
            tableName = "t2",
            expected = false,
            expectedEnablementInfo = false)
        }
        sql("REPLACE TABLE t2 USING delta AS SELECT * FROM t1")
        checkAnswer(sql("SELECT * FROM t2"), Seq(Row(1), Row(2)))
        validateInCommitTimestampPresent(
          tableName = "t2",
          expected = false,
          expectedEnablementInfo = false)
        validateCatalogOwnedAndUCTableId(tableName = "t2", expected = false)
        sql("INSERT INTO t2 VALUES (3), (4)")
        checkAnswer(sql("SELECT * FROM t2"), Seq(Row(1), Row(2), Row(3), Row(4)))
      }
    }
  }

  test("[REPLACE] ICT-related properties should not be present after replacing existing " +
      "normal delta table w/ ICT w/ default spark configuration") {
    withSQLConf(
        TableFeatureProtocolUtils.defaultPropertyKey(CatalogOwnedTableFeature) -> "supported") {
      withTable("t1", "t2", "t3") {
        withoutDefaultCCTableFeature {
          sql("CREATE TABLE t1 (id LONG) USING delta")
          sql("INSERT INTO t1 VALUES (1), (2)")
          sql("CREATE TABLE t2 (id LONG) USING delta TBLPROPERTIES " +
            s"('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true')")
          validateInCommitTimestampPresent(
            tableName = "t2",
            expected = true,
            expectedEnablementInfo = false)
          sql("CREATE TABLE t3 (id LONG) USING delta")
          sql("INSERT INTO t3 VALUES (1), (2)")
          sql("ALTER TABLE t3 SET TBLPROPERTIES " +
            s"('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true')")
          validateInCommitTimestampPresent(
            tableName = "t3",
            expected = true,
            expectedEnablementInfo = true)
        }
        sql("REPLACE TABLE t2 USING delta AS SELECT * FROM t1")
        sql("REPLACE TABLE t3 USING delta AS SELECT * FROM t1")
        sql("INSERT INTO t2 VALUES (3)")
        sql("INSERT INTO t3 VALUES (3), (4)")
        checkAnswer(sql("SELECT * FROM t2"), Seq(Row(1), Row(2), Row(3)))
        checkAnswer(sql("SELECT * FROM t3"), Seq(Row(1), Row(2), Row(3), Row(4)))
        Seq("t2", "t3").foreach { tableName =>
          // ICT-related properties should *NOT* be present after replacing existing normal
          // delta table w/o explicit overrides or default spark configuration.
          validateInCommitTimestampTableProperties(
            tableName = tableName,
            expectedEnabled = false,
            // All three ICT properties should not be present,
            // though currently there is only one for `t2`.
            expectedEnablementInfo = false)
          // ICT table feature will be preserved after REPLACE.
          validateInCommitTimestampTableFeature(tableName = tableName, expected = true)
          validateCatalogOwnedAndUCTableId(tableName = tableName, expected = false)
        }
        sql("INSERT INTO t2 VALUES (4), (5)")
        sql("INSERT INTO t3 VALUES (5)")
        checkAnswer(sql("SELECT * FROM t2"), Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))
        checkAnswer(sql("SELECT * FROM t3"), Seq(Row(1), Row(2), Row(3), Row(4), Row(5)))
      }
    }
  }

  test("[REPLACE] ICT should be present after replacing existing normal delta " +
      "table w/o ICT w/ default spark configuration w/ explicitly overrides") {
    withSQLConf(
        TableFeatureProtocolUtils.defaultPropertyKey(CatalogOwnedTableFeature) -> "supported") {
      withTable("t1", "t2", "t3") {
        withoutDefaultCCTableFeature {
          sql("CREATE TABLE t1 (id LONG) USING delta")
          sql("INSERT INTO t1 VALUES (1), (2)")
          sql("CREATE TABLE t2 (id LONG) USING delta")
          sql("CREATE TABLE t3 (id LONG) USING delta")
          sql("INSERT INTO t3 VALUES (1), (2)")
          Seq("t2", "t3").foreach { tableName =>
            validateInCommitTimestampPresent(
              tableName = tableName,
              expected = false,
              expectedEnablementInfo = false)
          }
        }
        Seq("t2", "t3").foreach { tableName =>
          sql(s"""
                 | REPLACE TABLE $tableName USING delta TBLPROPERTIES
                 | ('${DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key}' = 'true')
                 | AS SELECT * FROM t1
              """.stripMargin)
          checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1), Row(2)))
          validateCatalogOwnedAndUCTableId(tableName = tableName, expected = false)
          sql(s"INSERT INTO $tableName VALUES (3), (4)")
          checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1), Row(2), Row(3), Row(4)))
          // ICT enablement info is present in both `t2` and `t3`.
          validateInCommitTimestampPresent(
            tableName = "t2",
            expected = true,
            expectedEnablementInfo = true)
        }
      }
    }
  }

  test("[REPLACE] ICT should be present after replacing existing normal delta " +
     "table w/ ICT enabled via default spark configuration") {
    withTable("t1", "t2", "t3") {
      sql("CREATE TABLE t1 (id LONG) USING delta")
      sql("INSERT INTO t1 VALUES (1), (2)")

      sql("CREATE TABLE t2 (id LONG) USING delta")
      sql("INSERT INTO t2 VALUES (1), (2)")
      sql("CREATE TABLE t3 (id LONG) USING delta")
      Seq("t2", "t3").foreach { tableName =>
        validateInCommitTimestampPresent(
          tableName = tableName,
          expected = false,
          expectedEnablementInfo = false)
        validateCatalogOwnedAndUCTableId(tableName, expected = false)
      }

      // w/ default CO enabled
      withSQLConf(
          TableFeatureProtocolUtils.defaultPropertyKey(CatalogOwnedTableFeature) -> "supported",
          DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "true") {
        sql("REPLACE TABLE t2 USING delta AS SELECT * FROM t1")
        validateInCommitTimestampPresent(
          tableName = "t2",
          expected = true,
          expectedEnablementInfo = true)
        validateCatalogOwnedAndUCTableId(tableName = "t2", expected = false)
      }

      // w/o default CO enabled
      withSQLConf(DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.defaultTablePropertyKey -> "true") {
        sql("REPLACE TABLE t3 USING delta AS SELECT * FROM t1")
        validateInCommitTimestampPresent(
          tableName = "t3",
          expected = true,
          expectedEnablementInfo = true)
        validateCatalogOwnedAndUCTableId(tableName = "t3", expected = false)
      }

      Seq("t1", "t2", "t3").foreach { tableName =>
        sql(s"INSERT INTO $tableName VALUES (3), (4)")
        checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(1), Row(2), Row(3), Row(4)))
      }
    }
  }

  test("Table protocol should be kept intact before and after REPLACE " +
      "regardless of default CC enablement") {
    def getReaderAndWriterFeatureNamesFromTable(tableName: String): Set[String] = {
      val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
      snapshot.protocol.readerAndWriterFeatureNames
    }
    withTable("t1", "t2", "source") {
      // Source table.
      sql("CREATE TABLE source (id LONG) USING delta")
      sql("INSERT INTO source VALUES (3), (4), (5)")

      // Create a normal delta table w/ protocol (1, 2).
      sql("CREATE TABLE t1 (id LONG) USING delta")
      val featuresBeforeReplaceT1 = getReaderAndWriterFeatureNamesFromTable(tableName = "t1")
      validateProtocolMinReaderWriterVersion(
        tableName = "t1",
        expectedMinReaderVersion = 1,
        expectedMinWriterVersion = 2)
      validateCatalogOwnedAndUCTableId(tableName = "t1", expected = false)
      CatalogOwnedTableFeature.requiredFeatures.foreach { feature =>
        assert(!featuresBeforeReplaceT1.contains(feature.name))
      }

      // Create a CC table w/ protocol (3, 7).
      createTableAndValidateCatalogOwned(tableName = "t2", withCatalogOwned = true)
      val featuresBeforeReplaceT2 = getReaderAndWriterFeatureNamesFromTable(tableName = "t2")

      // Insert initial data.
      Seq("t1", "t2").foreach { tableName =>
        sql(s"INSERT INTO $tableName VALUES (1), (2)")
      }

      // Enable CC by default
      withDefaultCCTableFeature {
        Seq("t1", "t2").foreach { tableName =>
          sql(s"REPLACE TABLE $tableName USING delta AS SELECT * FROM source")
          checkAnswer(sql(s"SELECT * FROM $tableName"), Seq(Row(3), Row(4), Row(5)))
        }
        // REPLACE TABLE should not change the protocol.
        val featuresAfterReplaceT1 = getReaderAndWriterFeatureNamesFromTable(tableName = "t1")
        validateProtocolMinReaderWriterVersion(
          tableName = "t1",
          expectedMinReaderVersion = 1,
          expectedMinWriterVersion = 2)
        assert(featuresBeforeReplaceT1 === featuresAfterReplaceT1)
        validateCatalogOwnedAndUCTableId(tableName = "t1", expected = false)

        val featuresAfterReplaceT2 = getReaderAndWriterFeatureNamesFromTable(tableName = "t2")
        assert(featuresBeforeReplaceT2 === featuresAfterReplaceT2)
        validateCatalogOwnedAndUCTableId(tableName = "t2", expected = true)
      }
    }
  }
}
