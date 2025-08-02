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

import com.databricks.spark.util.Log4jUsageLogger
import org.apache.spark.sql.delta._
import org.apache.spark.sql.delta.DeltaOperations._
import org.apache.spark.sql.delta.test.{DeltaExceptionTestUtils, DeltaSQLCommandTest, DeltaSQLTestUtils}
import org.apache.spark.sql.delta.test.DeltaTestImplicits._
import org.apache.spark.sql.delta.util.JsonUtils
import org.apache.commons.lang3.NotImplementedException

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier

class CatalogManagedEnablementSuite
  extends QueryTest
  with CatalogManagedTestBaseSuite
  with DeltaSQLTestUtils
  with DeltaSQLCommandTest
  with DeltaTestUtilsBase
  with DeltaExceptionTestUtils {

  override def beforeEach(): Unit = {
    super.beforeEach()
    CatalogManagedCommitCoordinatorProvider.clearBuilders()
    CatalogManagedCommitCoordinatorProvider.registerBuilder(
      catalogName = CatalogManagedTableUtils.DEFAULT_CATALOG_NAME_FOR_TESTING,
      commitCoordinatorBuilder = TrackingInMemoryCommitCoordinatorBuilder(batchSize = 3)
    )
  }

  private val ICT_ENABLED_KEY = DeltaConfigs.IN_COMMIT_TIMESTAMPS_ENABLED.key

  /**
   * Validate that the snapshot has the expected enablement of Catalog-Managed table feature.
   *
   * @param snapshot The snapshot to validate.
   * @param expectEnabled Whether the Catalog-Managed table features should be enabled or not.
   */
  private def validateCatalogManagedCompleteEnablement(
      snapshot: Snapshot, expectEnabled: Boolean): Unit = {
    assert(snapshot.isCatalogManaged == expectEnabled)
    Seq(
      CatalogManagedTableFeature,
      VacuumProtocolCheckTableFeature,
      InCommitTimestampTableFeature
    ).foreach { feature =>
      assert(snapshot.protocol.writerFeatures.exists(_.contains(feature.name)) == expectEnabled)
    }
    Seq(
      CatalogManagedTableFeature,
      VacuumProtocolCheckTableFeature
    ).foreach { feature =>
      assert(snapshot.protocol.readerFeatures.exists(_.contains(feature.name)) == expectEnabled)
    }
  }

  /**
   * Test setup for ALTER TABLE commands on Catalog-Managed enabled tables.
   *
   * @param tableName The name of the table to be created for the test.
   * @param createCatalogManagedTableAtInit Whether to enable Catalog-Managed table feature
   *                                      at the time of table creation.
   */
  private def testCatalogManagedAlterTableSetup(
      tableName: String,
      createCatalogManagedTableAtInit: Boolean): Unit = {
    if (createCatalogManagedTableAtInit) {
      spark.sql(s"CREATE TABLE $tableName (id INT) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogManagedTableFeature.name}' = 'supported')")
    } else {
      spark.sql(s"CREATE TABLE $tableName (id INT) USING delta")
    }
    // Insert initial data to the table
    spark.sql(s"INSERT INTO $tableName VALUES 1") // commit 1
    spark.sql(s"INSERT INTO $tableName VALUES 2") // commit 2
    val (_, snapshot) = DeltaLog.forTableWithSnapshot(spark, TableIdentifier(tableName))
    validateCatalogManagedCompleteEnablement(
      snapshot = snapshot,
      expectEnabled = createCatalogManagedTableAtInit)
  }

  /**
   * Helper function to create a table and run the test.
   *
   * @param f The test function to run with the random-generated table name.
   */
  private def withRandomTable(
      createCatalogManagedTableAtInit: Boolean)(f: String => Unit): Unit = {
    val randomTableName = s"testTable_${UUID.randomUUID().toString.replace("-", "")}"
    withTable(randomTableName) {
      testCatalogManagedAlterTableSetup(randomTableName, createCatalogManagedTableAtInit)
      f(randomTableName)
    }
  }

  /**
   * Validate the usage log blob.
   *
   * @param usageLogBlob The usage log blob to validate.
   * @param expectedPresentFields The fields that should be present in the usage log blob.
   * @param expectedAbsentFields The fields that should not be present in the usage log blob.
   * @param expectedValues The expected values for the fields.
   */
  private def validateUsageLogBlob(
      usageLogBlob: Map[String, Any],
      expectedPresentFields: Seq[String] = Seq.empty,
      expectedAbsentFields: Seq[String] = Seq.empty,
      expectedValues: Map[String, Any] = Map.empty): Unit = {
    expectedPresentFields.foreach { field =>
      assert(usageLogBlob.contains(field), s"Field '$field' should be present in usage log blob")
    }
    expectedAbsentFields.foreach { field =>
      assert(!usageLogBlob.contains(field),
        s"Field '$field' should not be present in usage log blob")
    }
    assert(expectedValues.size === expectedPresentFields.size,
      "The size of `expectedValues` should match the size of `expectedPresentFields`.")
    expectedValues.foreach { case (field, expectedValue) =>
      if (field == "stackTrace") {
        // Only validate the start of the stack trace.
        assert(
          usageLogBlob(field).asInstanceOf[String].startsWith(expectedValue.asInstanceOf[String]),
          s"Field '$field' should start with '$expectedValue' but was '${usageLogBlob(field)}'"
        )
      } else if (field == "checksumOpt" || field == "properties") {
        // Validate a portion of the entire `checksumOpt` or `properties` map.
        val properties = expectedValue.asInstanceOf[Map[String, Any]]
        properties.foreach { case (key, value) =>
          assert(
            usageLogBlob(field).asInstanceOf[Map[String, Any]].get(key).contains(value),
            s"Field '$field' should contain '$key' with value '$value' " +
              s"but was '${usageLogBlob(field)}'"
          )
        }
      } else {
        assert(usageLogBlob(field) === expectedValue,
          s"Field '$field' should have value '$expectedValue' but was '${usageLogBlob(field)}'")
      }
    }
  }

  test("ALTER TABLE should be blocked if attempts to disable ICT") {
    withRandomTable(createCatalogManagedTableAtInit = true) { tableName =>
      val error = interceptWithUnwrapping[DeltaIllegalArgumentException] {
        spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES ('$ICT_ENABLED_KEY' = 'false')")
      }
      checkError(
        error,
        "DELTA_CANNOT_MODIFY_CATALOG_MANAGED_DEPENDENCIES",
        sqlState = "42616",
        parameters = Map[String, String]())
    }
  }

  test("ALTER TABLE should be blocked if attempts to unset ICT") {
    withRandomTable(createCatalogManagedTableAtInit = true) { tableName =>
      val error = interceptWithUnwrapping[DeltaIllegalArgumentException] {
        spark.sql(s"ALTER TABLE $tableName UNSET TBLPROPERTIES ('$ICT_ENABLED_KEY')")
      }
      checkError(
        error,
        "DELTA_CANNOT_MODIFY_CATALOG_MANAGED_DEPENDENCIES",
        sqlState = "42616",
        parameters = Map[String, String]())
    }
  }

  test("ALTER TABLE should be blocked if attempts to downgrade Catalog-Managed") {
    withRandomTable(createCatalogManagedTableAtInit = true)  { tableName =>
      val error = intercept[DeltaTableFeatureException] {
        spark.sql(s"ALTER TABLE $tableName DROP FEATURE '${CatalogManagedTableFeature.name}'")
      }
      checkError(
        error,
        "DELTA_FEATURE_DROP_UNSUPPORTED_CLIENT_FEATURE",
        sqlState = "0AKDC",
        parameters = Map("feature" -> CatalogManagedTableFeature.name))
    }
  }

  test("Upgrade should be blocked since it is not supported yet") {
    withRandomTable(
      // Do not enable Catalog-Managed at the beginning when creating table
      createCatalogManagedTableAtInit = false
    ) { tableName =>
      val error = intercept[NotImplementedException] {
        spark.sql(s"ALTER TABLE $tableName SET TBLPROPERTIES " +
          s"('delta.feature.${CatalogManagedTableFeature.name}' = 'supported')")
      }
      assert(error.getMessage.contains("Upgrading to CatalogManaged table is not yet supported."))
    }
  }

  test("Dropping CatalogManaged dependent features should be blocked") {
    withRandomTable(createCatalogManagedTableAtInit = true) { tableName =>
      val error1 = intercept[DeltaTableFeatureException] {
        spark.sql(s"ALTER TABLE $tableName DROP FEATURE '${InCommitTimestampTableFeature.name}'")
      }
      checkError(
        error1,
        "DELTA_FEATURE_DROP_DEPENDENT_FEATURE",
        sqlState = "55000",
        parameters = Map(
          "feature" -> InCommitTimestampTableFeature.name,
          "dependentFeatures" -> CatalogManagedTableFeature.name))

      val error2 = intercept[DeltaTableFeatureException] {
        spark.sql(s"ALTER TABLE $tableName DROP FEATURE '${VacuumProtocolCheckTableFeature.name}'")
      }
      checkError(
        error2,
        "DELTA_FEATURE_DROP_DEPENDENT_FEATURE",
        sqlState = "55000",
        parameters = Map(
          "feature" -> VacuumProtocolCheckTableFeature.name,
          "dependentFeatures" -> CatalogManagedTableFeature.name))
    }
  }

  test("CM_COMMIT should be recorded in usage_log for normal CM commit") {
    withRandomTable(createCatalogManagedTableAtInit = true) { tableName =>
      val usageLog = Log4jUsageLogger.track {
        sql(s"INSERT INTO $tableName VALUES 3")
      }
      val commitStatsUsageLog = filterUsageRecords(usageLog, "delta.commit.stats")
      val commitStats = JsonUtils.fromJson[CommitStats](commitStatsUsageLog.head.blob)
      assert(commitStats.coordinatedCommitsInfo ===
        CoordinatedCommitsStats(
          coordinatedCommitsType = CoordinatedCommitType.CO_COMMIT.toString,
          commitCoordinatorName = "spark_catalog",
          commitCoordinatorConf = Map.empty))
    }
  }

  test("FS_TO_CO_UPGRADE_COMMIT should be recorded in usage_log when creating " +
       "CatalogManaged table") {
    withTable("t1") {
      val usageLog = Log4jUsageLogger.track {
        sql(s"CREATE TABLE t1 (id INT) USING delta TBLPROPERTIES " +
          s"('delta.feature.${CatalogManagedTableFeature.name}' = 'supported')")
      }
      val commitStatsUsageLog = filterUsageRecords(usageLog, "delta.commit.stats")
      val commitStats = JsonUtils.fromJson[CommitStats](commitStatsUsageLog.head.blob)
      assert(commitStats.coordinatedCommitsInfo ===
        CoordinatedCommitsStats(
          coordinatedCommitsType = CoordinatedCommitType.FS_TO_CO_UPGRADE_COMMIT.toString,
          // catalogTable is not available for FS_TO_CO_UPGRADE_COMMIT
          commitCoordinatorName = "CATALOG_MISSING",
          commitCoordinatorConf = Map.empty))
    }
  }

  test("Testing usage log for commit coordinator population for invalid path-based access") {
    // Clear all potential CC builders so that we are not entering the UT-only path when
    // populating the commit coordinator.
    clearBuilders()
    withTempDir { tempDir =>
      val log = DeltaLog.forTable(spark, tempDir.getCanonicalPath)
      // Create a path-based table so that we can simulate the scenario
      // where catalog table is not available.
      sql(s"CREATE TABLE delta.`$tempDir` (id INT) USING delta TBLPROPERTIES " +
        s"('delta.feature.${CatalogManagedTableFeature.name}' = 'supported')")
      val usageLog = Log4jUsageLogger.track {
        val error = intercept[IllegalStateException] {
          // Hence, we simply ignore the exception and focus on the usage log validation.
          sql(s"INSERT INTO TABLE delta.`$tempDir` VALUES (1), (2), (3)")
        }
        assert(error.getMessage.contains(
          "Path based access is not supported for Catalog-Managed table"))
      }.filter { log =>
        log.metric == "tahoeEvent" &&
          log.tags.getOrElse("opType", null) ==
            CatalogManagedUsageLogs.COMMIT_COORDINATOR_POPULATION_INVALID_PATH_BASED_ACCESS
      }

      assert(usageLog.nonEmpty, "Should have usage log for INVALID_PATH_BASED_ACCESS scenario")
      val usageLogBlob = JsonUtils.fromJson[Map[String, Any]](usageLog.head.blob)

      val logStore =
        "org.apache.spark.sql.delta.storage.DelegatingLogStore"

      validateUsageLogBlob(
        usageLogBlob,
        expectedPresentFields = Seq(
          "path",
          "version",
          "stackTrace",
          "latestCheckpointVersion",
          "checksumOpt",
          "properties",
          "logStore"
        ),
        expectedAbsentFields = Seq(
          "catalogTable.identifier",
          "catalogTable.tableType",
          "commitCoordinator.getClass"
        ),
        expectedValues = Map(
          "path" -> log.logPath.toString,
          "version" -> "0",
          "stackTrace" ->
            ("org.apache.spark.sql.delta.coordinatedcommits.CatalogManagedTableUtils$" +
             ".recordCommitCoordinatorPopulationUsageLog"),
          "latestCheckpointVersion" -> -1,
          // Only check for certain fields of `checksumOpt` since the entire map
          // is too large to validate.
          "checksumOpt" -> Map(
            "tableSizeBytes" -> 0,
            "numFiles" -> 0,
            "numMetadata" -> 1,
            "allFiles" -> List.empty
          ),
          "properties" -> Map(
            "delta.minReaderVersion" -> "3",
            "delta.minWriterVersion" -> "7",
            "delta.feature.appendOnly" -> "supported",
            "delta.feature.invariants" -> "supported",
            // To avoid potential naming change in the future.
            s"delta.feature.${CatalogManagedTableFeature.name}" -> "supported",
            "delta.feature.inCommitTimestamp" -> "supported",
            "delta.feature.vacuumProtocolCheck" -> "supported",
            "delta.enableInCommitTimestamps" -> "true"
          ),
          "logStore" -> logStore
        )
      )
    }
  }
}
