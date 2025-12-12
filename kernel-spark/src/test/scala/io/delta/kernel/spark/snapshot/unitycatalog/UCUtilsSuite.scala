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
package io.delta.kernel.spark.snapshot.unitycatalog

import java.net.URI
import java.util.{HashMap => JHashMap}

import io.delta.kernel.internal.tablefeatures.TableFeatures
import io.delta.kernel.spark.utils.CatalogTableTestUtils
import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Unit tests for [[UCUtils]].
 *
 * Tests use distinctive, high-entropy values that would fail if the implementation
 * had hardcoded defaults instead of actually extracting values from the inputs.
 */
class UCUtilsSuite extends SparkFunSuite with SharedSparkSession {

  // Use the same constants as CatalogTableUtils to ensure consistency
  private val FEATURE_CATALOG_MANAGED =
    TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX +
      TableFeatures.CATALOG_MANAGED_RW_FEATURE.featureName()
  private val FEATURE_SUPPORTED = TableFeatures.SET_TABLE_FEATURE_SUPPORTED_VALUE
  private val UC_TABLE_ID_KEY = UCCommitCoordinatorClient.UC_TABLE_ID_KEY
  private val UC_CATALOG_CONNECTOR = "io.unitycatalog.spark.UCSingleCatalog"

  // Distinctive values that would fail if hardcoded
  private val TABLE_ID_ALPHA = "uc_8f2b3c9a-d1e7-4a6f-b8c2"
  private val TABLE_PATH_ALPHA = "abfss://delta-store@prod.dfs.core.windows.net/warehouse/tbl_v3"
  private val UC_URI_ALPHA = "https://uc-server-westus2.example.net/api/2.1/unity-catalog"
  private val UC_TOKEN_ALPHA = "dapi_Xk7mP$9qRs#2vWz_prod"
  private val CATALOG_ALPHA = "uc_catalog_westus2_prod"

  // ==================== Helper Methods ====================

  private def makeNonUCTable(): CatalogTable = {
    CatalogTableTestUtils.createCatalogTable(locationUri = Some(new URI(TABLE_PATH_ALPHA)))
  }

  private def makeUCTable(
      tableId: String = TABLE_ID_ALPHA,
      tablePath: String = TABLE_PATH_ALPHA,
      catalogName: Option[String] = None): CatalogTable = {
    val storageProps = new JHashMap[String, String]()
    storageProps.put(FEATURE_CATALOG_MANAGED, FEATURE_SUPPORTED)
    storageProps.put(UC_TABLE_ID_KEY, tableId)

    CatalogTableTestUtils.createCatalogTable(
      catalogName = catalogName,
      storageProperties = storageProps,
      locationUri = Some(new URI(tablePath)))
  }

  private def withUCCatalogConfig(
      catalogName: String,
      uri: String,
      token: String)(testCode: => Unit): Unit = {
    val configs = Seq(
      s"spark.sql.catalog.$catalogName" -> UC_CATALOG_CONNECTOR,
      s"spark.sql.catalog.$catalogName.uri" -> uri,
      s"spark.sql.catalog.$catalogName.token" -> token)
    val originalValues = configs.map { case (key, _) => key -> spark.conf.getOption(key) }.toMap

    try {
      configs.foreach { case (key, value) => spark.conf.set(key, value) }
      testCode
    } finally {
      configs.foreach { case (key, _) =>
        originalValues.get(key).flatten match {
          case Some(v) => spark.conf.set(key, v)
          case None => spark.conf.unset(key)
        }
      }
    }
  }

  // ==================== Tests ====================

  test("returns empty for non-UC table") {
    val table = makeNonUCTable()
    val result = UCUtils.extractTableInfo(table, spark)
    assert(result.isEmpty, "Non-UC table should return empty Optional")
  }

  test("returns empty when UC table ID present but feature flag missing") {
    val storageProps = new JHashMap[String, String]()
    storageProps.put(UC_TABLE_ID_KEY, "orphan_id_9x7y5z")
    // No FEATURE_CATALOG_MANAGED - simulates corrupted/partial metadata

    val table = CatalogTableTestUtils.createCatalogTable(
      storageProperties = storageProps,
      locationUri = Some(new URI("gs://other-bucket/path")))
    val result = UCUtils.extractTableInfo(table, spark)
    assert(result.isEmpty, "Missing feature flag should return empty")
  }

  test("throws IllegalArgumentException for UC table with empty table ID") {
    val storageProps = new JHashMap[String, String]()
    storageProps.put(FEATURE_CATALOG_MANAGED, FEATURE_SUPPORTED)
    storageProps.put(UC_TABLE_ID_KEY, "")

    val table = CatalogTableTestUtils.createCatalogTable(
      storageProperties = storageProps,
      locationUri = Some(new URI("s3://empty-id-bucket/path")))
    val exception = intercept[IllegalArgumentException] {
      UCUtils.extractTableInfo(table, spark)
    }
    assert(exception.getMessage.contains("Cannot extract ucTableId"))
  }

  test("throws exception for UC table without location") {
    val storageProps = new JHashMap[String, String]()
    storageProps.put(FEATURE_CATALOG_MANAGED, FEATURE_SUPPORTED)
    storageProps.put(UC_TABLE_ID_KEY, "no_location_tbl_id_3k9m")

    val table = CatalogTableTestUtils.createCatalogTable(storageProperties = storageProps)
    // Spark throws AnalysisException when location is missing
    val exception = intercept[Exception] {
      UCUtils.extractTableInfo(table, spark)
    }
    assert(exception.getMessage.contains("locationUri") ||
      exception.getMessage.contains("location"))
  }

  test("throws IllegalArgumentException when no matching catalog configuration") {
    val table = makeUCTable(catalogName = Some("nonexistent_catalog_xyz"))

    val exception = intercept[IllegalArgumentException] {
      UCUtils.extractTableInfo(table, spark)
    }
    assert(exception.getMessage.contains("Unity Catalog configuration not found") ||
      exception.getMessage.contains("Cannot create UC client"))
  }

  test("extracts table info when UC catalog is properly configured") {
    val table = makeUCTable(catalogName = Some(CATALOG_ALPHA))

    withUCCatalogConfig(CATALOG_ALPHA, UC_URI_ALPHA, UC_TOKEN_ALPHA) {
      val result = UCUtils.extractTableInfo(table, spark)

      assert(result.isPresent, "Should return table info")
      val info = result.get()
      // Each assertion uses the specific expected value - would fail if hardcoded
      assert(info.getTableId == TABLE_ID_ALPHA, s"Table ID mismatch: got ${info.getTableId}")
      assert(
        info.getTablePath == TABLE_PATH_ALPHA,
        s"Table path mismatch: got ${info.getTablePath}")
      assert(info.getUcUri == UC_URI_ALPHA, s"UC URI mismatch: got ${info.getUcUri}")
      assert(info.getUcToken == UC_TOKEN_ALPHA, s"UC token mismatch: got ${info.getUcToken}")
    }
  }

  test("selects correct catalog when multiple catalogs configured") {
    // Use completely different values for each catalog to prove selection works
    val catalogBeta = "uc_catalog_eastus_staging"
    val ucUriBeta = "https://uc-server-eastus.example.net/api/2.1/uc"
    val ucTokenBeta = "dapi_Yz3nQ$8wRt#1vXa_staging"
    val tableIdBeta = "uc_tbl_staging_4d7e2f1a"
    val tablePathBeta = "s3://staging-bucket-us-east/delta/tables/v2"

    val catalogGamma = "uc_catalog_euwest_dev"
    val ucUriGamma = "https://uc-server-euwest.example.net/api/2.1/uc"
    val ucTokenGamma = "dapi_Jk5pL$3mNq#9vBc_dev"

    // Table is in catalogBeta
    val table = makeUCTable(
      tableId = tableIdBeta,
      tablePath = tablePathBeta,
      catalogName = Some(catalogBeta))

    val configs = Seq(
      // catalogGamma config (should NOT be used)
      s"spark.sql.catalog.$catalogGamma" -> UC_CATALOG_CONNECTOR,
      s"spark.sql.catalog.$catalogGamma.uri" -> ucUriGamma,
      s"spark.sql.catalog.$catalogGamma.token" -> ucTokenGamma,
      // catalogBeta config (should be used)
      s"spark.sql.catalog.$catalogBeta" -> UC_CATALOG_CONNECTOR,
      s"spark.sql.catalog.$catalogBeta.uri" -> ucUriBeta,
      s"spark.sql.catalog.$catalogBeta.token" -> ucTokenBeta)
    val originalValues = configs.map { case (key, _) => key -> spark.conf.getOption(key) }.toMap

    try {
      configs.foreach { case (key, value) => spark.conf.set(key, value) }

      val result = UCUtils.extractTableInfo(table, spark)
      assert(result.isPresent, "Should return table info")

      val info = result.get()
      // Verify it selected catalogBeta's config, not catalogGamma's
      assert(
        info.getUcUri == ucUriBeta,
        s"Should use catalogBeta's URI, got: ${info.getUcUri}")
      assert(info.getUcToken == ucTokenBeta, s"Should use catalogBeta's token, got: ${info.getUcToken}")
      assert(info.getTableId == tableIdBeta, s"Should extract tableIdBeta, got: ${info.getTableId}")
      assert(
        info.getTablePath == tablePathBeta,
        s"Should extract tablePathBeta, got: ${info.getTablePath}")
    } finally {
      configs.foreach { case (key, _) =>
        originalValues.get(key).flatten match {
          case Some(v) => spark.conf.set(key, v)
          case None => spark.conf.unset(key)
        }
      }
    }
  }
}
