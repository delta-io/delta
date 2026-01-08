/*
 * Copyright (2026) The Delta Lake Project Authors.
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
package org.apache.spark.sql.delta.util

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

/**
 * Test suite for [[CatalogTableUtils]].
 */
class CatalogTableUtilsSuite extends SparkFunSuite {

  // Define constants locally to avoid dependency on kernel-api module
  // These match: TableFeatures.SET_TABLE_FEATURE_SUPPORTED_PREFIX + featureName
  private val FEATURE_CATALOG_MANAGED = "delta.feature.catalogManaged"
  private val FEATURE_CATALOG_OWNED_PREVIEW = "delta.feature.catalogOwned-preview"

  private def catalogTable(
      tableProperties: Map[String, String],
      storageProperties: Map[String, String]): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier("test_table"),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = storageProperties
      ),
      schema = new StructType(),
      properties = tableProperties
    )
  }

  test("isCatalogManaged - catalog flag enabled returns true") {
    val table = catalogTable(Map.empty, Map(FEATURE_CATALOG_MANAGED -> "supported"))
    assert(CatalogTableUtils.isCatalogManaged(table),
      "Catalog-managed flag should enable detection")
  }

  test("isCatalogManaged - preview flag enabled returns true") {
    val table = catalogTable(Map.empty, Map(FEATURE_CATALOG_OWNED_PREVIEW -> "SuPpOrTeD"))
    assert(CatalogTableUtils.isCatalogManaged(table),
      "Preview flag should enable detection ignoring case")
  }

  test("isCatalogManaged - no flags returns false") {
    val table = catalogTable(Map.empty, Map.empty)
    assert(!CatalogTableUtils.isCatalogManaged(table),
      "No catalog flags should disable detection")
  }

  test("isUnityCatalogManagedTable - flag and ID present returns true") {
    val table = catalogTable(
      Map.empty,
      Map(
        FEATURE_CATALOG_MANAGED -> "supported",
        UCCommitCoordinatorClient.UC_TABLE_ID_KEY -> "test-uc-id"
      )
    )
    assert(CatalogTableUtils.isUnityCatalogManagedTable(table),
      "Unity Catalog detection should require flag and identifier")
  }

  test("isUnityCatalogManagedTable - missing ID returns false") {
    val table = catalogTable(Map.empty, Map(FEATURE_CATALOG_MANAGED -> "supported"))
    assert(!CatalogTableUtils.isUnityCatalogManagedTable(table),
      "Missing table identifier should break Unity detection")
  }

  test("isUnityCatalogManagedTable - preview flag without ID returns false") {
    val table = catalogTable(Map.empty, Map(FEATURE_CATALOG_OWNED_PREVIEW -> "supported"))
    assert(!CatalogTableUtils.isUnityCatalogManagedTable(table),
      "Preview flag without ID should not be considered Unity managed")
  }

  test("isCatalogManaged - null storage returns false") {
    val table = catalogTable(Map.empty, Map.empty).copy(storage = null)
    assert(!CatalogTableUtils.isCatalogManaged(table),
      "Null storage should not be considered catalog managed")
  }

  test("isUnityCatalogManagedTable - null storage returns false") {
    val table = catalogTable(Map.empty, Map.empty).copy(storage = null)
    assert(!CatalogTableUtils.isUnityCatalogManagedTable(table),
      "Null storage should not be considered Unity managed")
  }

  test("isCatalogManaged - null storage properties returns false") {
    val table = catalogTable(Map.empty, Map.empty)
      .copy(storage = CatalogStorageFormat(None, None, None, None, false, null))
    assert(!CatalogTableUtils.isCatalogManaged(table),
      "Null storage properties should not be considered catalog managed")
  }

  test("isUnityCatalogManagedTable - null storage properties returns false") {
    val table = catalogTable(Map.empty, Map.empty)
      .copy(storage = CatalogStorageFormat(None, None, None, None, false, null))
    assert(!CatalogTableUtils.isUnityCatalogManagedTable(table),
      "Null storage properties should not be considered Unity managed")
  }
}
