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
package io.delta.spark.internal.v2.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.junit.jupiter.api.Test;
import scala.Option;

/** Tests for {@link CatalogTableUtils}. */
class CatalogTableUtilsTest {

  @Test
  void testIsCatalogManaged_CatalogFlagEnabled_ReturnsTrue() {
    CatalogTable table =
        catalogTable(
            Collections.emptyMap(), Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Catalog-managed flag should enable detection");
  }

  @Test
  void testIsCatalogManaged_PreviewFlagEnabled_ReturnsTrue() {
    CatalogTable table =
        catalogTable(
            Collections.emptyMap(),
            Map.of(CatalogTableUtils.FEATURE_CATALOG_OWNED_PREVIEW, "SuPpOrTeD"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table),
        "Preview flag should enable detection ignoring case");
  }

  @Test
  void testIsCatalogManaged_NoFlags_ReturnsFalse() {
    CatalogTable table = catalogTable(Collections.emptyMap(), Collections.emptyMap());

    assertFalse(
        CatalogTableUtils.isCatalogManaged(table), "No catalog flags should disable detection");
  }

  @Test
  void testIsUnityCatalogManaged_FlagAndIdPresent_ReturnsTrue() {
    CatalogTable table =
        catalogTable(
            Collections.emptyMap(),
            Map.of(
                CatalogTableUtils.FEATURE_CATALOG_MANAGED,
                "supported",
                UCCommitCoordinatorClient.UC_TABLE_ID_KEY,
                "abc-123"));

    assertTrue(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Unity Catalog detection should require flag and identifier");
  }

  @Test
  void testIsUnityCatalogManaged_MissingId_ReturnsFalse() {
    CatalogTable table =
        catalogTable(
            Collections.emptyMap(), Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Missing table identifier should break Unity detection");
  }

  @Test
  void testIsUnityCatalogManaged_PreviewFlagMissingId_ReturnsFalse() {
    CatalogTable table =
        catalogTable(
            Collections.emptyMap(),
            Map.of(CatalogTableUtils.FEATURE_CATALOG_OWNED_PREVIEW, "supported"));

    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Preview flag without ID should not be considered Unity managed");
  }

  @Test
  void testIsCatalogManaged_NullStorage_ReturnsFalse() {
    CatalogTable table = catalogTableWithNullStorage(Collections.emptyMap());

    assertFalse(
        CatalogTableUtils.isCatalogManaged(table),
        "Null storage should not be considered catalog managed");
  }

  @Test
  void testIsUnityCatalogManaged_NullStorage_ReturnsFalse() {
    CatalogTable table = catalogTableWithNullStorage(Collections.emptyMap());

    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Null storage should not be considered Unity managed");
  }

  @Test
  void testIsCatalogManaged_NullStorageProperties_ReturnsFalse() {
    CatalogTable table = catalogTableWithNullStorageProperties(Collections.emptyMap());

    assertFalse(
        CatalogTableUtils.isCatalogManaged(table),
        "Null storage properties should not be considered catalog managed");
  }

  @Test
  void testIsUnityCatalogManaged_NullStorageProperties_ReturnsFalse() {
    CatalogTable table = catalogTableWithNullStorageProperties(Collections.emptyMap());

    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Null storage properties should not be considered Unity managed");
  }

  private static CatalogTable catalogTable(
      Map<String, String> properties, Map<String, String> storageProperties) {
    return CatalogTableTestUtils$.MODULE$.createCatalogTable(
        "tbl" /* tableName */,
        Option.empty() /* catalogName */,
        properties,
        storageProperties,
        Option.empty() /* locationUri */,
        false /* nullStorage */,
        false /* nullStorageProperties */);
  }

  private static CatalogTable catalogTableWithNullStorage(Map<String, String> properties) {
    return CatalogTableTestUtils$.MODULE$.createCatalogTable(
        "tbl" /* tableName */,
        Option.empty() /* catalogName */,
        properties,
        new HashMap<>() /* storageProperties */,
        Option.empty() /* locationUri */,
        true /* nullStorage */,
        false /* nullStorageProperties */);
  }

  private static CatalogTable catalogTableWithNullStorageProperties(
      Map<String, String> properties) {
    return CatalogTableTestUtils$.MODULE$.createCatalogTable(
        "tbl" /* tableName */,
        Option.empty() /* catalogName */,
        properties,
        new HashMap<>() /* storageProperties */,
        Option.empty() /* locationUri */,
        false /* nullStorage */,
        true /* nullStorageProperties */);
  }
}
