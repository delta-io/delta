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
package io.delta.kernel.spark.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient;
import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.junit.jupiter.api.Test;

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
  void testIsCatalogManaged_NullTable_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> CatalogTableUtils.isCatalogManaged(null),
        "Null table should throw NullPointerException");
  }

  @Test
  void testIsUnityCatalogManaged_NullTable_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> CatalogTableUtils.isUnityCatalogManagedTable(null),
        "Null table should throw NullPointerException");
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

  @Test
  void testGetCatalogName_NullTable_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> CatalogTableUtils.getCatalogName(null, null),
        "Null table should throw NullPointerException");
  }

  @Test
  void testGetUCCatalogConfig_NullTable_ThrowsException() {
    assertThrows(
        NullPointerException.class,
        () -> CatalogTableUtils.getUCCatalogConfig(null, null),
        "Null table should throw NullPointerException");
  }

  private static CatalogTable catalogTable(
      Map<String, String> properties, Map<String, String> storageProperties) {
    return CatalogTableTestUtils$.MODULE$.catalogTableWithProperties(properties, storageProperties);
  }

  private static CatalogTable catalogTableWithNullStorage(Map<String, String> properties) {
    return CatalogTableTestUtils$.MODULE$.catalogTableWithNullStorage(properties);
  }

  private static CatalogTable catalogTableWithNullStorageProperties(
      Map<String, String> properties) {
    return CatalogTableTestUtils$.MODULE$.catalogTableWithNullStorageProperties(properties);
  }
}
