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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.Map;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.junit.jupiter.api.Test;

/** Tests for {@link CatalogTableUtils}. */
class CatalogTableUtilsTest {

  @Test
  void catalogManagedFlagEnablesDetection() {
    CatalogTable table =
        catalogTableWithProperties(
            Collections.emptyMap(), Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect catalog management with flag");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should not detect Unity without ID");
  }

  @Test
  void previewFlagEnablesDetectionIgnoringCase() {
    CatalogTable table =
        catalogTableWithProperties(
            Collections.emptyMap(),
            Map.of(CatalogTableUtils.FEATURE_CATALOG_OWNED_PREVIEW, "SuPpOrTeD"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect via preview flag ignoring case");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should not detect Unity without ID");
  }

  @Test
  void noFlagsMeansNotManaged() {
    CatalogTable table = catalogTableWithProperties(Collections.emptyMap());

    assertFalse(
        CatalogTableUtils.isCatalogManaged(table), "Should not detect management without flags");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Should not detect Unity without ID or flags");
  }

  @Test
  void unityManagementRequiresFlagAndId() {
    CatalogTable table =
        catalogTableWithProperties(
            Collections.emptyMap(),
            Map.of(
                CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported",
                io.delta.storage.commit.uccommitcoordinator.UCCommitCoordinatorClient
                        .UC_TABLE_ID_KEY,
                    "abc-123"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect general catalog management");
    assertTrue(
        CatalogTableUtils.isUnityCatalogManagedTable(table), "Should detect Unity with ID present");
  }

  @Test
  void unityManagementFailsWithoutId() {
    CatalogTable table =
        catalogTableWithProperties(
            Collections.emptyMap(), Map.of(CatalogTableUtils.FEATURE_CATALOG_MANAGED, "supported"));

    assertTrue(
        CatalogTableUtils.isCatalogManaged(table), "Should detect general catalog management");
    assertFalse(
        CatalogTableUtils.isUnityCatalogManagedTable(table),
        "Should fail Unity detection without ID");
  }

  @Test
  void storagePropertiesExposeStorageMetadata() {
    Map<String, String> storageProps = Map.of("fs.test.option", "value", "dfs.conf.key", "abc");
    CatalogTable table = catalogTableWithProperties(Collections.emptyMap(), storageProps);

    assertEquals(
        storageProps,
        CatalogTableUtils.getStorageProperties(table),
        "Should surface storage properties published by the catalog");
  }

  /**
   * Creates a CatalogTable with the given properties. This is a helper method to create a
   * CatalogTable for testing purposes - see interface {@link CatalogTable} for more details.
   *
   * @param properties the properties to set on the CatalogTable
   * @return a CatalogTable with the given properties
   */
  private static CatalogTable catalogTableWithProperties(Map<String, String> properties) {
    return catalogTableWithProperties(properties, Collections.emptyMap());
  }

  private static CatalogTable catalogTableWithProperties(
      Map<String, String> properties, Map<String, String> storageProperties) {
    return CatalogTableTestUtils$.MODULE$.catalogTableWithProperties(properties, storageProperties);
  }
}
