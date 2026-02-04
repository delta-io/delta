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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Test suite for Unity Catalog staging table API with Delta tables.
 *
 * <p>Tests the createStagingTable API that obtains storage locations from Unity Catalog for
 * catalog-owned managed tables.
 */
public class UCDeltaStagingTableTest extends UCDeltaTableIntegrationBaseTest {

  private static final Logger LOG = Logger.getLogger(UCDeltaStagingTableTest.class);

  private String tableName;

  @AfterEach
  public void cleanUp() {
    if (tableName != null) {
      try {
        UnityCatalogInfo uc = unityCatalogInfo();
        String fullTableName =
            String.format("%s.%s.%s", uc.catalogName(), uc.schemaName(), tableName);
        sql("DROP TABLE IF EXISTS %s", fullTableName);
      } catch (Exception e) {
        LOG.warn("Failed to clean up table: " + tableName, e);
      }
      tableName = null;
    }
  }

  /**
   * Test creating a catalog-owned managed table using CTAS (Create Table As Select). This tests
   * that the staging table API is properly invoked to obtain the storage location from Unity
   * Catalog.
   */
  @Test
  public void testCreateCatalogOwnedTableWithCTAS() {
    UnityCatalogInfo uc = unityCatalogInfo();
    tableName = "staging_table_test_" + UUID.randomUUID().toString().replace("-", "_");

    // Create a catalog-owned managed table using CTAS
    sql(
        "CREATE TABLE %s.%s.%s USING DELTA TBLPROPERTIES "
            + "('delta.feature.catalogManaged'='supported') AS SELECT 1 as i, 'AAA' as s",
        uc.catalogName(), uc.schemaName(), tableName);

    // Verify the table was created successfully
    String fullTableName = String.format("%s.%s.%s", uc.catalogName(), uc.schemaName(), tableName);
    List<List<String>> result = sql("SELECT * FROM %s ORDER BY i", fullTableName);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).get(0)).isEqualTo("1");
    assertThat(result.get(0).get(1)).isEqualTo("AAA");

    // Verify table properties include catalog-managed feature
    List<List<String>> props = sql("SHOW TBLPROPERTIES %s", fullTableName);
    boolean hasCatalogManagedFeature =
        props.stream()
            .anyMatch(
                row ->
                    row.get(0).equals("delta.feature.catalogManaged")
                        && row.get(1).equals("supported"));
    assertThat(hasCatalogManagedFeature)
        .as("Table should have catalogManaged feature enabled")
        .isTrue();
  }
}
