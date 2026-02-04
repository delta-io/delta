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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests Unity Catalog staging table API with CTAS/RTAS for managed tables.
 *
 * <p>Validates that CREATE TABLE AS SELECT and REPLACE TABLE AS SELECT properly invoke UC's staging
 * table API to provision storage locations for catalog-managed tables.
 */
public class UCDeltaStageCreateOrReplaceTest extends UCDeltaTableIntegrationBaseTest {

  private static final String MANAGED =
      "TBLPROPERTIES ('delta.feature.catalogManaged'='supported')";

  @Test
  public void testCTAS() {
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = "ctas_test";
    String table = uc.catalogName() + "." + uc.schemaName() + "." + tableName;
    try {
      sql("CREATE TABLE %s USING DELTA %s AS SELECT 1 as id, 'Alice' as name", table, MANAGED);

      assertThat(sql("SELECT * FROM %s", table))
          .hasSize(1)
          .first()
          .asList()
          .containsExactly("1", "Alice");

      assertManagedTable(table);
    } finally {
      sql("DROP TABLE IF EXISTS %s", table);
    }
  }

  @Test
  @Disabled("RTAS tests disabled - needs property sync fix")
  public void testRTAS() {
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = "rtas_test";
    String table = uc.catalogName() + "." + uc.schemaName() + "." + tableName;
    try {
      sql("CREATE TABLE %s USING DELTA %s AS SELECT 1 as id, 'Old' as value", table, MANAGED);
      assertThat(sql("SELECT value FROM %s", table).get(0).get(0)).isEqualTo("Old");

      sql("REPLACE TABLE %s USING DELTA %s AS SELECT 2 as id, 'New' as value", table, MANAGED);
      assertThat(sql("SELECT value FROM %s", table).get(0).get(0)).isEqualTo("New");

      assertManagedTable(table);
    } finally {
      sql("DROP TABLE IF EXISTS %s", table);
    }
  }

  @Test
  public void testDynamicPartitionOverwrite() {
    UnityCatalogInfo uc = unityCatalogInfo();
    String tableName = "dynamic_partition";
    String table = uc.catalogName() + "." + uc.schemaName() + "." + tableName;
    try {
      sql(
          "CREATE TABLE %s USING DELTA PARTITIONED BY (date) %s AS SELECT 1 as id, '2026-01-01' as date",
          table, MANAGED);
      sql("INSERT INTO %s VALUES (2, '2026-01-01'), (3, '2026-01-02')", table);

      sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = true");
      sql("INSERT OVERWRITE %s VALUES (4, '2026-01-01')", table);
      sql("SET spark.databricks.delta.dynamicPartitionOverwrite.enabled = false");

      List<List<String>> result = sql("SELECT * FROM %s ORDER BY id", table);
      assertThat(result).hasSize(2);
      assertThat(result.get(0)).containsExactly("3", "2026-01-02"); // Unchanged
      assertThat(result.get(1)).containsExactly("4", "2026-01-01"); // Replaced
    } finally {
      sql("DROP TABLE IF EXISTS %s", table);
    }
  }

  private void assertManagedTable(String table) {
    assertThat(sql("SHOW TBLPROPERTIES %s", table))
        .anyMatch(
            row ->
                row.get(0).equals("delta.feature.catalogManaged")
                    && row.get(1).equals("supported"));
  }
}
