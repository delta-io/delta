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

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.TableInfo;

/** Focused integration tests for CREATE TABLE via the CCv2 + Kernel path. */
public class UCDeltaTableKernelCreateTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testStrictModeCreateTable(TableType tableType) throws Exception {
    String originalMode = spark().conf().get("spark.databricks.delta.v2.enableMode", "NONE");
    spark().conf().set("spark.databricks.delta.v2.enableMode", "STRICT");

    try {
      withNewTable(
          "kernel_strict_create",
          "id INT, name STRING",
          tableType,
          tableName -> {
            check(tableName, java.util.List.of());
            assertDeltaTableRegistered(tableName, tableType, unityCatalogInfo());
          });
    } finally {
      spark().conf().set("spark.databricks.delta.v2.enableMode", originalMode);
    }
  }

  private void assertDeltaTableRegistered(
      String tableName, TableType tableType, UnityCatalogInfo uc) throws Exception {
    TablesApi tablesApi = new TablesApi(uc.createApiClient());
    TableInfo tableInfo = tablesApi.getTable(tableName, false, false);

    assertThat(tableInfo.getDataSourceFormat().name()).isEqualTo(DataSourceFormat.DELTA.name());
    assertThat(tableInfo.getTableType().name()).isEqualTo(tableType.name());
    assertThat(tableInfo.getStorageLocation()).isNotNull();
  }
}
