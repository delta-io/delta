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

package io.sparkuctest;

import static org.assertj.core.api.Assertions.assertThat;

import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.TableInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Test suite for creating UC Managed Delta Tables. */
public class UCManagedTableCreationTest extends UCDeltaTableIntegrationBaseTest {

  @Test
  public void testCreateManagedTable() throws Exception {
    // TODO: make this a thorough test of managed table creation with different parameters.
    UnityCatalogInfo catalogInfo = unityCatalogInfo();
    String tableName = "test_managed_table";
    String fullTableName = catalogInfo.catalogName() + ".default." + tableName;
    String tableSchema = "id INT, active BOOLEAN";
    try {
      sql(
          "CREATE TABLE %s (%s) USING DELTA "
              + "TBLPROPERTIES ('delta.feature.catalogManaged'='supported', 'Foo'='Bar')",
          fullTableName, tableSchema);

      // Verify that properties are set on server. This can not be done by DESC EXTENDED.
      TablesApi tablesApi = new TablesApi(catalogInfo.createApiClient());
      TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
      List<ColumnInfo> columns = tableInfo.getColumns();
      Map<String, String> properties = tableInfo.getProperties();

      assertThat(columns).isNotNull();
      // At this point table schema can not be sent to server yet because it won't be updated
      // later and that would cause problem.
      assertThat(columns).isEmpty();

      final String SUPPORTED = "supported";
      HashMap<String, String> expectedProperties = new HashMap<>();
      expectedProperties.put("delta.enableInCommitTimestamps", "true");
      expectedProperties.put("delta.feature.appendOnly", SUPPORTED);
      expectedProperties.put("delta.feature.catalogManaged", SUPPORTED);
      expectedProperties.put("delta.feature.inCommitTimestamp", SUPPORTED);
      expectedProperties.put("delta.feature.invariants", SUPPORTED);
      expectedProperties.put("delta.feature.vacuumProtocolCheck", SUPPORTED);
      expectedProperties.put("delta.lastUpdateVersion", "0");
      expectedProperties.put("delta.minReaderVersion", "3");
      expectedProperties.put("delta.minWriterVersion", "7");
      expectedProperties.put("io.unitycatalog.tableId", tableInfo.getTableId());
      // User specified custom table property is also sent.
      expectedProperties.put("Foo", "Bar");
      // Verify that all entries are present
      expectedProperties.forEach((key, value) -> assertThat(properties).containsEntry(key, value));
      // Lastly the timestamp value is always changing so skip checking its value
      assertThat(properties).containsKey("delta.lastCommitTimestamp");
    } finally {
      sql("DROP TABLE IF EXISTS %s", fullTableName);
    }
  }
}
