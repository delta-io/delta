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
import io.unitycatalog.client.model.ColumnTypeName;
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
    withNewTable(
        "managed_table",
        "id INT, active BOOLEAN",
        TableType.MANAGED,
        tableName -> {
          // Verify that properties are set on server. This can not be done by DESC EXTENDED.
          TablesApi tablesApi = new TablesApi(createClient());
          TableInfo tableInfo = tablesApi.getTable(tableName, false, false);
          List<ColumnInfo> columns = tableInfo.getColumns();
          Map<String, String> properties = tableInfo.getProperties();

          assertThat(columns).isNotNull();
          assertThat(columns.size()).isEqualTo(2);
          assertThat(columns.get(0).getName()).isEqualTo("id");
          assertThat(columns.get(0).getTypeName()).isEqualTo(ColumnTypeName.INT);
          assertThat(columns.get(1).getName()).isEqualTo("active");
          assertThat(columns.get(1).getTypeName()).isEqualTo(ColumnTypeName.BOOLEAN);

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
          // Verify that all entries are present
          expectedProperties.forEach(
              (key, value) -> assertThat(properties).containsEntry(key, value));
          // Lastly the timestamp value is always changing so skip checking its value
          assertThat(properties).containsKey("delta.lastCommitTimestamp");
        });
  }
}
