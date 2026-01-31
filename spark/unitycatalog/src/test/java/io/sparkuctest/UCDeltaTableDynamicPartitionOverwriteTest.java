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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import io.unitycatalog.client.ApiException;
import io.unitycatalog.client.api.TablesApi;
import io.unitycatalog.client.model.TableInfo;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Test;

public class UCDeltaTableDynamicPartitionOverwriteTest extends UCDeltaTableIntegrationBaseTest {

  @Test
  public void testSaveAsTableWithDynamicPartitionOverwriteOnManagedTable() throws Exception {
    withNewTable(
        "dpo_saveastable_managed_test",
        "value INT, date_level STRING, hour_level INT",
        "date_level, hour_level",
        TableType.MANAGED,
        tableName -> {
          String tableId = accessTableId(tableName);

          // Step 1: Insert initial data into multiple partitions
          sql(
              "INSERT INTO %s VALUES "
                  + "(1, '2025-10-15', 10), "
                  + "(2, '2025-10-15', 11), "
                  + "(3, '2025-10-15', 12)",
              tableName);
          assertThat(tableId).isEqualTo(accessTableId(tableName));

          // Verify initial data
          check(
              tableName,
              List.of(
                  List.of("1", "2025-10-15", "10"),
                  List.of("2", "2025-10-15", "11"),
                  List.of("3", "2025-10-15", "12")));

          // Step 2: Create new data to overwrite partition hour_level=12
          // Using a new value (100) to verify the overwrite
          Dataset<Row> dfToWrite =
              spark().sql("SELECT 100 as value, '2025-10-15' as date_level, 12 as hour_level");

          // Step 3: Write using saveAsTable with partitionOverwriteMode=dynamic
          // This should only overwrite the partition hour_level=12, not the entire table
          dfToWrite
              .write()
              .format("delta")
              .mode(SaveMode.Overwrite)
              .option("partitionOverwriteMode", "dynamic")
              .saveAsTable(tableName);

          // Make sure the dynamic partition overwrite is an atomic txn, without dropping
          // and re-create the table.
          assertThat(tableId).isEqualTo(accessTableId(tableName));

          // Step 4: Verify the result
          // - Partitions hour_level=10 and hour_level=11 should be unchanged
          // - Partition hour_level=12 should have the new data (value=100)
          check(
              tableName,
              List.of(
                  List.of("1", "2025-10-15", "10"),
                  List.of("2", "2025-10-15", "11"),
                  List.of("100", "2025-10-15", "12")));

          System.out.println("Dynamic partition overwrite succeeded!");
        });
  }

  private String accessTableId(String fullTableName) throws ApiException {
    UnityCatalogInfo uc = unityCatalogInfo();
    TablesApi tablesApi = new TablesApi(uc.createApiClient());
    TableInfo tableInfo = tablesApi.getTable(fullTableName, false, false);
    return tableInfo.getTableId();
  }
}
