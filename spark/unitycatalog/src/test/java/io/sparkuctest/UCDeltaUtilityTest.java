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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class UCDeltaUtilityTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testDescribeHistory(TableType tableType) throws Exception {
    withNewTable(
        "describe_history",
        "id INT, name STRING",
        tableType,
        tableName -> {
          // Assert the initial history.
          assertDescribeHistory(tableName, List.of(List.of("0", "CREATE TABLE", "Serializable")));

          // The 1st operation.
          sql("INSERT INTO %s VALUES (1, 'AAA')", tableName);
          check(tableName, List.of(List.of("1", "AAA")));
          // Assert the history.
          assertDescribeHistory(
              tableName,
              List.of(
                  List.of("1", "WRITE", "Serializable"),
                  List.of("0", "CREATE TABLE", "Serializable")));

          // The 2nd operation.
          sql("UPDATE %s SET name='BBB' WHERE id = 1", tableName, tableName);
          check(tableName, List.of(List.of("1", "BBB")));
          // Assert the history
          assertDescribeHistory(
              tableName,
              List.of(
                  List.of("2", "UPDATE", "Serializable"),
                  List.of("1", "WRITE", "Serializable"),
                  List.of("0", "CREATE TABLE", "Serializable")));
        });
  }

  private void assertDescribeHistory(String tableName, List<List<String>> expected) {
    List<List<String>> results = sql("DESCRIBE HISTORY %s", tableName);

    // Only assert below columns, since other columns are null or undetermined (such as timestamp).
    // index  0: version
    // index  4: operation
    // index 10: isolationLevel
    List<List<String>> prunedResults = new ArrayList<>();
    for (List<String> row : results) {
      prunedResults.add(List.of(row.get(0), row.get(4), row.get(10)));
    }

    Assertions.assertThat(prunedResults).isEqualTo(expected);
  }

  @Test
  public void testMaintenanceOpsBlockedOnManagedTable() throws Exception {
    withNewTable(
        "maintenance_blocked",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);

          assertThatThrownBy(() -> sql("OPTIMIZE %s", tableName))
              .hasMessageContaining("DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION")
              .hasMessageContaining("OPTIMIZE");

          assertThatThrownBy(() -> sql("VACUUM %s", tableName))
              .hasMessageContaining("DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION")
              .hasMessageContaining("VACUUM");

          assertThatThrownBy(() -> sql("REORG TABLE %s APPLY (PURGE)", tableName))
              .hasMessageContaining("DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION")
              .hasMessageContaining("OPTIMIZE");
        });
  }
}
