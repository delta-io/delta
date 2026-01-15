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

import io.delta.tables.DeltaTable;
import java.util.List;
import org.junit.jupiter.api.Assertions;

/**
 * Read operation test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers time travel, change data feed, and path-based access scenarios. Tests are parameterized
 * to support different table types (EXTERNAL and MANAGED).
 */
public class UCDeltaTableReadTest extends UCDeltaTableIntegrationBaseTest {

  @TestAllTableTypes
  public void testTimeTravelRead(TableType tableType) throws Exception {
    withNewTable(
        "time_travel_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Get current version and timestamp.
          long currentVersion = currentVersion(tableName);
          String currentTimestamp = currentTimestamp(tableName);

          // Add more data
          sql("INSERT INTO %s VALUES (4), (5)", tableName);

          // Test VERSION AS OF with SQL syntax
          List<List<String>> versionResult =
              sql("SELECT * FROM %s VERSION AS OF %d ORDER BY id", tableName, currentVersion);
          check(versionResult, List.of(List.of("1"), List.of("2"), List.of("3")));

          // Test TIMESTAMP AS OF with SQL syntax
          List<List<String>> timestampResult =
              sql("SELECT * FROM %s TIMESTAMP AS OF '%s' ORDER BY id", tableName, currentTimestamp);
          check(timestampResult, List.of(List.of("1"), List.of("2"), List.of("3")));
        });
  }

  @TestAllTableTypes
  public void testChangeDataFeed(TableType tableType) throws Exception {
    withNewTable(
        "cdf_timestamp_test",
        "id INT",
        null,
        tableType,
        "'delta.enableChangeDataFeed'='true'",
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Get current version to determine the next version's timestamp
          long version0 = currentVersion(tableName);

          // Add more data
          sql("INSERT INTO %s VALUES (4), (5)", tableName);

          // Get the timestamp of the second insert (version after first)
          String timestamp = timestampForVersion(tableName, version0 + 1);

          // Query changes from the version after the 1st insert.
          check(
              sql(
                  "SELECT id, _change_type FROM table_changes('%s', %d) ORDER BY id",
                  tableName, version0 + 1),
              List.of(List.of("4", "insert"), List.of("5", "insert")));

          // Query changes from the timestamp after the 1st insert.
          check(
              sql(
                  "SELECT id, _change_type FROM table_changes('%s', '%s') ORDER BY id",
                  tableName, timestamp),
              List.of(List.of("4", "insert"), List.of("5", "insert")));
        });
  }

  @TestAllTableTypes
  public void testDeltaTableForPath(TableType tableType) throws Exception {
    withNewTable(
        "delta_table_for_path_test",
        "id INT",
        tableType,
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Get table path
          List<List<String>> describeResult = sql("DESCRIBE EXTENDED %s", tableName);

          // Find the Location row in the describe output
          String tablePath =
              describeResult.stream()
                  .filter(row -> row.size() >= 2 && "Location".equals(row.get(0)))
                  .map(row -> row.get(1))
                  .findFirst()
                  .orElse(null);
          Assertions.assertTrue(
              tablePath != null && !tablePath.isEmpty(),
              "Could not retrieve table location from DESCRIBE EXTENDED");

          // Path-based access isn't supported for catalog-owned (MANAGED) tables.
          if (tableType == TableType.MANAGED) {
            Assertions.assertThrows(
                Exception.class,
                () -> sql("SELECT * FROM delta.`%s`", tablePath),
                "For managed tables, path-based access should fail");
          } else {
            // For EXTERNAL tables, path-based access should work
            check(
                sql("SELECT * FROM delta.`%s` ORDER BY id", tablePath),
                List.of(List.of("1"), List.of("2"), List.of("3")));
          }
        });
  }

  private void check(List<List<String>> actual, List<List<String>> expected) {
    if (!actual.equals(expected)) {
      throw new AssertionError(
          String.format("Query results do not match.\nExpected: %s\nActual: %s", expected, actual));
    }
  }

  private long currentVersion(String tableName) {
    return DeltaTable.forName(spark(), tableName)
        .history()
        .selectExpr("max(version)")
        .collectAsList()
        .get(0)
        .getLong(0);
  }

  private String currentTimestamp(String tableName) {
    long currentVersion = currentVersion(tableName);
    return timestampForVersion(tableName, currentVersion);
  }

  private String timestampForVersion(String tableName, long version) {
    // Get timestamp for a specific version
    return DeltaTable.forName(spark(), tableName)
        .history()
        .filter("version = " + version)
        .selectExpr("timestamp")
        .collectAsList()
        .get(0)
        .getTimestamp(0)
        .toString();
  }
}
