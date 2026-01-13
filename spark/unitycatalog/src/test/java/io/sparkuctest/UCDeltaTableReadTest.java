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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Read operation test suite for Delta Table operations through Unity Catalog.
 *
 * <p>Covers time travel, change data feed, streaming, and path-based access scenarios. Tests are
 * parameterized to support different table types (EXTERNAL and MANAGED).
 */
public class UCDeltaTableReadTest extends UCDeltaTableIntegrationBaseTest {

  @ParameterizedTest
  @MethodSource("allTableTypes")
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

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testChangeDataFeedWithTimestamp(TableType tableType) throws Exception {
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

          // Query changes from the timestamp of the second insert
          List<List<String>> cdfResult =
              sql(
                  "SELECT id, _change_type FROM table_changes('%s', '%s') ORDER BY id",
                  tableName, timestamp);
          check(cdfResult, List.of(List.of("4", "insert"), List.of("5", "insert")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testChangeDataFeedWithVersion(TableType tableType) throws Exception {
    withNewTable(
        "cdf_version_test",
        "id INT",
        null,
        tableType,
        "'delta.enableChangeDataFeed'='true'",
        tableName -> {
          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Get current version
          long currentVersion = currentVersion(tableName);

          // Add more data
          sql("INSERT INTO %s VALUES (4), (5)", tableName);

          // Query changes from the version after first insert
          List<List<String>> cdfResult =
              sql(
                  "SELECT id, _change_type FROM table_changes('%s', %d) ORDER BY id",
                  tableName, currentVersion + 1);
          check(cdfResult, List.of(List.of("4", "insert"), List.of("5", "insert")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
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
          String tablePathMutable = null;
          for (List<String> row : describeResult) {
            if (row.size() >= 2 && "Location".equals(row.get(0))) {
              tablePathMutable = row.get(1);
              break;
            }
          }

          if (tablePathMutable == null || tablePathMutable.isEmpty()) {
            // If location not found, fail the test
            Assertions.fail("Could not retrieve table location from DESCRIBE EXTENDED");
          }

          final String tablePath = tablePathMutable;

          // Path-based access isn't supported for catalog-owned (MANAGED) tables.
          if (tableType == TableType.MANAGED) {
            // For managed tables, path-based access should fail
            Exception exception =
                Assertions.assertThrows(
                    Exception.class, () -> sql("SELECT * FROM delta.`%s`", tablePath));
            String message = exception.getMessage();
            Assertions.assertTrue(
                message.contains("AccessDeniedException")
                    || message.toLowerCase().contains("access denied")
                    || message.toLowerCase().contains("catalog-managed"),
                () ->
                    "Expected AccessDeniedException for path-based access on managed table, but got: "
                        + message);
          } else {
            // For EXTERNAL tables, path-based access should work
            List<List<String>> pathBasedResult =
                sql("SELECT * FROM delta.`%s` ORDER BY id", tablePath);
            check(pathBasedResult, List.of(List.of("1"), List.of("2"), List.of("3")));
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
    // Get current version
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
