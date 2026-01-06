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

          // Get current version and timestamp
          List<List<String>> versionResult =
              sql("SELECT max(version) FROM (DESCRIBE HISTORY %s)", tableName);
          long currentVersion = Long.parseLong(versionResult.get(0).get(0));

          List<List<String>> timestampResult =
              sql("SELECT max(timestamp) FROM (DESCRIBE HISTORY %s)", tableName);
          String currentTimestamp = timestampResult.get(0).get(0);

          // Add more data
          sql("INSERT INTO %s VALUES (4), (5)", tableName);

          // Test VERSION AS OF with DataFrameReader API
          List<List<String>> versionResult1 =
              sql("SELECT * FROM %s VERSION AS OF %d ORDER BY id", tableName, currentVersion);
          check(versionResult1, List.of(List.of("1"), List.of("2"), List.of("3")));

          // Test VERSION AS OF with SQL syntax
          List<List<String>> versionResult2 =
              sql("SELECT * FROM %s VERSION AS OF %d ORDER BY id", tableName, currentVersion);
          check(versionResult2, List.of(List.of("1"), List.of("2"), List.of("3")));

          // Test TIMESTAMP AS OF with SQL syntax
          List<List<String>> timestampResult1 =
              sql("SELECT * FROM %s TIMESTAMP AS OF '%s' ORDER BY id", tableName, currentTimestamp);
          check(timestampResult1, List.of(List.of("1"), List.of("2"), List.of("3")));
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testChangeDataFeedWithTimestamp(TableType tableType) throws Exception {
    withNewTable(
        "cdf_timestamp_test",
        "id INT",
        tableType,
        tableName -> {
          // Enable change data feed
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
              tableName);

          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Get current timestamp
          List<List<String>> timestampResult =
              sql("SELECT max(timestamp) FROM (DESCRIBE HISTORY %s)", tableName);
          String currentTimestamp = timestampResult.get(0).get(0);

          // Add more data
          sql("INSERT INTO %s VALUES (4), (5)", tableName);

          // CDC (Timestamps) are currently unsupported for Catalog owned tables.
          if (tableType == TableType.MANAGED) {
            Exception exception =
                Assertions.assertThrows(
                    Exception.class,
                    () ->
                        sql(
                            "SELECT id, _change_type FROM table_changes('%s', '%s') ORDER BY id",
                            tableName, currentTimestamp));
            String message = exception.getMessage();
            Assertions.assertTrue(
                message.toLowerCase().contains("path based access")
                    || message.toLowerCase().contains("catalog-managed"),
                () ->
                    "Expected error message about path based access or catalog-managed, but got: "
                        + message);
          } else {
            // For EXTERNAL tables, CDC should work
            List<List<String>> cdfResult =
                sql(
                    "SELECT id, _change_type FROM table_changes('%s', '%s') ORDER BY id",
                    tableName, currentTimestamp);
            check(cdfResult, List.of(List.of("4", "insert"), List.of("5", "insert")));
          }
        });
  }

  @ParameterizedTest
  @MethodSource("allTableTypes")
  public void testChangeDataFeedWithVersion(TableType tableType) throws Exception {
    withNewTable(
        "cdf_version_test",
        "id INT",
        tableType,
        tableName -> {
          // Enable change data feed
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
              tableName);

          // Setup initial data
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);

          // Get current version
          List<List<String>> versionResult =
              sql("SELECT max(version) FROM (DESCRIBE HISTORY %s)", tableName);
          long currentVersion = Long.parseLong(versionResult.get(0).get(0));

          // Add more data
          sql("INSERT INTO %s VALUES (4), (5)", tableName);

          // CDC (Versions) are currently unsupported for Catalog owned tables.
          if (tableType == TableType.MANAGED) {
            Exception exception =
                Assertions.assertThrows(
                    Exception.class,
                    () ->
                        sql(
                            "SELECT id, _change_type FROM table_changes('%s', %d) ORDER BY id",
                            tableName, currentVersion));
            String message = exception.getMessage();
            Assertions.assertTrue(
                message.contains("UPDATE_DELTA_METADATA")
                    || message.toLowerCase().contains("catalog-managed"),
                () ->
                    "Expected error message about UPDATE_DELTA_METADATA or catalog-managed, but got: "
                        + message);
          } else {
            // For EXTERNAL tables, CDC should work
            List<List<String>> cdfResult =
                sql(
                    "SELECT id, _change_type FROM table_changes('%s', %d) ORDER BY id",
                    tableName, currentVersion);
            check(cdfResult, List.of(List.of("4", "insert"), List.of("5", "insert")));
          }
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
          List<List<String>> pathResult =
              sql(
                  "SELECT location FROM (DESCRIBE EXTENDED %s) WHERE col_name = 'Location'",
                  tableName);

          if (pathResult.isEmpty()) {
            // Try alternative method to get location
            pathResult = sql("SHOW CREATE TABLE %s", tableName);
            // This test expects to be able to get the path, if we can't get it, fail
            Assertions.assertFalse(pathResult.isEmpty(), "Could not retrieve table location");
          }

          String tablePath = pathResult.get(0).get(0);

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
}
