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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** DDL test suite for Delta Table operations through Unity Catalog. */
public class UCDeltaTableDDLTest extends UCDeltaTableIntegrationBaseTest {

  // ---------------------------------------------------------------------------
  // TRUNCATE TABLE
  // ---------------------------------------------------------------------------

  @TestAllTableTypes
  public void testTruncateNonEmptyTable(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_non_empty",
        "id INT, name STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')", tableName);
          long versionBeforeTruncate = currentVersion(tableName);
          String ucTableIdBeforeTruncate = currentUcTableId(tableName);

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate + 1);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("TRUNCATE");
          assertThat(currentUcTableId(tableName)).isEqualTo(ucTableIdBeforeTruncate);

          // The table should remain usable after the DDL operation.
          sql("INSERT INTO %s VALUES (4, 'delta')", tableName);
          check(tableName, List.of(row("4", "delta")));
        });
  }

  @TestAllTableTypes
  public void testTruncateEmptyTableIsNoOp(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_empty",
        "id INT",
        tableType,
        tableName -> {
          long versionBeforeTruncate = currentVersion(tableName);
          String ucTableIdBeforeTruncate = currentUcTableId(tableName);

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("CREATE TABLE");
          assertThat(currentUcTableId(tableName)).isEqualTo(ucTableIdBeforeTruncate);
        });
  }

  @TestAllTableTypes
  public void testRepeatedTruncateIsIdempotent(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_repeated",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);
          long versionBeforeFirstTruncate = currentVersion(tableName);

          sql("TRUNCATE TABLE %s", tableName);
          long versionAfterFirstTruncate = currentVersion(tableName);
          assertThat(versionAfterFirstTruncate).isEqualTo(versionBeforeFirstTruncate + 1);
          check(tableName, List.of());

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionAfterFirstTruncate);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("TRUNCATE");
        });
  }

  @TestAllTableTypes
  public void testTruncatePartitionedTablePreservesMetadata(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_partitioned",
        "id INT, name STRING, part INT",
        "part",
        tableType,
        "'ddlSuiteProp'='preserved'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'alpha', 0), (2, 'beta', 1), (3, 'gamma', 1)", tableName);
          long versionBeforeTruncate = currentVersion(tableName);
          String partitionColumnsBeforeTruncate = describeDetailPartitionColumns(tableName);
          Map<String, String> propertiesBeforeTruncate = tableProperties(tableName);

          assertThat(partitionColumnsBeforeTruncate).contains("part");
          assertThat(propertiesBeforeTruncate).containsEntry("ddlSuiteProp", "preserved");

          sql("TRUNCATE TABLE %s", tableName);

          check(tableName, List.of());
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate + 1);
          assertThat(latestHistoryOperation(tableName)).isEqualTo("TRUNCATE");
          assertThat(describeDetailPartitionColumns(tableName))
              .isEqualTo(partitionColumnsBeforeTruncate);
          assertThat(tableProperties(tableName)).containsEntry("ddlSuiteProp", "preserved");

          sql("INSERT INTO %s VALUES (4, 'delta', 0), (5, 'epsilon', 2)", tableName);
          check(tableName, List.of(row("4", "delta", "0"), row("5", "epsilon", "2")));
        });
  }

  @TestAllTableTypes
  public void testTruncateAppendOnlyTableFailsAtomically(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_append_only",
        "id INT",
        null,
        tableType,
        "'delta.appendOnly'='true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);
          long versionBeforeTruncate = currentVersion(tableName);
          String ucTableIdBeforeTruncate = currentUcTableId(tableName);

          assertThrowsWithCauseContaining(
              "DELTA_CANNOT_MODIFY_APPEND_ONLY", () -> sql("TRUNCATE TABLE %s", tableName));

          check(tableName, List.of(row("1"), row("2")));
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate);
          assertThat(currentUcTableId(tableName)).isEqualTo(ucTableIdBeforeTruncate);
        });
  }

  @TestAllTableTypes
  public void testTruncateWithPartitionSpecFailsAtomically(TableType tableType) throws Exception {
    withNewTable(
        "ddl_truncate_partition_spec",
        "id INT, part INT",
        "part",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 0), (2, 1), (3, 1)", tableName);
          long versionBeforeTruncate = currentVersion(tableName);

          assertThatThrownBy(() -> sql("TRUNCATE TABLE %s PARTITION (part = 1)", tableName))
              .satisfies(
                  e ->
                      assertThat(causeChainMessages(e).toLowerCase(Locale.ROOT))
                          .contains("truncate")
                          .contains("partition"));

          check(tableName, List.of(row("1", "0"), row("2", "1"), row("3", "1")));
          assertThat(currentVersion(tableName)).isEqualTo(versionBeforeTruncate);
        });
  }

  private String latestHistoryOperation(String tableName) {
    return sql("DESCRIBE HISTORY %s LIMIT 1", tableName).get(0).get(4);
  }

  private String describeDetailPartitionColumns(String tableName) {
    Object partitionColumns =
        spark()
            .sql(String.format("DESCRIBE DETAIL %s", tableName))
            .select("partitionColumns")
            .collectAsList()
            .get(0)
            .get(0);
    return partitionColumns.toString();
  }

  private Map<String, String> tableProperties(String tableName) {
    Map<String, String> properties = new LinkedHashMap<>();
    for (List<String> row : sql("SHOW TBLPROPERTIES %s", tableName)) {
      if (row.size() >= 2) {
        properties.put(row.get(0), row.get(1));
      }
    }
    return properties;
  }

  private String causeChainMessages(Throwable throwable) {
    StringBuilder builder = new StringBuilder();
    Throwable current = throwable;
    while (current != null) {
      if (current.getMessage() != null) {
        builder.append(current.getMessage()).append('\n');
      }
      current = current.getCause();
    }
    return builder.toString();
  }
}
