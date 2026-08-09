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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

  @TestAllTableTypes
  public void testFsPropertiesHiddenFromTableProperties(TableType tableType) throws Exception {
    withNewTable(
        "fs_props_hidden",
        "id INT, name STRING",
        null, // no partition
        tableType,
        "'myCustomProp'='myCustomValue'",
        tableName -> {
          // SHOW TBLPROPERTIES returns one row per property (key, value).
          List<List<String>> propRows = sql("SHOW TBLPROPERTIES %s", tableName);
          List<String> propKeys = new ArrayList<>();
          for (List<String> row : propRows) {
            propKeys.add(row.get(0));
          }

          // Verify no key starts with option.fs. — these are internal catalog-vended
          // credentials/metadata that should not be user-visible.
          for (String key : propKeys) {
            Assertions.assertThat(key)
                .as("SHOW TBLPROPERTIES should not expose option.fs.* keys")
                .doesNotStartWith("option.fs.");
          }

          // Verify that non-fs storage properties and user-set table properties ARE
          // still present — confirming the filter is selective, not a blanket removal.
          Assertions.assertThat(propKeys)
              .as("User-set table properties should still be visible")
              .contains("myCustomProp");
          Assertions.assertThat(propKeys)
              .as("Delta table properties should still be visible")
              .contains("delta.minReaderVersion");

          // DESCRIBE EXTENDED returns a "Table Properties" row with all properties
          // in a single string like "[key1=val1,key2=val2,...]".
          boolean foundTableProperties = false;
          List<List<String>> descRows = sql("DESCRIBE EXTENDED %s", tableName);
          for (List<String> row : descRows) {
            if (row.size() >= 2 && "Table Properties".equals(row.get(0))) {
              foundTableProperties = true;
              Assertions.assertThat(row.get(1))
                  .as("DESCRIBE EXTENDED should not expose option.fs.* storage properties")
                  .doesNotContain("option.fs.");
              Assertions.assertThat(row.get(1))
                  .as("DESCRIBE EXTENDED should not expose fs.* storage properties either")
                  .doesNotContain("fs.");
              Assertions.assertThat(row.get(1))
                  .as("DESCRIBE EXTENDED should still show user-set properties")
                  .contains("myCustomProp=myCustomValue");
            }
          }
          Assertions.assertThat(foundTableProperties)
              .as("DESCRIBE EXTENDED must include a 'Table Properties' row")
              .isTrue();

          // Verify the data path still works — credentials still flow to the filesystem
          // via CatalogTable.storage.properties even though they are hidden from properties().
          sql("INSERT INTO %s VALUES (1, 'hello'), (2, 'world')", tableName);
          check(tableName, List.of(List.of("1", "hello"), List.of("2", "world")));
          sql("INSERT INTO %s VALUES (3, 'foo')", tableName);
          check(
              tableName,
              List.of(List.of("1", "hello"), List.of("2", "world"), List.of("3", "foo")));
        });
  }

  @Test
  public void testManagedTableMaintenancePolicy() throws Exception {
    withNewTable(
        "maintenance_blocked",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          for (int id = 1; id <= 4; id++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, id);
          }

          long filesBeforeOptimize =
              ((Number)
                      spark()
                          .sql(String.format("DESCRIBE DETAIL %s", tableName))
                          .head()
                          .getAs("numFiles"))
                  .longValue();
          Assertions.assertThat(filesBeforeOptimize).isGreaterThan(1L);

          sql("OPTIMIZE %s", tableName);
          long filesAfterOptimize =
              ((Number)
                      spark()
                          .sql(String.format("DESCRIBE DETAIL %s", tableName))
                          .head()
                          .getAs("numFiles"))
                  .longValue();
          Assertions.assertThat(filesAfterOptimize).isLessThan(filesBeforeOptimize);

          String tableLocation =
              spark().sql(String.format("DESCRIBE DETAIL %s", tableName)).head().getAs("location");
          Path rejectedStagedCommit =
              new Path(
                  tableLocation,
                  "_delta_log/_staged_commits/"
                      + "00000000000000000999.00000000-0000-0000-0000-000000000000.json");
          FileSystem tableFileSystem =
              rejectedStagedCommit.getFileSystem(spark().sparkContext().hadoopConfiguration());
          tableFileSystem.create(rejectedStagedCommit, false).close();
          tableFileSystem.setTimes(rejectedStagedCommit, 1L, -1L);
          Assertions.assertThat(tableFileSystem.exists(rejectedStagedCommit)).isTrue();

          String retentionCheck = "spark.databricks.delta.retentionDurationCheck.enabled";
          String previousRetentionCheck = spark().conf().get(retentionCheck, "true");
          try {
            spark().conf().set(retentionCheck, "false");
            List<List<String>> vacuumCandidates =
                sql("VACUUM %s RETAIN 0 HOURS DRY RUN", tableName);
            Assertions.assertThat(vacuumCandidates).isNotEmpty();
            List<Path> vacuumCandidatePaths = new ArrayList<>();
            for (List<String> candidate : vacuumCandidates) {
              Assertions.assertThat(candidate).hasSize(1);
              Path candidatePath = new Path(candidate.get(0));
              Assertions.assertThat(candidatePath.toUri().getPath())
                  .isNotEqualTo(rejectedStagedCommit.toUri().getPath());
              vacuumCandidatePaths.add(candidatePath);
            }
            sql("VACUUM %s RETAIN 0 HOURS", tableName);
            for (Path candidatePath : vacuumCandidatePaths) {
              FileSystem candidateFileSystem =
                  candidatePath.getFileSystem(spark().sparkContext().hadoopConfiguration());
              Assertions.assertThat(candidateFileSystem.exists(candidatePath)).isFalse();
            }
            Assertions.assertThat(tableFileSystem.exists(rejectedStagedCommit)).isTrue();
          } finally {
            spark().conf().set(retentionCheck, previousRetentionCheck);
          }

          assertThatThrownBy(() -> sql("REORG TABLE %s APPLY (PURGE)", tableName))
              .hasMessageContaining("DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION")
              .hasMessageContaining("REORG");
          assertThatThrownBy(
                  () ->
                      sql(
                          "REORG TABLE %s APPLY "
                              + "(UPGRADE UNIFORM (ICEBERG_COMPAT_VERSION = 2))",
                          tableName))
              .hasMessageContaining("DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION")
              .hasMessageContaining("REORG");
        });
  }
}
