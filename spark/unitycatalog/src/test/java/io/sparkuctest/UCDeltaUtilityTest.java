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
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Assumptions;
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
  public void testOptimizeRewritesManagedTableFiles() throws Exception {
    withNewTable(
        "optimize_rewrites_files",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          sql("INSERT INTO %s VALUES (2)", tableName);
          sql("INSERT INTO %s VALUES (3)", tableName);

          Map<String, List<Integer>> filesBefore = activeFileRows(tableName);
          Assertions.assertThat(filesBefore.values())
              .containsExactlyInAnyOrder(List.of(1), List.of(2), List.of(3));

          long versionBefore = currentVersion(tableName);
          sql("OPTIMIZE %s", tableName);

          Map<String, List<Integer>> filesAfter = activeFileRows(tableName);
          Assertions.assertThat(filesAfter).hasSize(1);
          Assertions.assertThat(filesAfter.values()).containsExactly(List.of(1, 2, 3));
          Assertions.assertThat(filesAfter.keySet())
              .doesNotContainAnyElementsOf(filesBefore.keySet());
          Assertions.assertThat(currentVersion(tableName)).isEqualTo(versionBefore + 1);
          assertLatestOperation(tableName, "OPTIMIZE");
        });
  }

  @Test
  public void testVacuumDeletesObsoleteManagedTableFiles() throws Exception {
    withNewTable(
        "vacuum_deletes_files",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);
          sql("INSERT INTO %s VALUES (2)", tableName);
          sql("INSERT INTO %s VALUES (3)", tableName);

          Set<String> obsoleteFiles = activeFileRows(tableName).keySet();
          sql("OPTIMIZE %s", tableName);
          Map<String, List<Integer>> activeFiles = activeFileRows(tableName);
          Assertions.assertThat(activeFiles).hasSize(1);

          String retentionCheck = "spark.databricks.delta.retentionDurationCheck.enabled";
          String previousRetentionCheck = spark().conf().get(retentionCheck, "true");
          spark().conf().set(retentionCheck, "false");
          try {
            Set<String> candidatesBefore = vacuumCandidateNames(tableName);
            Set<String> obsoleteFileNames =
                obsoleteFiles.stream().map(this::fileName).collect(Collectors.toSet());
            Assertions.assertThat(candidatesBefore).containsAll(obsoleteFileNames);

            sql("VACUUM %s RETAIN 0 HOURS", tableName);

            Assertions.assertThat(vacuumCandidateNames(tableName))
                .doesNotContainAnyElementsOf(obsoleteFileNames);
            Assertions.assertThat(activeFileRows(tableName)).isEqualTo(activeFiles);
            check(tableName, List.of(row("1"), row("2"), row("3")));
          } finally {
            spark().conf().set(retentionCheck, previousRetentionCheck);
          }
        });
  }

  @Test
  public void testReorgPurgesManagedTableDeletionVector() throws Exception {
    withNewTable(
        "reorg_purges_deletion_vector",
        "id INT",
        TableType.MANAGED,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2), (3)", tableName);
          sql("DELETE FROM %s WHERE id = 2", tableName);

          Map<String, String> deleteMetrics = latestOperationMetrics(tableName, "DELETE");
          Assertions.assertThat(Long.parseLong(deleteMetrics.get("numDeletionVectorsAdded")))
              .isGreaterThan(0);
          Map<String, List<Integer>> filesBefore = activeFileRows(tableName);
          Assertions.assertThat(allRows(filesBefore)).containsExactly(1, 3);

          sql("REORG TABLE %s APPLY (PURGE)", tableName);

          Map<String, String> reorgMetrics = latestOperationMetrics(tableName, "REORG");
          long removedDeletionVectors =
              Long.parseLong(reorgMetrics.get("numDeletionVectorsRemoved"));
          long addedFiles = Long.parseLong(reorgMetrics.get("numAddedFiles"));
          long removedFiles = Long.parseLong(reorgMetrics.get("numRemovedFiles"));
          Assertions.assertThat(removedDeletionVectors).isGreaterThan(0);
          Assertions.assertThat(addedFiles).isGreaterThan(0);
          Assertions.assertThat(removedFiles).isGreaterThan(0);
          Map<String, List<Integer>> filesAfter = activeFileRows(tableName);
          Assertions.assertThat(allRows(filesAfter)).containsExactly(1, 3);
          Assertions.assertThat(
                  filesAfter.keySet().stream()
                      .filter(path -> !filesBefore.containsKey(path))
                      .count())
              .isEqualTo(addedFiles);
          Assertions.assertThat(
                  filesBefore.keySet().stream()
                      .filter(path -> !filesAfter.containsKey(path))
                      .count())
              .isEqualTo(removedFiles);
        });
  }

  @Test
  public void testCheckpointDeletesExpiredManagedTableLogs() throws Exception {
    Assumptions.assumeFalse(
        isUCRemoteConfigured(), "This test changes timestamps in the local fake S3 filesystem.");
    withNewTable(
        "checkpoint_deletes_logs",
        "id INT",
        null,
        TableType.MANAGED,
        "'delta.checkpointInterval'='1', "
            + "'delta.logRetentionDuration'='interval 0 seconds', "
            + "'delta.deletedFileRetentionDuration'='interval 0 seconds'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1)", tableName);

          Path logPath = localDeltaLogPath(tableName);
          Path versionZero = logPath.resolve("00000000000000000000.json");
          Path versionOne = logPath.resolve("00000000000000000001.json");
          Assertions.assertThat(versionZero).exists();
          Assertions.assertThat(versionOne).exists();
          try (Stream<Path> files = Files.list(logPath)) {
            for (Path path : files.filter(this::isVersionedLogFile).collect(Collectors.toList())) {
              Files.setLastModifiedTime(path, FileTime.fromMillis(0));
            }
          }

          sql("INSERT INTO %s VALUES (2)", tableName);

          Assertions.assertThat(versionZero).doesNotExist();
          Assertions.assertThat(versionOne).doesNotExist();
          try (Stream<Path> files = Files.list(logPath)) {
            Assertions.assertThat(files.filter(this::isCheckpointFile).count()).isGreaterThan(0);
          }
          check(tableName, List.of(row("1"), row("2")));
        });
  }

  private Map<String, List<Integer>> activeFileRows(String tableName) {
    Map<String, List<Integer>> rowsByFile = new HashMap<>();
    for (Row row : spark().sql("SELECT input_file_name(), id FROM " + tableName).collectAsList()) {
      rowsByFile.computeIfAbsent(row.getString(0), ignored -> new ArrayList<>()).add(row.getInt(1));
    }
    rowsByFile.values().forEach(Collections::sort);
    return rowsByFile;
  }

  private Set<String> vacuumCandidateNames(String tableName) {
    return sql("VACUUM %s RETAIN 0 HOURS DRY RUN", tableName).stream()
        .map(row -> fileName(row.get(0)))
        .collect(Collectors.toSet());
  }

  private List<Integer> allRows(Map<String, List<Integer>> rowsByFile) {
    return rowsByFile.values().stream().flatMap(List::stream).sorted().collect(Collectors.toList());
  }

  private String fileName(String path) {
    return Path.of(URI.create(path).getPath()).getFileName().toString();
  }

  private void assertLatestOperation(String tableName, String expectedOperation) {
    latestOperationMetrics(tableName, expectedOperation);
  }

  private Map<String, String> latestOperationMetrics(String tableName, String expectedOperation) {
    Row history =
        DeltaTable.forName(spark(), tableName)
            .history(1)
            .select("operation", "operationMetrics")
            .head();
    Assertions.assertThat(history.getString(0)).isEqualTo(expectedOperation);
    return history.getJavaMap(1);
  }

  private Path localDeltaLogPath(String tableName) {
    String location =
        sql("DESCRIBE FORMATTED %s", tableName).stream()
            .filter(row -> row.size() >= 2 && "Location".equalsIgnoreCase(row.get(0).trim()))
            .map(row -> row.get(1).trim())
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not retrieve table location"));
    URI locationUri = URI.create(location);
    if ("file".equals(locationUri.getScheme())) {
      return Path.of(locationUri).resolve("_delta_log");
    }
    Assertions.assertThat(locationUri.getHost()).as(location).isEqualTo(FAKE_S3_BUCKET);
    return Path.of(locationUri.getPath()).resolve("_delta_log");
  }

  private boolean isVersionedLogFile(Path path) {
    return path.getFileName().toString().matches("[0-9]{20}\\..*");
  }

  private boolean isCheckpointFile(Path path) {
    return path.getFileName().toString().contains(".checkpoint.");
  }
}
