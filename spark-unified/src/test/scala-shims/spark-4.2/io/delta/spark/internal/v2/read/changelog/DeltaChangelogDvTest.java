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
package io.delta.spark.internal.v2.read.changelog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for Auto-CDF / DSv2 changelog with Deletion Vectors (DVs) that are NOT
 * covered by parameterized DV-on/off variants of
 * {@link DeltaChangelogDirectBatchExecutionTest}. The parameterized tests already cover the
 * basic single-file DELETE and UPDATE; this file holds scenarios that need DV-specific table
 * shape (multi-file, multi-commit).
 *
 * <p>Correctness relies on Catalyst post-processing (Phase 1 carry-over removal via
 * {@code row_commit_version} comparison, Phase 2 net-changes matrix).
 */
public class DeltaChangelogDvTest extends DeltaChangelogTestBase {

  // ===========================================================================================
  // Fixtures
  // ===========================================================================================

  /**
   * Create a DV-enabled, row-tracking-enabled table, then run the body under
   * {@code spark.databricks.delta.v2.enableMode=STRICT} so CHANGES reads route through the V2
   * path.
   */
  private void withDvTable(String suffix, ThrowingConsumer body) throws Exception {
    String tableName = "dsv2_cdc_dv_" + suffix + "_" + System.nanoTime();
    String tablePath = System.getProperty("java.io.tmpdir") + "/" + tableName;
    withTable(
        tablePath,
        () ->
            withTable(
                new String[] {tableName},
                () -> {
                  spark.sql(
                      String.format(
                          "CREATE TABLE %s (id BIGINT, name STRING) USING delta LOCATION '%s' "
                              + "TBLPROPERTIES ('delta.enableDeletionVectors'='true', "
                              + "'delta.enableRowTracking'='true')",
                          tableName, tablePath));
                  withSQLConf(
                      "spark.databricks.delta.v2.enableMode",
                      "STRICT",
                      () -> body.accept(tableName, tablePath));
                }));
  }

  @FunctionalInterface
  private interface ThrowingConsumer {
    void accept(String tableName, String tablePath) throws Exception;
  }

  /** Read changelog as ordered list of (id, name, _change_type, _commit_version) rows. */
  private List<Row> readChanges(String tableName, long startVersion, long endVersion) {
    return spark
        .sql(
            String.format(
                "SELECT id, name, _change_type, _commit_version "
                    + "FROM %s CHANGES FROM VERSION %d TO VERSION %d",
                tableName, startVersion, endVersion))
        .orderBy("_commit_version", "id", "_change_type", "name")
        .collectAsList();
  }

  private static void assertChange(
      Row row, long expectedId, String expectedName, String expectedChangeType,
      long expectedVersion) {
    assertEquals(
        expectedId, ((Number) row.getAs("id")).longValue(), "id mismatch in row: " + row);
    assertEquals(expectedName, row.getAs("name"), "name mismatch in row: " + row);
    assertEquals(
        expectedChangeType, row.getAs("_change_type"), "_change_type mismatch in row: " + row);
    assertEquals(
        expectedVersion,
        ((Number) row.getAs("_commit_version")).longValue(),
        "_commit_version mismatch in row: " + row);
  }

  // ===========================================================================================
  // Test cases
  // ===========================================================================================

  /**
   * Two files in one commit, each with its own DV after the DELETE. Verifies that the per-file
   * DV branch in {@code planInputPartitions} sets the DV constants independently per file.
   */
  @Test
  public void test_mixedDvDelete_perFileBranching() throws Exception {
    withDvTable(
        "mixed_dv_delete",
        (tableName, tablePath) -> {
          spark.sql(String.format("INSERT INTO %s VALUES (1,'a'),(2,'b'),(3,'c')", tableName));
          spark.sql(String.format("INSERT INTO %s VALUES (4,'d'),(5,'e'),(6,'f')", tableName));
          spark.sql(String.format("DELETE FROM %s WHERE id IN (2, 5)", tableName));

          List<Row> rows = readChanges(tableName, 3, 3);
          assertEquals(2, rows.size(), "Expected two deletes (one per file) at v3");
          assertChange(rows.get(0), 2L, "b", "delete", 3L);
          assertChange(rows.get(1), 5L, "e", "delete", 3L);
        });
  }

  /**
   * Multi-version range covering two DV-DELETE commits. Phase 1 partitions on
   * {@code (row_id, version)} so each commit's carry-overs collapse independently.
   */
  @Test
  public void test_multiVersionDvDeletes_perCommitIsolation() throws Exception {
    withDvTable(
        "multi_version_dv",
        (tableName, tablePath) -> {
          spark.sql(
              String.format(
                  "INSERT INTO %s VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e')", tableName));
          spark.sql(String.format("DELETE FROM %s WHERE id = 2", tableName)); // v2
          spark.sql(String.format("DELETE FROM %s WHERE id = 4", tableName)); // v3

          List<Row> rows = readChanges(tableName, 2, 3);
          assertEquals(2, rows.size(), "Expected one delete per commit in v2..v3");
          assertChange(rows.get(0), 2L, "b", "delete", 2L);
          assertChange(rows.get(1), 4L, "d", "delete", 3L);
        });
  }
}
