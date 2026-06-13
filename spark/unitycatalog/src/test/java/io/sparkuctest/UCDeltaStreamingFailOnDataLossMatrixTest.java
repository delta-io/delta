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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingFailOnDataLossMatrixTest}.
 *
 * <p>{@code failOnDataLoss=false} crossed with feature-bearing tables on UC EXTERNAL + MANAGED.
 * Pattern: write N commits, force a checkpoint, manually delete a commit JSON to simulate {@code
 * logRetentionDuration} expiry, start a stream with {@code failOnDataLoss=false} and assert the
 * stream proceeds rather than throwing {@code DELTA_VERSION_NOT_FOUND}.
 *
 * <p>KNOWN-GAP for UC MANAGED: log-pruning cases ({@link Files#delete} on commit JSONs) require
 * direct access to the table's {@code _delta_log} directory. UC MANAGED hides the physical location
 * and the test process lacks the cloud credentials to mutate those files. We skip those cases for
 * MANAGED via {@link Assumptions#assumeFalse}; EXTERNAL still exercises them by resolving the
 * table's LOCATION via {@code DESCRIBE EXTENDED} and translating the (fake) {@code s3://} path back
 * to the local filesystem (see {@link S3CredentialFileSystem}).
 */
public class UCDeltaStreamingFailOnDataLossMatrixTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int counter;

  /** Fresh local checkpoint dir per query. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + counter++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /**
   * Returns the table's storage LOCATION (e.g. {@code s3://bucket/path}) from {@code DESCRIBE
   * EXTENDED}.
   */
  private String tableLocation(String tableName) {
    List<List<String>> describe = sql("DESCRIBE EXTENDED %s", tableName);
    return describe.stream()
        .filter(row -> row.size() >= 2 && "Location".equals(row.get(0)))
        .map(row -> row.get(1))
        .findFirst()
        .orElseThrow(() -> new AssertionError("No Location row in DESCRIBE EXTENDED output"));
  }

  /**
   * Returns the local-disk {@code _delta_log} directory for an EXTERNAL UC table. The fake S3
   * filesystem maps {@code s3://bucket/...} to {@code file:///...} (bucket stripped); we
   * reconstruct that local path so we can mutate commit JSONs directly.
   */
  private Path resolveLocalDeltaLogDir(String tableName) {
    String location = tableLocation(tableName);
    String stripped;
    if (location.startsWith("s3://")) {
      stripped = location.replaceFirst("^s3://[^/]+/", "/");
    } else if (location.startsWith("file:")) {
      stripped = location.replaceFirst("^file:", "");
    } else {
      stripped = location;
    }
    return Paths.get(stripped, "_delta_log");
  }

  /**
   * Force a checkpoint at the current snapshot so the table can be read back without the pruned
   * commit JSONs. Uses {@code DeltaLog.forTable(spark, <table-location>).checkpoint()} - the v2
   * source called the path overload directly; for UC we look up the location first.
   */
  @SuppressWarnings("deprecation")
  private void checkpointTable(String tableName) {
    org.apache.spark.sql.delta.DeltaLog.forTable(spark(), tableLocation(tableName)).checkpoint();
  }

  /** Delete commit JSON for {@code version} and its CRC sibling on the local filesystem. */
  private void pruneCommitJson(Path deltaLogDir, long version) throws Exception {
    String name = String.format("%020d.json", version);
    Path json = deltaLogDir.resolve(name);
    Files.delete(json);
    Path crc = deltaLogDir.resolve(String.format("%020d.crc", version));
    if (Files.exists(crc)) {
      Files.delete(crc);
    }
  }

  /** Builds a DSv2 streaming reader with {@code failOnDataLoss=false} starting from version 0. */
  private Dataset<Row> failOnDataLossFalseStream(String tableName) {
    return spark()
        .readStream()
        .format("delta")
        .option("failOnDataLoss", "false")
        .option("startingVersion", "0")
        .table(tableName);
  }

  /** Drain the streaming dataframe via memory sink and return collected rows. */
  private List<Row> drainToMemory(Dataset<Row> df, String queryName) throws Exception {
    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", checkpoint())
            .start();
    try {
      q.awaitTermination();
      return spark().sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      q.stop();
      spark().sql("DROP VIEW IF EXISTS " + queryName);
    }
  }

  /**
   * Skip log-pruning tests on UC MANAGED: the {@code _delta_log} dir is not directly accessible to
   * the test process. EXTERNAL tables expose their LOCATION and the fake S3 filesystem maps to
   * local disk, so we can prune commit JSONs by hand.
   */
  private void skipManagedForLogPruning(TableType tableType) {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED,
        "KNOWN-GAP: log-pruning tests require direct _delta_log access; UC MANAGED hides the"
            + " location.");
  }

  /** Case 1. fnl=false x DV table, prune the commit JSON containing the DV DELETE. */
  @TestAllTableTypes
  public void testFnl_onDvTable_acrossDvDelete(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_dv",
        "id INT",
        null,
        tableType,
        "'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          spark()
              .range(0, 10)
              .selectExpr("cast(id as int) as id")
              .coalesce(1)
              .write()
              .format("delta")
              .mode("append")
              .saveAsTable(tableName);
          sql("DELETE FROM %s WHERE id < 5", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 1L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_dv_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              5, rows.size(), () -> "expected 5 surviving rows after DV delete, got: " + rows);
        });
  }

  /** Case 2. fnl=false x column mapping mode=name. Pruned middle commit; stream skips ahead. */
  @TestAllTableTypes
  public void testFnl_onColMappedNameTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_cm_name",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'name'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);
          sql("INSERT INTO %s VALUES (2, 'b')", tableName);
          sql("INSERT INTO %s VALUES (3, 'c')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_cm_name_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              3,
              rows.size(),
              () -> "expected 3 rows on CM-name table with pruned v2, got: " + rows);
        });
  }

  /** Case 3. fnl=false x column mapping mode=id. */
  @TestAllTableTypes
  public void testFnl_onColMappedIdTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_cm_id",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.columnMapping.mode' = 'id', "
            + "'delta.minReaderVersion' = '2', "
            + "'delta.minWriterVersion' = '5'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);
          sql("INSERT INTO %s VALUES (2, 'b')", tableName);
          sql("INSERT INTO %s VALUES (3, 'c')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_cm_id_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              3, rows.size(), () -> "expected 3 rows on CM-id table with pruned v2, got: " + rows);
        });
  }

  /** Case 4. fnl=false x row-tracking enabled. */
  @TestAllTableTypes
  public void testFnl_onRowTrackedTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_rt",
        "id LONG, name STRING",
        null,
        tableType,
        "'delta.enableRowTracking' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
          sql("INSERT INTO %s VALUES (4, 'd')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 1L);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .option("startingVersion", "0")
                  .table(tableName)
                  .selectExpr("id", "_metadata.row_id AS row_id");
          List<Row> rows =
              drainToMemory(df, "uc_fnl_rt_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              4, rows.size(), () -> "expected 4 rows on RT table with pruned v1, got: " + rows);
          for (Row r : rows) {
            assertFalse(r.isNullAt(1), () -> "row_id must be assigned on RT table, got: " + r);
          }
        });
  }

  /** Case 5. fnl=false x ICT. */
  @TestAllTableTypes
  public void testFnl_onIctTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_ict",
        "id BIGINT",
        null,
        tableType,
        "'delta.enableInCommitTimestamps' = 'true'",
        tableName -> {
          spark().range(0, 5).write().format("delta").mode("append").saveAsTable(tableName);
          spark().range(5, 10).write().format("delta").mode("append").saveAsTable(tableName);
          spark().range(10, 15).write().format("delta").mode("append").saveAsTable(tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_ict_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              15, rows.size(), () -> "expected 15 rows on ICT table with pruned v2, got: " + rows);
        });
  }

  /** Case 6. fnl=false x v2 checkpoint policy. */
  @TestAllTableTypes
  public void testFnl_onV2CheckpointTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_v2cp",
        "id BIGINT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          spark().range(0, 3).write().format("delta").mode("append").saveAsTable(tableName);
          spark().range(3, 6).write().format("delta").mode("append").saveAsTable(tableName);
          spark().range(6, 9).write().format("delta").mode("append").saveAsTable(tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_v2cp_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              9,
              rows.size(),
              () -> "expected 9 rows on v2-checkpoint table with pruned v2, got: " + rows);
        });
  }

  /** Case 7. fnl=false x TIMESTAMP_NTZ. */
  @TestAllTableTypes
  public void testFnl_onTimestampNtzTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_tsntz",
        "id INT, ts TIMESTAMP_NTZ",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, TIMESTAMP_NTZ'2024-01-01 00:00:00')", tableName);
          sql("INSERT INTO %s VALUES (2, TIMESTAMP_NTZ'2024-02-01 00:00:00')", tableName);
          sql("INSERT INTO %s VALUES (3, TIMESTAMP_NTZ'2024-03-01 00:00:00')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_tsntz_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              3,
              rows.size(),
              () -> "expected 3 rows on TIMESTAMP_NTZ table with pruned v2, got: " + rows);
        });
  }

  /** Creates a clustered table directly because the standard helper does not support CLUSTER BY. */
  private void withNewClusteredTable(
      String simpleName,
      String schemaSql,
      String clusterCols,
      TableType tableType,
      TestCode testCode)
      throws Exception {
    if (tableType == TableType.EXTERNAL) {
      withTempDir(
          dir -> {
            org.apache.hadoop.fs.Path tablePath = new org.apache.hadoop.fs.Path(dir, simpleName);
            String name = fullTableName(simpleName);
            sql(
                "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s) LOCATION '%s'",
                name, schemaSql, clusterCols, tablePath.toString());
            try {
              testCode.run(name);
            } finally {
              sql("DROP TABLE IF EXISTS %s", name);
            }
          });
    } else {
      String name = fullTableName(simpleName);
      sql(
          "CREATE TABLE %s (%s) USING DELTA CLUSTER BY (%s)"
              + " TBLPROPERTIES ('delta.feature.catalogManaged'='supported')",
          name, schemaSql, clusterCols);
      try {
        testCode.run(name);
      } finally {
        sql("DROP TABLE IF EXISTS %s", name);
      }
    }
  }

  /** Case 8. fnl=false x CLUSTER BY (Liquid-clustered) table; pruned middle commit. */
  @TestAllTableTypes
  public void testFnl_onClusteredTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewClusteredTable(
        "uc_fnl_clustered",
        "id INT, name STRING",
        "id",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);
          sql("INSERT INTO %s VALUES (2, 'b')", tableName);
          sql("INSERT INTO %s VALUES (3, 'c')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_clustered_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              3,
              rows.size(),
              () -> "expected 3 rows on clustered table with pruned v2, got: " + rows);
        });
  }

  /** Case 9. fnl=false x delta.appendOnly=true; pruned middle commit. */
  @TestAllTableTypes
  public void testFnl_onAppendOnlyTable(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_appendonly",
        "id INT, name STRING",
        null,
        tableType,
        "'delta.appendOnly' = 'true'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a')", tableName);
          sql("INSERT INTO %s VALUES (2, 'b')", tableName);
          sql("INSERT INTO %s VALUES (3, 'c')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_appendonly_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              3,
              rows.size(),
              () -> "expected 3 rows on appendOnly table with pruned v2, got: " + rows);
        });
  }

  /** Case 10. fnl=false x partitioned table. */
  @TestAllTableTypes
  public void testFnl_onPartitionedTable_partLast(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_part",
        "id INT, p STRING",
        "p",
        tableType,
        null,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'x'), (2, 'y')", tableName);
          sql("INSERT INTO %s VALUES (3, 'x'), (4, 'y')", tableName);
          sql("INSERT INTO %s VALUES (5, 'z')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_part_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              5,
              rows.size(),
              () -> "expected 5 rows on partitioned table with pruned v2, got: " + rows);
        });
  }

  /**
   * Case 11. fnl=false x prune a commit containing an ADD COLUMN. Pinned to whichever behavior DSv2
   * produces.
   *
   * <p>KNOWN-GAP for MANAGED additionally: ALTER TABLE ADD COLUMN is blocked by UC on MANAGED
   * tables (catalogManaged kill switch). Skipping for MANAGED here covers both ALTER and log
   * pruning blockers.
   */
  @TestAllTableTypes
  public void testFnl_pruneCommitWithSchemaChange(TableType tableType) throws Exception {
    Assumptions.assumeFalse(
        tableType == TableType.MANAGED,
        "KNOWN-GAP: ALTER TABLE ADD COLUMN blocked on UC MANAGED (catalogManaged kill switch); also"
            + " requires direct _delta_log access for the prune.");
    withNewTable(
        "uc_fnl_schema_change",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1), (2)", tableName);
          sql("ALTER TABLE %s ADD COLUMN val STRING", tableName);
          sql("INSERT INTO %s VALUES (3, 'c')", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          Dataset<Row> df = failOnDataLossFalseStream(tableName);
          try {
            List<Row> rows =
                drainToMemory(
                    df, "uc_fnl_schema_change_" + tableType.name().toLowerCase() + "_" + counter);
            assertEquals(
                3,
                rows.size(),
                () ->
                    "expected 3 rows on schema-change table with pruned v2 (skip-forward), got: "
                        + rows);
            assertEquals(
                2,
                rows.get(0).length(),
                () -> "expected 2-column schema after ADD COLUMN, got: " + rows.get(0));
          } catch (StreamingQueryException ex) {
            Throwable cause = ex.getCause();
            String causeMsg = cause == null ? "" : String.valueOf(cause);
            assertTrue(
                causeMsg.contains("schema") || causeMsg.contains("Schema"),
                () -> "expected schema-related cause if stream errors, got: " + causeMsg);
          }
        });
  }

  /** Case 12. fnl=false x prune v0 AND v1, startingVersion=0. */
  @TestAllTableTypes
  public void testFnl_pruneFirstFewCommits_startingVersion0(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_prune_head",
        "id BIGINT",
        tableType,
        tableName -> {
          for (int i = 0; i < 4; i++) {
            spark().range(i, i + 1).write().format("delta").mode("append").saveAsTable(tableName);
          }

          checkpointTable(tableName);
          Path deltaLogDir = resolveLocalDeltaLogDir(tableName);
          pruneCommitJson(deltaLogDir, /* version= */ 0L);
          pruneCommitJson(deltaLogDir, /* version= */ 1L);

          List<Row> rows =
              drainToMemory(
                  failOnDataLossFalseStream(tableName),
                  "uc_fnl_prune_head_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              4, rows.size(), () -> "expected 4 rows with v0+v1 pruned + fnl=false, got: " + rows);
        });
  }

  /** Case 13. fnl=false combined with ignoreChanges=true on an UPDATE. */
  @TestAllTableTypes
  public void testFnl_combinedWithIgnoreChanges(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_ignore_changes",
        "id INT, val STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
          sql("UPDATE %s SET val = 'b2' WHERE id = 2", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 1L);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .option("ignoreChanges", "true")
                  .option("startingVersion", "0")
                  .table(tableName);
          List<Row> rows =
              drainToMemory(
                  df, "uc_fnl_ignore_changes_" + tableType.name().toLowerCase() + "_" + counter);
          assertThat(rows.size())
              .as("expected at least 2 rows from fnl=false + ignoreChanges, got: " + rows)
              .isGreaterThanOrEqualTo(2);
        });
  }

  /** Case 14. fnl=false combined with skipChangeCommits=true on an UPDATE. */
  @TestAllTableTypes
  public void testFnl_combinedWithSkipChangeCommits(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_skip_change",
        "id INT, val STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
          sql("UPDATE %s SET val = 'b2' WHERE id = 2", tableName);

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 1L);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .option("skipChangeCommits", "true")
                  .option("startingVersion", "0")
                  .table(tableName);
          List<Row> rows =
              drainToMemory(
                  df, "uc_fnl_skip_change_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              2,
              rows.size(),
              () -> "expected 2 rows from fnl=false + skipChangeCommits, got: " + rows);
        });
  }

  /** Case 15. fnl=false combined with maxFilesPerTrigger=1 across the pruned region. */
  @TestAllTableTypes
  public void testFnl_combinedWithMaxFilesPerTrigger(TableType tableType) throws Exception {
    skipManagedForLogPruning(tableType);
    withNewTable(
        "uc_fnl_maxfiles",
        "id BIGINT",
        tableType,
        tableName -> {
          for (int i = 0; i < 5; i++) {
            spark().range(i, i + 1).write().format("delta").mode("append").saveAsTable(tableName);
          }

          checkpointTable(tableName);
          pruneCommitJson(resolveLocalDeltaLogDir(tableName), /* version= */ 2L);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .option("maxFilesPerTrigger", "1")
                  .option("startingVersion", "0")
                  .table(tableName);
          List<Row> rows =
              drainToMemory(
                  df, "uc_fnl_maxfiles_" + tableType.name().toLowerCase() + "_" + counter);
          assertEquals(
              5,
              rows.size(),
              () ->
                  "expected 5 rows from fnl=false + maxFilesPerTrigger=1 across pruned region,"
                      + " got: "
                      + rows);
        });
  }
}
