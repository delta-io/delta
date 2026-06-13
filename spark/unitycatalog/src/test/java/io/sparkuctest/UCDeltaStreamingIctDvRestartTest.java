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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@link io.delta.spark.internal.v2.V2StreamingIctDvRestartTest} - in-commit-timestamps
 * x deletion-vectors x stream-restart triple composition.
 *
 * <p>Most cases in the V2 reference are {@code @Disabled} for the KNOWN-GAP that DSv2 streaming
 * does not honor DV-only deletes mid-stream on restart (V2StreamingIctTest#case9). The same
 * disables carry over here.
 *
 * <p>UC adds two infra constraints:
 *
 * <ul>
 *   <li>{@code IctTestUtils.modifyCommitTimestamp} and {@code setFileMtimeOnly} are local-FS only.
 *       UC managed tables live behind {@code S3CredentialFileSystem}, so synthetic-timestamp tests
 *       cannot run. Tests that depend on planted timestamps drop the {@code IctTestUtils} calls and
 *       rely on the real commit timestamps; semantics are unchanged (we never compare against the
 *       planted value).
 *   <li>OPTIMIZE on MANAGED throws {@code DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION}, so
 *       the OPTIMIZE-between-runs case aborts on MANAGED.
 * </ul>
 */
public class UCDeltaStreamingIctDvRestartTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  private static final String ICT_DV_PROPS =
      "'delta.enableInCommitTimestamps' = 'true', 'delta.enableDeletionVectors' = 'true'";

  /** Append [start, end) as a single coalesced file (so DV-DELETE goes through the DV path). */
  private void appendSingleFile(String tableName, long start, long end) {
    spark()
        .range(start, end)
        .selectExpr("cast(id as int) as id")
        .coalesce(1)
        .write()
        .format("delta")
        .mode("append")
        .saveAsTable(tableName);
  }

  /**
   * Mirror of {@code V2StreamingIctDvRestartTest#testBasic_ict_dv_restart}. Disabled with the same
   * KNOWN-GAP: DSv2 streaming does not honor DV-only deletes mid-stream on restart.
   */
  @TestAllTableTypes
  public void testBasic_ict_dv_restart(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: DSv2 streaming does not yet honor DV-only deletes when a DELETE commit lies "
            + "between the checkpoint offset and the latest version on restart. Mirror of "
            + "V2StreamingIctTest#case9_ictWithDeletionVectors.");
  }

  /** Mirror of {@code V2StreamingIctDvRestartTest#testStartingTimestamp_acrossDvDelete_restart}. */
  @TestAllTableTypes
  public void testStartingTimestamp_acrossDvDelete_restart(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: DSv2 streaming does not honor DV-only deletes mid-stream on restart (see "
            + "case 1 and V2StreamingIctTest#case9).");
  }

  /** Mirror of {@code V2StreamingIctDvRestartTest#testIctMidHistory_dvAfterIctEnable_restart}. */
  @TestAllTableTypes
  public void testIctMidHistory_dvAfterIctEnable_restart(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: composes V2StreamingIctTest#case7 (ICT mid-history) with the DV-only-delete-"
            + "on-restart gap pinned by case9. UC also blocks ALTER on MANAGED.");
  }

  /** Mirror of {@code V2StreamingIctDvRestartTest#testSubsecondIctCommits_restartAcrossDv}. */
  @TestAllTableTypes
  public void testSubsecondIctCommits_restartAcrossDv(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: DSv2 streaming does not honor DV-only deletes mid-stream on restart. UC "
            + "also cannot plant sub-second skew via IctTestUtils on s3:// paths.");
  }

  /**
   * Mirror of {@code V2StreamingIctDvRestartTest#testStartingTimestampExactlyIctOfDvDeleteCommit}.
   * Uses the real commit timestamp from {@code DESCRIBE HISTORY} instead of a planted one because
   * UC managed tables are not on local FS.
   */
  @TestAllTableTypes
  public void testStartingTimestampExactlyIctOfDvDeleteCommit(TableType tableType)
      throws Exception {
    withNewTable(
        "ictdv_start_ts_at_dv",
        "id INT",
        null,
        tableType,
        ICT_DV_PROPS,
        tableName -> {
          appendSingleFile(tableName, 0, 10); // v1
          appendSingleFile(tableName, 10, 20); // v2
          // v3: DV-DELETE on v2's file.
          sql("DELETE FROM %s WHERE id BETWEEN 10 AND 12", tableName);
          String t3 = currentTimestamp(tableName);
          // v4: another append, so we can distinguish "started at v3" (skipped, 0 rows) from
          // "started at v4" (10 rows leak through).
          appendSingleFile(tableName, 20, 30); // v4

          // skipChangeCommits=true so v3 (DV-DELETE) is dropped without IGNORE_CHANGES.
          String queryName =
              "ictdv_start_ts_at_dv_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          StreamingQuery q =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .option("startingTimestamp", t3)
                  .table(tableName)
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          q.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          // At-commit semantics: start lands on v3 itself; v3 is skipped; v4 emits 10 rows.
          assertEquals(
              10,
              rows.size(),
              () ->
                  "startingTimestamp==ICT(v3) + skipChangeCommits should drop v3 and emit v4's "
                      + "10 rows, got: "
                      + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingIctDvRestartTest#testIctEnabledMidStream_then_dv}. */
  @TestAllTableTypes
  public void testIctEnabledMidStream_then_dv(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: DSv2 streaming does not honor DV-only deletes mid-stream on restart (case 1). "
            + "UC also blocks ALTER on MANAGED.");
  }

  /**
   * Mirror of {@code V2StreamingIctDvRestartTest#testIctTable_dvDelete_skipChangeCommits_restart}.
   */
  @TestAllTableTypes
  public void testIctTable_dvDelete_skipChangeCommits_restart(TableType tableType)
      throws Exception {
    withNewTable(
        "ictdv_skip_change_restart",
        "id INT",
        null,
        tableType,
        ICT_DV_PROPS,
        tableName -> {
          appendSingleFile(tableName, 0, 10); // v1

          String queryName =
              "ictdv_skip_change_restart_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          // Run 1: drain v1 with skipChangeCommits=true (still works for an append).
          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          // v2: DV-DELETE.
          sql("DELETE FROM %s WHERE id < 3", tableName);

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          // Run 1 emitted 10 rows; v2 (DV-DELETE) is dropped under skipChangeCommits. Total = 10.
          assertEquals(
              10,
              rows.size(),
              () -> "ICT + DV-DELETE + skipChangeCommits + restart should pin at 10 rows: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingIctDvRestartTest#testIctTable_dvDelete_ignoreChanges_restart}. */
  @TestAllTableTypes
  public void testIctTable_dvDelete_ignoreChanges_restart(TableType tableType) throws Exception {
    withNewTable(
        "ictdv_ignore_changes_restart",
        "id INT",
        null,
        tableType,
        ICT_DV_PROPS,
        tableName -> {
          appendSingleFile(tableName, 0, 10); // v1 (one file)

          String queryName =
              "ictdv_ignore_changes_restart_"
                  + tableType.name().toLowerCase()
                  + "_"
                  + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark().readStream().format("delta").option("ignoreChanges", "true").table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();
          List<Row> initial = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(10, initial.size(), () -> "Run 1 should emit v1 (10 rows): " + initial);

          // v2: DV-DELETE on 3 rows.
          sql("DELETE FROM %s WHERE id < 3", tableName);

          Dataset<Row> df2 =
              spark().readStream().format("delta").option("ignoreChanges", "true").table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          // Run 1 emitted 10. The v2 AddFile-with-DV is admitted as a "change"; DSv2 reads the
          // file ignoring the DV, so the post-restart batch re-emits the full file -> +10.
          // Total = 20. (Per-row identity is the Bug #1 surface; this test pins row count.)
          assertEquals(
              20,
              rows.size(),
              () ->
                  "ICT + DV + ignoreChanges + restart: DSv2 currently re-emits AddFile-with-DV "
                      + "without applying the DV (see Bug #1); expected 20 rows, got "
                      + rows.size()
                      + ": "
                      + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Mirror of {@code V2StreamingIctDvRestartTest#testIctTable_dvDelete_maxFilesPerTrigger_restart}.
   */
  @TestAllTableTypes
  public void testIctTable_dvDelete_maxFilesPerTrigger_restart(TableType tableType) {
    Assumptions.assumeTrue(
        false,
        "KNOWN-GAP: DSv2 streaming does not honor DV-only deletes mid-stream on restart (case 1).");
  }

  /**
   * Mirror of {@code V2StreamingIctDvRestartTest#testIctTable_dvDelete_optimizeBetweenRuns}.
   * Aborted on MANAGED because OPTIMIZE throws {@code
   * DELTA_UNSUPPORTED_CATALOG_MANAGED_TABLE_OPERATION}. EXTERNAL runs the full case.
   */
  @TestAllTableTypes
  public void testIctTable_dvDelete_optimizeBetweenRuns(TableType tableType) throws Exception {
    Assumptions.assumeTrue(
        tableType == TableType.EXTERNAL,
        "UC-INFRA-BLOCKED: OPTIMIZE is blocked on MANAGED via DELTA_UNSUPPORTED_CATALOG_MANAGED_-"
            + "TABLE_OPERATION");
    withNewTable(
        "ictdv_optimize_between",
        "id INT",
        null,
        tableType,
        ICT_DV_PROPS,
        tableName -> {
          appendSingleFile(tableName, 0, 10); // v1

          String queryName =
              "ictdv_optimize_between_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          // v2: DV-DELETE.
          sql("DELETE FROM %s WHERE id < 3", tableName);

          // v3: OPTIMIZE materializes the DV. Raise minFileSize so OPTIMIZE actually rewrites.
          String prevMinFileSize = null;
          try {
            prevMinFileSize = spark().conf().get("spark.databricks.delta.optimize.minFileSize");
          } catch (java.util.NoSuchElementException ignored) {
            // not previously set
          }
          spark()
              .conf()
              .set(
                  "spark.databricks.delta.optimize.minFileSize",
                  Long.toString(1024L * 1024L * 1024L));
          try {
            sql("OPTIMIZE %s", tableName);
          } finally {
            if (prevMinFileSize != null) {
              spark().conf().set("spark.databricks.delta.optimize.minFileSize", prevMinFileSize);
            } else {
              spark().conf().unset("spark.databricks.delta.optimize.minFileSize");
            }
          }

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          // Run 1: 10 rows. v2 dropped under skipChangeCommits. v3 is dataChange=false (OPTIMIZE).
          assertEquals(
              10,
              rows.size(),
              () -> "ICT + DV + OPTIMIZE + restart should pin at 10 rows, got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Mirror of {@code V2StreamingIctDvRestartTest#testRollback_ict_dv_restart}. */
  @TestAllTableTypes
  public void testRollback_ict_dv_restart(TableType tableType) throws Exception {
    withNewTable(
        "ictdv_restore_restart",
        "id INT",
        null,
        tableType,
        ICT_DV_PROPS,
        tableName -> {
          appendSingleFile(tableName, 0, 10); // v1
          long v1Version = currentVersion(tableName);

          String queryName =
              "ictdv_restore_restart_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          String ck = checkpoint();

          Dataset<Row> df1 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q1 =
              df1.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q1.processAllAvailable();
          q1.stop();

          // v2: DV-DELETE.
          sql("DELETE FROM %s WHERE id < 5", tableName);

          // v3: RESTORE to v1 - removes the DV-bearing AddFile, re-adds the DV-free AddFile.
          sql("RESTORE TABLE %s TO VERSION AS OF %d", tableName, v1Version);

          Dataset<Row> df2 =
              spark()
                  .readStream()
                  .format("delta")
                  .option("skipChangeCommits", "true")
                  .table(tableName);
          StreamingQuery q2 =
              df2.writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .option("checkpointLocation", ck)
                  .start();
          q2.processAllAvailable();
          q2.stop();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          // Run 1: 10 rows. v2 (DV-DELETE) and v3 (RESTORE) both dropped under skipChangeCommits.
          assertEquals(
              10,
              rows.size(),
              () ->
                  "ICT + DV-DELETE + RESTORE + skipChangeCommits + restart should pin at 10: "
                      + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }
}
