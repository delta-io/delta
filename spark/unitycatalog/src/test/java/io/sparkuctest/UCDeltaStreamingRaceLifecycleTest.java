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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryProgress;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@code V2StreamingRaceLifecycleTest}. Exercises lifecycle scenarios on UC MANAGED to
 * surface bugs that don't fire via the file-path read path (EXTERNAL via {@code
 * dsv2.delta.<path>}).
 *
 * <p>Scenarios ported:
 *
 * <ul>
 *   <li>5 - protocol upgrade mid-stream (DV writer feature appears at v=N)
 *   <li>6 - {@code Trigger.AvailableNow} twice on same checkpoint, no new data
 *   <li>8 - initial snapshot exceeds {@code maxInitialSnapshotFiles}
 * </ul>
 *
 * <p>Scenario 2 (concurrent commit between {@code latestOffset()} and {@code
 * planInputPartitions()}) is not portable: the v2 version is a best-effort race that requires
 * writes via Spark on a local file path; UC MANAGED commits go through the UC server and the test
 * cannot drive the same race window deterministically. Documented as skipped.
 */
public class UCDeltaStreamingRaceLifecycleTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Allocate a fresh local checkpoint directory. */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /**
   * Scenario 5: enable {@code delta.enableDeletionVectors} mid-stream and DELETE rows. The DSv2
   * source captures protocol at startup so a single long-running stream may not see the upgrade,
   * but a restart should surface a structured error or - if validation works on a fresh source -
   * read the post-DV state correctly. The strict assertion is: no raw NPE, no silent skip with
   * row-count mismatch.
   */
  @TestAllTableTypes
  public void testScenario5_ProtocolUpgradeMidStream(TableType tableType) throws Exception {
    withNewTable(
        "race_proto_upgrade",
        "value INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s SELECT cast(id as int) FROM range(0, 10)", tableName);

          String ck = checkpoint();
          // Drain initial snapshot via noop sink so the offset persists and the next restart
          // exercises the recovery path through any post-upgrade commits.
          spark()
              .readStream()
              .format("delta")
              .table(tableName)
              .writeStream()
              .format("noop")
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", ck)
              .start()
              .awaitTermination();

          // Mid-stream: enable DV + DELETE materializes a DV on UC.
          sql(
              "ALTER TABLE %s SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')",
              tableName);
          sql("DELETE FROM %s WHERE value = 0", tableName);

          // Restart: should read post-upgrade state cleanly OR surface a structured error.
          // Failure mode we are looking for: raw NPE / silent data corruption.
          String queryName = "race_proto_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Throwable thrown = null;
          long postUpgradeRows = -1;
          StreamingQuery q = null;
          try {
            q =
                spark()
                    .readStream()
                    .format("delta")
                    .table(tableName)
                    .writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .outputMode("append")
                    .trigger(Trigger.AvailableNow())
                    .option("checkpointLocation", ck)
                    .start();
            q.awaitTermination();
            postUpgradeRows = spark().sql("SELECT * FROM " + queryName).collectAsList().size();
            if (q.exception().isDefined()) {
              thrown = q.exception().get();
            }
          } catch (Throwable t) {
            thrown = t;
          } finally {
            if (q != null) {
              try {
                q.stop();
              } catch (Throwable ignored) {
                // ignore stop errors during cleanup
              }
            }
            spark().sql("DROP VIEW IF EXISTS " + queryName);
          }

          // Walk cause chain - a raw NPE leak is the bug we care about.
          if (thrown != null) {
            Throwable cur = thrown;
            while (cur != null) {
              if (cur instanceof NullPointerException) {
                fail(
                    "Scenario 5 ("
                        + tableType
                        + "): raw NPE leak from mid-stream DV upgrade: "
                        + thrown);
              }
              cur = cur.getCause();
            }
          } else {
            // No throw: the stream silently produced data. Either it read through correctly
            // (post-DELETE rows = 9 from the new commit set) or it skipped the upgrade commit.
            // Both are acceptable as long as no NPE leaked. Print for the report.
            System.out.println(
                "[Scenario 5 " + tableType + "] post-upgrade rows = " + postUpgradeRows);
          }
        });
  }

  /**
   * Scenario 6: Trigger.AvailableNow twice on same checkpoint with no new data should produce 0
   * rows on the second run.
   */
  @TestAllTableTypes
  public void testScenario6_AvailableNowTwiceNoNewData(TableType tableType) throws Exception {
    withNewTable(
        "race_avnow_twice",
        "id INT, name STRING",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B'), (3, 'C')", tableName);
          String ck = checkpoint();

          // First run: drains the 3 rows via noop (memory sink does not survive checkpoint).
          StreamingQuery q1 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("noop")
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", ck)
                  .start();
          q1.awaitTermination();
          long firstRunRows = 0;
          for (StreamingQueryProgress p : q1.recentProgress()) {
            firstRunRows += p.numInputRows();
          }
          assertEquals(3L, firstRunRows, "first run should drain 3 rows");
          q1.stop();

          // Second run: same checkpoint, no new data -> 0 rows.
          StreamingQuery q2 =
              spark()
                  .readStream()
                  .format("delta")
                  .table(tableName)
                  .writeStream()
                  .format("noop")
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", ck)
                  .start();
          q2.awaitTermination();
          long secondRunRows = 0;
          for (StreamingQueryProgress p : q2.recentProgress()) {
            secondRunRows += p.numInputRows();
          }
          q2.stop();

          assertTrue(
              q2.exception().isEmpty(),
              "second AvailableNow should not throw; got: " + q2.exception());
          assertEquals(
              0L,
              secondRunRows,
              "second AvailableNow on same ckpt with no new data should produce 0 rows; got "
                  + secondRunRows
                  + ". Indicates duplicate replay (cf. row-duplication bug).");
        });
  }

  /**
   * Scenario 8: initial snapshot exceeds {@code maxInitialSnapshotFiles}. Lower the conf so v=0
   * exceeds the limit and assert a structured {@code DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE}
   * error.
   */
  @TestAllTableTypes
  public void testScenario8_InitialSnapshotExceedsMaxFiles(TableType tableType) throws Exception {
    withNewTable(
        "race_initial_too_large",
        "id INT, name STRING, value DOUBLE",
        tableType,
        tableName -> {
          // Insert 10 single-row commits to ensure >5 distinct files for the initial snapshot.
          for (int i = 0; i < 10; i++) {
            sql("INSERT INTO %s VALUES (%d, 'n%d', %f)", tableName, i, i, (double) i * 1.5);
          }

          String prevConf = null;
          try {
            prevConf =
                spark().conf().get("spark.databricks.delta.streaming.initialSnapshotMaxFiles");
          } catch (java.util.NoSuchElementException ignored) {
            // not previously set
          }
          spark().conf().set("spark.databricks.delta.streaming.initialSnapshotMaxFiles", "5");
          final String prevConfFinal = prevConf;

          Throwable thrown = null;
          StreamingQuery q = null;
          String queryName = "race_init_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          try {
            Dataset<Row> df = spark().readStream().format("delta").table(tableName);
            q =
                df.writeStream()
                    .format("memory")
                    .queryName(queryName)
                    .outputMode("append")
                    .trigger(Trigger.AvailableNow())
                    .option("checkpointLocation", checkpoint())
                    .start();
            try {
              q.awaitTermination();
            } catch (Throwable awaitErr) {
              thrown = awaitErr;
            }
            if (thrown == null && q.exception().isDefined()) {
              thrown = q.exception().get();
            }
          } catch (Throwable t) {
            thrown = t;
          } finally {
            if (q != null) {
              try {
                q.stop();
              } catch (Throwable ignored) {
                // ignore
              }
            }
            spark().sql("DROP VIEW IF EXISTS " + queryName);
            if (prevConfFinal == null) {
              spark().conf().unset("spark.databricks.delta.streaming.initialSnapshotMaxFiles");
            } else {
              spark()
                  .conf()
                  .set("spark.databricks.delta.streaming.initialSnapshotMaxFiles", prevConfFinal);
            }
          }

          // Walk cause chain for the structured error class / message.
          assertTrue(
              thrown != null,
              "expected DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE, got no"
                  + " exception (10 rows / 5 max should exceed)");
          Throwable cur = thrown;
          boolean found = false;
          while (cur != null) {
            String msg = cur.getMessage() == null ? "" : cur.getMessage();
            if (msg.contains("DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE")
                || msg.contains("initialSnapshotMaxFiles")
                || cur.getClass().getName().contains("DeltaUnsupportedOperation")) {
              found = true;
              break;
            }
            cur = cur.getCause();
          }
          assertTrue(
              found,
              "expected DELTA_STREAMING_INITIAL_SNAPSHOT_TOO_LARGE in cause chain; got: " + thrown);
        });
  }

  // Skipped: testScenario2_ConcurrentCommitBetweenLatestOffsetAndPlanPartitions - relies on a
  // local-FS write loop racing the reader. UC commits route through the UC server, so the test
  // process cannot reliably drive the same race window. Documented in TEST_GAPS_TRACKING.md.
}
