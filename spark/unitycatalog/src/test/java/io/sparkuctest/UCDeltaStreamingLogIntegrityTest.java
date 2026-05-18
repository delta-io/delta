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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.io.TempDir;

/**
 * UC port of {@code V2StreamingLogIntegrityTest}. Wave-5 task: surface MANAGED bugs alongside the
 * EXTERNAL behaviour the v2 test already exercises.
 *
 * <p>Most cases in the source DSv2 file (truncate commit JSON, half-write {@code _last_checkpoint},
 * fabricate {@code N.compacted.json}, prune commit JSONs) require direct local-FS access to the
 * table's {@code _delta_log} directory. UC MANAGED tables hide their physical location behind UC,
 * and the test process does not hold the cloud credentials needed to mutate those files. We
 * therefore SKIP those cases on the UC side and document them at the bottom of this file.
 *
 * <p>The two cases we do port:
 *
 * <ol>
 *   <li>{@code failOnDataLoss=false} on a clean log - confirm option is accepted and the stream
 *       drains all rows for both EXTERNAL and MANAGED. PR #6395 made DSv2 accept this option;
 *       wave-5 needs MANAGED behaviour coverage.
 *   <li>{@code failOnDataLoss=true} (default) on a clean log - confirm normal completion. Sanity
 *       baseline that the option machinery works on MANAGED.
 * </ol>
 */
public class UCDeltaStreamingLogIntegrityTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Allocate a fresh local checkpoint directory (UC catalog write-side handles cloud creds). */
  private String checkpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /**
   * failOnDataLoss=false on a clean log: option is accepted and the stream drains all rows.
   * Originally a rejection assertion in the v2 source; flipped to a BEHAVIOR assertion per PR #6395
   * which made the option supported on DSv2.
   */
  @TestAllTableTypes
  public void testFailOnDataLossFalseAcceptedAndDrainsAllRows(TableType tableType)
      throws Exception {
    withNewTable(
        "log_integrity_fnl_false",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (0), (1), (2), (3)", tableName);

          String queryName =
              "log_integrity_fnl_false_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          Dataset<Row> input =
              spark()
                  .readStream()
                  .format("delta")
                  .option("failOnDataLoss", "false")
                  .table(tableName);
          StreamingQuery query =
              input
                  .writeStream()
                  .format("memory")
                  .queryName(queryName)
                  .outputMode("append")
                  .trigger(Trigger.AvailableNow())
                  .option("checkpointLocation", checkpoint())
                  .start();
          query.awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(
              4,
              rows.size(),
              "failOnDataLoss=false on clean log should drain all rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /** Sanity baseline: failOnDataLoss=true on a clean log behaves identically (all rows). */
  @TestAllTableTypes
  public void testFailOnDataLossTrueBaselineDrainsAllRows(TableType tableType) throws Exception {
    withNewTable(
        "log_integrity_fnl_true",
        "id INT",
        tableType,
        tableName -> {
          sql("INSERT INTO %s VALUES (10), (20), (30)", tableName);

          String queryName =
              "log_integrity_fnl_true_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          spark()
              .readStream()
              .format("delta")
              .option("failOnDataLoss", "true")
              .table(tableName)
              .writeStream()
              .format("memory")
              .queryName(queryName)
              .outputMode("append")
              .trigger(Trigger.AvailableNow())
              .option("checkpointLocation", checkpoint())
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  /**
   * Restart-from-checkpoint baseline: cross-restart streaming still produces every row exactly once
   * on MANAGED. The v2 test's "log retention prune mid-stream" case relies on direct commit-JSON
   * deletion under {@code _delta_log}, which UC does not expose to the test process, so we cannot
   * reproduce the data-loss leg here. The rest of that flow - drain via noop, add commits, restart
   * from same checkpoint - is exercised on UC by this test.
   */
  @TestAllTableTypes
  public void testRestartFromCheckpointResumesCleanly(TableType tableType) throws Exception {
    withNewTable(
        "log_integrity_restart",
        "id INT",
        tableType,
        tableName -> {
          for (int i = 0; i < 4; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          String ck = checkpoint();
          // First run: drain via noop sink so the offset gets persisted to ck.
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

          // Add new commits that the restart should pick up.
          sql("INSERT INTO %s VALUES (100)", tableName);
          sql("INSERT INTO %s VALUES (200)", tableName);

          // Restart with same checkpoint -> should see 100 and 200, not the original 4.
          String queryName =
              "log_integrity_restart_" + tableType.name().toLowerCase() + "_" + checkpointCount;
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
              .start()
              .awaitTermination();

          List<Row> rows = spark().sql("SELECT * FROM " + queryName).collectAsList();
          assertEquals(2, rows.size(), "restart should see 2 new rows; got: " + rows);
          // Sanity: confirm the rows are 100 and 200 (idempotent restart, no replay).
          assertTrue(
              rows.stream().anyMatch(r -> r.getInt(0) == 100),
              "expected row 100 in restart batch; got: " + rows);
          assertTrue(
              rows.stream().anyMatch(r -> r.getInt(0) == 200),
              "expected row 200 in restart batch; got: " + rows);
          spark().sql("DROP VIEW IF EXISTS " + queryName);
        });
  }

  // Skipped cases (need direct _delta_log mutation; UC MANAGED hides the path and the test
  // process lacks cloud creds): testScenario1_truncatedCommitJson,
  // testScenario3_lastCheckpointHalfWritten,
  // testScenario11_lastCheckpointHalfWritten_dsv1Differential,
  // testScenario12_logCompactionFilePresent, testScenarioLogRetentionPruneMidStream.
  // Documented in testgap/TEST_GAPS_TRACKING.md.
}
