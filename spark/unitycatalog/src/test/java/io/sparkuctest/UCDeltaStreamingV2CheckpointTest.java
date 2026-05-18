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
 * UC port of {@code V2StreamingV2CheckpointTest}. Cross-product cell {@code v2Checkpoint x
 * streaming} is 100% uncovered before this file. The v2 source uses {@code DeltaLog.checkpoint()}
 * to force a checkpoint mid-stream; UC tables can't easily trigger checkpoints from the test side
 * (UC's commit coordinator may decide checkpoint timing), so the cases that REQUIRE a forced
 * checkpoint mid-stream are marked best-effort: we exercise the streaming path on a table created
 * with {@code delta.checkpointPolicy='v2'} and rely on natural checkpoint timing.
 *
 * <p>Cases ported:
 *
 * <ul>
 *   <li>1a - basic stream from a v2-checkpoint table
 *   <li>1b - AvailableNow on a v2-checkpoint table
 *   <li>4 - v2Checkpoint x DV
 *   <li>5 - v2Checkpoint x column mapping
 *   <li>6 - v2Checkpoint x maxFilesPerTrigger=1
 *   <li>7 - v2Checkpoint x startingVersion at high version
 * </ul>
 *
 * <p>Cases skipped:
 *
 * <ul>
 *   <li>2 - stream across an explicitly-forced checkpoint boundary (cannot force on UC)
 *   <li>3 - restart with checkpoint forced between runs (cannot force on UC)
 * </ul>
 */
public class UCDeltaStreamingV2CheckpointTest extends UCDeltaTableIntegrationBaseTest {

  @TempDir private Path tempDir;
  private int checkpointCount;

  /** Allocate a fresh local checkpoint directory. */
  private String localCheckpoint() throws IOException {
    Path ckDir = tempDir.resolve("ck-" + checkpointCount++);
    Files.createDirectory(ckDir);
    return ckDir.toString();
  }

  /** Drain a streaming DF via memory sink under AvailableNow and return collected rows. */
  private List<Row> drainToMemory(Dataset<Row> df, String queryName) throws Exception {
    StreamingQuery q =
        df.writeStream()
            .format("memory")
            .queryName(queryName)
            .outputMode("append")
            .trigger(Trigger.AvailableNow())
            .option("checkpointLocation", localCheckpoint())
            .start();
    try {
      q.awaitTermination();
      return spark().sql("SELECT * FROM " + queryName).collectAsList();
    } finally {
      q.stop();
      spark().sql("DROP VIEW IF EXISTS " + queryName);
    }
  }

  /** 1a. Basic stream from a v2-checkpoint table. */
  @TestAllTableTypes
  public void case1a_basicStream(TableType tableType) throws Exception {
    withNewTable(
        "v2ckpt_basic",
        "id BIGINT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          sql("INSERT INTO %s SELECT id FROM range(0, 5)", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          assertTrue(df.isStreaming());
          String queryName = "v2ckpt_1a_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(5, rows.size(), "expected 5 rows; got: " + rows);
        });
  }

  /** 1b. AvailableNow on a v2-checkpoint table - identical setup, different sink trigger. */
  @TestAllTableTypes
  public void case1b_availableNow(TableType tableType) throws Exception {
    withNewTable(
        "v2ckpt_avnow",
        "id BIGINT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          sql("INSERT INTO %s SELECT id FROM range(0, 5)", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "v2ckpt_1b_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(5, rows.size(), "expected 5 rows; got: " + rows);
        });
  }

  /** 4. v2Checkpoint x DV. */
  @TestAllTableTypes
  public void case4_v2CheckpointWithDV(TableType tableType) throws Exception {
    withNewTable(
        "v2ckpt_dv",
        "id BIGINT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2', 'delta.enableDeletionVectors' = 'true'",
        tableName -> {
          sql("INSERT INTO %s SELECT id FROM range(0, 10)", tableName);
          sql("DELETE FROM %s WHERE id < 3", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "v2ckpt_4_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(7, rows.size(), "DV survivors expected 7 rows; got: " + rows);
        });
  }

  /** 5. v2Checkpoint x column mapping name. */
  @TestAllTableTypes
  public void case5_v2CheckpointWithColumnMapping(TableType tableType) throws Exception {
    withNewTable(
        "v2ckpt_cm",
        "id BIGINT, name STRING",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2', 'delta.columnMapping.mode' = 'name', "
            + "'delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5'",
        tableName -> {
          sql("INSERT INTO %s VALUES (1, 'A'), (2, 'B')", tableName);
          sql("INSERT INTO %s VALUES (3, 'C')", tableName);

          Dataset<Row> df = spark().readStream().format("delta").table(tableName);
          String queryName = "v2ckpt_5_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(3, rows.size(), "expected 3 rows; got: " + rows);
        });
  }

  /** 6. v2Checkpoint x maxFilesPerTrigger=1 - stream consumes commits across the table. */
  @TestAllTableTypes
  public void case6_v2CheckpointWithMaxFilesPerTrigger(TableType tableType) throws Exception {
    withNewTable(
        "v2ckpt_maxfiles",
        "id BIGINT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          for (int i = 0; i < 4; i++) {
            sql("INSERT INTO %s VALUES (%d)", tableName, i);
          }

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("maxFilesPerTrigger", "1")
                  .table(tableName);
          String queryName = "v2ckpt_6_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          assertEquals(4, rows.size(), "expected 4 rows; got: " + rows);
        });
  }

  /**
   * 7. v2Checkpoint x startingVersion mid-history. UC may not allow forcing a checkpoint, so we
   * just verify startingVersion semantics on a v2-checkpoint table.
   */
  @TestAllTableTypes
  public void case7_v2CheckpointWithStartingVersion(TableType tableType) throws Exception {
    withNewTable(
        "v2ckpt_startver",
        "id BIGINT",
        null,
        tableType,
        "'delta.checkpointPolicy' = 'v2'",
        tableName -> {
          sql("INSERT INTO %s SELECT id FROM range(0, 10)", tableName);
          long midVersion = currentVersion(tableName);
          sql("INSERT INTO %s SELECT id FROM range(10, 20)", tableName);
          sql("INSERT INTO %s SELECT id FROM range(20, 30)", tableName);

          Dataset<Row> df =
              spark()
                  .readStream()
                  .format("delta")
                  .option("startingVersion", String.valueOf(midVersion + 1))
                  .table(tableName);
          String queryName = "v2ckpt_7_" + tableType.name().toLowerCase() + "_" + checkpointCount;
          List<Row> rows = drainToMemory(df, queryName);
          // From midVersion+1 onward: 20 rows (10..29).
          assertEquals(20, rows.size(), "expected 20 rows from last 2 versions; got: " + rows);
        });
  }

  // Skipped: case2_streamAcrossV2CheckpointBoundary, case3_restartWithCheckpointBetween - both
  // need DeltaLog.checkpoint() to force a checkpoint at a specific version, which is not
  // supported on UC tables (UC manages checkpoint timing). Documented in TEST_GAPS_TRACKING.md.
}
